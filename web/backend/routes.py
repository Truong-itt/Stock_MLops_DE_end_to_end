import logging
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from pydantic import BaseModel, Field
from cachetools import TTLCache
import httpx
import yfinance as yf
from database import db
from clickhouse_db import ch_db
from symbol_registry import SymbolRegistry

try:
    from confluent_kafka.admin import AdminClient, NewPartitions
except ImportError:
    AdminClient = None
    NewPartitions = None

try:
    from cassandra.util import Date as CassDate
except ImportError:
    CassDate = None

logger = logging.getLogger("backend.routes")
registry = SymbolRegistry()
symbol_validation_cache = TTLCache(maxsize=512, ttl=900)
translation_cache = TTLCache(maxsize=256, ttl=3600)
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
VALID_QUOTE_TYPES = {"EQUITY", "ETF", "MUTUALFUND", "INDEX"}
MYMEMORY_TRANSLATE_URL = "https://api.mymemory.translated.net/get"
WHALE_ML_URL = os.getenv("WHALE_ML_URL", "http://whale-ml-service:8090").rstrip("/")
WHALE_ML_TIMEOUT = float(os.getenv("WHALE_ML_TIMEOUT_SEC", "6.0"))


def _serialise(obj):
    """Recursively convert ScyllaDB types to JSON-safe types."""
    if isinstance(obj, dict):
        return {k: _serialise(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialise(v) for v in obj]
    if CassDate and isinstance(obj, CassDate):
        try:
            return obj.date().isoformat()
        except Exception:
            return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, set):
        return [_serialise(v) for v in obj]
    return obj


def ok(data):
    return {"status": "ok", "data": _serialise(data)}


router = APIRouter(prefix="/api")


class AddSymbolRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    market: str = Field(..., min_length=2, max_length=10)


class TranslateRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=20000)
    from_lang: str = Field(default="en", min_length=2, max_length=8)
    to_lang: str = Field(default="vi", min_length=2, max_length=8)


def _kafka_admin():
    if AdminClient is None:
        raise RuntimeError("Kafka admin client is unavailable")
    return AdminClient({"bootstrap.servers": "kafka-1:29092,kafka-2:29092,kafka-3:29092"})


def _ensure_topic_partitions(topic: str, desired_count: int) -> int:
    admin = _kafka_admin()
    md = admin.list_topics(topic=topic, timeout=10)
    topic_meta = md.topics.get(topic)
    if topic_meta is None or getattr(topic_meta, "error", None):
        raise RuntimeError(f"Topic '{topic}' not found")

    current = len(topic_meta.partitions)
    if desired_count <= current:
        return current

    futures = admin.create_partitions([NewPartitions(topic, desired_count)])
    futures[topic].result(20)
    logger.info("Expanded topic %s partitions: %d -> %d", topic, current, desired_count)
    return desired_count


def _symbol_registry_payload():
    data = registry.snapshot()
    markets = {}
    for market, meta in data["markets"].items():
        symbols = list(meta.get("symbols", []))
        markets[market] = {
            "label": meta.get("label"),
            "topic": meta.get("topic"),
            "symbols": symbols,
            "count": len(symbols),
        }
    return {
        "updated_at": data.get("updated_at"),
        "markets": markets,
        "total_symbols": sum(item["count"] for item in markets.values()),
    }


def _translation_chunks(text: str, max_len: int = 450):
    text = str(text or "").strip()
    if not text:
        return []

    chunks = []
    remaining = text
    while remaining:
        if len(remaining) <= max_len:
            chunks.append(remaining)
            break
        break_point = remaining.rfind(". ", 0, max_len)
        if break_point < 100:
            break_point = remaining.rfind(" ", 0, max_len)
        if break_point < 100:
            break_point = max_len
        chunks.append(remaining[: break_point + 1].strip())
        remaining = remaining[break_point + 1 :].strip()
    return [chunk for chunk in chunks if chunk]


async def _translate_via_mymemory(text: str, from_lang: str, to_lang: str) -> dict:
    text = str(text or "").strip()
    from_lang = str(from_lang or "").strip().lower()
    to_lang = str(to_lang or "").strip().lower()

    if not text or from_lang == to_lang:
        return {"translated_text": text, "source": "noop", "cached": False}

    cache_key = f"{from_lang}|{to_lang}|{text}"
    cached = translation_cache.get(cache_key)
    if cached is not None:
        return {"translated_text": cached, "source": "mymemory", "cached": True}

    translated_chunks = []
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for chunk in _translation_chunks(text):
            try:
                response = await client.get(
                    MYMEMORY_TRANSLATE_URL,
                    params={"q": chunk, "langpair": f"{from_lang}|{to_lang}"},
                )
                response.raise_for_status()
                payload = response.json()
                translated = payload.get("responseData", {}).get("translatedText")
                if payload.get("responseStatus") == 200 and translated:
                    translated_chunks.append(translated)
                else:
                    translated_chunks.append(chunk)
            except Exception as exc:
                logger.warning("translation chunk failed (%s->%s): %s", from_lang, to_lang, exc)
                translated_chunks.append(chunk)

    translated_text = " ".join(translated_chunks).strip()
    translation_cache[cache_key] = translated_text
    return {"translated_text": translated_text, "source": "mymemory", "cached": False}


async def _validate_symbol_exists(symbol: str, market: str) -> dict:
    sym = str(symbol).strip().upper()
    cache_key = f"{market}:{sym}"
    cached = symbol_validation_cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            response = await client.get(YAHOO_QUOTE_URL, params={"symbols": sym})
            response.raise_for_status()
            payload = response.json()
        results = payload.get("quoteResponse", {}).get("result", []) or []
        quote = None
        for item in results:
            item_symbol = str(item.get("symbol") or "").strip().upper()
            if item_symbol == sym:
                quote = item
                break
        if quote is None and results:
            quote = results[0]

        if quote:
            quote_type = str(quote.get("quoteType") or "").strip().upper()
            if quote_type and quote_type not in VALID_QUOTE_TYPES:
                raise HTTPException(
                    status_code=400,
                    detail=f"Ma '{sym}' khong phai ma co phieu/chi so hop le ({quote_type})",
                )

            has_market_data = any(
                quote.get(field) is not None
                for field in (
                    "regularMarketPrice",
                    "regularMarketPreviousClose",
                    "regularMarketDayHigh",
                    "regularMarketDayLow",
                    "regularMarketVolume",
                )
            )
            if has_market_data:
                validation = {
                    "symbol": sym,
                    "market": market,
                    "name": quote.get("shortName") or quote.get("longName") or sym,
                    "exchange": quote.get("fullExchangeName") or quote.get("exchange") or "Unknown",
                    "quote_type": quote_type or None,
                    "currency": quote.get("currency"),
                }
                symbol_validation_cache[cache_key] = validation
                return validation
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("primary symbol validation failed for %s: %s", sym, exc)

    try:
        ticker = yf.Ticker(sym)
        history = ticker.history(period="5d", interval="1d", auto_adjust=False)
        if history.empty:
            raise HTTPException(
                status_code=400,
                detail=f"Ma '{sym}' khong ton tai hoac khong co du lieu giao dich tren thi truong",
            )

        info = {}
        try:
            info = ticker.fast_info or {}
        except Exception:
            info = {}

        validation = {
            "symbol": sym,
            "market": market,
            "name": sym,
            "exchange": str(info.get("exchange") or info.get("quote_type") or "Yahoo Finance"),
            "quote_type": "EQUITY",
            "currency": info.get("currency"),
        }
        symbol_validation_cache[cache_key] = validation
        return validation
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("fallback symbol validation failed for %s: %s", sym, exc)
        raise HTTPException(
            status_code=503,
            detail="Khong kiem tra duoc ma luc nay, vui long thu lai sau",
        ) from exc


def _build_daily_map(daily_rows):
    """Build lookup: symbol → latest daily_summary row (most recent trade_date)."""
    daily_map = {}
    for r in daily_rows:
        sym = r["symbol"]
        if sym not in daily_map:
            daily_map[sym] = r
        else:
            existing_date = daily_map[sym].get("trade_date")
            new_date = r.get("trade_date")
            if new_date and existing_date and str(new_date) > str(existing_date):
                daily_map[sym] = r
    return daily_map


def _build_latest_map(latest_rows):
    """Build lookup: symbol → latest intraday row."""
    latest_map = {}
    for row in latest_rows:
        sym = row.get("symbol")
        if sym:
            latest_map[sym] = row
    return latest_map


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _symbol_market_sets():
    snapshot = registry.snapshot()
    markets = snapshot.get("markets", {})
    return {
        "vn": set(markets.get("vn", {}).get("symbols", []) or []),
        "world": set(markets.get("world", {}).get("symbols", []) or []),
    }


def _sql_quote(value: str) -> str:
    return "'" + str(value or "").replace("\\", "\\\\").replace("'", "''") + "'"


def _load_changepoint_latest_rows():
    try:
        if not ch_db.is_connected():
            ch_db.connect()
        rows = ch_db.query("SELECT * FROM v_changepoint_latest")
        if rows:
            return rows
    except Exception as exc:
        logger.warning("ClickHouse changepoint latest query failed, fallback Scylla: %s", exc)
    return list(db.execute("SELECT * FROM stock_changepoint_latest"))


def _load_changepoint_latest_row(symbol: str):
    sym = symbol.upper()
    try:
        if not ch_db.is_connected():
            ch_db.connect()
        rows = ch_db.query(
            f"SELECT * FROM v_changepoint_latest WHERE symbol = {_sql_quote(sym)} LIMIT 1"
        )
        if rows:
            return rows[0]
    except Exception as exc:
        logger.warning("ClickHouse changepoint latest symbol query failed, fallback Scylla: %s", exc)
    rows = list(
        db.execute(
            "SELECT * FROM stock_changepoint_latest WHERE symbol = %s",
            [sym],
        )
    )
    return rows[0] if rows else None


def _build_abnormal_alerts(latest_rows, daily_rows, cp_rows, limit: int = 16):
    """
    Derive suspicious/abnormal symbols from the BOCPD module.

    We keep the BOCPD outputs as the primary signal and only enrich them with
    price/volume context from market tables so the web UI can explain *why* a
    symbol is being surfaced as a potential whale / abnormal move.
    """
    daily_map = _build_daily_map(daily_rows)
    latest_map = _build_latest_map(latest_rows)
    market_sets = _symbol_market_sets()
    tracked_symbols = market_sets["vn"] | market_sets["world"]

    alerts = []
    for row in cp_rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        if tracked_symbols and symbol not in tracked_symbols:
            continue

        latest = latest_map.get(symbol, {})
        daily = daily_map.get(symbol, {})

        cp_prob = _safe_float(row.get("cp_prob"))
        whale_score = _safe_float(row.get("whale_score"))
        innovation_zscore = _safe_float(row.get("innovation_zscore"))
        expected_run_length = _safe_float(row.get("expected_run_length"))
        map_run_length = _safe_float(row.get("map_run_length"))
        predictive_volatility = _safe_float(row.get("predictive_volatility"))
        return_value = _safe_float(row.get("return_value"))
        hazard = _safe_float(row.get("hazard"))
        evidence = _safe_float(row.get("evidence"))
        regime_label = str(row.get("regime_label") or "stable").strip().lower()

        price = latest.get("price")
        if price is None:
            price = daily.get("close")
        change = latest.get("change")
        if change is None:
            change = daily.get("change")
        pct = latest.get("change_percent")
        if pct is None:
            pct = daily.get("change_percent")
        volume = latest.get("day_volume")
        if not volume:
            volume = daily.get("volume")

        pct = _safe_float(pct)
        change = _safe_float(change)
        price = _safe_float(price, default=None)
        volume = _safe_int(volume)

        market = "other"
        if symbol in market_sets["vn"]:
            market = "vn"
        elif symbol in market_sets["world"]:
            market = "world"

        freshness_score = 1.0 - min(max(expected_run_length, 0.0) / 90.0, 1.0)
        innovation_score = min(max(innovation_zscore, 0.0), 4.0) / 4.0
        movement_score = min(abs(pct) / 7.0, 1.0)
        volatility_score = min(max(predictive_volatility, 0.0) / 0.03, 1.0)

        suspicion_score = min(
            1.0,
            (whale_score * 0.42)
            + (cp_prob * 0.28)
            + (innovation_score * 0.12)
            + (freshness_score * 0.10)
            + (movement_score * 0.05)
            + (volatility_score * 0.03),
        )
        if regime_label == "whale-watch":
            suspicion_score = min(1.0, suspicion_score + 0.08)

        is_abnormal = (
            regime_label == "whale-watch"
            or whale_score >= 0.14
            or (cp_prob >= 0.18 and innovation_zscore >= 2.0)
            or suspicion_score >= 0.26
        )
        if not is_abnormal:
            continue

        if pct >= 1.5:
            bias = "pump-watch"
            bias_label = "Nghi van pump"
        elif pct <= -1.5:
            bias = "dump-watch"
            bias_label = "Nghi van dump"
        else:
            bias = "volatile"
            bias_label = "Bien dong bat thuong"

        reason_tags = []
        if regime_label == "whale-watch":
            reason_tags.append("Whale-watch")
        if whale_score >= 0.12:
            reason_tags.append(f"Whale {whale_score * 100:.1f}%")
        if cp_prob >= 0.18:
            reason_tags.append(f"CP {cp_prob * 100:.1f}%")
        if innovation_zscore >= 2.0:
            reason_tags.append(f"z {innovation_zscore:.1f}")
        if abs(pct) >= 2.0:
            reason_tags.append(f"{pct:+.2f}%")
        if freshness_score >= 0.70:
            reason_tags.append("Reset moi")
        if predictive_volatility >= 0.015:
            reason_tags.append(f"Vol {predictive_volatility * 100:.2f}%")

        description = " | ".join(reason_tags[:5]) if reason_tags else "BOCPD phat hien thay doi che do giao dich"

        alerts.append(
            {
                "symbol": symbol,
                "market": market,
                "price": price,
                "change": change,
                "pct": pct,
                "volume": volume,
                "cp_prob": round(cp_prob, 6),
                "whale_score": round(whale_score, 6),
                "innovation_zscore": round(innovation_zscore, 4),
                "expected_run_length": round(expected_run_length, 4),
                "map_run_length": round(map_run_length, 4),
                "predictive_volatility": round(predictive_volatility, 8),
                "return_value": round(return_value, 8),
                "hazard": round(hazard, 8),
                "evidence": round(evidence, 8),
                "regime_label": regime_label,
                "suspicion_score": round(suspicion_score, 6),
                "bias": bias,
                "bias_label": bias_label,
                "reason_tags": reason_tags[:5],
                "reason_text": description,
                "event_time": row.get("event_time") or latest.get("timestamp") or daily.get("trade_date"),
            }
        )

    alerts.sort(
        key=lambda item: (
            item.get("suspicion_score") or 0,
            item.get("whale_score") or 0,
            item.get("cp_prob") or 0,
            abs(item.get("pct") or 0),
        ),
        reverse=True,
    )

    summary = {
        "total_alerts": len(alerts),
        "whale_watch_count": sum(1 for item in alerts if item.get("regime_label") == "whale-watch"),
        "pump_watch_count": sum(1 for item in alerts if item.get("bias") == "pump-watch"),
        "dump_watch_count": sum(1 for item in alerts if item.get("bias") == "dump-watch"),
    }
    return alerts[:limit], summary


async def _attach_ml_forecast(alerts):
    """
    Enrich BOCPD abnormal alerts with ML forecast:
    - direction up/down
    - probability up/down
    - expected number of sessions in that direction
    """
    if not alerts:
        return alerts, {
            "enabled": True,
            "requested": 0,
            "predicted": 0,
            "up_forecast_count": 0,
            "down_forecast_count": 0,
        }

    request_events = []
    base_row_map = {}
    for index, row in enumerate(alerts):
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        raw_event_time = row.get("event_time")
        if isinstance(raw_event_time, datetime):
            event_time_value = raw_event_time.isoformat()
        elif isinstance(raw_event_time, date):
            event_time_value = raw_event_time.isoformat()
        elif raw_event_time is None:
            event_time_value = None
        else:
            event_time_value = str(raw_event_time)
        event_key = f"{symbol}|{row.get('event_time') or ''}|{index}"
        request_events.append(
            {
                "event_key": event_key,
                "symbol": symbol,
                "event_time": event_time_value,
                "cp_prob": row.get("cp_prob"),
                "whale_score": row.get("whale_score"),
                "innovation_zscore": row.get("innovation_zscore"),
                "expected_run_length": row.get("expected_run_length"),
                "map_run_length": row.get("map_run_length"),
                "predictive_volatility": row.get("predictive_volatility"),
                "return_value": row.get("return_value"),
                "hazard": row.get("hazard"),
                "evidence": row.get("evidence"),
                "price": row.get("price"),
            }
        )
        base_row_map[event_key] = row

    if not request_events:
        return alerts, {
            "enabled": True,
            "requested": len(alerts),
            "predicted": 0,
            "up_forecast_count": 0,
            "down_forecast_count": 0,
        }

    try:
        async with httpx.AsyncClient(timeout=WHALE_ML_TIMEOUT, follow_redirects=True) as client:
            response = await client.post(
                f"{WHALE_ML_URL}/predict-batch",
                json={"events": request_events},
            )
            response.raise_for_status()
            payload = response.json()

        if payload.get("status") != "ok":
            raise RuntimeError(payload.get("detail") or "Whale ML service returned non-ok status")

        data = payload.get("data", {}) or {}
        predictions = data.get("predictions", []) or []
        prediction_map = {str(p.get("event_key") or ""): p for p in predictions}

        enriched = []
        up_count = 0
        down_count = 0
        for event in request_events:
            key = event["event_key"]
            base = base_row_map.get(key)
            if base is None:
                continue
            row = dict(base)
            pred = prediction_map.get(key)
            if pred:
                row["ml_direction"] = pred.get("direction")
                row["ml_prob_up"] = _safe_float(pred.get("prob_up"))
                row["ml_prob_down"] = _safe_float(pred.get("prob_down"))
                row["ml_expected_sessions"] = round(_safe_float(pred.get("expected_sessions"), 1.0), 4)
                row["ml_confidence"] = _safe_float(pred.get("confidence"))
                row["ml_text"] = pred.get("text")
                if row["ml_direction"] == "up":
                    up_count += 1
                elif row["ml_direction"] == "down":
                    down_count += 1
            enriched.append(row)

        model_info = data.get("model", {}) or {}
        summary = {
            "enabled": True,
            "requested": len(request_events),
            "predicted": len(prediction_map),
            "up_forecast_count": up_count,
            "down_forecast_count": down_count,
            "model_version": model_info.get("version"),
            "trained_at": model_info.get("trained_at"),
        }
        return enriched, summary
    except Exception as exc:
        logger.warning("ML forecast enrichment skipped: %s", exc)
        return alerts, {
            "enabled": True,
            "requested": len(request_events),
            "predicted": 0,
            "up_forecast_count": 0,
            "down_forecast_count": 0,
            "error": "ml_service_unavailable",
        }


@router.get("/system/symbols")
async def get_system_symbols():
    """Danh sách mã cấu hình cho producer/UI."""
    try:
        return ok(_symbol_registry_payload())
    except Exception as e:
        logger.error(f"get_system_symbols: {e}")
        raise HTTPException(500, detail=str(e))


@router.post("/system/symbols")
async def add_system_symbol(payload: AddSymbolRequest):
    """Thêm mã mới vào registry và nới Kafka partitions nếu cần."""
    try:
        validation = await _validate_symbol_exists(payload.symbol, payload.market)
        saved, created, partition = registry.add_symbol(payload.market, payload.symbol)
        topic = saved["markets"][payload.market]["topic"]
        desired_partitions = len(saved["markets"][payload.market]["symbols"])
        actual_partitions = _ensure_topic_partitions(topic, desired_partitions)
        response = _symbol_registry_payload()
        response["added"] = created
        response["symbol"] = payload.symbol.upper()
        response["market"] = payload.market
        response["topic"] = topic
        response["partition"] = partition
        response["topic_partitions"] = actual_partitions
        response["validation"] = validation
        return ok(response)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"add_system_symbol: {e}")
        raise HTTPException(500, detail=str(e))


@router.post("/translate")
async def translate_text(payload: TranslateRequest):
    """Proxy article translation through backend to avoid browser-side CORS/rate-limit issues."""
    try:
        result = await _translate_via_mymemory(payload.text, payload.from_lang, payload.to_lang)
        result["from_lang"] = payload.from_lang
        result["to_lang"] = payload.to_lang
        return ok(result)
    except Exception as e:
        logger.error(f"translate_text: {e}")
        raise HTTPException(500, detail="Khong dich duoc bai viet luc nay")


# ────────────────────────────── Symbols ──────────────────────────────
@router.get("/symbols")
async def get_all_symbols():
    """Lấy danh sách tất cả mã cổ phiếu (gộp từ daily_summary + news)."""
    try:
        syms = set()
        for row in db.execute("SELECT DISTINCT symbol FROM stock_daily_summary"):
            syms.add(row["symbol"])
        for row in db.execute("SELECT DISTINCT stock_code FROM stock_news"):
            syms.add(row["stock_code"])
        for row in db.execute("SELECT symbol FROM stock_latest_prices"):
            syms.add(row["symbol"])
        return ok(sorted(syms))
    except Exception as e:
        logger.error(f"get_all_symbols: {e}")
        raise HTTPException(500, detail=str(e))


# ────────────────────────────── Latest Prices ────────────────────────
@router.get("/stocks/latest")
async def get_latest_prices():
    """Lấy giá mới nhất: merge real-time + daily fallback."""
    try:
        latest = list(db.execute(
            "SELECT symbol, price, change, change_percent, day_volume, "
            "exchange, last_size, market_hours, quote_type, timestamp "
            "FROM stock_latest_prices"
        ))
        daily = list(db.execute(
            "SELECT symbol, trade_date, open, high, low, close, volume, "
            "change, change_percent, vwap, exchange "
            "FROM stock_daily_summary"
        ))
        registry_symbols = set(registry.get_all_symbols())
        if registry_symbols:
            latest = [row for row in latest if row.get("symbol") in registry_symbols]
            daily = [row for row in daily if row.get("symbol") in registry_symbols]

        if not latest:
            return ok(daily)

        # Merge: latest wins, daily fills OHLCV gaps
        daily_map = _build_daily_map(daily)
        merged = []
        seen = set()
        for r in latest:
            sym = r["symbol"]
            seen.add(sym)
            if r.get("price") is not None:
                d = daily_map.get(sym)
                if d:
                    r["open"] = d.get("open")
                    r["high"] = d.get("high")
                    r["low"] = d.get("low")
                    r["vwap"] = d.get("vwap")
                    if not r.get("day_volume"):
                        r["day_volume"] = d.get("volume")
                    r["trade_date"] = d.get("trade_date")
                merged.append(r)
            elif sym in daily_map:
                merged.append(daily_map[sym])
        for sym, r in daily_map.items():
            if sym not in seen:
                merged.append(r)

        # Keep the UI aligned with the configured registry even when a symbol
        # has just been added and no latest/daily row has arrived yet.
        for sym in registry.get_all_symbols():
            if sym in seen or sym in daily_map:
                continue
            merged.append({
                "symbol": sym,
                "price": None,
                "change": 0,
                "change_percent": 0,
                "open": None,
                "high": None,
                "low": None,
                "day_volume": None,
                "vwap": None,
                "exchange": registry.get_market_for_symbol(sym) or "",
                "timestamp": None,
                "market_hours": None,
                "quote_type": None,
                "is_placeholder": True,
            })

        return ok(merged)
    except Exception as e:
        logger.error(f"get_latest_prices: {e}")
        raise HTTPException(500, detail=str(e))


@router.get("/stocks/latest/{symbol}")
async def get_latest_price(symbol: str):
    try:
        rows = list(db.execute(
            "SELECT * FROM stock_latest_prices WHERE symbol = %s", [symbol.upper()]
        ))
        if rows:
            return ok(rows[0])

        rows = list(db.execute(
            "SELECT * FROM stock_daily_summary WHERE symbol = %s LIMIT 1", [symbol.upper()]
        ))
        if rows:
            return ok(rows[0])
        raise HTTPException(404, detail="Symbol not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_latest_price({symbol}): {e}")
        raise HTTPException(500, detail=str(e))


# ────────────────────────────── Tick History ─────────────────────────
@router.get("/stocks/ticks/{symbol}")
async def get_stock_ticks(
    symbol: str,
    limit: int = Query(default=500, le=5000),
):
    try:
        rows = list(db.execute(
            "SELECT * FROM stock_prices WHERE symbol = %s LIMIT %s",
            [symbol.upper(), limit],
        ))
        return ok(rows)
    except Exception as e:
        logger.error(f"get_stock_ticks({symbol}): {e}")
        raise HTTPException(500, detail=str(e))


# ────────────────────────────── Matched Orders (khớp lệnh) ──────────
@router.get("/stocks/matched-orders/{symbol}")
async def get_matched_orders(
    symbol: str,
    limit: int = Query(default=50, le=200),
):
    """Lấy lệnh khớp gần nhất từ stock_prices (tick-level)."""
    try:
        sym = symbol.upper()
        rows = list(db.execute(
            "SELECT timestamp, price, last_size, change, change_percent "
            "FROM stock_prices WHERE symbol = %s LIMIT %s",
            [sym, limit],
        ))
        # Total count (approximate)
        count_rows = list(db.execute(
            "SELECT COUNT(*) as cnt FROM stock_prices WHERE symbol = %s",
            [sym],
        ))
        total = count_rows[0]["cnt"] if count_rows else len(rows)
        return {"status": "ok", "data": _serialise(rows), "total_count": total}
    except Exception as e:
        logger.error(f"get_matched_orders({symbol}): {e}")
        raise HTTPException(500, detail=str(e))


def _find_ohlcv(symbol: str, interval: str, bucket_date: str = None):
    """
    Query OHLCV cho interval intraday (1m, 5m, 1h, 3h, 6h).
    Tự tìm ngày gần nhất nếu không chỉ định bucket_date.
    """
    sym = symbol.upper()
    if bucket_date:
        return list(db.execute(
            "SELECT * FROM stock_prices_agg WHERE symbol=%s AND bucket_date=%s AND interval=%s",
            [sym, bucket_date, interval],
        ))
    # Thử today → lùi tối đa 5 ngày (bỏ qua weekend/holiday)
    from datetime import timedelta
    for offset in range(6):
        d = date.today() - timedelta(days=offset)
        rows = list(db.execute(
            "SELECT * FROM stock_prices_agg WHERE symbol=%s AND bucket_date=%s AND interval=%s",
            [sym, str(d), interval],
        ))
        if rows:
            return rows
    return []


def _find_daily_ohlcv(symbol: str, days: int):
    """
    Query OHLCV daily cho interval dài hạn (15d, 1mo, 6mo, 1y).
    Trả về N ngày gần nhất từ stock_daily_summary.
    """
    sym = symbol.upper()
    rows = list(db.execute(
        "SELECT * FROM stock_daily_summary WHERE symbol = %s LIMIT %s",
        [sym, days],
    ))
    # Chuyển format: trade_date → bucket/ts để frontend hiển thị
    result = []
    for r in rows:
        result.append({
            "symbol": r["symbol"],
            "ts": r["trade_date"],
            "bucket": r["trade_date"],
            "open": r["open"],
            "high": r["high"],
            "low": r["low"],
            "close": r["close"],
            "volume": r["volume"],
            "vwap": r.get("vwap"),
            "change_percent": r.get("change_percent"),
        })
    return result


def _sort_ohlcv_rows(rows):
    def _key(item):
        value = item.get("ts") or item.get("bucket") or item.get("trade_date")
        return str(value or "")
    return sorted(rows, key=_key)


def _resolve_ohlcv(symbol: str, interval: str, bucket_date: Optional[str] = None):
    requested = interval
    intraday_candidates = {
        "1m": ["1m", "5m", "1h", "3h", "6h", "15d"],
        "5m": ["5m", "1m", "1h", "3h", "6h", "15d"],
        "1h": ["1h", "3h", "6h", "5m", "1m", "15d"],
        "3h": ["3h", "1h", "6h", "5m", "1m", "15d"],
        "6h": ["6h", "3h", "1h", "5m", "1m", "15d"],
    }
    daily_candidates = {
        "15d": ["15d", "1mo", "6mo", "1y"],
        "1mo": ["1mo", "15d", "6mo", "1y"],
        "6mo": ["6mo", "1mo", "1y", "15d"],
        "1y": ["1y", "6mo", "1mo", "15d"],
    }
    daily_lengths = {
        "15d": 15,
        "1mo": 30,
        "6mo": 180,
        "1y": 365,
    }

    if requested in daily_candidates:
        candidates = daily_candidates[requested]
    else:
        candidates = intraday_candidates.get(requested, [requested, "1h", "15d"])

    for candidate in candidates:
        if candidate in daily_lengths:
            rows = _find_daily_ohlcv(symbol, daily_lengths[candidate])
        else:
            rows = _find_ohlcv(symbol, candidate, bucket_date)
        rows = _sort_ohlcv_rows(rows)
        if rows:
            return rows, {
                "requested_interval": requested,
                "resolved_interval": candidate,
                "fallback_used": candidate != requested,
                "points": len(rows),
            }

    return [], {
        "requested_interval": requested,
        "resolved_interval": requested,
        "fallback_used": False,
        "points": 0,
    }


# ────────────────────────────── OHLCV Aggregated ─────────────────────
@router.get("/stocks/ohlcv/{symbol}")
async def get_ohlcv(
    symbol: str,
    interval: str = Query(default="1m"),
    bucket_date: Optional[str] = Query(default=None),
):
    try:
        rows, meta = _resolve_ohlcv(symbol, interval, bucket_date)
        return {"status": "ok", "data": _serialise(rows), "meta": _serialise(meta)}
    except Exception as e:
        logger.error(f"get_ohlcv({symbol}, {interval}): {e}")
        raise HTTPException(500, detail=str(e))


# ────────────────────────────── Daily Summary ────────────────────────
@router.get("/stocks/daily")
async def get_all_daily_summary():
    """Trả về toàn bộ daily summary (tất cả symbols)."""
    try:
        rows = list(db.execute("SELECT * FROM stock_daily_summary"))
        return ok(rows)
    except Exception as e:
        logger.error(f"get_all_daily_summary: {e}")
        raise HTTPException(500, detail=str(e))


@router.get("/stocks/daily/{symbol}")
async def get_daily_summary(
    symbol: str,
    limit: int = Query(default=30, le=365),
):
    try:
        rows = list(db.execute(
            "SELECT * FROM stock_daily_summary WHERE symbol = %s LIMIT %s",
            [symbol.upper(), limit],
        ))
        return ok(rows)
    except Exception as e:
        logger.error(f"get_daily_summary({symbol}): {e}")
        raise HTTPException(500, detail=str(e))


# ────────────────────────────── Changepoint / BOCPD ─────────────────
def _load_changepoint_history(symbol: str, days: int = 5, limit: int = 120):
    sym = symbol.upper()
    try:
        if not ch_db.is_connected():
            ch_db.connect()
        safe_days = max(1, int(days))
        safe_limit = max(1, int(limit))
        rows = ch_db.query(
            f"""
            SELECT *
            FROM stock_changepoint_events
            WHERE symbol = {_sql_quote(sym)}
              AND event_time >= now() - INTERVAL {safe_days} DAY
            ORDER BY event_time DESC
            LIMIT {safe_limit}
            """
        )
        rows.reverse()
        if rows:
            return rows
    except Exception as exc:
        logger.warning("ClickHouse changepoint history query failed, fallback Scylla: %s", exc)

    rows = []
    for offset in range(days):
        d = date.today() - timedelta(days=offset)
        part = list(db.execute(
            """
            SELECT *
            FROM stock_changepoint_history
            WHERE symbol = %s AND bucket_date = %s
            LIMIT %s
            """,
            [sym, str(d), limit],
        ))
        rows.extend(part)
        if len(rows) >= limit:
            break
    rows.sort(key=lambda item: item.get("event_time") or datetime.min)
    return rows[-limit:]


@router.get("/changepoint/latest")
async def get_all_changepoint_latest():
    """Trả về trạng thái changepoint mới nhất của toàn bộ mã."""
    try:
        rows = _load_changepoint_latest_rows()
        rows.sort(key=lambda item: item.get("whale_score") or 0, reverse=True)
        return ok(rows)
    except Exception as e:
        logger.error(f"get_all_changepoint_latest: {e}")
        raise HTTPException(500, detail=str(e))


@router.get("/changepoint/abnormal")
async def get_abnormal_changepoint_alerts(limit: int = Query(default=16, ge=1, le=50)):
    """Danh sách mã nghi vấn bất thường / whale-watch lấy từ module BOCPD."""
    try:
        latest = list(
            db.execute(
                "SELECT symbol, price, change, change_percent, day_volume, "
                "exchange, last_size, market_hours, timestamp "
                "FROM stock_latest_prices"
            )
        )
        daily = list(
            db.execute(
                "SELECT symbol, trade_date, open, high, low, close, volume, "
                "change, change_percent, vwap, exchange "
                "FROM stock_daily_summary"
            )
        )
        cp_rows = _load_changepoint_latest_rows()
        alerts, summary = _build_abnormal_alerts(latest, daily, cp_rows, limit=limit)
        alerts, ml_summary = await _attach_ml_forecast(alerts)
        summary["ml_forecast"] = ml_summary
        return ok({"summary": summary, "alerts": alerts})
    except Exception as e:
        logger.error(f"get_abnormal_changepoint_alerts: {e}")
        raise HTTPException(500, detail=str(e))


@router.get("/changepoint/{symbol}")
async def get_changepoint_latest(symbol: str):
    """Trả về trạng thái changepoint mới nhất của một mã."""
    try:
        row = _load_changepoint_latest_row(symbol)
        if not row:
            raise HTTPException(404, detail="Changepoint signal not found for symbol")
        return ok(row)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_changepoint_latest({symbol}): {e}")
        raise HTTPException(500, detail=str(e))


@router.get("/changepoint/{symbol}/history")
async def get_changepoint_history(
    symbol: str,
    limit: int = Query(default=120, le=500),
    days: int = Query(default=5, le=14),
):
    """Trả về lịch sử BOCPD để web vẽ biểu đồ r_t theo thời gian."""
    try:
        return ok(_load_changepoint_history(symbol, days=days, limit=limit))
    except Exception as e:
        logger.error(f"get_changepoint_history({symbol}): {e}")
        raise HTTPException(500, detail=str(e))


# ────────────────────────────── News ─────────────────────────────────
@router.get("/news")
async def get_all_news(limit: int = Query(default=50, le=200)):
    """Trả về tin tức mới nhất từ tất cả mã."""
    try:
        codes = [r["stock_code"] for r in db.execute("SELECT DISTINCT stock_code FROM stock_news")]
        all_news = []
        for code in codes:
            rows = list(db.execute(
                "SELECT * FROM stock_news WHERE stock_code = %s LIMIT %s",
                [code, 5],
            ))
            all_news.extend(rows)
        all_news.sort(key=lambda x: x.get("date") or "", reverse=True)
        return ok(all_news[:limit])
    except Exception as e:
        logger.error(f"get_all_news: {e}")
        raise HTTPException(500, detail=str(e))


@router.get("/news/search")
async def search_news(
    q: str = Query(default=""),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    stock_code: Optional[str] = Query(default=None),
    limit: int = Query(default=50, le=200),
):
    """Search news with optional filters: keyword, date range, stock_code."""
    try:
        codes = [stock_code.upper()] if stock_code else [
            r["stock_code"] for r in db.execute("SELECT DISTINCT stock_code FROM stock_news")
        ]
        all_news = []
        per_code_limit = 20 if not stock_code else limit
        for code in codes:
            rows = list(db.execute(
                "SELECT * FROM stock_news WHERE stock_code = %s LIMIT %s",
                [code, per_code_limit],
            ))
            all_news.extend(rows)

        # Apply filters
        if q:
            ql = q.lower()
            all_news = [n for n in all_news if ql in (n.get("title") or "").lower() or ql in (n.get("content") or "").lower()]

        # Mặc định: 7 ngày gần nhất nếu không truyền date range
        if not date_from and not date_to:
            df = datetime.utcnow() - timedelta(days=7)
            all_news = [n for n in all_news if n.get("date") and n["date"] >= df]

        if date_from:
            try:
                df = datetime.fromisoformat(date_from)
                all_news = [n for n in all_news if n.get("date") and n["date"] >= df]
            except Exception:
                pass

        if date_to:
            try:
                dt = datetime.fromisoformat(date_to)
                all_news = [n for n in all_news if n.get("date") and n["date"] <= dt]
            except Exception:
                pass

        all_news.sort(key=lambda x: x.get("date") or "", reverse=True)
        return ok(all_news[:limit])
    except Exception as e:
        logger.error(f"search_news: {e}")
        raise HTTPException(500, detail=str(e))


@router.get("/news/{stock_code}")
async def get_stock_news(
    stock_code: str,
    limit: int = Query(default=20, le=100),
    days: int = Query(default=7, le=365),
):
    try:
        from datetime import datetime, timedelta
        cutoff = datetime.utcnow() - timedelta(days=days)
        # ScyllaDB: lọc theo stock_code, sau đó filter theo date trong Python
        all_rows = list(db.execute(
            "SELECT * FROM stock_news WHERE stock_code = %s",
            [stock_code.upper()],
        ))
        # Filter các tin trong khoảng thời gian mong muốn
        filtered = [r for r in all_rows if r.get("date") and r["date"] >= cutoff]
        # Sort theo date mới nhất trước, lấy tối đa limit
        filtered.sort(key=lambda x: x.get("date") or datetime.min, reverse=True)
        return ok(filtered[:limit])
    except Exception as e:
        logger.error(f"get_stock_news({stock_code}): {e}")
        raise HTTPException(500, detail=str(e))


# ────────────────────────────── Market Overview ─────────────────────
@router.get("/market/overview")
async def get_market_overview():
    """Dữ liệu tổng quan thị trường: breadth chart + top stocks."""
    try:
        latest = list(db.execute(
            "SELECT symbol, price, change, change_percent, day_volume, "
            "exchange, last_size, market_hours, timestamp "
            "FROM stock_latest_prices"
        ))
        daily = list(db.execute(
            "SELECT symbol, trade_date, open, high, low, close, volume, "
            "change, change_percent, vwap, exchange "
            "FROM stock_daily_summary"
        ))
        cp_rows = _load_changepoint_latest_rows()

        # Merge latest + daily
        daily_map = _build_daily_map(daily)
        merged = []
        seen = set()
        for r in latest:
            sym = r["symbol"]
            seen.add(sym)
            pct = r.get("change_percent") or 0
            price = r.get("price")
            vol = r.get("day_volume") or 0
            d = daily_map.get(sym)
            if price is None and d:
                price = d.get("close")
                pct = d.get("change_percent") or 0
                vol = d.get("volume") or 0
            elif d and not vol:
                vol = d.get("volume") or 0
            merged.append({
                "symbol": sym,
                "price": price,
                "pct": float(pct) if pct else 0,
                "change": float(r.get("change") or 0),
                "volume": int(vol) if vol else 0,
                "exchange": r.get("exchange") or "",
            })
        for sym, d in daily_map.items():
            if sym not in seen:
                merged.append({
                    "symbol": sym,
                    "price": d.get("close"),
                    "pct": float(d.get("change_percent") or 0),
                    "change": float(d.get("change") or 0),
                    "volume": int(d.get("volume") or 0),
                    "exchange": d.get("exchange") or "",
                })

        # Breadth distribution
        bucket_labels = ["< -7%", "-7~-5%", "-5~-3%", "-3~-1%", "-1~0%",
                         "0%", "0~1%", "1~3%", "3~5%", "5~7%", "> 7%"]
        bucket_vals = [0] * 11
        advancers = 0
        decliners = 0
        for s in merged:
            p = s["pct"]
            if p < -7:   bucket_vals[0] += 1
            elif p < -5: bucket_vals[1] += 1
            elif p < -3: bucket_vals[2] += 1
            elif p < -1: bucket_vals[3] += 1
            elif p < 0:  bucket_vals[4] += 1
            elif p == 0: bucket_vals[5] += 1
            elif p < 1:  bucket_vals[6] += 1
            elif p < 3:  bucket_vals[7] += 1
            elif p < 5:  bucket_vals[8] += 1
            elif p < 7:  bucket_vals[9] += 1
            else:        bucket_vals[10] += 1
            if p > 0:
                advancers += 1
            elif p < 0:
                decliners += 1

        alerts, alert_summary = _build_abnormal_alerts(latest, daily, cp_rows, limit=50)
        alerts, ml_summary = await _attach_ml_forecast(alerts)
        alert_summary["ml_forecast"] = ml_summary

        return ok({
            "breadth": {
                "labels": bucket_labels,
                "values": bucket_vals,
                "total": len(merged),
                "advancers": advancers,
                "decliners": decliners,
            },
            "stocks": _serialise(merged),
            "alerts": _serialise(alerts),
            "alert_summary": _serialise(alert_summary),
        })
    except Exception as e:
        logger.error(f"get_market_overview: {e}")
        raise HTTPException(500, detail=str(e))


# ────────────────────────────── Dashboard Stats ──────────────────────
@router.get("/dashboard/stats")
async def get_dashboard_stats():
    """Thống kê tổng quan cho dashboard."""
    try:
        daily = list(db.execute("SELECT * FROM stock_daily_summary"))
        latest = list(db.execute("SELECT * FROM stock_latest_prices"))
        news_codes = list(db.execute("SELECT DISTINCT stock_code FROM stock_news"))

        # Dùng latest nếu có, fallback daily
        stocks = latest if latest else daily

        up = sum(1 for s in stocks if (s.get("change_percent") or 0) > 0)
        down = sum(1 for s in stocks if (s.get("change_percent") or 0) < 0)
        flat = len(stocks) - up - down
        total_volume = sum(s.get("day_volume") or s.get("volume") or 0 for s in stocks)

        return ok({
            "total_symbols": len(stocks),
            "up": up,
            "down": down,
            "flat": flat,
            "total_volume": total_volume,
            "news_stocks": len(news_codes),
            "has_realtime": len(latest) > 0,
        })
    except Exception as e:
        logger.error(f"get_dashboard_stats: {e}")
        raise HTTPException(500, detail=str(e))

# ────────────────────────────── Sectors ─────────────────────────────
SECTOR_MAP = {
    # Vietnam
    "VCB": "Financial Services", "BID": "Financial Services", "FPT": "Technology",
    "HPG": "Basic Materials", "CTG": "Financial Services", "VHM": "Real Estate",
    "TCB": "Financial Services", "VPB": "Financial Services", "VNM": "Consumer Defensive",
    "MBB": "Financial Services", "GAS": "Energy", "ACB": "Financial Services",
    "MSN": "Consumer Defensive", "GVR": "Basic Materials", "LPB": "Financial Services",
    "SSB": "Financial Services", "STB": "Financial Services", "VIB": "Financial Services",
    "MWG": "Consumer Cyclical", "HDB": "Financial Services",
    "PLX": "Energy", "POW": "Utilities", "SAB": "Consumer Defensive",
    "BCM": "Industrials", "PDR": "Real Estate", "KDH": "Real Estate",
    "NVL": "Real Estate", "DGC": "Basic Materials", "SHB": "Financial Services",
    "EIB": "Financial Services",
    # International
    "AAPL": "Technology", "MSFT": "Technology", "NVDA": "Technology", "AMZN": "Consumer Cyclical",
    "GOOGL": "Communication Services", "META": "Communication Services",
    "TSLA": "Consumer Cyclical", "BRK-B": "Financial Services", "LLY": "Healthcare",
    "AVGO": "Technology", "JPM": "Financial Services", "V": "Financial Services",
    "UNH": "Healthcare", "WMT": "Consumer Defensive", "MA": "Financial Services",
    "XOM": "Energy", "JNJ": "Healthcare", "PG": "Consumer Defensive",
    "HD": "Consumer Cyclical", "COST": "Consumer Defensive",
    "NFLX": "Communication Services", "AMD": "Technology", "INTC": "Technology",
    "DIS": "Communication Services", "PYPL": "Financial Services", "BA": "Industrials",
    "CRM": "Technology", "ORCL": "Technology", "CSCO": "Technology", "ABT": "Healthcare",
}


@router.get("/sectors")
async def get_sectors():
    """Trả về danh sách sectors và mapping symbol → sector."""
    sectors = sorted(set(SECTOR_MAP.values()))
    return ok({"sectors": sectors, "mapping": SECTOR_MAP})


@router.get("/sectors/{sector}")
async def get_stocks_by_sector(sector: str):
    """Trả về danh sách symbols thuộc một sector."""
    symbols = [sym for sym, sec in SECTOR_MAP.items() if sec.lower() == sector.lower()]
    if not symbols:
        raise HTTPException(404, detail=f"Sector '{sector}' not found")
    return ok({"sector": sector, "symbols": symbols})


# ────────────────────────────── Sentiment Overview ──────────────────
@router.get("/sentiment/overview")
async def get_sentiment_overview():
    """Phân bố sentiment theo mã: positive / negative / neutral counts + per-stock details."""
    try:
        codes = [r["stock_code"] for r in db.execute("SELECT DISTINCT stock_code FROM stock_news")]
        per_stock = []
        total_pos = 0
        total_neg = 0
        total_neu = 0
        for code in codes:
            rows = list(db.execute(
                "SELECT sentiment_score FROM stock_news WHERE stock_code = %s LIMIT 50",
                [code],
            ))
            pos = sum(1 for r in rows if (r.get("sentiment_score") or 0) > 0)
            neg = sum(1 for r in rows if (r.get("sentiment_score") or 0) < 0)
            neu = len(rows) - pos - neg
            avg_score = sum(r.get("sentiment_score") or 0 for r in rows) / len(rows) if rows else 0
            total_pos += pos
            total_neg += neg
            total_neu += neu
            market = registry.get_market_for_symbol(code) or "world"
            per_stock.append({
                "symbol": code,
                "positive": pos,
                "negative": neg,
                "neutral": neu,
                "total": len(rows),
                "avg_score": round(avg_score, 4),
                "market": market,
            })
        per_stock.sort(key=lambda x: x["avg_score"], reverse=True)
        return ok({
            "summary": {"positive": total_pos, "negative": total_neg, "neutral": total_neu, "total": total_pos + total_neg + total_neu},
            "stocks": per_stock,
        })
    except Exception as e:
        logger.error(f"get_sentiment_overview: {e}")
        raise HTTPException(500, detail=str(e))


# ────────────────────────────── Health ───────────────────────────────
@router.get("/health")
async def health_check():
    try:
        db.execute("SELECT now() FROM system.local")
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": "disconnected", "detail": str(e)}


# ────────────────────────────── Sentiment Reprocess ──────────────────
@router.post("/sentiment/reprocess")
async def reprocess_sentiment(
    stock_code: Optional[str] = Query(default=None, description="Stock code to reprocess (optional, all if not provided)"),
    limit: int = Query(default=1000, le=10000, description="Max number of articles to reprocess"),
):
    """
    Trigger re-processing sentiment for existing news articles.
    Publishes articles to Kafka topic 'news-sentiment' for FinBERT analysis.
    """
    import json
    try:
        from confluent_kafka import Producer
    except ImportError:
        raise HTTPException(500, detail="Kafka not available")
    
    try:
        # Create Kafka producer
        producer = Producer({
            "bootstrap.servers": "kafka-1:29092,kafka-2:29092,kafka-3:29092",
            "acks": "all",
        })
        
        # Query news from database
        if stock_code:
            rows = list(db.execute(
                "SELECT * FROM stock_news WHERE stock_code = %s",
                [stock_code.upper()]
            ))
        else:
            # Get all distinct stock codes first
            codes = list(db.execute("SELECT DISTINCT stock_code FROM stock_news"))
            rows = []
            for code_row in codes[:50]:  # Limit to 50 symbols
                code = code_row.get("stock_code")
                if code:
                    code_rows = list(db.execute(
                        "SELECT * FROM stock_news WHERE stock_code = %s",
                        [code]
                    ))
                    rows.extend(code_rows)
                if len(rows) >= limit:
                    break
        
        rows = rows[:limit]
        published = 0
        
        for row in rows:
            try:
                # Prepare message
                data = {
                    "stock_code": row.get("stock_code"),
                    "article_id": row.get("article_id"),
                    "title": row.get("title", ""),
                    "content": row.get("content", ""),
                    "link": row.get("link", ""),
                    "date": row.get("date").isoformat() if row.get("date") else None,
                }
                
                producer.produce(
                    "news-sentiment",
                    key=data["article_id"].encode("utf-8") if data["article_id"] else None,
                    value=json.dumps(data).encode("utf-8"),
                )
                published += 1
                
                if published % 100 == 0:
                    producer.flush()
            except Exception as e:
                logger.warning(f"Failed to publish article: {e}")
        
        producer.flush()
        
        return ok({
            "message": f"Published {published} articles to Kafka for reprocessing",
            "total_found": len(rows),
            "published": published,
            "stock_code": stock_code or "all",
        })
    except Exception as e:
        logger.error(f"reprocess_sentiment error: {e}")
        raise HTTPException(500, detail=str(e))


@router.get("/sentiment/status")
async def get_sentiment_status():
    """Get statistics about sentiment analysis status."""
    try:
        # Count articles with/without FinBERT sentiment
        all_codes = list(db.execute("SELECT DISTINCT stock_code FROM stock_news"))
        
        total = 0
        with_finbert = 0
        without_finbert = 0
        
        for code_row in all_codes[:50]:
            code = code_row.get("stock_code")
            if code:
                rows = list(db.execute(
                    "SELECT sentiment_score, sentiment_label FROM stock_news WHERE stock_code = %s",
                    [code]
                ))
                for r in rows:
                    total += 1
                    if r.get("sentiment_label"):
                        with_finbert += 1
                    else:
                        without_finbert += 1
        
        return ok({
            "total_articles": total,
            "with_finbert_sentiment": with_finbert,
            "without_finbert_sentiment": without_finbert,
            "completion_percentage": round(with_finbert / total * 100, 2) if total > 0 else 0,
        })
    except Exception as e:
        logger.error(f"get_sentiment_status error: {e}")
        raise HTTPException(500, detail=str(e))
