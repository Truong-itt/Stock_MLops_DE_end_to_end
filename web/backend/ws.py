import asyncio
import hashlib
import json
import logging
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal
from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict, List, Set
from cachetools import TTLCache
from database import db
from symbol_registry import SymbolRegistry

try:
    from cassandra.util import Date as CassDate
except ImportError:
    CassDate = None

logger = logging.getLogger("backend.ws")


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
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
            return list(obj)
        return super().default(obj)


def _quick_hash(data) -> str:
    """Fast hash of serialised data for change detection."""
    raw = json.dumps(data, cls=CustomEncoder, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()


class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WS client connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        logger.info(f"WS client disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return
        text = json.dumps(message, cls=CustomEncoder)
        dead = set()
        for conn in self.active_connections:
            try:
                await conn.send_text(text)
            except Exception:
                dead.add(conn)
        self.active_connections -= dead


manager = ConnectionManager()
registry = SymbolRegistry()
daily_latest_rows_cache = TTLCache(maxsize=8, ttl=120)
merged_snapshot_cache = TTLCache(maxsize=1, ttl=2)


def _build_daily_map(rows: List[dict]) -> Dict[str, dict]:
    """Keep only the latest daily row per symbol."""
    daily_map: Dict[str, dict] = {}
    for row in rows:
        sym = row["symbol"]
        if sym not in daily_map:
            daily_map[sym] = row
            continue
        existing_date = daily_map[sym].get("trade_date")
        new_date = row.get("trade_date")
        if new_date and existing_date and str(new_date) > str(existing_date):
            daily_map[sym] = row
    return daily_map


def _positive_float_or_none(value):
    try:
        if value is None:
            return None
        f = float(value)
        return f if f > 0 else None
    except Exception:
        return None


def _is_price_scale_compatible(base_price, candidate_price, low_ratio: float = 0.2, high_ratio: float = 5.0) -> bool:
    base = _positive_float_or_none(base_price)
    cand = _positive_float_or_none(candidate_price)
    if base is None or cand is None:
        return True
    ratio = cand / base
    return low_ratio <= ratio <= high_ratio


def _placeholder_row(symbol: str) -> dict:
    """Create a stable placeholder for symbols not yet populated in Scylla."""
    return {
        "symbol": symbol,
        "price": None,
        "change": 0,
        "change_percent": 0,
        "open": None,
        "high": None,
        "low": None,
        "day_volume": None,
        "vwap": None,
        "exchange": registry.get_market_for_symbol(symbol) or "",
        "timestamp": None,
        "market_hours": None,
        "quote_type": None,
        "is_placeholder": True,
    }


def _configured_symbols() -> List[str]:
    return sorted({str(sym or "").upper().strip() for sym in registry.get_all_symbols() if sym})


def _load_latest_daily_rows(symbols: List[str] = None) -> List[dict]:
    tracked = symbols or _configured_symbols()
    tracked = [str(sym or "").upper().strip() for sym in tracked if sym]
    tracked = sorted(set(tracked))

    if not tracked:
        tracked = sorted(
            {
                str(row.get("symbol") or "").upper().strip()
                for row in db.execute("SELECT symbol FROM stock_latest_prices")
                if row.get("symbol")
            }
        )
    if not tracked:
        return []

    cache_key = tuple(tracked)
    cached = daily_latest_rows_cache.get(cache_key)
    if cached is not None:
        return [dict(row) for row in cached]

    # Batch query instead of N+1: fetch all symbols in one query, deduplicate by trade_date desc
    placeholders = ", ".join(["%s"] * len(tracked))
    query = f"""
        SELECT symbol, trade_date, open, high, low, close, volume,
               change, change_percent, vwap, exchange
        FROM stock_daily_summary
        WHERE symbol IN ({placeholders})
        ORDER BY symbol, trade_date DESC
    """
    all_rows = list(db.execute(query, tracked))

    # Keep only the latest (most recent trade_date) row per symbol
    rows = []
    seen_symbols = set()
    for row in all_rows:
        sym = row.get("symbol")
        if sym not in seen_symbols:
            rows.append(row)
            seen_symbols.add(sym)

    daily_latest_rows_cache[cache_key] = rows
    return [dict(row) for row in rows]


def _fetch_merged_data() -> tuple[List[dict], str]:
    """
    Lấy stock_latest_prices (real-time), merge thêm daily_summary
    cho các symbol thiếu hoặc price=null.
    Trả về (rows, source).
    """
    tracked = _configured_symbols()
    tracked_set = set(tracked)

    latest_rows = list(db.execute(
        "SELECT symbol, price, change, change_percent, day_volume, "
        "exchange, last_size, market_hours, quote_type, timestamp "
        "FROM stock_latest_prices"
    ))
    if tracked_set:
        latest_rows = [row for row in latest_rows if row.get("symbol") in tracked_set]
    daily_rows = _load_latest_daily_rows(tracked if tracked else None)

    daily_map = _build_daily_map(daily_rows)

    if not latest_rows:
        merged = list(daily_rows)
        seen = {row.get("symbol") for row in merged if row.get("symbol")}
        for sym in tracked:
            if sym not in seen:
                merged.append(_placeholder_row(sym))
        return merged, "daily"

    # Merge: use latest if price exists, otherwise fill from daily
    merged: List[dict] = []
    seen: Set[str] = set()
    for r in latest_rows:
        sym = r["symbol"]
        seen.add(sym)
        if r.get("price") is not None:
            # Merge daily OHLC data (open, high, low, vwap, volume) into latest row
            daily = daily_map.get(sym)
            if daily and not _is_price_scale_compatible(r.get("price"), daily.get("close")):
                daily = None
            if daily:
                r["open"] = daily.get("open")
                r["high"] = daily.get("high")
                r["low"] = daily.get("low")
                r["vwap"] = daily.get("vwap")
                # Fill volume from daily when day_volume is null
                if not r.get("day_volume"):
                    r["day_volume"] = daily.get("volume")
                r["trade_date"] = daily.get("trade_date")
            merged.append(r)
        else:
            # price is null → try to get from daily
            daily = daily_map.get(sym)
            if daily:
                merged.append(daily)

    # Add daily-only symbols not in latest
    for sym, r in daily_map.items():
        if sym not in seen:
            merged.append(r)

    # Keep WS payload shape consistent with REST /stocks/latest.
    for sym in tracked:
        if sym in seen or sym in daily_map:
            continue
        merged.append(_placeholder_row(sym))

    return merged, "merged"


def _fetch_merged_data_cached() -> tuple[List[dict], str]:
    cached = merged_snapshot_cache.get("merged")
    if cached is not None:
        rows, source = cached
        return [dict(row) for row in rows], source
    rows, source = _fetch_merged_data()
    merged_snapshot_cache["merged"] = (rows, source)
    return [dict(row) for row in rows], source


def _fetch_merged_with_hash():
    rows, source = _fetch_merged_data_cached()
    return rows, source, _quick_hash(rows)


async def poll_latest_prices():
    """Background: poll ScyllaDB mỗi 1.5s, broadcast khi data thay đổi."""
    last_hash = ""

    while True:
        try:
            rows, source, h = await asyncio.to_thread(_fetch_merged_with_hash)

            if h != last_hash and manager.active_connections:
                await manager.broadcast({
                    "type": "price_update",
                    "source": source,
                    "timestamp": datetime.utcnow().isoformat(),
                    "data": rows,
                })
                last_hash = h

        except Exception as e:
            logger.error(f"Poll error: {e}")

        await asyncio.sleep(1.5)


def _to_int_or_none(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_tick_epoch_ms(value):
    if value is None:
        return -1

    raw = str(value).strip()
    if not raw:
        return -1

    if raw.isdigit():
        iv = int(raw)
        if iv < 10_000_000_000:
            iv *= 1000
        return iv

    try:
        normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return -1

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def _matched_sort_key(row: dict) -> int:
    producer_ts = _to_int_or_none(row.get("producer_timestamp"))
    if producer_ts is not None and producer_ts > 0:
        if producer_ts < 10_000_000_000:
            producer_ts *= 1000
        return producer_ts
    return _to_tick_epoch_ms(row.get("timestamp"))


def _with_matched_size(rows: List[dict]) -> List[dict]:
    out: List[dict] = []
    for idx, row in enumerate(rows):
        item = dict(row)

        last_size = _to_int_or_none(item.get("last_size"))
        if last_size is not None and last_size > 0:
            item["matched_size"] = last_size
            out.append(item)
            continue

        matched_size = 0
        cur_day_volume = _to_int_or_none(item.get("day_volume"))
        next_day_volume = None
        if idx + 1 < len(rows):
            next_day_volume = _to_int_or_none(rows[idx + 1].get("day_volume"))

        if cur_day_volume is not None and next_day_volume is not None:
            delta = cur_day_volume - next_day_volume
            if delta > 0:
                matched_size = delta

        item["matched_size"] = matched_size
        out.append(item)
    return out


async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Gửi snapshot ban đầu
        try:
            rows, source = await asyncio.to_thread(_fetch_merged_data_cached)
            await websocket.send_text(json.dumps({
                "type": "snapshot",
                "source": source,
                "timestamp": datetime.utcnow().isoformat(),
                "data": rows,
            }, cls=CustomEncoder))
        except Exception as e:
            logger.error(f"Snapshot error: {e}")

        # Lắng nghe request từ client
        while True:
            try:
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=25)
                try:
                    req = json.loads(msg)
                    rtype = req.get("type", "")

                    if rtype == "ping":
                        await websocket.send_text(json.dumps({"type": "pong"}))

                    elif rtype == "get_ohlcv":
                        symbol = req.get("symbol", "").upper()
                        interval = req.get("interval", "1d")
                        req_date = req.get("date")
                        rows = []
                        
                        # Map interval → query strategy
                        DAILY_INTERVALS = {
                            "1d": 1,
                            "1w": 7,
                            "1mo": 30,
                            "3mo": 90,
                            "1y": 365,
                            "5y": 1825,
                            # Backward-compatible aliases for older clients.
                            "15d": 15,
                            "6mo": 180,
                        }
                        if interval in DAILY_INTERVALS:
                            # Query daily data
                            rows = list(db.execute(
                                "SELECT * FROM stock_daily_summary WHERE symbol = %s LIMIT %s",
                                [symbol, DAILY_INTERVALS[interval]],
                            ))
                            # Chuyển format: trade_date → bucket/ts
                            formatted = []
                            for r in rows:
                                formatted.append({
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
                            rows = formatted
                        else:
                            # Intraday: 1m, 5m, 1h, 3h, 6h
                            if req_date:
                                rows = list(db.execute(
                                    "SELECT * FROM stock_prices_agg WHERE symbol=%s AND bucket_date=%s AND interval=%s",
                                    [symbol, req_date, interval],
                                ))
                            else:
                                for offset in range(6):
                                    d = date.today() - timedelta(days=offset)
                                    rows = list(db.execute(
                                        "SELECT * FROM stock_prices_agg WHERE symbol=%s AND bucket_date=%s AND interval=%s",
                                        [symbol, str(d), interval],
                                    ))
                                    if rows:
                                        break
                        
                        await websocket.send_text(json.dumps({
                            "type": "ohlcv_data",
                            "symbol": symbol,
                            "data": rows,
                        }, cls=CustomEncoder))

                    elif rtype == "get_news":
                        code = req.get("stock_code", "").upper()
                        rows = list(db.execute(
                            "SELECT * FROM stock_news WHERE stock_code=%s LIMIT 50", [code]
                        ))
                        cutoff = datetime.utcnow() - timedelta(days=7)
                        rows = [r for r in rows if r.get("date") and r["date"] >= cutoff]
                        rows.sort(key=lambda x: x.get("date") or datetime.min, reverse=True)
                        rows = rows[:20]
                        await websocket.send_text(json.dumps({
                            "type": "news_data",
                            "stock_code": code,
                            "data": rows,
                        }, cls=CustomEncoder))

                    elif rtype == "get_daily":
                        symbol = req.get("symbol", "").upper()
                        rows = list(db.execute(
                            "SELECT * FROM stock_daily_summary WHERE symbol=%s LIMIT 30", [symbol]
                        ))
                        await websocket.send_text(json.dumps({
                            "type": "daily_data",
                            "symbol": symbol,
                            "data": rows,
                        }, cls=CustomEncoder))

                    elif rtype == "get_matched_orders":
                        symbol = req.get("symbol", "").upper()
                        limit = min(int(req.get("limit", 50)), 200)
                        fetch_limit = min(max(limit * 4, 200), 2000)
                        rows = list(db.execute(
                            "SELECT timestamp, producer_timestamp, price, last_size, day_volume, change, change_percent "
                            "FROM stock_prices WHERE symbol=%s LIMIT %s",
                            [symbol, fetch_limit],
                        ))
                        rows.sort(key=_matched_sort_key, reverse=True)
                        rows = _with_matched_size(rows)
                        rows = rows[:limit]
                        count_rows = list(db.execute(
                            "SELECT COUNT(*) as cnt FROM stock_prices WHERE symbol=%s",
                            [symbol],
                        ))
                        total = count_rows[0]["cnt"] if count_rows else len(rows)
                        await websocket.send_text(json.dumps({
                            "type": "matched_orders",
                            "symbol": symbol,
                            "data": rows,
                            "total_count": total,
                        }, cls=CustomEncoder))

                except json.JSONDecodeError:
                    pass
            except asyncio.TimeoutError:
                try:
                    await websocket.send_text(json.dumps({"type": "heartbeat"}))
                except Exception:
                    break

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WS error: {e}")
    finally:
        manager.disconnect(websocket)
