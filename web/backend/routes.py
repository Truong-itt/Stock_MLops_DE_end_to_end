import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from database import db

try:
    from cassandra.util import Date as CassDate
except ImportError:
    CassDate = None

logger = logging.getLogger("backend.routes")


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


# ────────────────────────────── OHLCV Aggregated ─────────────────────
@router.get("/stocks/ohlcv/{symbol}")
async def get_ohlcv(
    symbol: str,
    interval: str = Query(default="1m"),
    bucket_date: Optional[str] = Query(default=None),
):
    try:
        # Map interval → query strategy
        DAILY_INTERVALS = {
            "15d": 15,
            "1mo": 30,
            "6mo": 180,
            "1y": 365,
        }
        if interval in DAILY_INTERVALS:
            rows = _find_daily_ohlcv(symbol, DAILY_INTERVALS[interval])
        else:
            # Intraday: 1m, 5m, 1h, 3h, 6h
            rows = _find_ohlcv(symbol, interval, bucket_date)
        return ok(rows)
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

        return ok({
            "breadth": {
                "labels": bucket_labels,
                "values": bucket_vals,
                "total": len(merged),
                "advancers": advancers,
                "decliners": decliners,
            },
            "stocks": _serialise(merged),
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


VIETNAM_STOCKS_SET = {
    "VCB", "BID", "FPT", "HPG", "CTG", "VHM", "TCB", "VPB", "VNM", "MBB",
    "GAS", "ACB", "MSN", "GVR", "LPB", "SSB", "STB", "VIB", "MWG", "HDB",
    "PLX", "POW", "SAB", "BCM", "PDR", "KDH", "NVL", "DGC", "SHB", "EIB",
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
            market = "vn" if code in VIETNAM_STOCKS_SET else "world"
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
