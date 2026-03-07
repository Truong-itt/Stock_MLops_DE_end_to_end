import logging
from datetime import date, datetime
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

        # Merge: latest wins, daily fills gaps
        daily_map = {r["symbol"]: r for r in daily}
        merged = []
        seen = set()
        for r in latest:
            sym = r["symbol"]
            seen.add(sym)
            if r.get("price") is not None:
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


# ────────────────────────────── OHLCV Aggregated ─────────────────────
@router.get("/stocks/ohlcv/{symbol}")
async def get_ohlcv(
    symbol: str,
    interval: str = Query(default="1m"),
    bucket_date: Optional[str] = Query(default=None),
):
    try:
        target_date = bucket_date or str(date.today())
        rows = list(db.execute(
            "SELECT * FROM stock_prices_agg WHERE symbol=%s AND bucket_date=%s AND interval=%s",
            [symbol.upper(), target_date, interval],
        ))
        return ok(rows)
    except Exception as e:
        logger.error(f"get_ohlcv({symbol}): {e}")
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


@router.get("/news/{stock_code}")
async def get_stock_news(
    stock_code: str,
    limit: int = Query(default=20, le=100),
):
    try:
        rows = list(db.execute(
            "SELECT * FROM stock_news WHERE stock_code = %s LIMIT %s",
            [stock_code.upper(), limit],
        ))
        return ok(rows)
    except Exception as e:
        logger.error(f"get_stock_news({stock_code}): {e}")
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


# ────────────────────────────── Health ───────────────────────────────
@router.get("/health")
async def health_check():
    try:
        db.execute("SELECT now() FROM system.local")
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": "disconnected", "detail": str(e)}
