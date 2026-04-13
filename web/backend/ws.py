import asyncio
import hashlib
import json
import logging
from datetime import datetime, date, timedelta
from decimal import Decimal
from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict, List, Set
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


def _fetch_merged_data() -> tuple[List[dict], str]:
    """
    Lấy stock_latest_prices (real-time), merge thêm daily_summary
    cho các symbol thiếu hoặc price=null.
    Trả về (rows, source).
    """
    latest_rows = list(db.execute(
        "SELECT symbol, price, change, change_percent, day_volume, "
        "exchange, last_size, market_hours, quote_type, timestamp "
        "FROM stock_latest_prices"
    ))
    daily_rows = list(db.execute(
        "SELECT symbol, trade_date, open, high, low, close, volume, "
        "change, change_percent, vwap, exchange "
        "FROM stock_daily_summary"
    ))
    registry_symbols = set(registry.get_all_symbols())
    if registry_symbols:
        latest_rows = [row for row in latest_rows if row.get("symbol") in registry_symbols]
        daily_rows = [row for row in daily_rows if row.get("symbol") in registry_symbols]

    daily_map = _build_daily_map(daily_rows)

    if not latest_rows:
        merged = list(daily_rows)
        seen = {row.get("symbol") for row in merged if row.get("symbol")}
        for sym in registry.get_all_symbols():
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
    for sym in registry.get_all_symbols():
        if sym in seen or sym in daily_map:
            continue
        merged.append(_placeholder_row(sym))

    return merged, "merged"


async def poll_latest_prices():
    """Background: poll ScyllaDB mỗi 1.5s, broadcast khi data thay đổi."""
    last_hash = ""

    while True:
        try:
            rows, source = _fetch_merged_data()
            h = _quick_hash(rows)

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


async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Gửi snapshot ban đầu
        try:
            rows, source = _fetch_merged_data()
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
                        interval = req.get("interval", "1m")
                        req_date = req.get("date")
                        rows = []
                        
                        # Map interval → query strategy
                        DAILY_INTERVALS = {
                            "15d": 15,
                            "1mo": 30,
                            "6mo": 180,
                            "1y": 365,
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
                        rows = list(db.execute(
                            "SELECT timestamp, price, last_size, change, change_percent "
                            "FROM stock_prices WHERE symbol=%s LIMIT %s",
                            [symbol, limit],
                        ))
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
