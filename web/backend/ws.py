import asyncio
import hashlib
import json
import logging
from datetime import datetime, date
from decimal import Decimal
from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict, List, Set
from database import db

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

    # Build daily lookup
    daily_map: Dict[str, dict] = {}
    for r in daily_rows:
        daily_map[r["symbol"]] = r

    if not latest_rows:
        # No real-time data at all → pure daily
        return daily_rows, "daily"

    # Merge: use latest if price exists, otherwise fill from daily
    merged: List[dict] = []
    seen: Set[str] = set()
    for r in latest_rows:
        sym = r["symbol"]
        seen.add(sym)
        if r.get("price") is not None:
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
                        target_date = req.get("date", str(date.today()))
                        rows = list(db.execute(
                            "SELECT * FROM stock_prices_agg WHERE symbol=%s AND bucket_date=%s AND interval=%s",
                            [symbol, target_date, interval],
                        ))
                        await websocket.send_text(json.dumps({
                            "type": "ohlcv_data",
                            "symbol": symbol,
                            "data": rows,
                        }, cls=CustomEncoder))

                    elif rtype == "get_news":
                        code = req.get("stock_code", "").upper()
                        rows = list(db.execute(
                            "SELECT * FROM stock_news WHERE stock_code=%s LIMIT 20", [code]
                        ))
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
