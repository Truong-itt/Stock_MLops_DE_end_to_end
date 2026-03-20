"""
ws_producer_avro.py
──────────────────
Kết nối trực tiếp Yahoo Finance WebSocket, nhận dữ liệu real-time
và đẩy thẳng vào Kafka dưới dạng Avro (thay vì replay từ file JSONL).

Usage (local):
    python ws_producer_avro.py

Usage (Docker):
    docker compose -f docker-compose.ws.yml up --build
"""

import asyncio
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

import yfinance as yf

from logger import get_logger
from symbol_registry import SymbolRegistry

logger = get_logger()

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# CONFIG
BOOTSTRAP_SERVERS = "kafka-1:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

AVRO_SCHEMA_STR = """
{
  "type": "record",
  "name": "StockPrice",
  "namespace": "com.example.stock",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "price", "type": ["null", "double"], "default": null},
    {"name": "time", "type": ["null", "long"], "default": null},
    {"name": "exchange", "type": ["null", "string"], "default": null},
    {"name": "quote_type", "type": ["null", "int"], "default": null},
    {"name": "market_hours", "type": ["null", "int"], "default": null},
    {"name": "change_percent", "type": ["null", "double"], "default": null},
    {"name": "change", "type": ["null", "double"], "default": null},
    {"name": "price_hint", "type": ["null", "string"], "default": null},
    {"name": "received_at", "type": ["null", "long"], "default": null},
    {"name": "day_volume", "type": ["null", "long"], "default": null},
    {"name": "last_size", "type": ["null", "long"], "default": null}
  ]
}
"""


# HELPERS
def ensure_ms(v: Any) -> Optional[int]:
    """Chuẩn hóa timestamp về milliseconds."""
    if v is None:
        return None
    try:
        x = int(float(v))
    except (ValueError, TypeError):
        return None
    if x < 10_000_000_000:      # seconds → ms
        x *= 1000
    return x


def _safe_long(v: Any) -> Optional[int]:
    """Chuyển giá trị sang long/int, trả None nếu không hợp lệ."""
    if v is None:
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


def to_avro_record(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Chuyển raw WebSocket message sang Avro record."""
    try:
        return {
            "id":             str(msg.get("id", "UNKNOWN")),
            "price":          float(msg["price"]) if msg.get("price") is not None else None,
            "time":           ensure_ms(msg.get("time")),
            "exchange":       str(msg["exchange"]) if msg.get("exchange") else None,
            "quote_type":     int(msg["quote_type"]) if msg.get("quote_type") is not None else None,
            "market_hours":   int(msg["market_hours"]) if msg.get("market_hours") is not None else None,
            "change_percent": float(msg["change_percent"]) if msg.get("change_percent") is not None else None,
            "change":         float(msg["change"]) if msg.get("change") is not None else None,
            "price_hint":     str(msg["price_hint"]) if msg.get("price_hint") else None,
            "received_at":    int(time.time() * 1000),
            "day_volume":     _safe_long(msg.get("dayVolume") or msg.get("day_volume")),
            "last_size":      _safe_long(msg.get("lastSize") or msg.get("last_size")),
        }
    except (ValueError, TypeError, KeyError) as e:
        logger.warning("Skipped invalid message: %s", e)
        return None


def resolve_topic_and_partition(stock_id: str, registry: SymbolRegistry):
    """Xác định topic và partition dựa vào mã cổ phiếu."""
    _, topic, partition = registry.get_symbol_location(stock_id)
    return topic, partition


# KAFKA PRODUCER
def create_producer(bootstrap: str, schema_registry_url: str) -> SerializingProducer:
    sr = SchemaRegistryClient({"url": schema_registry_url})
    avro_serializer = AvroSerializer(sr, AVRO_SCHEMA_STR)

    return SerializingProducer({
        "bootstrap.servers":  bootstrap,
        "key.serializer":     StringSerializer("utf_8"),
        "value.serializer":   avro_serializer,
        "linger.ms":          50,       # gom batch nhỏ để tăng throughput
        "batch.size":         65536,
        "compression.type":   "snappy",
    })


# MAIN — WebSocket → Kafka
async def run(bootstrap: str, schema_registry_url: str):
    logger.info("Initializing Kafka Avro producer ...")
    producer = create_producer(bootstrap, schema_registry_url)
    registry = SymbolRegistry()

    sent = 0
    skipped = 0

    def delivery_report(err, msg):
        if err is not None:
            logger.error("Delivery failed: %s", err)

    def on_message(raw: Dict[str, Any]):
        nonlocal sent, skipped

        # ── Log ngay khi nhận được message từ WebSocket ──
        raw_id = raw.get("id", "?")
        raw_price = raw.get("price", "?")
        raw_time = raw.get("time")
        ts_str = "N/A"
        if raw_time is not None:
            try:
                ts_ms = int(float(raw_time))
                if ts_ms < 10_000_000_000:
                    ts_ms *= 1000
                ts_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            except Exception:
                pass
        logger.info("[WS RECV] %s  price=%s  time=%s  exchange=%s  change=%.4s%%",
                    raw_id, raw_price, ts_str,
                    raw.get("exchange", "?"), str(raw.get("change_percent", "?")))

        record = to_avro_record(raw)
        if record is None:
            skipped += 1
            logger.debug("Record conversion returned None, skipped")
            return

        stock_id = record["id"]
        topic, partition = resolve_topic_and_partition(stock_id, registry)
        if topic is None:
            skipped += 1
            logger.debug("Unknown stock_id=%s, skipped", stock_id)
            return

        producer.produce(
            topic=topic,
            key=stock_id,
            value=record,
            partition=partition,
            on_delivery=delivery_report,
        )
        producer.poll(0)

        sent += 1
        logger.info("[KAFKA SEND] %s -> %s[p%d]  price=%s  (total sent=%d)",
                    stock_id, topic, partition, record.get("price"), sent)

    # Graceful shutdown
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutting down ...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    subscribed_symbols: Set[str] = set()

    async def sync_subscriptions(ws):
        while not stop_event.is_set():
            try:
                registry.reload_if_changed()
                all_symbols = registry.get_all_symbols()
                new_symbols = [sym for sym in all_symbols if sym not in subscribed_symbols]
                if new_symbols:
                    await ws.subscribe(new_symbols)
                    subscribed_symbols.update(new_symbols)
                    logger.info(
                        "Subscribed %d new symbols from registry (total=%d)",
                        len(new_symbols),
                        len(subscribed_symbols),
                    )
            except Exception as e:
                logger.warning("Registry sync error: %s", e)
            await asyncio.sleep(10)

    initial_symbols = registry.get_all_symbols()
    logger.info("Connecting to Yahoo Finance WebSocket ...")
    logger.info("Subscribing to %d symbols from registry", len(initial_symbols))

    try:
        async with yf.AsyncWebSocket() as ws:
            if initial_symbols:
                await ws.subscribe(initial_symbols)
                subscribed_symbols.update(initial_symbols)
            logger.info("Subscribed! Listening for real-time ticks ...")

            # listen() chạy mãi — ta dùng stop_event để thoát gracefully
            listen_task = asyncio.create_task(
                ws.listen(message_handler=on_message)
            )
            registry_task = asyncio.create_task(sync_subscriptions(ws))

            # chờ stop signal hoặc listen kết thúc
            done, pending = await asyncio.wait(
                [listen_task, registry_task, asyncio.create_task(stop_event.wait())],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()

    except Exception as e:
        logger.error("WebSocket error: %s", e)
    finally:
        # flush hết message còn lại trong buffer
        remaining = producer.flush(timeout=10)
        logger.info("Done. Total sent=%d  skipped=%d  unflushed=%d", sent, skipped, remaining)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Yahoo Finance WebSocket → Kafka (Avro)")
    p.add_argument("--bootstrap", default=BOOTSTRAP_SERVERS,
                    help="Kafka bootstrap servers")
    p.add_argument("--schema", default=SCHEMA_REGISTRY_URL,
                    help="Schema Registry URL")
    args = p.parse_args()

    asyncio.run(run(
        bootstrap=args.bootstrap,
        schema_registry_url=args.schema,
    ))
