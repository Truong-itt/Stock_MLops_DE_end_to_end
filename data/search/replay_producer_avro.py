import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from utils import get_partition_with_least_messages

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
    {"name": "received_at", "type": ["null", "long"], "default": null}
  ]
}
"""

# Define two separate lists for Vietnamese and international stock codes
vietnam_stock_list = [
    "VCB", "BID", "FPT", "HPG", "CTG", "VHM", "TCB", "VPB", "VNM", "MBB",
    "GAS", "ACB", "MSN", "GVR", "LPB", "SSB", "STB", "VIB", "MWG", "HDB"
]

international_stock_list = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B", "LLY", "AVGO",
    "JPM", "V", "UNH", "WMT", "MA", "XOM", "JNJ", "PG", "HD", "COST"
    ]

def ensure_ms(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        x = int(float(v))
    except (ValueError, TypeError):
        return None
    # nếu lỡ là seconds thì nhân 1000
    if x < 10_000_000_000:
        x *= 1000
    return x

def to_avro_record(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    msg là 1 dòng từ file JSONL.
    File của bạn có thể là:
      - mỗi dòng là message trực tiếp: {"id":..., "price":..., "time":...}
      - hoặc wrapper: {"received_at_ms":..., "message": {...}}
    """
    if "message" in msg and isinstance(msg["message"], dict):
        msg = msg["message"]

    try:
        return {
            "id": str(msg.get("id", "UNKNOWN")),
            "price": float(msg["price"]) if msg.get("price") is not None else None,
            "time": ensure_ms(msg.get("time")),
            "exchange": str(msg["exchange"]) if msg.get("exchange") else None,
            "quote_type": int(msg["quote_type"]) if msg.get("quote_type") is not None else None,
            "market_hours": int(msg["market_hours"]) if msg.get("market_hours") is not None else None,
            "change_percent": float(msg["change_percent"]) if msg.get("change_percent") is not None else None,
            "change": float(msg["change"]) if msg.get("change") is not None else None,
            "price_hint": str(msg["price_hint"]) if msg.get("price_hint") else None,
            "received_at": int(time.time() * 1000),
        }
    except (ValueError, TypeError, KeyError):
        return None

def main(
    file_path: str,
    bootstrap: str = "kafka-1:29092",
    schema_registry_url: str = "http://schema-registry:8081",
    speed: float = 10.0,
    max_messages: int = 0,
):
    # Schema Registry + Avro serializer
    sr = SchemaRegistryClient({"url": schema_registry_url})
    avro_serializer = AvroSerializer(sr, AVRO_SCHEMA_STR)

    producer = SerializingProducer(
        {
            "bootstrap.servers": bootstrap,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(file_path)

    prev_received_at = None
    sent = 0

    def delivery_report(err, msg):
        if err is not None:
            print(f"❌ Delivery failed: {err}", flush=True)

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            raw = json.loads(line)
            record = to_avro_record(raw)
            if record is None:
                continue

            # Determine the topic and partition based on the stock ID
            stock_id = record["id"]
            # if stock_id not in stock_list:
            #     continue

            if stock_id in vietnam_stock_list:
                topic = "stock_price_vn"
                partition = vietnam_stock_list.index(stock_id) % 20
            elif stock_id in international_stock_list:
                topic = "stock_price_dif"
                partition = international_stock_list.index(stock_id) % 20
            else:
                continue

            # Simulate timing based on received_at_ms if available
            received_at_ms = None
            if isinstance(raw, dict):
                received_at_ms = raw.get("received_at_ms") or raw.get("received_at") or raw.get("received_at_ms".upper())
            if received_at_ms is None and "message" in raw and isinstance(raw["message"], dict):
                received_at_ms = raw["message"].get("received_at_ms")

            if received_at_ms is not None:
                try:
                    received_at_ms = int(received_at_ms)
                except Exception:
                    received_at_ms = None

            if prev_received_at is not None and received_at_ms is not None:
                delta = received_at_ms - prev_received_at
                if delta > 0:
                    time.sleep((delta / 1000.0) / max(speed, 1e-9))

            if received_at_ms is not None:
                prev_received_at = received_at_ms

            producer.produce(
                topic=topic,
                key=record["id"],
                value=record,
                partition=partition,
                on_delivery=delivery_report,
            )
            producer.poll(0)

            sent += 1
            if sent % 200 == 0:
                print(f"✅ sent={sent}", flush=True)

            if max_messages > 0 and sent >= max_messages:
                break

    producer.flush()
    print(f"✅ DONE. Total sent={sent}", flush=True)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--file", default="/app/ws_20260302.jsonl")
    p.add_argument("--bootstrap", default="kafka-1:29092")
    p.add_argument("--schema", default="http://schema-registry:8081")
    p.add_argument("--speed", type=float, default=10.0)
    p.add_argument("--max", type=int, default=0, help="0 = no limit")
    args = p.parse_args()

    main(
        file_path=args.file,
        bootstrap=args.bootstrap,
        schema_registry_url=args.schema,
        speed=args.speed,
        max_messages=args.max,
    )