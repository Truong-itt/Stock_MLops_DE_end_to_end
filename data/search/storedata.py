import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import yfinance as yf


# =========================
# CONFIG
# =========================
STOCK_LIST = [
    "VCB", "BID", "FPT", "HPG", "CTG", "VHM", "TCB", "VPB", "VNM", "MBB",
    "GAS", "ACB", "MSN", "GVR", "LPB", "SSB", "STB", "VIB", "MWG", "HDB",
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B", "LLY", "AVGO",
    "JPM", "V", "UNH", "WMT", "MA", "XOM", "JNJ", "PG", "HD", "COST"
]

OUT_DIR = "recordings"          # thư mục lưu
ROTATE_DAILY = True             # True: mỗi ngày tạo 1 file mới
FLUSH_EVERY_N = 20              # cứ N message flush 1 lần
DEDUP_ENABLED = False           # bật nếu bạn muốn bỏ message trùng
DEDUP_CACHE_SIZE = 20000        # cache dedup lớn hơn nếu cần


# =========================
# Helpers
# =========================
def ensure_ms(ts: Any) -> Optional[int]:
    """
    time trong message của bạn đang là string milliseconds '1772435327000'.
    Hàm này chuẩn hóa về int milliseconds.
    """
    if ts is None:
        return None
    try:
        v = int(float(ts))
    except (ValueError, TypeError):
        return None

    # heuristic: nếu là seconds (10 chữ số ~ 1e9) -> nhân 1000
    if v < 10_000_000_000:  # < 1e10
        v *= 1000
    return v


def sanitize_message(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Chuẩn hóa message để:
    - time luôn là int ms
    - received_at_ms luôn có
    - đảm bảo JSON dump không lỗi
    """
    out = dict(msg)  # copy
    out["id"] = str(out.get("id", "UNKNOWN"))

    # chuẩn hóa time
    out["time"] = ensure_ms(out.get("time"))

    # thêm timestamp nhận được
    out["received_at_ms"] = int(time.time() * 1000)

    # nếu có field numeric dạng weird string, bạn có thể ép kiểu ở đây (tuỳ)
    # ví dụ price_hint đang là '2' -> int
    if "price_hint" in out and out["price_hint"] is not None:
        try:
            out["price_hint"] = str(out["price_hint"])
        except Exception:
            out["price_hint"] = None

    return out

@dataclass
class Recorder:
    out_dir: str
    rotate_daily: bool = True
    flush_every_n: int = 20

    def __post_init__(self):
        self.base = Path(self.out_dir)
        self.base.mkdir(parents=True, exist_ok=True)
        self.current_date = None
        self.f = None
        self.count_since_flush = 0

    def _filepath_for_today(self) -> Path:
        # file theo ngày: ws_YYYYMMDD.jsonl
        day = datetime.now().strftime("%Y%m%d")
        return self.base / f"ws_{day}.jsonl"

    def _open_if_needed(self):
        if not self.rotate_daily:
            if self.f is None:
                path = self.base / "ws_live.jsonl"
                self.f = path.open("a", encoding="utf-8")
            return

        today = datetime.now().strftime("%Y%m%d")
        if self.current_date != today or self.f is None:
            # đóng file cũ
            if self.f:
                self.f.flush()
                self.f.close()

            path = self._filepath_for_today()
            self.f = path.open("a", encoding="utf-8")
            self.current_date = today
            self.count_since_flush = 0
            print(f"📝 Recording file: {path}")

    def write(self, record: Dict[str, Any]):
        self._open_if_needed()
        self.f.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.count_since_flush += 1

        if self.count_since_flush >= self.flush_every_n:
            self.f.flush()
            os.fsync(self.f.fileno())  # chắc ăn (tốn IO hơn)
            self.count_since_flush = 0

    def close(self):
        if self.f:
            self.f.flush()
            self.f.close()
            self.f = None


# =========================
# Optional dedup
# =========================
from collections import deque

dedup_cache = deque(maxlen=DEDUP_CACHE_SIZE)

def is_duplicate(msg: Dict[str, Any]) -> bool:
    """
    Dedup theo (id, time, price).
    Bạn có thể thay logic nếu muốn.
    """
    key = (msg.get("id"), msg.get("time"), msg.get("price"))
    if key in dedup_cache:
        return True
    dedup_cache.append(key)
    return False


# =========================
# Main
# =========================
async def run_recording():
    recorder = Recorder(OUT_DIR, rotate_daily=ROTATE_DAILY, flush_every_n=FLUSH_EVERY_N)

    def handler(raw: Dict[str, Any]):
        try:
            msg = sanitize_message(raw)

            if DEDUP_ENABLED and is_duplicate(msg):
                return

            recorder.write(msg)

            # log nhẹ để biết đang chạy
            if msg.get("time"):
                t = datetime.fromtimestamp(msg["time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            else:
                t = "N/A"

            print(f"✅ saved: {msg.get('id')} price={msg.get('price')} time={t}", flush=True)

        except Exception as e:
            print(f"❌ handler error: {e} | raw={raw}", flush=True)

    try:
        async with yf.AsyncWebSocket() as ws:
            await ws.subscribe(STOCK_LIST)
            await ws.listen(message_handler=handler)
    except KeyboardInterrupt:
        print("\n🛑 Stop by Ctrl+C")
    finally:
        recorder.close()
        print("✅ Recorder closed.")


if __name__ == "__main__":
    asyncio.run(run_recording())