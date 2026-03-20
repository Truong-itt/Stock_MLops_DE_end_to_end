import json
import os
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Tuple


REGISTRY_PATH = Path(os.getenv("SYMBOL_REGISTRY_PATH", "/app/config/symbol_registry.json"))


class SymbolRegistry:
    def __init__(self, path: Optional[Path] = None):
        self.path = Path(path or REGISTRY_PATH)
        self._lock = Lock()
        self._data: Dict = {}
        self._mtime_ns: Optional[int] = None
        self.load(force=True)

    def _default(self) -> Dict:
        return {
            "version": 1,
            "updated_at": None,
            "markets": {
                "vn": {"label": "Vietnam", "topic": "stock_price_vn", "symbols": []},
                "world": {"label": "World", "topic": "stock_price_dif", "symbols": []},
            },
        }

    def _normalize(self, data: Dict) -> Dict:
        base = self._default()
        markets = data.get("markets") or {}
        for market in ("vn", "world"):
            raw = markets.get(market) or {}
            seen = set()
            symbols: List[str] = []
            for symbol in raw.get("symbols") or []:
                sym = str(symbol).strip().upper()
                if sym and sym not in seen:
                    seen.add(sym)
                    symbols.append(sym)
            base["markets"][market] = {
                "label": raw.get("label") or base["markets"][market]["label"],
                "topic": raw.get("topic") or base["markets"][market]["topic"],
                "symbols": symbols,
            }
        base["version"] = data.get("version", base["version"])
        base["updated_at"] = data.get("updated_at")
        return base

    def load(self, force: bool = False) -> Dict:
        with self._lock:
            if not self.path.exists():
                self._data = self._default()
                self._mtime_ns = None
                return self._data

            stat = self.path.stat()
            if not force and self._mtime_ns == stat.st_mtime_ns and self._data:
                return self._data

            with self.path.open("r", encoding="utf-8") as fh:
                self._data = self._normalize(json.load(fh))
            self._mtime_ns = stat.st_mtime_ns
            return self._data

    def save(self, data: Dict) -> Dict:
        normalised = self._normalize(data)
        normalised["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self.path.open("w", encoding="utf-8") as fh:
                json.dump(normalised, fh, ensure_ascii=True, indent=2)
                fh.write("\n")
            self._data = normalised
            self._mtime_ns = self.path.stat().st_mtime_ns
        return normalised

    def snapshot(self) -> Dict:
        return self.load(force=False)

    def get_market_symbols(self, market: str) -> List[str]:
        return list(self.snapshot()["markets"].get(market, {}).get("symbols", []))

    def get_all_symbols(self) -> List[str]:
        data = self.snapshot()
        return data["markets"]["vn"]["symbols"] + data["markets"]["world"]["symbols"]

    def get_market_for_symbol(self, symbol: str) -> Optional[str]:
        sym = str(symbol).strip().upper()
        data = self.snapshot()
        for market, meta in data["markets"].items():
            if sym in meta.get("symbols", []):
                return market
        return None

    def get_symbol_location(self, symbol: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        sym = str(symbol).strip().upper()
        data = self.snapshot()
        for market, meta in data["markets"].items():
            symbols = meta.get("symbols", [])
            if sym in symbols:
                return market, meta.get("topic"), symbols.index(sym)
        return None, None, None

    def add_symbol(self, market: str, symbol: str) -> Tuple[Dict, bool, int]:
        mkt = str(market).strip().lower()
        if mkt not in {"vn", "world"}:
            raise ValueError("market must be 'vn' or 'world'")

        sym = str(symbol).strip().upper()
        if not sym:
            raise ValueError("symbol is required")

        data = self.snapshot()
        symbols = list(data["markets"][mkt]["symbols"])
        if sym in symbols:
            return data, False, symbols.index(sym)

        symbols.append(sym)
        data["markets"][mkt]["symbols"] = symbols
        saved = self.save(data)
        return saved, True, len(symbols) - 1
