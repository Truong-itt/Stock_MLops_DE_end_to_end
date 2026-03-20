import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple


REGISTRY_PATH = Path(os.getenv("SYMBOL_REGISTRY_PATH", "/app/config/symbol_registry.json"))


class SymbolRegistry:
    def __init__(self, path: Optional[Path] = None):
        self.path = Path(path or REGISTRY_PATH)
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

    def reload_if_changed(self) -> Dict:
        return self.load(force=False)

    def get_market_symbols(self, market: str) -> List[str]:
        return list(self.load()["markets"].get(market, {}).get("symbols", []))

    def get_all_symbols(self) -> List[str]:
        data = self.load()
        return data["markets"]["vn"]["symbols"] + data["markets"]["world"]["symbols"]

    def get_symbol_location(self, symbol: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        sym = str(symbol).strip().upper()
        data = self.load()
        for market, meta in data["markets"].items():
            symbols = meta.get("symbols", [])
            if sym in symbols:
                return market, meta.get("topic"), symbols.index(sym)
        return None, None, None
