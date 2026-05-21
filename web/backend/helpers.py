"""Shared utility functions for backend routes and websocket."""
from typing import List, Optional
from cachetools import TTLCache
from database import db
from symbol_registry import SymbolRegistry

registry = SymbolRegistry()
daily_latest_rows_cache = TTLCache(maxsize=8, ttl=120)


def _positive_float_or_none(value):
    """Convert value to positive float or return None."""
    try:
        if value is None:
            return None
        f = float(value)
        return f if f > 0 else None
    except Exception:
        return None


def _is_price_scale_compatible(base_price, candidate_price, low_ratio: float = 0.2, high_ratio: float = 5.0) -> bool:
    """Check if candidate price is within acceptable ratio of base price."""
    base = _positive_float_or_none(base_price)
    cand = _positive_float_or_none(candidate_price)
    if base is None or cand is None:
        return True
    ratio = cand / base
    return low_ratio <= ratio <= high_ratio


def _tracked_symbols() -> List[str]:
    """Get sorted list of configured tracked symbols."""
    return sorted({str(sym or "").upper().strip() for sym in registry.get_all_symbols() if sym})


def _load_latest_daily_rows(symbols: Optional[List[str]] = None) -> List[dict]:
    """Load latest daily OHLCV row per symbol using batch query.

    Fetches the most recent trade_date row for each symbol to avoid N+1 queries.
    Results are cached by symbol tuple.
    """
    tracked = symbols or _tracked_symbols()
    tracked = [str(sym or "").upper().strip() for sym in tracked if sym]
    tracked = sorted(set(tracked))

    if not tracked:
        # Fallback when registry is empty: at least follow symbols that already
        # have realtime rows.
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
