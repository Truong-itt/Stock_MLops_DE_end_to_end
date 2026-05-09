import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from modeling_shared import FEATURE_NAMES, _parse_event_time, _safe_float, _sanitize_symbol


class FeatureDatasetMixin:
    def _market_flags(self, symbol: str) -> Tuple[float, float]:
        sym = str(symbol or "").upper()
        if sym in self.market_sets["vn"]:
            return 1.0, 0.0
        if sym in self.market_sets["world"]:
            return 0.0, 1.0
        return 0.0, 0.0

    def _build_feature_vector(self, event: Dict[str, Any]) -> np.ndarray:
        sym = str(event.get("symbol") or "").upper()
        ts = _parse_event_time(event.get("event_time"))

        cp_prob = _safe_float(event.get("cp_prob"))
        whale_score = _safe_float(event.get("whale_score"))
        innovation = _safe_float(event.get("innovation_zscore"))
        expected_run = _safe_float(event.get("expected_run_length"))
        map_run = _safe_float(event.get("map_run_length"))
        pred_vol = _safe_float(event.get("predictive_volatility"))
        ret = _safe_float(event.get("return_value"))
        hazard = _safe_float(event.get("hazard"))
        evidence = _safe_float(event.get("evidence"))
        price = max(_safe_float(event.get("price"), default=1.0), 1e-9)

        hour = float(ts.hour) + (float(ts.minute) / 60.0)
        hour_angle = (2.0 * math.pi * hour) / 24.0
        dow_angle = (2.0 * math.pi * float(ts.weekday())) / 7.0
        is_vn, is_world = self._market_flags(sym)

        feature_map = {
            "cp_prob": cp_prob,
            "whale_score": whale_score,
            "innovation_zscore": innovation,
            "innovation_abs": abs(innovation),
            "expected_run_length": expected_run,
            "map_run_length": map_run,
            "predictive_volatility": pred_vol,
            "return_value": ret,
            "return_abs": abs(ret),
            "hazard": hazard,
            "evidence": evidence,
            "log_price": math.log(price),
            "hour_sin": math.sin(hour_angle),
            "hour_cos": math.cos(hour_angle),
            "dow_sin": math.sin(dow_angle),
            "dow_cos": math.cos(dow_angle),
            "is_vn": is_vn,
            "is_world": is_world,
        }

        return np.asarray([feature_map[name] for name in FEATURE_NAMES], dtype=np.float64)

    def _load_training_events(
        self,
        lookback_days: int,
        max_rows: int,
        symbol: Optional[str] = None,
    ) -> pd.DataFrame:
        filters = [f"event_time >= now() - INTERVAL {int(lookback_days)} DAY"]
        if symbol:
            sym = _sanitize_symbol(symbol)
            filters.append(f"upper(symbol) = '{sym}'")

        where_clause = " AND ".join(filters)
        sql = f"""
        SELECT
            symbol,
            event_time,
            price,
            return_value,
            cp_prob,
            expected_run_length,
            map_run_length,
            predictive_volatility,
            innovation_zscore,
            whale_score,
            hazard,
            evidence
        FROM stock_changepoint_events
        WHERE {where_clause}
        ORDER BY event_time DESC
        LIMIT {int(max_rows)}
        """
        df = self._query_df(sql)
        if df.empty:
            return df
        df["symbol"] = df["symbol"].astype(str).str.upper()
        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce", utc=True)
        df = df.dropna(subset=["symbol", "event_time"])
        df["event_date"] = df["event_time"].dt.date
        return df

    def _load_daily_closes(
        self,
        lookback_days: int,
        horizon: int,
        symbol: Optional[str] = None,
    ) -> pd.DataFrame:
        days = int(lookback_days) + int(horizon) + 10
        filters = [f"trade_date >= toDate(now()) - INTERVAL {days} DAY"]
        if symbol:
            sym = _sanitize_symbol(symbol)
            filters.append(f"upper(symbol) = '{sym}'")

        where_clause = " AND ".join(filters)
        sql = f"""
        SELECT
            symbol,
            trade_date,
            close
        FROM v_ohlcv_daily
        WHERE {where_clause}
        ORDER BY symbol, trade_date
        """
        df = self._query_df(sql)
        if df.empty:
            return df

        df["symbol"] = df["symbol"].astype(str).str.upper()
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["symbol", "trade_date", "close"])
        return df

    def _build_training_dataset(
        self,
        event_df: pd.DataFrame,
        daily_df: pd.DataFrame,
        horizon: int,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        if event_df.empty or daily_df.empty:
            return np.empty((0, len(FEATURE_NAMES))), np.empty((0,)), np.empty((0,))

        daily_map: Dict[str, Dict[str, Any]] = {}
        for symbol, group in daily_df.groupby("symbol"):
            g = group.sort_values("trade_date")
            closes = g["close"].to_numpy(dtype=np.float64)
            dates = g["trade_date"].to_list()
            daily_map[str(symbol)] = {
                "close": closes,
                "date_to_idx": {d: i for i, d in enumerate(dates)},
            }

        features: List[np.ndarray] = []
        labels_dir: List[int] = []
        labels_sessions: List[float] = []

        for row in event_df.to_dict("records"):
            symbol = str(row.get("symbol") or "").upper()
            state = daily_map.get(symbol)
            if not state:
                continue

            event_date = row.get("event_date")
            idx = state["date_to_idx"].get(event_date)
            closes = state["close"]
            if idx is None or (idx + 1) >= len(closes):
                continue

            base_close = closes[idx]
            next_close = closes[idx + 1]
            if not np.isfinite(base_close) or base_close <= 0 or not np.isfinite(next_close):
                continue

            first_step = (next_close / base_close) - 1.0
            direction_up = 1 if first_step >= 0 else 0
            direction_sign = 1 if first_step >= 0 else -1
            sessions = 1

            for offset in range(2, int(horizon) + 1):
                pos = idx + offset
                if pos >= len(closes):
                    break
                prev_close = closes[pos - 1]
                curr_close = closes[pos]
                if not np.isfinite(prev_close) or prev_close <= 0 or not np.isfinite(curr_close):
                    break
                step = (curr_close / prev_close) - 1.0
                sign = 1 if step >= 0 else -1
                if sign == direction_sign:
                    sessions += 1
                else:
                    break

            features.append(self._build_feature_vector(row))
            labels_dir.append(direction_up)
            labels_sessions.append(float(sessions))

        if not features:
            return np.empty((0, len(FEATURE_NAMES))), np.empty((0,)), np.empty((0,))

        X = np.vstack(features).astype(np.float64)
        y_dir = np.asarray(labels_dir, dtype=np.int64)
        y_sessions = np.asarray(labels_sessions, dtype=np.float64)
        return X, y_dir, y_sessions
