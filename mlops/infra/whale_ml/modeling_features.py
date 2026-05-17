import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from modeling_shared import FEATURE_NAMES, _parse_event_time, _safe_float, _sanitize_symbol


DAILY_TECHNICAL_FEATURE_NAMES = {
    "daily_return_1d",
    "daily_return_3d",
    "daily_return_5d",
    "daily_volatility_5d",
    "daily_range_pct",
    "daily_close_position",
    "daily_volume_zscore_5d",
    "daily_change_percent",
}


class FeatureDatasetMixin:
    @staticmethod
    def _event_order_by_clause(strategy: Optional[str]) -> Tuple[str, str]:
        token = str(strategy or "latest").strip().lower().replace("-", "_")
        clauses = {
            "latest": "event_time DESC",
            "cp_prob": "cp_prob DESC, event_time DESC",
            "whale_score": "whale_score DESC, event_time DESC",
            "innovation_abs": "abs(innovation_zscore) DESC, event_time DESC",
            "strongest": "(cp_prob + abs(innovation_zscore) + greatest(whale_score, 0)) DESC, event_time DESC",
        }
        if token not in clauses:
            token = "latest"
        return clauses[token], token

    def _market_flags(self, symbol: str) -> Tuple[float, float]:
        sym = str(symbol or "").upper()
        if sym in self.market_sets["vn"]:
            return 1.0, 0.0
        if sym in self.market_sets["world"]:
            return 0.0, 1.0
        return 0.0, 0.0

    @staticmethod
    def _safe_ratio(numerator: float, denominator: float, default: float = 0.0) -> float:
        if not np.isfinite(numerator) or not np.isfinite(denominator) or abs(denominator) <= 1e-12:
            return default
        return float(numerator / denominator)

    @staticmethod
    def _daily_technical_features(
        closes: np.ndarray,
        highs: np.ndarray,
        lows: np.ndarray,
        volumes: np.ndarray,
        change_percent: np.ndarray,
        idx: int,
    ) -> Dict[str, float]:
        def close_return(period: int) -> float:
            prev_idx = int(idx) - int(period)
            if prev_idx < 0:
                return 0.0
            prev_close = float(closes[prev_idx])
            curr_close = float(closes[idx])
            if not np.isfinite(prev_close) or prev_close <= 0 or not np.isfinite(curr_close):
                return 0.0
            return float((curr_close / prev_close) - 1.0)

        ret_values: List[float] = []
        start_idx = max(1, int(idx) - 4)
        for pos in range(start_idx, int(idx) + 1):
            prev_close = float(closes[pos - 1])
            curr_close = float(closes[pos])
            if np.isfinite(prev_close) and prev_close > 0 and np.isfinite(curr_close):
                ret_values.append(float((curr_close / prev_close) - 1.0))

        curr_close = float(closes[idx])
        curr_high = float(highs[idx]) if len(highs) > idx else curr_close
        curr_low = float(lows[idx]) if len(lows) > idx else curr_close
        curr_volume = float(volumes[idx]) if len(volumes) > idx else 0.0
        curr_change = float(change_percent[idx]) if len(change_percent) > idx else 0.0

        price_range = curr_high - curr_low
        range_pct = FeatureDatasetMixin._safe_ratio(price_range, curr_close)
        close_position = FeatureDatasetMixin._safe_ratio(curr_close - curr_low, price_range, default=0.5)
        close_position = min(max(close_position, 0.0), 1.0)

        vol_start = max(0, int(idx) - 5)
        prev_volumes = volumes[vol_start:int(idx)]
        prev_volumes = prev_volumes[np.isfinite(prev_volumes)] if len(prev_volumes) else np.asarray([])
        if len(prev_volumes) >= 2:
            vol_mean = float(np.mean(prev_volumes))
            vol_std = float(np.std(prev_volumes))
            volume_z = FeatureDatasetMixin._safe_ratio(curr_volume - vol_mean, vol_std)
        else:
            volume_z = 0.0

        return {
            "daily_return_1d": close_return(1),
            "daily_return_3d": close_return(3),
            "daily_return_5d": close_return(5),
            "daily_volatility_5d": float(np.std(ret_values)) if len(ret_values) >= 2 else 0.0,
            "daily_range_pct": range_pct,
            "daily_close_position": close_position,
            "daily_volume_zscore_5d": volume_z,
            "daily_change_percent": curr_change / 100.0,
        }

    def _build_feature_vector(
        self,
        event: Dict[str, Any],
        feature_names: Optional[List[str]] = None,
    ) -> np.ndarray:
        names = list(feature_names or FEATURE_NAMES)
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
            "daily_return_1d": _safe_float(event.get("daily_return_1d")),
            "daily_return_3d": _safe_float(event.get("daily_return_3d")),
            "daily_return_5d": _safe_float(event.get("daily_return_5d")),
            "daily_volatility_5d": _safe_float(event.get("daily_volatility_5d")),
            "daily_range_pct": _safe_float(event.get("daily_range_pct")),
            "daily_close_position": _safe_float(event.get("daily_close_position")),
            "daily_volume_zscore_5d": _safe_float(event.get("daily_volume_zscore_5d")),
            "daily_change_percent": _safe_float(event.get("daily_change_percent")),
        }

        return np.asarray([feature_map.get(name, 0.0) for name in names], dtype=np.float64)

    def _load_training_events(
        self,
        lookback_days: int,
        max_rows: int,
        symbol: Optional[str] = None,
        max_events_per_symbol_day: Optional[int] = None,
        min_cp_prob: Optional[float] = None,
        min_whale_score: Optional[float] = None,
        min_innovation_abs: Optional[float] = None,
        event_selection_strategy: Optional[str] = None,
    ) -> pd.DataFrame:
        per_day_cap = int(max_events_per_symbol_day or getattr(self, "train_max_events_per_symbol_day", 40))
        per_day_cap = max(1, per_day_cap)
        event_order_by, _ = self._event_order_by_clause(
            event_selection_strategy or getattr(self, "train_event_selection_strategy", "latest")
        )

        filters = [f"event_time >= now() - INTERVAL {int(lookback_days)} DAY"]
        cp_floor = max(0.0, _safe_float(min_cp_prob, default=0.0))
        whale_floor = max(0.0, _safe_float(min_whale_score, default=0.0))
        innovation_floor = max(0.0, _safe_float(min_innovation_abs, default=0.0))
        if cp_floor > 0.0:
            filters.append(f"cp_prob >= {cp_floor:.12g}")
        if whale_floor > 0.0:
            filters.append(f"whale_score >= {whale_floor:.12g}")
        if innovation_floor > 0.0:
            filters.append(f"abs(innovation_zscore) >= {innovation_floor:.12g}")
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
        FROM
        (
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
                evidence,
                row_number() OVER (
                    PARTITION BY symbol, toDate(event_time)
                    ORDER BY {event_order_by}
                ) AS rn
            FROM stock_changepoint_events
            WHERE {where_clause}
        )
        WHERE rn <= {per_day_cap}
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
        days = int(lookback_days) + int(horizon) + 15
        filters = [f"trade_date >= toDate(now()) - INTERVAL {days} DAY"]
        if symbol:
            sym = _sanitize_symbol(symbol)
            filters.append(f"upper(symbol) = '{sym}'")

        where_clause = " AND ".join(filters)
        sql = f"""
        SELECT
            symbol,
            trade_date,
            close,
            high,
            low,
            volume,
            change_percent
        FROM v_ohlcv_daily
        WHERE {where_clause}
        ORDER BY symbol, trade_date
        """
        df = self._query_df(sql)
        if df.empty:
            return df

        df["symbol"] = df["symbol"].astype(str).str.upper()
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
        for column in ("close", "high", "low", "volume", "change_percent"):
            if column not in df.columns:
                df[column] = 0.0
            df[column] = pd.to_numeric(df[column], errors="coerce")
        df = df.dropna(subset=["symbol", "trade_date", "close"])
        df["high"] = df["high"].fillna(df["close"])
        df["low"] = df["low"].fillna(df["close"])
        df["volume"] = df["volume"].fillna(0.0)
        df["change_percent"] = df["change_percent"].fillna(0.0)
        return df

    @staticmethod
    def _needs_daily_technical_features(feature_names: List[str]) -> bool:
        return any(name in DAILY_TECHNICAL_FEATURE_NAMES for name in feature_names)

    def _enrich_event_with_daily_technical_features(
        self,
        event: Dict[str, Any],
        feature_names: List[str],
    ) -> Dict[str, Any]:
        enriched = dict(event)
        if not self._needs_daily_technical_features(feature_names):
            return enriched
        if all(name in enriched for name in DAILY_TECHNICAL_FEATURE_NAMES):
            return enriched

        try:
            sym = _sanitize_symbol(str(enriched.get("symbol") or ""))
            event_date = _parse_event_time(enriched.get("event_time")).date().isoformat()
            sql = f"""
            SELECT
                symbol,
                trade_date,
                close,
                high,
                low,
                volume,
                change_percent
            FROM v_ohlcv_daily
            WHERE upper(symbol) = '{sym}'
              AND trade_date <= toDate('{event_date}')
              AND trade_date >= toDate('{event_date}') - INTERVAL 20 DAY
            ORDER BY trade_date
            """
            df = self._query_df(sql)
            if df.empty:
                return enriched

            df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
            for column in ("close", "high", "low", "volume", "change_percent"):
                if column not in df.columns:
                    df[column] = 0.0
                df[column] = pd.to_numeric(df[column], errors="coerce")
            df = df.dropna(subset=["trade_date", "close"]).sort_values("trade_date")
            if df.empty:
                return enriched

            closes = df["close"].to_numpy(dtype=np.float64)
            highs = df["high"].fillna(df["close"]).to_numpy(dtype=np.float64)
            lows = df["low"].fillna(df["close"]).to_numpy(dtype=np.float64)
            volumes = df["volume"].fillna(0.0).to_numpy(dtype=np.float64)
            change_percent = df["change_percent"].fillna(0.0).to_numpy(dtype=np.float64)
            idx = len(df) - 1
            enriched.update(
                self._daily_technical_features(
                    closes=closes,
                    highs=highs,
                    lows=lows,
                    volumes=volumes,
                    change_percent=change_percent,
                    idx=idx,
                )
            )
            return enriched
        except Exception:
            return enriched

    def _build_training_dataset(
        self,
        event_df: pd.DataFrame,
        daily_df: pd.DataFrame,
        horizon: int,
        direction_return_threshold: Optional[float] = None,
        direction_neutral_policy: Optional[str] = None,
        direction_label_target: Optional[str] = None,
        include_outcomes: bool = False,
    ) -> Tuple[np.ndarray, ...]:
        def empty_result() -> Tuple[np.ndarray, ...]:
            base = (
                np.empty((0, len(FEATURE_NAMES))),
                np.empty((0,)),
                np.empty((0,)),
                np.empty((0,), dtype=object),
                np.empty((0,), dtype=object),
            )
            if not include_outcomes:
                return base
            return (
                *base,
                {
                    "next_close_return": np.empty((0,), dtype=np.float64),
                    "horizon_close_return": np.empty((0,), dtype=np.float64),
                    "future_max_return": np.empty((0,), dtype=np.float64),
                    "future_min_return": np.empty((0,), dtype=np.float64),
                },
            )

        if event_df.empty or daily_df.empty:
            return empty_result()

        return_threshold = max(
            0.0,
            _safe_float(
                direction_return_threshold,
                default=_safe_float(getattr(self, "direction_return_threshold", 0.0)),
            ),
        )
        neutral_policy = str(
            direction_neutral_policy or getattr(self, "direction_neutral_policy", "drop")
        ).strip().lower()
        if neutral_policy not in {"drop", "sign"}:
            neutral_policy = "drop"
        label_target = str(
            direction_label_target or getattr(self, "direction_label_target", "next_close")
        ).strip().lower()
        if label_target not in {"next_close", "horizon_extreme"}:
            label_target = "next_close"

        daily_map: Dict[str, Dict[str, Any]] = {}
        for symbol, group in daily_df.groupby("symbol"):
            g = group.sort_values("trade_date")
            closes = g["close"].to_numpy(dtype=np.float64)
            highs = g["high"].to_numpy(dtype=np.float64)
            lows = g["low"].to_numpy(dtype=np.float64)
            volumes = g["volume"].to_numpy(dtype=np.float64)
            change_percent = g["change_percent"].to_numpy(dtype=np.float64)
            dates = g["trade_date"].to_list()
            daily_map[str(symbol)] = {
                "close": closes,
                "high": highs,
                "low": lows,
                "volume": volumes,
                "change_percent": change_percent,
                "date_to_idx": {d: i for i, d in enumerate(dates)},
            }

        features: List[np.ndarray] = []
        labels_dir: List[int] = []
        labels_sessions: List[float] = []
        sample_groups: List[str] = []
        sample_dates: List[Any] = []
        next_close_returns: List[float] = []
        horizon_close_returns: List[float] = []
        future_max_returns: List[float] = []
        future_min_returns: List[float] = []

        for row in event_df.to_dict("records"):
            symbol = str(row.get("symbol") or "").upper()
            state = daily_map.get(symbol)
            if not state:
                continue

            event_date = row.get("event_date")
            idx = state["date_to_idx"].get(event_date)
            closes = state["close"]
            highs = state["high"]
            lows = state["low"]
            if idx is None or (idx + 1) >= len(closes):
                continue

            base_close = closes[idx]
            next_close = closes[idx + 1]
            if not np.isfinite(base_close) or base_close <= 0 or not np.isfinite(next_close):
                continue

            max_pos = min(idx + int(horizon), len(closes) - 1)
            future_closes = closes[idx + 1 : max_pos + 1]
            if len(future_closes) == 0:
                continue
            if not np.all(np.isfinite(future_closes)):
                continue

            first_step = (next_close / base_close) - 1.0
            horizon_close_return = (future_closes[-1] / base_close) - 1.0
            future_close_returns = (future_closes / base_close) - 1.0
            future_max_return = float(np.max(future_close_returns))
            future_min_return = float(np.min(future_close_returns))
            if not all(
                np.isfinite(value)
                for value in (first_step, horizon_close_return, future_max_return, future_min_return)
            ):
                continue

            if label_target == "horizon_extreme":
                up_hit = future_max_return >= return_threshold if return_threshold > 0.0 else True
                down_hit = future_min_return <= -return_threshold if return_threshold > 0.0 else True
                if return_threshold > 0.0 and not up_hit and not down_hit:
                    if neutral_policy == "sign":
                        direction_up = 1 if horizon_close_return >= 0 else 0
                    else:
                        continue
                elif up_hit and not down_hit:
                    direction_up = 1
                elif down_hit and not up_hit:
                    direction_up = 0
                else:
                    direction_up = 1 if future_max_return >= abs(future_min_return) else 0

                direction_sign = 1 if direction_up == 1 else -1
                sessions = (
                    int(np.argmax(future_close_returns) + 1)
                    if direction_up == 1
                    else int(np.argmin(future_close_returns) + 1)
                )
            else:
                if return_threshold > 0.0:
                    if first_step >= return_threshold:
                        direction_up = 1
                        direction_sign = 1
                    elif first_step <= -return_threshold:
                        direction_up = 0
                        direction_sign = -1
                    elif neutral_policy == "sign":
                        direction_up = 1 if first_step >= 0 else 0
                        direction_sign = 1 if first_step >= 0 else -1
                    else:
                        continue
                else:
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

            enriched_row = dict(row)
            enriched_row.update(
                self._daily_technical_features(
                    closes=closes,
                    highs=state["high"],
                    lows=state["low"],
                    volumes=state["volume"],
                    change_percent=state["change_percent"],
                    idx=int(idx),
                )
            )

            features.append(self._build_feature_vector(enriched_row))
            labels_dir.append(direction_up)
            labels_sessions.append(float(sessions))
            sample_groups.append(f"{symbol}|{event_date.isoformat()}")
            sample_dates.append(event_date)
            next_close_returns.append(float(first_step))
            horizon_close_returns.append(float(horizon_close_return))
            future_max_returns.append(float(future_max_return))
            future_min_returns.append(float(future_min_return))

        if not features:
            return empty_result()

        X = np.vstack(features).astype(np.float64)
        y_dir = np.asarray(labels_dir, dtype=np.int64)
        y_sessions = np.asarray(labels_sessions, dtype=np.float64)
        groups = np.asarray(sample_groups, dtype=object)
        dates = np.asarray(sample_dates, dtype=object)
        if not include_outcomes:
            return X, y_dir, y_sessions, groups, dates
        outcomes = {
            "next_close_return": np.asarray(next_close_returns, dtype=np.float64),
            "horizon_close_return": np.asarray(horizon_close_returns, dtype=np.float64),
            "future_max_return": np.asarray(future_max_returns, dtype=np.float64),
            "future_min_return": np.asarray(future_min_returns, dtype=np.float64),
        }
        return X, y_dir, y_sessions, groups, dates, outcomes
