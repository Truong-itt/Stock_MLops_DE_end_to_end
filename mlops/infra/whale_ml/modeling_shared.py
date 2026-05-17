import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

import joblib
import mlflow
import numpy as np
import pandas as pd


logger = logging.getLogger("whale_ml.modeling")

FEATURE_NAMES = [
    "cp_prob",
    "whale_score",
    "innovation_zscore",
    "innovation_abs",
    "expected_run_length",
    "map_run_length",
    "predictive_volatility",
    "return_value",
    "return_abs",
    "hazard",
    "evidence",
    "log_price",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "is_vn",
    "is_world",
    "daily_return_1d",
    "daily_return_3d",
    "daily_return_5d",
    "daily_volatility_5d",
    "daily_range_pct",
    "daily_close_position",
    "daily_volume_zscore_5d",
    "daily_change_percent",
]

SYMBOL_TOKEN_PATTERN = re.compile(r"^[A-Z0-9_.-]{1,20}$")


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _is_truthy(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_metric_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        casted = float(value)
    except Exception:
        return None
    if not np.isfinite(casted):
        return None
    return casted


def _metric_token(text: str) -> str:
    raw = "".join(ch if ch.isalnum() else "_" for ch in str(text).lower()).strip("_")
    return raw or "metric"


def _parse_event_time(raw_value: Any) -> datetime:
    if isinstance(raw_value, datetime):
        return raw_value if raw_value.tzinfo else raw_value.replace(tzinfo=timezone.utc)
    if raw_value is None:
        return _utc_now()

    text = str(raw_value).strip()
    if not text:
        return _utc_now()

    if text.isdigit():
        value = int(text)
        if value < 10_000_000_000:
            value *= 1000
        return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return _utc_now()


def _sanitize_symbol(symbol: str) -> str:
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise ValueError("symbol is empty")
    if not SYMBOL_TOKEN_PATTERN.fullmatch(sym):
        raise ValueError(f"Invalid symbol '{symbol}'")
    return sym


class WhaleBundlePyfuncModel(mlflow.pyfunc.PythonModel):
    """
    PyFunc wrapper to persist a full whale model bundle in MLflow Registry.

    Online serving still uses native classifier/regressor bundles directly,
    but this wrapper allows a first-class MLflow model version.
    """

    def load_context(self, context):
        bundle_path = context.artifacts["bundle"]
        self.bundle = joblib.load(bundle_path)

    def predict(self, context, model_input):
        df = pd.DataFrame(model_input)
        feature_names = list(self.bundle.get("feature_names", FEATURE_NAMES))
        for name in feature_names:
            if name not in df.columns:
                df[name] = 0.0

        matrix = (
            df[feature_names]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0.0)
            .to_numpy(dtype=np.float64)
        )

        classifier = self.bundle["classifier"]
        regressor = self.bundle["regressor"]
        meta = self.bundle.get("meta", {})
        horizon = int(meta.get("horizon_sessions", 5))
        direction_threshold = float(meta.get("direction_threshold", 0.5) or 0.5)

        class_values = list(classifier.classes_)
        up_index = class_values.index(1) if 1 in class_values else (1 if len(class_values) > 1 else 0)
        probs = classifier.predict_proba(matrix)
        prob_up = probs[:, up_index] if probs.shape[1] > up_index else probs[:, -1]
        prob_down = 1.0 - prob_up
        expected_sessions = np.clip(regressor.predict(matrix), 1.0, float(horizon))
        direction = np.where(prob_up >= direction_threshold, "up", "down")

        return pd.DataFrame(
            {
                "direction": direction,
                "prob_up": prob_up.astype(float),
                "prob_down": prob_down.astype(float),
                "direction_threshold": np.full(len(prob_up), direction_threshold, dtype=float),
                "expected_sessions": expected_sessions.astype(float),
            }
        )
