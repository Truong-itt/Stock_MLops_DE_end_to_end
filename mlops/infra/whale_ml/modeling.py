import json
import logging
import math
import os
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import clickhouse_connect
import joblib
import mlflow
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.ensemble import (
    ExtraTreesRegressor,
    GradientBoostingClassifier,
    GradientBoostingRegressor,
    RandomForestClassifier,
    RandomForestRegressor,
)
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, mean_absolute_error, mean_squared_error, roc_auc_score
from sklearn.model_selection import train_test_split


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
]


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


def _parse_event_time(raw_value: Any) -> datetime:
    if isinstance(raw_value, datetime):
        return raw_value if raw_value.tzinfo else raw_value.replace(tzinfo=timezone.utc)
    if raw_value is None:
        return datetime.now(tz=timezone.utc)
    text = str(raw_value).strip()
    if not text:
        return datetime.now(tz=timezone.utc)
    if text.isdigit():
        value = int(text)
        if value < 10_000_000_000:
            value *= 1000
        return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


class WhaleBundlePyfuncModel(mlflow.pyfunc.PythonModel):
    """
    PyFunc wrapper to persist the full whale model bundle in MLflow Registry.

    This model is used for packaging/registry, while online serving still uses
    the native classifier/regressor bundle for richer output.
    """

    def load_context(self, context):
        bundle_path = context.artifacts["bundle"]
        self.bundle = joblib.load(bundle_path)

    def predict(self, context, model_input):
        df = pd.DataFrame(model_input)
        for name in FEATURE_NAMES:
            if name not in df.columns:
                df[name] = 0.0

        matrix = (
            df[FEATURE_NAMES]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0.0)
            .to_numpy(dtype=np.float64)
        )

        classifier = self.bundle["classifier"]
        regressor = self.bundle["regressor"]
        meta = self.bundle.get("meta", {})
        horizon = int(meta.get("horizon_sessions", 5))

        class_values = list(classifier.classes_)
        up_index = class_values.index(1) if 1 in class_values else (1 if len(class_values) > 1 else 0)
        probs = classifier.predict_proba(matrix)
        prob_up = probs[:, up_index] if probs.shape[1] > up_index else probs[:, -1]
        prob_down = 1.0 - prob_up
        expected_sessions = np.clip(regressor.predict(matrix), 1.0, float(horizon))
        direction = np.where(prob_up >= 0.5, "up", "down")

        return pd.DataFrame(
            {
                "direction": direction,
                "prob_up": prob_up.astype(float),
                "prob_down": prob_down.astype(float),
                "expected_sessions": expected_sessions.astype(float),
            }
        )


class WhaleMoveForecaster:
    """
    Train + inference model for post-anomaly direction and expected number of sessions.

    Labels are created from daily closes after each BOCPD event:
    - direction label: next session up/down
    - sessions label: consecutive sessions in that same direction (clipped by horizon)
    """

    def __init__(self):
        self.clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        self.clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
        self.clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "truongittstock")
        self.clickhouse_db = os.getenv("CLICKHOUSE_DB", "stock_warehouse")
        self.model_artifact_path = Path(
            os.getenv("MODEL_ARTIFACT_PATH", "/app/artifacts/whale_move_model.joblib")
        )
        self.train_lookback_days = int(os.getenv("TRAIN_LOOKBACK_DAYS", "240"))
        self.train_max_rows = int(os.getenv("TRAIN_MAX_ROWS", "120000"))
        self.max_forecast_horizon = int(os.getenv("MAX_FORECAST_HORIZON", "5"))
        self.registry_path = Path(
            os.getenv("SYMBOL_REGISTRY_PATH", "/app/config/symbol_registry.json")
        )

        self.mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        self.mlflow_experiment = os.getenv("WHALE_ML_EXPERIMENT", "whale_ml_training")
        self.registered_model_name = os.getenv("WHALE_ML_MODEL_NAME", "whale_move_forecaster")
        self.model_alias = os.getenv("WHALE_ML_MODEL_ALIAS", "production")
        self.prefer_registry = _is_truthy(os.getenv("PREFER_MLFLOW_REGISTRY", "1"), default=True)
        self.mlflow_registry_required = _is_truthy(
            os.getenv("MLFLOW_REGISTRY_REQUIRED", "1"), default=True
        )

        self.client = None
        self.bundle: Optional[Dict[str, Any]] = None
        self.lock = threading.RLock()
        self.train_lock = threading.Lock()
        self.market_sets = self._load_market_sets()
        self.model_source = "none"

    def _configure_mlflow(self):
        if not self.mlflow_tracking_uri:
            raise RuntimeError("MLFLOW_TRACKING_URI is empty")
        mlflow.set_tracking_uri(self.mlflow_tracking_uri)
        if self.mlflow_experiment:
            mlflow.set_experiment(self.mlflow_experiment)

    def _load_market_sets(self) -> Dict[str, set]:
        default = {"vn": set(), "world": set()}
        if not self.registry_path.exists():
            return default
        try:
            payload = json.loads(self.registry_path.read_text(encoding="utf-8"))
            markets = payload.get("markets", {})
            return {
                "vn": set(markets.get("vn", {}).get("symbols", []) or []),
                "world": set(markets.get("world", {}).get("symbols", []) or []),
            }
        except Exception as exc:
            logger.warning("Cannot load symbol registry at %s: %s", self.registry_path, exc)
            return default

    def connect(self):
        if self.client is not None:
            return
        self.client = clickhouse_connect.get_client(
            host=self.clickhouse_host,
            port=self.clickhouse_port,
            username=self.clickhouse_user,
            password=self.clickhouse_password,
            database=self.clickhouse_db,
        )
        logger.info("Connected ClickHouse for whale ML at %s:%s", self.clickhouse_host, self.clickhouse_port)

    def close(self):
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
        self.client = None

    def _query_df(self, sql: str) -> pd.DataFrame:
        if self.client is None:
            self.connect()
        result = self.client.query(sql)
        return pd.DataFrame(result.result_rows, columns=result.column_names)

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

    def _load_training_events(self, lookback_days: int, max_rows: int) -> pd.DataFrame:
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
        WHERE event_time >= now() - INTERVAL {int(lookback_days)} DAY
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

    def _load_daily_closes(self, lookback_days: int, horizon: int) -> pd.DataFrame:
        days = int(lookback_days) + int(horizon) + 10
        sql = f"""
        SELECT
            symbol,
            trade_date,
            close
        FROM v_ohlcv_daily
        WHERE trade_date >= toDate(now()) - INTERVAL {days} DAY
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

    def _load_bundle_from_local(self) -> bool:
        if not self.model_artifact_path.exists():
            return False
        try:
            bundle = joblib.load(self.model_artifact_path)
        except Exception as exc:
            logger.warning("Cannot load existing local model bundle: %s", exc)
            return False

        meta = dict(bundle.get("meta", {}))
        meta.setdefault("model_source", "local")
        meta.setdefault("model_name", self.registered_model_name)
        if "model_version" not in meta:
            meta["model_version"] = meta.get("version")
        bundle["meta"] = meta

        with self.lock:
            self.bundle = bundle
            self.model_source = "local"
        return True

    def _load_bundle_from_registry(self) -> bool:
        try:
            self._configure_mlflow()
            client = MlflowClient()
            version_info = client.get_model_version_by_alias(
                self.registered_model_name,
                self.model_alias,
            )
            model_version = str(version_info.version)
            model_uri = f"models:/{self.registered_model_name}@{self.model_alias}"

            model_dir = Path(mlflow.artifacts.download_artifacts(artifact_uri=model_uri))
            candidates = sorted(model_dir.rglob("*.joblib"))
            if not candidates:
                raise RuntimeError(f"No .joblib artifact found under {model_dir}")

            bundle = joblib.load(candidates[0])
            meta = dict(bundle.get("meta", {}))
            meta.update(
                {
                    "model_source": "registry",
                    "model_name": self.registered_model_name,
                    "model_alias": self.model_alias,
                    "model_version": model_version,
                    "model_uri": f"models:/{self.registered_model_name}/{model_version}",
                }
            )
            if version_info.run_id:
                meta["mlflow_run_id"] = version_info.run_id
            bundle["meta"] = meta

            with self.lock:
                self.bundle = bundle
                self.model_source = "registry"

            # Keep a local fallback snapshot synced with currently served registry model.
            self.model_artifact_path.parent.mkdir(parents=True, exist_ok=True)
            joblib.dump(bundle, self.model_artifact_path)
            return True
        except Exception as exc:
            logger.warning(
                "Cannot load model from MLflow Registry (%s@%s): %s",
                self.registered_model_name,
                self.model_alias,
                exc,
            )
            return False

    def load_bundle(self) -> bool:
        if self.prefer_registry:
            if self._load_bundle_from_registry():
                return True
            return self._load_bundle_from_local()
        if self._load_bundle_from_local():
            return True
        return self._load_bundle_from_registry()

    def save_bundle(self) -> None:
        with self.lock:
            if self.bundle is None:
                return
            self.model_artifact_path.parent.mkdir(parents=True, exist_ok=True)
            joblib.dump(self.bundle, self.model_artifact_path)

    def is_ready(self) -> bool:
        with self.lock:
            return self.bundle is not None

    def model_info(self) -> Dict[str, Any]:
        with self.lock:
            if self.bundle is None:
                return {"ready": False}
            meta = dict(self.bundle.get("meta", {}))
            selected_models = dict(meta.get("selected_models", {}))
            selected_models.setdefault("direction", self.bundle.get("classifier_name"))
            selected_models.setdefault("sessions", self.bundle.get("regressor_name"))
            if selected_models.get("direction") or selected_models.get("sessions"):
                meta["selected_models"] = selected_models
            meta["ready"] = True
            meta["feature_names"] = list(self.bundle.get("feature_names", FEATURE_NAMES))
            meta["model_source"] = meta.get("model_source", self.model_source or "local")
            meta["model_name"] = meta.get("model_name", self.registered_model_name)
            if "model_version" not in meta:
                meta["model_version"] = meta.get("version")
            return meta

    @staticmethod
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

    @staticmethod
    def _metric_token(text: str) -> str:
        raw = "".join(ch if ch.isalnum() else "_" for ch in str(text).lower()).strip("_")
        return raw or "metric"

    def _build_classifier_candidates(self) -> Dict[str, Any]:
        return {
            "logistic_regression": LogisticRegression(
                max_iter=500,
                class_weight="balanced",
                random_state=42,
            ),
            "random_forest_classifier": RandomForestClassifier(
                n_estimators=240,
                max_depth=12,
                min_samples_leaf=6,
                class_weight="balanced_subsample",
                random_state=42,
                n_jobs=-1,
            ),
            "gradient_boosting_classifier": GradientBoostingClassifier(
                n_estimators=220,
                learning_rate=0.05,
                max_depth=3,
                random_state=42,
            ),
        }

    def _build_regressor_candidates(self) -> Dict[str, Any]:
        return {
            "random_forest_regressor": RandomForestRegressor(
                n_estimators=180,
                max_depth=10,
                min_samples_leaf=8,
                random_state=42,
                n_jobs=-1,
            ),
            "extra_trees_regressor": ExtraTreesRegressor(
                n_estimators=240,
                max_depth=12,
                min_samples_leaf=5,
                random_state=42,
                n_jobs=-1,
            ),
            "gradient_boosting_regressor": GradientBoostingRegressor(
                n_estimators=220,
                learning_rate=0.05,
                max_depth=3,
                random_state=42,
            ),
        }

    def _evaluate_classifier_candidate(
        self,
        name: str,
        model: Any,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
    ) -> Dict[str, Any]:
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        accuracy = self._safe_metric_value(accuracy_score(y_test, y_pred))
        f1 = self._safe_metric_value(f1_score(y_test, y_pred, zero_division=0))
        roc_auc: Optional[float] = None

        if hasattr(model, "predict_proba"):
            try:
                y_prob = model.predict_proba(X_test)
                classes = np.asarray(getattr(model, "classes_", []))
                up_positions = np.where(classes == 1)[0] if classes.size else np.asarray([])
                up_index = int(up_positions[0]) if len(up_positions) else (1 if y_prob.shape[1] > 1 else 0)
                roc_auc = self._safe_metric_value(roc_auc_score(y_test, y_prob[:, up_index]))
            except Exception:
                roc_auc = None

        score = roc_auc if roc_auc is not None else accuracy
        if score is None:
            raise RuntimeError(f"Classifier candidate '{name}' produced invalid evaluation score.")

        return {
            "name": name,
            "selection_metric": "roc_auc" if roc_auc is not None else "accuracy",
            "score": float(score),
            "metrics": {
                "accuracy": accuracy,
                "roc_auc": roc_auc,
                "f1_direction": f1,
            },
            "model": model,
        }

    def _evaluate_regressor_candidate(
        self,
        name: str,
        model: Any,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        horizon: int,
    ) -> Dict[str, Any]:
        model.fit(X_train, y_train)
        y_pred = np.clip(model.predict(X_test), 1.0, float(horizon))

        mae = self._safe_metric_value(mean_absolute_error(y_test, y_pred))
        rmse = self._safe_metric_value(np.sqrt(mean_squared_error(y_test, y_pred)))
        score = None if mae is None else float(-mae)
        if score is None:
            raise RuntimeError(f"Regressor candidate '{name}' produced invalid evaluation score.")

        return {
            "name": name,
            "selection_metric": "neg_mae",
            "score": score,
            "metrics": {
                "mae_sessions": mae,
                "rmse_sessions": rmse,
            },
            "model": model,
        }

    @staticmethod
    def _classifier_rank_key(candidate: Dict[str, Any]) -> Tuple[float, float]:
        metrics = candidate.get("metrics", {})
        score = candidate.get("score")
        accuracy = metrics.get("accuracy")
        return (
            float(score) if score is not None else float("-inf"),
            float(accuracy) if accuracy is not None else float("-inf"),
        )

    @staticmethod
    def _regressor_rank_key(candidate: Dict[str, Any]) -> Tuple[float, float]:
        metrics = candidate.get("metrics", {})
        score = candidate.get("score")
        mae = metrics.get("mae_sessions")
        return (
            float(score) if score is not None else float("-inf"),
            float(-mae) if mae is not None else float("-inf"),
        )

    @staticmethod
    def _candidate_to_meta(candidate: Dict[str, Any]) -> Dict[str, Any]:
        metrics = {
            key: (None if value is None else float(value))
            for key, value in dict(candidate.get("metrics", {})).items()
        }
        return {
            "name": str(candidate.get("name")),
            "selection_metric": str(candidate.get("selection_metric")),
            "score": float(candidate.get("score")),
            "metrics": metrics,
        }

    def _flatten_candidate_metrics_for_mlflow(
        self,
        prefix: str,
        candidates: List[Dict[str, Any]],
    ) -> Dict[str, float]:
        payload: Dict[str, float] = {}
        for candidate in candidates:
            candidate_name = self._metric_token(str(candidate.get("name")))
            candidate_score = candidate.get("score")
            if candidate_score is not None:
                payload[f"{prefix}_{candidate_name}_score"] = float(candidate_score)
            metrics = dict(candidate.get("metrics", {}))
            for metric_name, metric_value in metrics.items():
                if metric_value is None:
                    continue
                payload[
                    f"{prefix}_{candidate_name}_{self._metric_token(str(metric_name))}"
                ] = float(metric_value)
        return payload

    def _wait_until_model_version_ready(
        self,
        client: MlflowClient,
        model_version: str,
        timeout_seconds: int = 180,
    ):
        deadline = time.time() + float(timeout_seconds)
        while time.time() < deadline:
            info = client.get_model_version(self.registered_model_name, model_version)
            status = str(info.status or "").upper()
            if status == "READY":
                return info
            if status in {"FAILED_REGISTRATION", "FAILED"}:
                raise RuntimeError(
                    f"Model version {self.registered_model_name}/{model_version} failed with status={status}"
                )
            time.sleep(2)
        raise TimeoutError(
            f"Timed out waiting MLflow model version READY: {self.registered_model_name}/{model_version}"
        )

    def _log_train_to_mlflow(
        self,
        bundle_path: Path,
        train_params: Dict[str, Any],
        metrics: Dict[str, Optional[float]],
        base_meta: Dict[str, Any],
    ) -> Dict[str, Any]:
        self._configure_mlflow()

        active_run = mlflow.active_run()
        if active_run is not None:
            logger.warning(
                "Closing stale active MLflow run before training: run_id=%s",
                active_run.info.run_id,
            )
            mlflow.end_run()

        run_name = f"whale_train_{base_meta.get('version', 'unknown')}"
        with mlflow.start_run(run_name=run_name) as run:
            run_id = run.info.run_id

            mlflow.log_params(
                {
                    "lookback_days": int(train_params["lookback_days"]),
                    "max_rows": int(train_params["max_rows"]),
                    "horizon_sessions": int(train_params["horizon_sessions"]),
                    "features_count": len(FEATURE_NAMES),
                }
            )
            mlflow.log_metric("samples", float(base_meta["samples"]))
            mlflow.log_metric("up_ratio", float(base_meta["up_ratio"]))
            for key, value in metrics.items():
                if value is None:
                    continue
                mlflow.log_metric(key, float(value))

            mlflow.log_dict(base_meta, "train_meta_base.json")

            mlflow.pyfunc.log_model(
                artifact_path="whale_bundle_model",
                python_model=WhaleBundlePyfuncModel(),
                artifacts={"bundle": str(bundle_path)},
                pip_requirements=[
                    "mlflow==2.17.2",
                    "joblib",
                    "numpy",
                    "pandas",
                    "scikit-learn",
                ],
            )

            run_model_uri = f"runs:/{run_id}/whale_bundle_model"
            registered = mlflow.register_model(run_model_uri, self.registered_model_name)
            client = MlflowClient()
            registered_info = self._wait_until_model_version_ready(client, str(registered.version))
            model_version = str(registered_info.version)

            client.set_registered_model_alias(
                self.registered_model_name,
                self.model_alias,
                model_version,
            )
            client.set_model_version_tag(
                self.registered_model_name,
                model_version,
                "pipeline",
                "whale_ml_retrain_pipeline",
            )

            registry_uri = f"models:/{self.registered_model_name}/{model_version}"
            alias_uri = f"models:/{self.registered_model_name}@{self.model_alias}"
            mlflow.set_tags(
                {
                    "registered_model_name": self.registered_model_name,
                    "registered_model_version": model_version,
                    "registered_model_alias": self.model_alias,
                    "registry_uri": registry_uri,
                }
            )

            return {
                "mlflow_run_id": run_id,
                "model_name": self.registered_model_name,
                "model_alias": self.model_alias,
                "model_version": model_version,
                "model_uri": registry_uri,
                "model_alias_uri": alias_uri,
                "model_source": "registry",
            }

    def train(
        self,
        lookback_days: Optional[int] = None,
        max_rows: Optional[int] = None,
        horizon: Optional[int] = None,
    ) -> Dict[str, Any]:
        with self.train_lock:
            lookback = int(lookback_days or self.train_lookback_days)
            limit = int(max_rows or self.train_max_rows)
            max_h = int(horizon or self.max_forecast_horizon)
            max_h = max(2, max_h)

            events = self._load_training_events(lookback, limit)
            daily = self._load_daily_closes(lookback, max_h)
            X, y_dir, y_sessions = self._build_training_dataset(events, daily, max_h)

            if len(X) < 800:
                raise RuntimeError(
                    f"Training data too small ({len(X)} rows). Need at least 800 labeled samples."
                )

            classes = np.unique(y_dir)
            if len(classes) < 2:
                raise RuntimeError("Training labels have only one class; cannot train direction classifier.")

            stratify = y_dir if len(classes) > 1 else None
            X_train, X_test, y_dir_train, y_dir_test, y_sess_train, y_sess_test = train_test_split(
                X,
                y_dir,
                y_sessions,
                test_size=0.2,
                random_state=42,
                stratify=stratify,
            )

            classifier_candidates = self._build_classifier_candidates()
            regressor_candidates = self._build_regressor_candidates()

            if len(classifier_candidates) < 3:
                raise RuntimeError("At least 3 classifier candidates are required.")
            if len(regressor_candidates) < 3:
                raise RuntimeError("At least 3 regressor candidates are required.")

            classifier_results: List[Dict[str, Any]] = []
            for name, candidate in classifier_candidates.items():
                classifier_results.append(
                    self._evaluate_classifier_candidate(
                        name=name,
                        model=candidate,
                        X_train=X_train,
                        y_train=y_dir_train,
                        X_test=X_test,
                        y_test=y_dir_test,
                    )
                )

            regressor_results: List[Dict[str, Any]] = []
            for name, candidate in regressor_candidates.items():
                regressor_results.append(
                    self._evaluate_regressor_candidate(
                        name=name,
                        model=candidate,
                        X_train=X_train,
                        y_train=y_sess_train,
                        X_test=X_test,
                        y_test=y_sess_test,
                        horizon=max_h,
                    )
                )

            best_classifier = max(classifier_results, key=self._classifier_rank_key)
            best_regressor = max(regressor_results, key=self._regressor_rank_key)

            classifier = best_classifier["model"]
            regressor = best_regressor["model"]

            logger.info(
                "Selected direction model=%s (score=%.6f via %s), sessions model=%s (score=%.6f via %s)",
                best_classifier["name"],
                best_classifier["score"],
                best_classifier["selection_metric"],
                best_regressor["name"],
                best_regressor["score"],
                best_regressor["selection_metric"],
            )

            metrics = {
                "accuracy": best_classifier["metrics"].get("accuracy"),
                "roc_auc": best_classifier["metrics"].get("roc_auc"),
                "f1_direction": best_classifier["metrics"].get("f1_direction"),
                "mae_sessions": best_regressor["metrics"].get("mae_sessions"),
                "rmse_sessions": best_regressor["metrics"].get("rmse_sessions"),
                "classifier_score": best_classifier["score"],
                "regressor_score": best_regressor["score"],
            }

            up_ratio = float(np.mean(y_dir))
            classifier_leaderboard = [
                self._candidate_to_meta(candidate)
                for candidate in sorted(
                    classifier_results,
                    key=self._classifier_rank_key,
                    reverse=True,
                )
            ]
            regressor_leaderboard = [
                self._candidate_to_meta(candidate)
                for candidate in sorted(
                    regressor_results,
                    key=self._regressor_rank_key,
                    reverse=True,
                )
            ]
            meta = {
                "trained_at": datetime.now(tz=timezone.utc).isoformat(),
                "samples": int(len(X)),
                "lookback_days": lookback,
                "max_rows": limit,
                "horizon_sessions": max_h,
                "up_ratio": up_ratio,
                "metrics": metrics,
                "selected_models": {
                    "direction": best_classifier["name"],
                    "sessions": best_regressor["name"],
                },
                "model_candidates": {
                    "direction": classifier_leaderboard,
                    "sessions": regressor_leaderboard,
                },
                "version": datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S"),
            }

            bundle = {
                "classifier": classifier,
                "regressor": regressor,
                "classifier_name": best_classifier["name"],
                "regressor_name": best_regressor["name"],
                "feature_names": list(FEATURE_NAMES),
                "meta": dict(meta),
            }

            train_params = {
                "lookback_days": lookback,
                "max_rows": limit,
                "horizon_sessions": max_h,
            }

            mlflow_meta: Dict[str, Any] = {}
            mlflow_error: Optional[str] = None
            mlflow_metrics = dict(metrics)
            mlflow_metrics.update(
                self._flatten_candidate_metrics_for_mlflow(
                    prefix="classifier",
                    candidates=classifier_results,
                )
            )
            mlflow_metrics.update(
                self._flatten_candidate_metrics_for_mlflow(
                    prefix="regressor",
                    candidates=regressor_results,
                )
            )
            with tempfile.TemporaryDirectory(prefix="whale_ml_train_") as temp_dir:
                temp_bundle_path = Path(temp_dir) / "whale_move_model.joblib"
                joblib.dump(bundle, temp_bundle_path)
                try:
                    mlflow_meta = self._log_train_to_mlflow(
                        temp_bundle_path,
                        train_params=train_params,
                        metrics=mlflow_metrics,
                        base_meta=meta,
                    )
                except Exception as exc:
                    mlflow_error = str(exc)
                    logger.error("MLflow registry registration failed: %s", exc)
                    if self.mlflow_registry_required:
                        raise RuntimeError(f"MLflow registry registration failed: {exc}")

            if mlflow_meta:
                meta.update(mlflow_meta)
            else:
                meta.setdefault("model_source", "local")
                meta.setdefault("model_name", self.registered_model_name)
                meta.setdefault("model_alias", self.model_alias)
                meta.setdefault("model_version", meta.get("version"))
                if mlflow_error:
                    meta["mlflow_error"] = mlflow_error

            bundle["meta"] = meta
            with self.lock:
                self.bundle = bundle
                self.model_source = str(meta.get("model_source", "local"))
            self.save_bundle()

            return meta

    def predict_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        with self.lock:
            if self.bundle is None:
                raise RuntimeError("Model is not trained yet")
            classifier = self.bundle["classifier"]
            regressor = self.bundle["regressor"]
            meta = self.bundle.get("meta", {})
            horizon = int(meta.get("horizon_sessions", self.max_forecast_horizon))

        if not events:
            return {"predictions": [], "model": meta}

        predictions = []
        class_values = list(classifier.classes_)
        up_index = class_values.index(1) if 1 in class_values else (1 if len(class_values) > 1 else 0)

        for event in events:
            event_key = str(event.get("event_key") or "").strip()
            symbol = str(event.get("symbol") or "").upper().strip()
            if not symbol:
                continue
            features = self._build_feature_vector(event)
            row = features.reshape(1, -1)
            probs = classifier.predict_proba(row)[0]
            prob_up = float(probs[up_index]) if len(probs) > up_index else float(probs[-1])
            prob_up = min(max(prob_up, 0.0), 1.0)
            prob_down = 1.0 - prob_up

            expected_sessions = float(regressor.predict(row)[0])
            expected_sessions = min(max(expected_sessions, 1.0), float(horizon))

            direction = "up" if prob_up >= 0.5 else "down"
            confidence = max(prob_up, prob_down)
            text = (
                f"Du kien {expected_sessions:.1f} phien tang (P={prob_up * 100:.1f}%)"
                if direction == "up"
                else f"Du kien {expected_sessions:.1f} phien giam (P={prob_down * 100:.1f}%)"
            )

            predictions.append(
                {
                    "event_key": event_key or f"{symbol}|{event.get('event_time') or ''}",
                    "symbol": symbol,
                    "direction": direction,
                    "prob_up": round(prob_up, 6),
                    "prob_down": round(prob_down, 6),
                    "expected_sessions": round(expected_sessions, 4),
                    "confidence": round(float(confidence), 6),
                    "text": text,
                }
            )

        return {"predictions": predictions, "model": meta}
