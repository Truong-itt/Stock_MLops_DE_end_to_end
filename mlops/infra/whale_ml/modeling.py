import os
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from modeling_features import FeatureDatasetMixin
from modeling_infra import ModelInfraMixin
from modeling_serving import ServingMixin
from modeling_shared import FEATURE_NAMES, WhaleBundlePyfuncModel, _is_truthy
from modeling_training import TrainingMixin


class WhaleMoveForecaster(ModelInfraMixin, FeatureDatasetMixin, TrainingMixin, ServingMixin):
    """
    Train + inference model for post-anomaly direction and expected sessions.

    Core flow:
    - Global model is the default production champion.
    - On anomaly events, symbol-specific challenger training can run in background.
    - Challenger is promoted only when it beats the global champion by configured deltas.
    """

    def __init__(self):
        # clickhouse config
        self.client = None
        self.clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        self.clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
        self.clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "truongittstock")
        self.clickhouse_db = os.getenv("CLICKHOUSE_DB", "stock_warehouse")
        
        # store model 
        self.model_artifact_path = Path(os.getenv("MODEL_ARTIFACT_PATH", "/app/artifacts/whale_move_model.joblib"))
        self.symbol_artifact_dir = Path(os.getenv("SYMBOL_MODEL_ARTIFACT_DIR", "/app/artifacts/symbol_models"))
        self.symbol_index_path = Path(os.getenv("SYMBOL_MODEL_INDEX_PATH", str(self.symbol_artifact_dir / "index.json")))
        
        # training global config 
        self.train_lookback_days = int(os.getenv("TRAIN_LOOKBACK_DAYS", "240"))
        self.train_max_rows = int(os.getenv("TRAIN_MAX_ROWS", "120000"))
        self.train_max_events_per_symbol_day = int(os.getenv("TRAIN_MAX_EVENTS_PER_SYMBOL_DAY", "40"))
        self.train_event_selection_strategy = os.getenv("TRAIN_EVENT_SELECTION_STRATEGY", "latest")
        self.max_forecast_horizon = int(os.getenv("MAX_FORECAST_HORIZON", "5"))
        self.global_min_train_samples = int(os.getenv("GLOBAL_MIN_TRAIN_SAMPLES", "800"))
        self.walk_forward_enabled = _is_truthy(os.getenv("WALK_FORWARD_ENABLED", "1"), default=True)
        self.walk_forward_folds = int(os.getenv("WALK_FORWARD_FOLDS", "4"))
        self.walk_forward_min_train_dates = int(os.getenv("WALK_FORWARD_MIN_TRAIN_DATES", "60"))
        self.walk_forward_test_dates = int(os.getenv("WALK_FORWARD_TEST_DATES", "20"))
        self.recency_weight_enabled = _is_truthy(os.getenv("RECENCY_WEIGHT_ENABLED", "1"), default=True)
        self.recency_weight_half_life_days = float(os.getenv("RECENCY_WEIGHT_HALF_LIFE_DAYS", "10"))
        self.recency_weight_min = float(os.getenv("RECENCY_WEIGHT_MIN", "0.25"))
        self.direction_threshold_tuning_enabled = _is_truthy(
            os.getenv("DIRECTION_THRESHOLD_TUNING_ENABLED", "1"),
            default=True,
        )
        self.direction_threshold_min = float(os.getenv("DIRECTION_THRESHOLD_MIN", "0.35"))
        self.direction_threshold_max = float(os.getenv("DIRECTION_THRESHOLD_MAX", "0.65"))
        self.direction_threshold_step = float(os.getenv("DIRECTION_THRESHOLD_STEP", "0.01"))
        self.direction_threshold_metric = os.getenv("DIRECTION_THRESHOLD_METRIC", "balanced_accuracy")
        self.direction_return_threshold = max(0.0, float(os.getenv("DIRECTION_RETURN_THRESHOLD", "0.0")))
        self.direction_neutral_policy = os.getenv("DIRECTION_NEUTRAL_POLICY", "drop").strip().lower()
        if self.direction_neutral_policy not in {"drop", "sign"}:
            self.direction_neutral_policy = "drop"
        self.direction_label_target = os.getenv("DIRECTION_LABEL_TARGET", "next_close").strip().lower()
        if self.direction_label_target not in {"next_close", "horizon_extreme"}:
            self.direction_label_target = "next_close"
        self.train_min_cp_prob = max(0.0, float(os.getenv("TRAIN_MIN_CP_PROB", "0.0")))
        self.train_min_whale_score = max(0.0, float(os.getenv("TRAIN_MIN_WHALE_SCORE", "0.0")))
        self.train_min_innovation_abs = max(0.0, float(os.getenv("TRAIN_MIN_INNOVATION_ABS", "0.0")))
        self.model_selection_std_penalty = float(os.getenv("MODEL_SELECTION_STD_PENALTY", "0.5"))
        self.prediction_audit_enabled = _is_truthy(os.getenv("PREDICTION_AUDIT_ENABLED", "1"), default=True)
        self.prediction_audit_table = os.getenv("PREDICTION_AUDIT_TABLE", "whale_ml_prediction_audit")
        self.trade_filter_config_path = Path(
            os.getenv("TRADE_FILTER_CONFIG_PATH", "/app/artifacts/trade_filter_config.json")
        )
        self.trade_filter_enabled = _is_truthy(os.getenv("TRADE_FILTER_ENABLED", "1"), default=True)
        self.trade_filter_default_min_prob_up = min(
            1.0,
            max(0.0, float(os.getenv("TRADE_FILTER_DEFAULT_MIN_PROB_UP", "0.88"))),
        )
        self.global_require_both_deltas = _is_truthy(os.getenv("GLOBAL_PROMOTION_REQUIRE_BOTH", "1"), default=True)
        self.global_min_direction_delta = float(os.getenv("GLOBAL_PROMOTION_MIN_DIRECTION_DELTA", "0.0"))
        self.global_min_sessions_delta = float(os.getenv("GLOBAL_PROMOTION_MIN_SESSIONS_DELTA", "0.0"))

        # training symbol-specific challenger config
        self.symbol_enabled = True
        self.symbol_train_on_anomaly = True
        # self.symbol_lookback_days = int(os.getenv("SYMBOL_TRAIN_LOOKBACK_DAYS", str(self.train_lookback_days)))
        self.symbol_lookback_days = self.train_lookback_days
        self.symbol_max_rows = int(os.getenv("SYMBOL_TRAIN_MAX_ROWS", "20000"))
        self.symbol_max_events_per_symbol_day = int(
            os.getenv("SYMBOL_TRAIN_MAX_EVENTS_PER_SYMBOL_DAY", str(self.train_max_events_per_symbol_day))
        )
        self.symbol_event_selection_strategy = os.getenv(
            "SYMBOL_TRAIN_EVENT_SELECTION_STRATEGY",
            self.train_event_selection_strategy,
        )
        self.symbol_train_cooldown_min = int(os.getenv("SYMBOL_TRAIN_COOLDOWN_MIN", "180"))
        self.symbol_min_train_samples = int(os.getenv("SYMBOL_MIN_TRAIN_SAMPLES", "350"))
        self.symbol_require_both_deltas = _is_truthy(os.getenv("SYMBOL_PROMOTION_REQUIRE_BOTH", "1"), default=True)
        self.symbol_min_direction_delta = float(os.getenv("SYMBOL_PROMOTION_MIN_DIRECTION_DELTA", "0.0"))
        self.symbol_min_sessions_delta = float(os.getenv("SYMBOL_PROMOTION_MIN_SESSIONS_DELTA", "0.0"))
        self.symbol_min_cp_prob = max(0.0, float(os.getenv("SYMBOL_TRAIN_MIN_CP_PROB", str(self.train_min_cp_prob))))
        self.symbol_min_whale_score = max(
            0.0,
            float(os.getenv("SYMBOL_TRAIN_MIN_WHALE_SCORE", str(self.train_min_whale_score))),
        )
        self.symbol_min_innovation_abs = max(
            0.0,
            float(os.getenv("SYMBOL_TRAIN_MIN_INNOVATION_ABS", str(self.train_min_innovation_abs))),
        )
        self.symbol_registry_required = True

        # registry config mlflow
        self.mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        self.registry_path = Path(os.getenv("SYMBOL_REGISTRY_PATH", "/app/config/symbol_registry.json"))
        self.mlflow_experiment = os.getenv("WHALE_ML_EXPERIMENT", "whale_ml_training")
        self.registered_model_name = os.getenv("WHALE_ML_MODEL_NAME", "whale_move_forecaster")
        self.model_alias = os.getenv("WHALE_ML_MODEL_ALIAS", "production")
        self.global_candidate_alias = os.getenv("WHALE_ML_GLOBAL_CANDIDATE_ALIAS", "candidate")
        self.symbol_candidate_alias = os.getenv("WHALE_ML_SYMBOL_CANDIDATE_ALIAS", "candidate")
        self.symbol_model_name_template = os.getenv("WHALE_ML_SYMBOL_MODEL_NAME_TEMPLATE", "{base}__{symbol}")
        self.prefer_registry = True
        self.mlflow_registry_required = True
        # self.prefer_registry = _is_truthy(os.getenv("PREFER_MLFLOW_REGISTRY", "1"), default=True)
        # self.mlflow_registry_required = _is_truthy(os.getenv("MLFLOW_REGISTRY_REQUIRED", "1"), default=True)

        self.bundle: Optional[Dict[str, Any]] = None
        self.symbol_bundles: Dict[str, Dict[str, Any]] = {}
        self.symbol_index: Dict[str, Any] = {"symbols": {}}

        self.lock = threading.RLock()
        self.train_lock = threading.Lock()

        self.symbol_train_state_lock = threading.Lock()
        self.symbol_training_in_progress: set[str] = set()
        self.symbol_last_train_at: Dict[str, float] = {}

        self.market_sets = self._load_market_sets()
        self.model_source = "none"
        self._load_trade_filter_config()


__all__ = [
    "FEATURE_NAMES",
    "WhaleBundlePyfuncModel",
    "WhaleMoveForecaster",
]
