import tempfile
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import joblib
import mlflow
import numpy as np
from mlflow.tracking import MlflowClient
from sklearn.base import clone
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import (
    ExtraTreesClassifier,
    ExtraTreesRegressor,
    GradientBoostingClassifier,
    GradientBoostingRegressor,
    HistGradientBoostingClassifier,
    RandomForestClassifier,
    RandomForestRegressor,
)
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)
from sklearn.model_selection import GroupShuffleSplit, train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from modeling_shared import (
    FEATURE_NAMES,
    WhaleBundlePyfuncModel,
    _metric_token,
    _safe_float,
    _safe_metric_value,
    _sanitize_symbol,
    _utc_now,
    logger,
)


class TrainingMixin:
    def _build_classifier_candidates(self) -> Dict[str, Any]:
        return {
            "logistic_regression": make_pipeline(
                StandardScaler(),
                LogisticRegression(
                    max_iter=500,
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
            "random_forest_classifier": RandomForestClassifier(
                n_estimators=240,
                max_depth=12,
                min_samples_leaf=6,
                class_weight="balanced_subsample",
                random_state=42,
                n_jobs=-1,
            ),
            "extra_trees_classifier": ExtraTreesClassifier(
                n_estimators=260,
                max_depth=12,
                min_samples_leaf=5,
                class_weight="balanced",
                random_state=42,
                n_jobs=-1,
            ),
            "gradient_boosting_classifier": GradientBoostingClassifier(
                n_estimators=220,
                learning_rate=0.05,
                max_depth=3,
                random_state=42,
            ),
            "hist_gradient_boosting_classifier": HistGradientBoostingClassifier(
                max_iter=220,
                learning_rate=0.04,
                max_leaf_nodes=31,
                l2_regularization=0.05,
                random_state=42,
            ),
        }

    def _build_regressor_candidates(self) -> Dict[str, Any]:
        return {
            "one_session_baseline": DummyRegressor(
                strategy="constant",
                constant=1.0,
            ),
            "median_sessions_baseline": DummyRegressor(
                strategy="median",
            ),
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
            candidate_name = _metric_token(str(candidate.get("name")))
            candidate_score = candidate.get("score")
            if candidate_score is not None:
                payload[f"{prefix}_{candidate_name}_score"] = float(candidate_score)

            metrics = dict(candidate.get("metrics", {}))
            for metric_name, metric_value in metrics.items():
                if metric_value is None:
                    continue
                payload[f"{prefix}_{candidate_name}_{_metric_token(str(metric_name))}"] = float(metric_value)
        return payload

    @staticmethod
    def _fallback_metric_if_perfect_on_small_sample(
        primary_value: Optional[float],
        fallback_value: Optional[float],
        sample_size: int,
        min_reliable_samples: int = 30,
    ) -> Optional[float]:
        if primary_value is None:
            return fallback_value
        value = float(primary_value)
        if sample_size < int(min_reliable_samples) and value >= 0.999999 and fallback_value is not None:
            return float(fallback_value)
        return value

    @staticmethod
    def _date_to_ordinal(value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date().toordinal()
        if isinstance(value, date):
            return value.toordinal()
        try:
            return datetime.fromisoformat(str(value)[:10]).date().toordinal()
        except Exception:
            return None

    def _build_recency_sample_weights(
        self,
        sample_dates: Optional[np.ndarray],
    ) -> Tuple[Optional[np.ndarray], Dict[str, Any]]:
        if not getattr(self, "recency_weight_enabled", False):
            return None, {"enabled": False}
        if sample_dates is None or len(sample_dates) == 0:
            return None, {"enabled": False, "reason": "missing_sample_dates"}

        ordinals = np.asarray(
            [
                -1 if ordinal is None else ordinal
                for ordinal in (self._date_to_ordinal(value) for value in sample_dates)
            ],
            dtype=np.int64,
        )
        valid_mask = ordinals >= 0
        if not np.any(valid_mask):
            return None, {"enabled": False, "reason": "invalid_sample_dates"}

        half_life = max(1.0, float(getattr(self, "recency_weight_half_life_days", 10.0)))
        min_raw_weight = min(1.0, max(0.0, float(getattr(self, "recency_weight_min", 0.25))))
        newest = int(np.max(ordinals[valid_mask]))
        oldest = int(np.min(ordinals[valid_mask]))
        ages = np.maximum(0, newest - ordinals).astype(np.float64)
        raw_weights = np.power(0.5, ages / half_life)
        raw_weights = np.maximum(raw_weights, min_raw_weight)
        raw_weights[~valid_mask] = min_raw_weight

        mean_weight = float(np.mean(raw_weights))
        if not np.isfinite(mean_weight) or mean_weight <= 0.0:
            return None, {"enabled": False, "reason": "invalid_weight_mean"}
        weights = (raw_weights / mean_weight).astype(np.float64)

        return weights, {
            "enabled": True,
            "mode": "exponential_decay_by_sample_date",
            "half_life_days": float(half_life),
            "min_raw_weight": float(min_raw_weight),
            "oldest_date": date.fromordinal(oldest).isoformat(),
            "newest_date": date.fromordinal(newest).isoformat(),
            "weight_min": float(np.min(weights)),
            "weight_max": float(np.max(weights)),
            "weight_mean": float(np.mean(weights)),
        }

    @staticmethod
    def _fit_model(model: Any, X: np.ndarray, y: np.ndarray, sample_weight: Optional[np.ndarray] = None) -> Any:
        if sample_weight is None:
            model.fit(X, y)
            return model

        weights = np.asarray(sample_weight, dtype=np.float64)
        if len(weights) != len(y) or not np.all(np.isfinite(weights)):
            model.fit(X, y)
            return model

        if hasattr(model, "steps") and getattr(model, "steps"):
            final_step_name = str(model.steps[-1][0])
            model.fit(X, y, **{f"{final_step_name}__sample_weight": weights})
            return model

        model.fit(X, y, sample_weight=weights)
        return model

    def _direction_threshold_grid(self) -> np.ndarray:
        min_value = min(0.99, max(0.01, float(getattr(self, "direction_threshold_min", 0.35))))
        max_value = min(0.99, max(min_value, float(getattr(self, "direction_threshold_max", 0.65))))
        step = min(0.25, max(0.001, float(getattr(self, "direction_threshold_step", 0.01))))
        return np.arange(min_value, max_value + (step / 2.0), step, dtype=np.float64)

    @staticmethod
    def _score_direction_predictions(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, Optional[float]]:
        return {
            "accuracy": _safe_metric_value(accuracy_score(y_true, y_pred)),
            "balanced_accuracy": _safe_metric_value(balanced_accuracy_score(y_true, y_pred)),
            "f1_direction": _safe_metric_value(f1_score(y_true, y_pred, zero_division=0)),
        }

    def _score_direction_threshold(
        self,
        y_true: np.ndarray,
        prob_up: np.ndarray,
        threshold: float,
    ) -> Dict[str, Optional[float]]:
        threshold_value = float(min(max(threshold, 0.0), 1.0))
        y_pred = (np.asarray(prob_up, dtype=np.float64) >= threshold_value).astype(np.int64)
        return self._score_direction_predictions(y_true, y_pred)

    def _find_best_direction_threshold(
        self,
        y_true: np.ndarray,
        prob_up: np.ndarray,
    ) -> Dict[str, Any]:
        metric_name = str(getattr(self, "direction_threshold_metric", "balanced_accuracy") or "balanced_accuracy")
        metric_name = _metric_token(metric_name)
        if metric_name not in {"balanced_accuracy", "accuracy", "f1_direction"}:
            metric_name = "balanced_accuracy"

        default_threshold = 0.5
        default_scores = self._score_direction_threshold(y_true, prob_up, default_threshold)
        if not getattr(self, "direction_threshold_tuning_enabled", True):
            return {
                "threshold": default_threshold,
                "threshold_metric": metric_name,
                "threshold_score": default_scores.get(metric_name),
                "scores": default_scores,
                "enabled": False,
            }

        best_threshold = default_threshold
        best_scores = default_scores
        best_key = (
            float(default_scores.get(metric_name) or float("-inf")),
            float(default_scores.get("balanced_accuracy") or float("-inf")),
            float(default_scores.get("accuracy") or float("-inf")),
            -abs(default_threshold - 0.5),
        )

        for threshold in self._direction_threshold_grid():
            scores = self._score_direction_threshold(y_true, prob_up, float(threshold))
            key = (
                float(scores.get(metric_name) or float("-inf")),
                float(scores.get("balanced_accuracy") or float("-inf")),
                float(scores.get("accuracy") or float("-inf")),
                -abs(float(threshold) - 0.5),
            )
            if key > best_key:
                best_threshold = float(threshold)
                best_scores = scores
                best_key = key

        return {
            "threshold": float(best_threshold),
            "threshold_metric": metric_name,
            "threshold_score": best_scores.get(metric_name),
            "scores": best_scores,
            "enabled": True,
        }

    @staticmethod
    def _classifier_prob_up(model: Any, X: np.ndarray) -> Tuple[Optional[np.ndarray], Optional[float]]:
        if not hasattr(model, "predict_proba"):
            return None, None
        try:
            y_prob = model.predict_proba(X)
            classes = np.asarray(getattr(model, "classes_", []))
            up_positions = np.where(classes == 1)[0] if classes.size else np.asarray([])
            up_index = int(up_positions[0]) if len(up_positions) else (1 if y_prob.shape[1] > 1 else 0)
            prob_up = y_prob[:, up_index] if y_prob.shape[1] > up_index else y_prob[:, -1]
            return np.asarray(prob_up, dtype=np.float64), float(up_index)
        except Exception:
            return None, None

    @staticmethod
    def _align_feature_matrix_for_bundle(bundle: Dict[str, Any], X: np.ndarray) -> np.ndarray:
        expected_count = len(bundle.get("feature_names") or [])
        if expected_count <= 0:
            expected_count = int(getattr(bundle.get("classifier"), "n_features_in_", X.shape[1]) or X.shape[1])
        if X.shape[1] == expected_count:
            return X
        if X.shape[1] > expected_count:
            return X[:, :expected_count]

        padding = np.zeros((X.shape[0], expected_count - X.shape[1]), dtype=X.dtype)
        return np.hstack([X, padding])

    def _evaluate_classifier_candidate(
        self,
        name: str,
        model: Any,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        sample_weight: Optional[np.ndarray] = None,
        direction_threshold: Optional[float] = None,
        tune_threshold: bool = True,
    ) -> Dict[str, Any]:
        self._fit_model(model, X_train, y_train, sample_weight)
        default_pred = model.predict(X_test)
        default_scores = self._score_direction_predictions(y_test, default_pred)

        accuracy = default_scores.get("accuracy")
        balanced_accuracy = default_scores.get("balanced_accuracy")
        f1 = default_scores.get("f1_direction")
        roc_auc: Optional[float] = None
        threshold_result: Dict[str, Any] = {
            "threshold": 0.5,
            "threshold_metric": "balanced_accuracy",
            "threshold_score": balanced_accuracy,
            "scores": default_scores,
            "enabled": False,
        }

        prob_up, _ = self._classifier_prob_up(model, X_test)
        if prob_up is not None:
            try:
                roc_auc = _safe_metric_value(roc_auc_score(y_test, prob_up))
            except Exception:
                roc_auc = None
            if tune_threshold:
                threshold_result = self._find_best_direction_threshold(y_test, prob_up)
            else:
                threshold = 0.5 if direction_threshold is None else float(direction_threshold)
                scores = self._score_direction_threshold(y_test, prob_up, threshold)
                threshold_result = {
                    "threshold": threshold,
                    "threshold_metric": str(getattr(self, "direction_threshold_metric", "balanced_accuracy")),
                    "threshold_score": scores.get(
                        _metric_token(str(getattr(self, "direction_threshold_metric", "balanced_accuracy")))
                    ),
                    "scores": scores,
                    "enabled": bool(getattr(self, "direction_threshold_tuning_enabled", True)),
                }

            tuned_scores = dict(threshold_result.get("scores") or {})
            accuracy = tuned_scores.get("accuracy", accuracy)
            balanced_accuracy = tuned_scores.get("balanced_accuracy", balanced_accuracy)
            f1 = tuned_scores.get("f1_direction", f1)

        score = roc_auc if roc_auc is not None else accuracy
        if score is None:
            raise RuntimeError(f"Classifier candidate '{name}' produced invalid evaluation score.")

        return {
            "name": name,
            "selection_metric": "roc_auc" if roc_auc is not None else "accuracy",
            "score": float(score),
            "metrics": {
                "accuracy": accuracy,
                "balanced_accuracy": balanced_accuracy,
                "roc_auc": roc_auc,
                "f1_direction": f1,
                "default_accuracy": default_scores.get("accuracy"),
                "default_balanced_accuracy": default_scores.get("balanced_accuracy"),
                "default_f1_direction": default_scores.get("f1_direction"),
                "direction_threshold": threshold_result.get("threshold"),
                "direction_threshold_score": threshold_result.get("threshold_score"),
            },
            "model": model,
            "direction_threshold": threshold_result.get("threshold"),
            "direction_threshold_metric": threshold_result.get("threshold_metric"),
            "direction_threshold_enabled": threshold_result.get("enabled"),
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
        sample_weight: Optional[np.ndarray] = None,
    ) -> Dict[str, Any]:
        self._fit_model(model, X_train, y_train, sample_weight)
        y_pred = np.clip(model.predict(X_test), 1.0, float(horizon))

        mae = _safe_metric_value(mean_absolute_error(y_test, y_pred))
        rmse = _safe_metric_value(np.sqrt(mean_squared_error(y_test, y_pred)))
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

    def _evaluate_bundle_on_holdout(
        self,
        bundle: Dict[str, Any],
        X_test: np.ndarray,
        y_dir_test: np.ndarray,
        y_sess_test: np.ndarray,
        horizon: int,
    ) -> Dict[str, Any]:
        classifier = bundle["classifier"]
        regressor = bundle["regressor"]
        meta = dict(bundle.get("meta", {}))
        direction_threshold = float(meta.get("direction_threshold", 0.5) or 0.5)
        X_eval = self._align_feature_matrix_for_bundle(bundle, X_test)

        default_pred = classifier.predict(X_eval)
        default_scores = self._score_direction_predictions(y_dir_test, default_pred)
        y_dir_pred = default_pred
        accuracy = default_scores.get("accuracy")
        balanced_accuracy = default_scores.get("balanced_accuracy")
        f1 = default_scores.get("f1_direction")

        roc_auc: Optional[float] = None
        prob_up, _ = self._classifier_prob_up(classifier, X_eval)
        if prob_up is not None:
            try:
                roc_auc = _safe_metric_value(roc_auc_score(y_dir_test, prob_up))
            except Exception:
                roc_auc = None
            threshold_scores = self._score_direction_threshold(y_dir_test, prob_up, direction_threshold)
            accuracy = threshold_scores.get("accuracy", accuracy)
            balanced_accuracy = threshold_scores.get("balanced_accuracy", balanced_accuracy)
            f1 = threshold_scores.get("f1_direction", f1)

        y_sess_pred = np.clip(regressor.predict(X_eval), 1.0, float(horizon))
        mae = _safe_metric_value(mean_absolute_error(y_sess_test, y_sess_pred))
        rmse = _safe_metric_value(np.sqrt(mean_squared_error(y_sess_test, y_sess_pred)))

        direction_score = roc_auc if roc_auc is not None else accuracy
        sessions_score = None if mae is None else float(-mae)

        return {
            "metrics": {
                "accuracy": accuracy,
                "balanced_accuracy": balanced_accuracy,
                "roc_auc": roc_auc,
                "f1_direction": f1,
                "default_accuracy": default_scores.get("accuracy"),
                "default_balanced_accuracy": default_scores.get("balanced_accuracy"),
                "default_f1_direction": default_scores.get("f1_direction"),
                "direction_threshold": direction_threshold,
                "mae_sessions": mae,
                "rmse_sessions": rmse,
            },
            "scores": {
                "direction": direction_score,
                "sessions": sessions_score,
            },
        }

    @staticmethod
    def _mean_metric(values: List[Optional[float]]) -> Optional[float]:
        valid = [float(value) for value in values if value is not None]
        if not valid:
            return None
        return float(np.mean(valid))

    @staticmethod
    def _std_metric(values: List[Optional[float]]) -> Optional[float]:
        valid = [float(value) for value in values if value is not None]
        if not valid:
            return None
        return float(np.std(valid))

    def _build_walk_forward_folds(
        self,
        sample_dates: np.ndarray,
        y_dir: np.ndarray,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        if not self.walk_forward_enabled:
            return [], "walk_forward_disabled"
        if sample_dates is None or len(sample_dates) != len(y_dir):
            return [], "missing_sample_dates"

        dates = np.asarray(sample_dates, dtype=object)
        unique_dates = np.asarray(sorted({value for value in dates if value is not None}), dtype=object)
        fold_count = max(2, int(self.walk_forward_folds))
        min_train_dates = max(2, int(self.walk_forward_min_train_dates))
        test_dates = max(1, int(self.walk_forward_test_dates))
        adaptive = False
        required_dates = min_train_dates + (fold_count * test_dates)
        if len(unique_dates) < required_dates:
            if len(unique_dates) < fold_count + 2:
                return [], f"insufficient_unique_dates:{len(unique_dates)}<{required_dates}"
            adaptive = True
            test_dates = 1
            min_train_dates = max(2, int(len(unique_dates) - fold_count))
            required_dates = min_train_dates + (fold_count * test_dates)
            if len(unique_dates) < required_dates:
                return [], f"insufficient_unique_dates:{len(unique_dates)}<{required_dates}"

        first_test_pos = len(unique_dates) - (fold_count * test_dates)
        folds: List[Dict[str, Any]] = []
        for fold_number in range(fold_count):
            test_start_pos = first_test_pos + (fold_number * test_dates)
            test_end_pos = test_start_pos + test_dates
            train_dates = unique_dates[:test_start_pos]
            test_dates_slice = unique_dates[test_start_pos:test_end_pos]

            train_mask = np.isin(dates, train_dates)
            test_mask = np.isin(dates, test_dates_slice)
            train_idx = np.flatnonzero(train_mask)
            test_idx = np.flatnonzero(test_mask)
            if len(train_idx) == 0 or len(test_idx) == 0:
                continue
            if len(np.unique(y_dir[train_idx])) < 2 or len(np.unique(y_dir[test_idx])) < 2:
                logger.warning(
                    "Skipping walk-forward fold %d due to single-class partition (train_classes=%s, test_classes=%s)",
                    fold_number + 1,
                    np.unique(y_dir[train_idx]).tolist(),
                    np.unique(y_dir[test_idx]).tolist(),
                )
                continue

            folds.append(
                {
                    "fold": fold_number + 1,
                    "train_idx": train_idx,
                    "test_idx": test_idx,
                    "train_start_date": train_dates[0].isoformat(),
                    "train_end_date": train_dates[-1].isoformat(),
                    "test_start_date": test_dates_slice[0].isoformat(),
                    "test_end_date": test_dates_slice[-1].isoformat(),
                    "train_date_count": int(len(train_dates)),
                    "test_date_count": int(len(test_dates_slice)),
                    "train_size": int(len(train_idx)),
                    "test_size": int(len(test_idx)),
                    "train_up_ratio": float(np.mean(y_dir[train_idx])),
                    "test_up_ratio": float(np.mean(y_dir[test_idx])),
                    "adaptive": bool(adaptive),
                }
            )

        if len(folds) < 2:
            return [], f"insufficient_valid_folds:{len(folds)}<2"
        return folds, None

    def _aggregate_classifier_fold_results(
        self,
        name: str,
        fold_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        accuracy_values = [result["metrics"].get("accuracy") for result in fold_results]
        balanced_accuracy_values = [result["metrics"].get("balanced_accuracy") for result in fold_results]
        roc_auc_values = [result["metrics"].get("roc_auc") for result in fold_results]
        f1_values = [result["metrics"].get("f1_direction") for result in fold_results]
        default_accuracy_values = [result["metrics"].get("default_accuracy") for result in fold_results]
        default_balanced_accuracy_values = [
            result["metrics"].get("default_balanced_accuracy") for result in fold_results
        ]
        default_f1_values = [result["metrics"].get("default_f1_direction") for result in fold_results]
        threshold_values = [result.get("direction_threshold") for result in fold_results]
        threshold_score_values = [result["metrics"].get("direction_threshold_score") for result in fold_results]

        accuracy_mean = self._mean_metric(accuracy_values)
        balanced_accuracy_mean = self._mean_metric(balanced_accuracy_values)
        roc_auc_mean = self._mean_metric(roc_auc_values)
        roc_auc_std = self._std_metric(roc_auc_values)
        f1_mean = self._mean_metric(f1_values)
        threshold_mean = self._mean_metric(threshold_values)
        raw_score = roc_auc_mean if roc_auc_mean is not None else accuracy_mean
        score_std = roc_auc_std if roc_auc_mean is not None else self._std_metric(accuracy_values)
        penalty = max(0.0, float(getattr(self, "model_selection_std_penalty", 0.0)))
        score = None if raw_score is None else float(raw_score) - (penalty * float(score_std or 0.0))
        if score is None:
            raise RuntimeError(f"Classifier candidate '{name}' produced invalid walk-forward score.")

        return {
            "name": name,
            "selection_metric": (
                "mean_roc_auc_minus_std_penalty" if roc_auc_mean is not None else "mean_accuracy_minus_std_penalty"
            ),
            "score": float(score),
            "metrics": {
                "raw_selection_score": raw_score,
                "selection_std_penalty": float(penalty),
                "accuracy": accuracy_mean,
                "accuracy_std": self._std_metric(accuracy_values),
                "balanced_accuracy": balanced_accuracy_mean,
                "balanced_accuracy_std": self._std_metric(balanced_accuracy_values),
                "roc_auc": roc_auc_mean,
                "roc_auc_std": roc_auc_std,
                "f1_direction": f1_mean,
                "f1_direction_std": self._std_metric(f1_values),
                "default_accuracy": self._mean_metric(default_accuracy_values),
                "default_accuracy_std": self._std_metric(default_accuracy_values),
                "default_balanced_accuracy": self._mean_metric(default_balanced_accuracy_values),
                "default_balanced_accuracy_std": self._std_metric(default_balanced_accuracy_values),
                "default_f1_direction": self._mean_metric(default_f1_values),
                "default_f1_direction_std": self._std_metric(default_f1_values),
                "direction_threshold": threshold_mean,
                "direction_threshold_std": self._std_metric(threshold_values),
                "direction_threshold_score": self._mean_metric(threshold_score_values),
                "direction_threshold_score_std": self._std_metric(threshold_score_values),
            },
            "direction_threshold": threshold_mean,
            "direction_threshold_metric": str(
                fold_results[0].get("direction_threshold_metric", "balanced_accuracy")
                if fold_results
                else "balanced_accuracy"
            ),
            "direction_threshold_enabled": bool(
                fold_results[0].get("direction_threshold_enabled", False) if fold_results else False
            ),
        }

    def _aggregate_regressor_fold_results(
        self,
        name: str,
        fold_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        mae_values = [result["metrics"].get("mae_sessions") for result in fold_results]
        rmse_values = [result["metrics"].get("rmse_sessions") for result in fold_results]

        mae_mean = self._mean_metric(mae_values)
        mae_std = self._std_metric(mae_values)
        rmse_mean = self._mean_metric(rmse_values)
        penalty = max(0.0, float(getattr(self, "model_selection_std_penalty", 0.0)))
        score = None if mae_mean is None else float(-mae_mean) - (penalty * float(mae_std or 0.0))
        if score is None:
            raise RuntimeError(f"Regressor candidate '{name}' produced invalid walk-forward score.")

        return {
            "name": name,
            "selection_metric": "neg_mean_mae_minus_std_penalty",
            "score": score,
            "metrics": {
                "raw_selection_score": float(-mae_mean),
                "selection_std_penalty": float(penalty),
                "mae_sessions": mae_mean,
                "mae_sessions_std": mae_std,
                "rmse_sessions": rmse_mean,
                "rmse_sessions_std": self._std_metric(rmse_values),
            },
        }

    def _run_walk_forward_training_round(
        self,
        X: np.ndarray,
        y_dir: np.ndarray,
        y_sessions: np.ndarray,
        horizon: int,
        sample_dates: np.ndarray,
        folds: List[Dict[str, Any]],
        sample_weight: Optional[np.ndarray] = None,
        sample_weight_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        classifier_candidates = self._build_classifier_candidates()
        regressor_candidates = self._build_regressor_candidates()
        if len(classifier_candidates) < 3:
            raise RuntimeError("At least 3 classifier candidates are required.")
        if len(regressor_candidates) < 3:
            raise RuntimeError("At least 3 regressor candidates are required.")

        selection_folds = folds[:-1]
        holdout_fold = folds[-1]
        classifier_fold_results: Dict[str, List[Dict[str, Any]]] = {
            name: [] for name in classifier_candidates
        }
        regressor_fold_results: Dict[str, List[Dict[str, Any]]] = {
            name: [] for name in regressor_candidates
        }

        for fold in selection_folds:
            train_idx = fold["train_idx"]
            test_idx = fold["test_idx"]
            train_weight = sample_weight[train_idx] if sample_weight is not None else None
            for name, candidate in classifier_candidates.items():
                classifier_fold_results[name].append(
                    self._evaluate_classifier_candidate(
                        name=name,
                        model=clone(candidate),
                        X_train=X[train_idx],
                        y_train=y_dir[train_idx],
                        X_test=X[test_idx],
                        y_test=y_dir[test_idx],
                        sample_weight=train_weight,
                    )
                )
            for name, candidate in regressor_candidates.items():
                regressor_fold_results[name].append(
                    self._evaluate_regressor_candidate(
                        name=name,
                        model=clone(candidate),
                        X_train=X[train_idx],
                        y_train=y_sessions[train_idx],
                        X_test=X[test_idx],
                        y_test=y_sessions[test_idx],
                        horizon=horizon,
                        sample_weight=train_weight,
                    )
                )

        classifier_results = [
            self._aggregate_classifier_fold_results(name, results)
            for name, results in classifier_fold_results.items()
        ]
        regressor_results = [
            self._aggregate_regressor_fold_results(name, results)
            for name, results in regressor_fold_results.items()
        ]
        best_classifier_summary = max(classifier_results, key=self._classifier_rank_key)
        best_regressor_summary = max(regressor_results, key=self._regressor_rank_key)

        holdout_train_idx = holdout_fold["train_idx"]
        holdout_test_idx = holdout_fold["test_idx"]
        holdout_train_weight = sample_weight[holdout_train_idx] if sample_weight is not None else None
        holdout_classifier = self._evaluate_classifier_candidate(
            name=best_classifier_summary["name"],
            model=clone(classifier_candidates[best_classifier_summary["name"]]),
            X_train=X[holdout_train_idx],
            y_train=y_dir[holdout_train_idx],
            X_test=X[holdout_test_idx],
            y_test=y_dir[holdout_test_idx],
            sample_weight=holdout_train_weight,
            direction_threshold=best_classifier_summary.get("direction_threshold"),
            tune_threshold=False,
        )
        holdout_regressor = self._evaluate_regressor_candidate(
            name=best_regressor_summary["name"],
            model=clone(regressor_candidates[best_regressor_summary["name"]]),
            X_train=X[holdout_train_idx],
            y_train=y_sessions[holdout_train_idx],
            X_test=X[holdout_test_idx],
            y_test=y_sessions[holdout_test_idx],
            horizon=horizon,
            sample_weight=holdout_train_weight,
        )

        best_classifier = dict(best_classifier_summary)
        best_classifier["model"] = holdout_classifier["model"]
        best_classifier["holdout_metrics"] = dict(holdout_classifier["metrics"])
        best_classifier["holdout_score"] = holdout_classifier["score"]
        best_classifier["direction_threshold"] = holdout_classifier.get("direction_threshold")
        best_classifier["direction_threshold_metric"] = holdout_classifier.get("direction_threshold_metric")

        best_regressor = dict(best_regressor_summary)
        best_regressor["model"] = holdout_regressor["model"]
        best_regressor["holdout_metrics"] = dict(holdout_regressor["metrics"])
        best_regressor["holdout_score"] = holdout_regressor["score"]

        logger.info(
            "Selected direction model=%s (cv_score=%.6f via %s), sessions model=%s (cv_score=%.6f via %s)",
            best_classifier_summary["name"],
            best_classifier_summary["score"],
            best_classifier_summary["selection_metric"],
            best_regressor_summary["name"],
            best_regressor_summary["score"],
            best_regressor_summary["selection_metric"],
        )

        holdout_sample_size = int(len(holdout_test_idx))
        best_metrics = {
            # Primary metrics use CV means for stability across folds.
            "accuracy": best_classifier_summary["metrics"].get("accuracy"),
            "balanced_accuracy": best_classifier_summary["metrics"].get("balanced_accuracy"),
            "roc_auc": best_classifier_summary["metrics"].get("roc_auc"),
            "f1_direction": best_classifier_summary["metrics"].get("f1_direction"),
            "default_accuracy": holdout_classifier["metrics"].get("default_accuracy"),
            "default_balanced_accuracy": holdout_classifier["metrics"].get("default_balanced_accuracy"),
            "default_f1_direction": holdout_classifier["metrics"].get("default_f1_direction"),
            "holdout_accuracy": holdout_classifier["metrics"].get("accuracy"),
            "holdout_balanced_accuracy": holdout_classifier["metrics"].get("balanced_accuracy"),
            "holdout_roc_auc": holdout_classifier["metrics"].get("roc_auc"),
            "holdout_f1_direction": holdout_classifier["metrics"].get("f1_direction"),
            "holdout_sample_size": float(holdout_sample_size),
            "direction_threshold": holdout_classifier["metrics"].get("direction_threshold"),
            "direction_threshold_score": holdout_classifier["metrics"].get("direction_threshold_score"),
            "baseline_accuracy": float(
                max(np.mean(y_dir[holdout_test_idx]), 1.0 - np.mean(y_dir[holdout_test_idx]))
            ),
            "mae_sessions": holdout_regressor["metrics"].get("mae_sessions"),
            "rmse_sessions": holdout_regressor["metrics"].get("rmse_sessions"),
            "classifier_score": holdout_classifier["score"],
            "regressor_score": holdout_regressor["score"],
            "cv_classifier_score_mean": best_classifier_summary["score"],
            "cv_regressor_score_mean": best_regressor_summary["score"],
            "cv_accuracy_mean": best_classifier_summary["metrics"].get("accuracy"),
            "cv_accuracy_std": best_classifier_summary["metrics"].get("accuracy_std"),
            "cv_balanced_accuracy_mean": best_classifier_summary["metrics"].get("balanced_accuracy"),
            "cv_balanced_accuracy_std": best_classifier_summary["metrics"].get("balanced_accuracy_std"),
            "cv_roc_auc_mean": best_classifier_summary["metrics"].get("roc_auc"),
            "cv_roc_auc_std": best_classifier_summary["metrics"].get("roc_auc_std"),
            "cv_f1_direction_mean": best_classifier_summary["metrics"].get("f1_direction"),
            "cv_f1_direction_std": best_classifier_summary["metrics"].get("f1_direction_std"),
            "cv_direction_threshold_mean": best_classifier_summary["metrics"].get("direction_threshold"),
            "cv_direction_threshold_std": best_classifier_summary["metrics"].get("direction_threshold_std"),
            "cv_mae_sessions_mean": best_regressor_summary["metrics"].get("mae_sessions"),
            "cv_mae_sessions_std": best_regressor_summary["metrics"].get("mae_sessions_std"),
            "cv_rmse_sessions_mean": best_regressor_summary["metrics"].get("rmse_sessions"),
            "cv_rmse_sessions_std": best_regressor_summary["metrics"].get("rmse_sessions_std"),
        }

        split_meta = {
            "strategy": "walk_forward_expanding_window",
            "fold_count": int(len(folds)),
            "selection_fold_count": int(len(selection_folds)),
            "holdout_fold": int(holdout_fold["fold"]),
            "unique_date_count": int(len({value for value in sample_dates if value is not None})),
            "min_train_dates": int(self.walk_forward_min_train_dates),
            "test_dates": int(self.walk_forward_test_dates),
            "effective_min_train_dates": int(holdout_fold["train_date_count"] - ((holdout_fold["fold"] - 1) * holdout_fold["test_date_count"])),
            "effective_test_dates": int(holdout_fold["test_date_count"]),
            "adaptive": bool(any(fold.get("adaptive") for fold in folds)),
            "folds": [
                {key: value for key, value in fold.items() if key not in {"train_idx", "test_idx"}}
                for fold in folds
            ],
        }
        split_meta["sample_weighting"] = dict(sample_weight_meta or {"enabled": False})

        classifier_leaderboard = [
            self._candidate_to_meta(candidate)
            for candidate in sorted(classifier_results, key=self._classifier_rank_key, reverse=True)
        ]
        regressor_leaderboard = [
            self._candidate_to_meta(candidate)
            for candidate in sorted(regressor_results, key=self._regressor_rank_key, reverse=True)
        ]

        return {
            "best_classifier": best_classifier,
            "best_regressor": best_regressor,
            "best_metrics": best_metrics,
            "classifier_results": classifier_results,
            "regressor_results": regressor_results,
            "classifier_leaderboard": classifier_leaderboard,
            "regressor_leaderboard": regressor_leaderboard,
            "split_meta": split_meta,
            "X_test": X[holdout_test_idx],
            "y_dir_test": y_dir[holdout_test_idx],
            "y_sess_test": y_sessions[holdout_test_idx],
        }

    def _run_single_split_training_round(
        self,
        X: np.ndarray,
        y_dir: np.ndarray,
        y_sessions: np.ndarray,
        horizon: int,
        sample_groups: Optional[np.ndarray] = None,
        sample_weight: Optional[np.ndarray] = None,
        sample_weight_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        classes = np.unique(y_dir)
        if len(classes) < 2:
            raise RuntimeError("Training labels have only one class; cannot train direction classifier.")

        split_meta: Dict[str, Any] = {}
        used_group_split = False
        train_weight: Optional[np.ndarray] = None

        if sample_groups is not None and len(sample_groups) == len(X):
            groups = np.asarray(sample_groups, dtype=str)
            unique_groups = np.unique(groups)
            if len(unique_groups) >= 5:
                splitter = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
                train_idx, test_idx = next(splitter.split(X, y_dir, groups=groups))
                y_train_candidate = y_dir[train_idx]
                y_test_candidate = y_dir[test_idx]

                if len(np.unique(y_train_candidate)) >= 2 and len(np.unique(y_test_candidate)) >= 2:
                    used_group_split = True
                    X_train, X_test = X[train_idx], X[test_idx]
                    y_dir_train, y_dir_test = y_dir[train_idx], y_dir[test_idx]
                    y_sess_train, y_sess_test = y_sessions[train_idx], y_sessions[test_idx]
                    train_weight = sample_weight[train_idx] if sample_weight is not None else None
                    split_meta = {
                        "strategy": "group_shuffle_symbol_event_date",
                        "group_count": int(len(unique_groups)),
                        "train_group_count": int(np.unique(groups[train_idx]).size),
                        "test_group_count": int(np.unique(groups[test_idx]).size),
                        "avg_rows_per_group": float(len(groups) / max(len(unique_groups), 1)),
                    }
                else:
                    logger.warning(
                        "Group split produced single-class partition (train_classes=%s, test_classes=%s). Falling back to stratified row split.",
                        np.unique(y_train_candidate).tolist(),
                        np.unique(y_test_candidate).tolist(),
                    )
            else:
                logger.warning(
                    "Too few unique groups (%d) for group-based split. Falling back to stratified row split.",
                    len(unique_groups),
                )

        if not used_group_split:
            stratify = y_dir if len(classes) > 1 else None
            if sample_weight is not None and len(sample_weight) == len(X):
                (
                    X_train,
                    X_test,
                    y_dir_train,
                    y_dir_test,
                    y_sess_train,
                    y_sess_test,
                    train_weight,
                    _,
                ) = train_test_split(
                    X,
                    y_dir,
                    y_sessions,
                    sample_weight,
                    test_size=0.2,
                    random_state=42,
                    stratify=stratify,
                )
            else:
                X_train, X_test, y_dir_train, y_dir_test, y_sess_train, y_sess_test = train_test_split(
                    X,
                    y_dir,
                    y_sessions,
                    test_size=0.2,
                    random_state=42,
                    stratify=stratify,
                )
            split_meta = {"strategy": "stratified_random_row_split"}

        split_meta.update(
            {
                "train_size": int(len(y_dir_train)),
                "test_size": int(len(y_dir_test)),
                "train_up_ratio": float(np.mean(y_dir_train)),
                "test_up_ratio": float(np.mean(y_dir_test)),
                "sample_weighting": dict(sample_weight_meta or {"enabled": False}),
            }
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
                    sample_weight=train_weight,
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
                    horizon=horizon,
                    sample_weight=train_weight,
                )
            )

        best_classifier = max(classifier_results, key=self._classifier_rank_key)
        best_regressor = max(regressor_results, key=self._regressor_rank_key)

        logger.info(
            "Selected direction model=%s (score=%.6f via %s), sessions model=%s (score=%.6f via %s)",
            best_classifier["name"],
            best_classifier["score"],
            best_classifier["selection_metric"],
            best_regressor["name"],
            best_regressor["score"],
            best_regressor["selection_metric"],
        )

        test_sample_size = int(len(y_dir_test))
        raw_accuracy = best_classifier["metrics"].get("accuracy")
        raw_balanced_accuracy = best_classifier["metrics"].get("balanced_accuracy")
        raw_f1 = best_classifier["metrics"].get("f1_direction")
        default_accuracy = best_classifier["metrics"].get("default_accuracy")
        default_balanced_accuracy = best_classifier["metrics"].get("default_balanced_accuracy")
        default_f1 = best_classifier["metrics"].get("default_f1_direction")

        best_metrics = {
            "accuracy": self._fallback_metric_if_perfect_on_small_sample(
                raw_accuracy,
                default_accuracy,
                test_sample_size,
            ),
            "balanced_accuracy": self._fallback_metric_if_perfect_on_small_sample(
                raw_balanced_accuracy,
                default_balanced_accuracy,
                test_sample_size,
            ),
            "roc_auc": best_classifier["metrics"].get("roc_auc"),
            "f1_direction": self._fallback_metric_if_perfect_on_small_sample(
                raw_f1,
                default_f1,
                test_sample_size,
            ),
            "default_accuracy": default_accuracy,
            "default_balanced_accuracy": default_balanced_accuracy,
            "default_f1_direction": default_f1,
            "raw_accuracy": raw_accuracy,
            "raw_balanced_accuracy": raw_balanced_accuracy,
            "raw_f1_direction": raw_f1,
            "test_sample_size": float(test_sample_size),
            "direction_threshold": best_classifier["metrics"].get("direction_threshold"),
            "direction_threshold_score": best_classifier["metrics"].get("direction_threshold_score"),
            "baseline_accuracy": float(max(np.mean(y_dir_test), 1.0 - np.mean(y_dir_test))),
            "mae_sessions": best_regressor["metrics"].get("mae_sessions"),
            "rmse_sessions": best_regressor["metrics"].get("rmse_sessions"),
            "classifier_score": best_classifier["score"],
            "regressor_score": best_regressor["score"],
        }

        classifier_leaderboard = [self._candidate_to_meta(candidate) for candidate in sorted(classifier_results, key=self._classifier_rank_key, reverse=True)]
        regressor_leaderboard = [self._candidate_to_meta(candidate) for candidate in sorted(regressor_results, key=self._regressor_rank_key, reverse=True)]

        return {
            "best_classifier": best_classifier,
            "best_regressor": best_regressor,
            "best_metrics": best_metrics,
            "classifier_results": classifier_results,
            "regressor_results": regressor_results,
            "classifier_leaderboard": classifier_leaderboard,
            "regressor_leaderboard": regressor_leaderboard,
            "split_meta": split_meta,
            "X_test": X_test,
            "y_dir_test": y_dir_test,
            "y_sess_test": y_sess_test,
        }

    def _run_training_round(
        self,
        X: np.ndarray,
        y_dir: np.ndarray,
        y_sessions: np.ndarray,
        horizon: int,
        sample_groups: Optional[np.ndarray] = None,
        sample_dates: Optional[np.ndarray] = None,
    ) -> Dict[str, Any]:
        classes = np.unique(y_dir)
        if len(classes) < 2:
            raise RuntimeError("Training labels have only one class; cannot train direction classifier.")

        sample_weight, sample_weight_meta = self._build_recency_sample_weights(sample_dates)
        folds, fallback_reason = self._build_walk_forward_folds(
            sample_dates=np.asarray(sample_dates, dtype=object) if sample_dates is not None else None,
            y_dir=y_dir,
        )
        if folds:
            return self._run_walk_forward_training_round(
                X,
                y_dir,
                y_sessions,
                horizon,
                sample_dates=np.asarray(sample_dates, dtype=object),
                folds=folds,
                sample_weight=sample_weight,
                sample_weight_meta=sample_weight_meta,
            )

        result = self._run_single_split_training_round(
            X,
            y_dir,
            y_sessions,
            horizon,
            sample_groups=sample_groups,
            sample_weight=sample_weight,
            sample_weight_meta=sample_weight_meta,
        )
        result["split_meta"]["walk_forward_fallback_reason"] = fallback_reason
        return result

    @staticmethod
    def _distribution_payload(values: np.ndarray) -> Dict[str, int]:
        unique_values, counts = np.unique(values, return_counts=True)
        return {
            str(int(value) if float(value).is_integer() else float(value)): int(count)
            for value, count in zip(unique_values, counts)
        }

    def _baseline_backtest_metrics(
        self,
        y_dir: np.ndarray,
        y_sessions: np.ndarray,
    ) -> Dict[str, Any]:
        up_ratio = float(np.mean(y_dir)) if len(y_dir) else 0.0
        majority_direction = 1 if up_ratio >= 0.5 else 0
        majority_pred = np.full(len(y_dir), majority_direction, dtype=np.int64)
        one_session_pred = np.ones(len(y_sessions), dtype=np.float64)

        return {
            "majority_direction": "up" if majority_direction == 1 else "down",
            "majority_accuracy": _safe_metric_value(accuracy_score(y_dir, majority_pred)) if len(y_dir) else None,
            "up_ratio": up_ratio,
            "one_session_mae": (
                _safe_metric_value(mean_absolute_error(y_sessions, one_session_pred)) if len(y_sessions) else None
            ),
            "one_session_rmse": (
                _safe_metric_value(np.sqrt(mean_squared_error(y_sessions, one_session_pred)))
                if len(y_sessions)
                else None
            ),
        }

    def _resolve_direction_label_config(
        self,
        direction_return_threshold: Optional[float] = None,
        direction_neutral_policy: Optional[str] = None,
        direction_label_target: Optional[str] = None,
    ) -> Dict[str, Any]:
        threshold = max(
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
        return {
            "direction_return_threshold": float(threshold),
            "direction_neutral_policy": neutral_policy,
            "direction_label_target": label_target,
        }

    def _resolve_event_filter_config(
        self,
        *,
        min_cp_prob: Optional[float] = None,
        min_whale_score: Optional[float] = None,
        min_innovation_abs: Optional[float] = None,
        symbol_scope: bool = False,
    ) -> Dict[str, float]:
        cp_default = getattr(self, "symbol_min_cp_prob" if symbol_scope else "train_min_cp_prob", 0.0)
        whale_default = getattr(
            self,
            "symbol_min_whale_score" if symbol_scope else "train_min_whale_score",
            0.0,
        )
        innovation_default = getattr(
            self,
            "symbol_min_innovation_abs" if symbol_scope else "train_min_innovation_abs",
            0.0,
        )
        return {
            "min_cp_prob": max(0.0, _safe_float(min_cp_prob, default=_safe_float(cp_default))),
            "min_whale_score": max(0.0, _safe_float(min_whale_score, default=_safe_float(whale_default))),
            "min_innovation_abs": max(
                0.0,
                _safe_float(min_innovation_abs, default=_safe_float(innovation_default)),
            ),
        }

    def backtest_current_model(
        self,
        lookback_days: Optional[int] = None,
        max_rows: Optional[int] = None,
        horizon: Optional[int] = None,
        holdout_days: int = 10,
        direction_return_threshold: Optional[float] = None,
        direction_neutral_policy: Optional[str] = None,
        direction_label_target: Optional[str] = None,
        min_cp_prob: Optional[float] = None,
        min_whale_score: Optional[float] = None,
        min_innovation_abs: Optional[float] = None,
    ) -> Dict[str, Any]:
        with self.lock:
            bundle = self.bundle
        if bundle is None:
            raise RuntimeError("Global model is not trained yet")

        model_meta = dict(bundle.get("meta", {}))
        if direction_return_threshold is None:
            direction_return_threshold = model_meta.get("direction_return_threshold")
        if direction_neutral_policy is None:
            direction_neutral_policy = model_meta.get("direction_neutral_policy")
        if direction_label_target is None:
            direction_label_target = model_meta.get("direction_label_target")
        label_config = self._resolve_direction_label_config(
            direction_return_threshold=direction_return_threshold,
            direction_neutral_policy=direction_neutral_policy,
            direction_label_target=direction_label_target,
        )
        default_event_filters = dict(model_meta.get("event_filters") or {})
        event_filters = self._resolve_event_filter_config(
            min_cp_prob=min_cp_prob if min_cp_prob is not None else default_event_filters.get("min_cp_prob"),
            min_whale_score=(
                min_whale_score if min_whale_score is not None else default_event_filters.get("min_whale_score")
            ),
            min_innovation_abs=(
                min_innovation_abs
                if min_innovation_abs is not None
                else default_event_filters.get("min_innovation_abs")
            ),
        )
        lookback = int(lookback_days or self.train_lookback_days)
        limit = int(max_rows or self.train_max_rows)
        max_h = max(2, int(horizon or self.max_forecast_horizon))
        days_to_eval = max(1, int(holdout_days))

        events = self._load_training_events(
            lookback,
            limit,
            max_events_per_symbol_day=self.train_max_events_per_symbol_day,
            **event_filters,
        )
        daily = self._load_daily_closes(lookback, max_h)
        X, y_dir, y_sessions, _, sample_dates, outcomes = self._build_training_dataset(
            events,
            daily,
            max_h,
            include_outcomes=True,
            **label_config,
        )
        if len(X) == 0:
            raise RuntimeError("No labeled samples available for backtest.")

        dates = np.asarray(sample_dates, dtype=object)
        unique_dates = np.asarray(sorted({value for value in dates if value is not None}), dtype=object)
        if len(unique_dates) == 0:
            raise RuntimeError("No labeled dates available for backtest.")

        selected_dates = unique_dates[-min(days_to_eval, len(unique_dates)) :]
        selected_mask = np.isin(dates, selected_dates)
        selected_idx = np.flatnonzero(selected_mask)
        if len(selected_idx) == 0:
            raise RuntimeError("No labeled samples in selected backtest window.")

        selected_outcomes = self._slice_outcomes(outcomes, selected_idx)
        y_pred, prob_values, sess_pred, default_pred, threshold = self._predict_bundle_outputs(
            bundle,
            X[selected_idx],
            max_h,
        )
        overall_eval = self._evaluate_prediction_outputs(
            y_dir[selected_idx],
            y_sessions[selected_idx],
            y_pred,
            prob_values,
            sess_pred,
            default_pred,
            threshold,
        )
        overall_eval["confidence_slices"] = self._confidence_slice_metrics(
            y_dir[selected_idx],
            y_sessions[selected_idx],
            y_pred,
            prob_values,
            sess_pred,
            default_pred,
            threshold,
        )
        overall_eval["trade_slices"] = self._trade_slice_metrics(
            y_dir[selected_idx],
            y_pred,
            prob_values,
            selected_outcomes,
            direction_return_threshold=float(label_config["direction_return_threshold"]),
        )
        overall_baseline = self._baseline_backtest_metrics(y_dir[selected_idx], y_sessions[selected_idx])

        daily_results: List[Dict[str, Any]] = []
        for current_date in selected_dates:
            day_idx = np.flatnonzero(dates == current_date)
            if len(day_idx) == 0:
                continue
            day_pred, day_prob, day_sess_pred, day_default_pred, day_threshold = self._predict_bundle_outputs(
                bundle,
                X[day_idx],
                max_h,
            )
            model_eval = self._evaluate_prediction_outputs(
                y_dir[day_idx],
                y_sessions[day_idx],
                day_pred,
                day_prob,
                day_sess_pred,
                day_default_pred,
                day_threshold,
            )
            trade_slices = self._trade_slice_metrics(
                y_dir[day_idx],
                day_pred,
                day_prob,
                self._slice_outcomes(outcomes, day_idx),
                direction_return_threshold=float(label_config["direction_return_threshold"]),
            )
            baseline_eval = self._baseline_backtest_metrics(y_dir[day_idx], y_sessions[day_idx])
            daily_results.append(
                {
                    "date": current_date.isoformat(),
                    "samples": int(len(day_idx)),
                    "up_ratio": float(np.mean(y_dir[day_idx])),
                    "sessions_mean": float(np.mean(y_sessions[day_idx])),
                    "sessions_distribution": self._distribution_payload(y_sessions[day_idx]),
                    "model": model_eval["metrics"],
                    "scores": model_eval["scores"],
                    "trade_slices": trade_slices,
                    "baseline": baseline_eval,
                }
            )

        def daily_mean(path: str) -> Optional[float]:
            values: List[Optional[float]] = []
            for row in daily_results:
                cursor: Any = row
                for part in path.split("."):
                    if not isinstance(cursor, dict):
                        cursor = None
                        break
                    cursor = cursor.get(part)
                values.append(cursor)
            return self._mean_metric(values)

        return {
            "backtested_at": _utc_now().isoformat(),
            "model": {
                "model_name": model_meta.get("model_name", self.registered_model_name),
                "model_version": model_meta.get("model_version", model_meta.get("version")),
                "model_source": model_meta.get("model_source", self.model_source or "local"),
                "selected_models": model_meta.get("selected_models", {}),
                "direction_threshold": model_meta.get("direction_threshold", 0.5),
            },
            "params": {
                "lookback_days": lookback,
                "max_rows": limit,
                "horizon_sessions": max_h,
                "holdout_days": int(days_to_eval),
                "max_events_per_symbol_day": self.train_max_events_per_symbol_day,
                "confidence_coverages": [0.1, 0.2, 0.3],
                "trade_coverages": [0.1, 0.2, 0.3],
                **label_config,
                "event_filters": event_filters,
            },
            "samples": int(len(selected_idx)),
            "unique_date_count": int(len(unique_dates)),
            "evaluated_date_count": int(len(daily_results)),
            "date_start": selected_dates[0].isoformat(),
            "date_end": selected_dates[-1].isoformat(),
            "overall": {
                "model": overall_eval["metrics"],
                "scores": overall_eval["scores"],
                "confidence_slices": overall_eval.get("confidence_slices", []),
                "trade_slices": overall_eval.get("trade_slices", []),
                "baseline": overall_baseline,
            },
            "daily_summary": {
                "accuracy_mean": daily_mean("model.accuracy"),
                "balanced_accuracy_mean": daily_mean("model.balanced_accuracy"),
                "roc_auc_mean": daily_mean("model.roc_auc"),
                "f1_direction_mean": daily_mean("model.f1_direction"),
                "mae_sessions_mean": daily_mean("model.mae_sessions"),
                "rmse_sessions_mean": daily_mean("model.rmse_sessions"),
                "majority_accuracy_mean": daily_mean("baseline.majority_accuracy"),
                "one_session_mae_mean": daily_mean("baseline.one_session_mae"),
            },
            "daily": daily_results,
        }

    def _predict_bundle_outputs(
        self,
        bundle: Dict[str, Any],
        X_test: np.ndarray,
        horizon: int,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, float]:
        classifier = bundle["classifier"]
        regressor = bundle["regressor"]
        meta = dict(bundle.get("meta", {}))
        direction_threshold = float(meta.get("direction_threshold", 0.5) or 0.5)
        X_eval = self._align_feature_matrix_for_bundle(bundle, X_test)

        default_pred = np.asarray(classifier.predict(X_eval), dtype=np.int64)
        prob_up, _ = self._classifier_prob_up(classifier, X_eval)
        if prob_up is None:
            prob_up = np.full(len(default_pred), np.nan, dtype=np.float64)
            y_dir_pred = default_pred
        else:
            prob_up = np.asarray(prob_up, dtype=np.float64)
            y_dir_pred = (prob_up >= direction_threshold).astype(np.int64)

        y_sess_pred = np.clip(regressor.predict(X_eval), 1.0, float(horizon))
        return y_dir_pred, prob_up, y_sess_pred, default_pred, direction_threshold

    def _evaluate_prediction_outputs(
        self,
        y_dir_true: np.ndarray,
        y_sess_true: np.ndarray,
        y_dir_pred: np.ndarray,
        prob_up: np.ndarray,
        y_sess_pred: np.ndarray,
        default_pred: np.ndarray,
        direction_threshold: float,
    ) -> Dict[str, Any]:
        direction_scores = self._score_direction_predictions(y_dir_true, y_dir_pred)
        default_scores = self._score_direction_predictions(y_dir_true, default_pred)

        roc_auc: Optional[float] = None
        prob_values = np.asarray(prob_up, dtype=np.float64)
        if len(np.unique(y_dir_true)) >= 2 and np.all(np.isfinite(prob_values)):
            try:
                roc_auc = _safe_metric_value(roc_auc_score(y_dir_true, prob_values))
            except Exception:
                roc_auc = None

        mae = _safe_metric_value(mean_absolute_error(y_sess_true, y_sess_pred))
        rmse = _safe_metric_value(np.sqrt(mean_squared_error(y_sess_true, y_sess_pred)))
        direction_score = roc_auc if roc_auc is not None else direction_scores.get("accuracy")

        return {
            "metrics": {
                "accuracy": direction_scores.get("accuracy"),
                "balanced_accuracy": direction_scores.get("balanced_accuracy"),
                "roc_auc": roc_auc,
                "f1_direction": direction_scores.get("f1_direction"),
                "default_accuracy": default_scores.get("accuracy"),
                "default_balanced_accuracy": default_scores.get("balanced_accuracy"),
                "default_f1_direction": default_scores.get("f1_direction"),
                "direction_threshold": float(direction_threshold),
                "mae_sessions": mae,
                "rmse_sessions": rmse,
            },
            "scores": {
                "direction": direction_score,
                "sessions": None if mae is None else float(-mae),
            },
        }

    @staticmethod
    def _slice_outcomes(outcomes: Dict[str, np.ndarray], indices: np.ndarray) -> Dict[str, np.ndarray]:
        return {
            str(key): np.asarray(values)[indices]
            for key, values in dict(outcomes or {}).items()
        }

    @staticmethod
    def _concat_outcomes(outcome_payloads: List[Dict[str, np.ndarray]]) -> Dict[str, np.ndarray]:
        payloads = [payload for payload in outcome_payloads if payload]
        if not payloads:
            return {}

        keys = sorted(set().union(*(payload.keys() for payload in payloads)))
        merged: Dict[str, np.ndarray] = {}
        for key in keys:
            arrays = [np.asarray(payload[key]) for payload in payloads if key in payload]
            if arrays:
                merged[str(key)] = np.concatenate(arrays)
        return merged

    @staticmethod
    def _safe_array_mean(values: np.ndarray) -> Optional[float]:
        if len(values) == 0:
            return None
        return _safe_metric_value(np.mean(values))

    @staticmethod
    def _safe_array_sum(values: np.ndarray) -> Optional[float]:
        if len(values) == 0:
            return None
        return _safe_metric_value(np.sum(values))

    def _trade_slice_metrics(
        self,
        y_dir_true: np.ndarray,
        y_dir_pred: np.ndarray,
        prob_up: np.ndarray,
        outcomes: Optional[Dict[str, np.ndarray]],
        direction_return_threshold: float = 0.0,
        coverages: Optional[List[float]] = None,
    ) -> List[Dict[str, Any]]:
        required = (
            "horizon_close_return",
            "future_max_return",
            "future_min_return",
        )
        if not outcomes or any(key not in outcomes for key in required):
            return []
        if len(y_dir_true) == 0:
            return []

        prob_values = np.asarray(prob_up, dtype=np.float64)
        y_dir_true = np.asarray(y_dir_true, dtype=np.int64)
        y_dir_pred = np.asarray(y_dir_pred, dtype=np.int64)
        horizon_return = np.asarray(outcomes["horizon_close_return"], dtype=np.float64)
        future_max_return = np.asarray(outcomes["future_max_return"], dtype=np.float64)
        future_min_return = np.asarray(outcomes["future_min_return"], dtype=np.float64)

        if not (
            len(prob_values)
            == len(y_dir_true)
            == len(y_dir_pred)
            == len(horizon_return)
            == len(future_max_return)
            == len(future_min_return)
        ):
            return []

        finite_mask = (
            np.isfinite(prob_values)
            & np.isfinite(horizon_return)
            & np.isfinite(future_max_return)
            & np.isfinite(future_min_return)
        )
        if not np.any(finite_mask):
            return []

        valid_indices = np.flatnonzero(finite_mask)
        total_samples = int(len(y_dir_true))
        valid_samples = int(len(valid_indices))
        selected_coverages = coverages or [0.1, 0.2, 0.3]
        target_move = max(0.0, float(direction_return_threshold or 0.0))
        slices: List[Dict[str, Any]] = []

        confidence = np.maximum(prob_values, 1.0 - prob_values)
        ranking_specs = [
            ("confidence", "confidence", confidence, None),
            ("prob_up_long", "prob_up", prob_values, 1),
        ]

        for ranking_name, bucket_suffix, rank_score, forced_prediction in ranking_specs:
            ordered_indices = valid_indices[np.argsort(-rank_score[valid_indices], kind="mergesort")]
            for coverage in selected_coverages:
                target_coverage = min(1.0, max(0.0, float(coverage)))
                if target_coverage <= 0.0:
                    continue

                slice_size = min(valid_samples, max(1, int(np.ceil(valid_samples * target_coverage))))
                idx = ordered_indices[:slice_size]
                pred = (
                    np.full(slice_size, int(forced_prediction), dtype=np.int64)
                    if forced_prediction is not None
                    else y_dir_pred[idx]
                )
                actual = y_dir_true[idx]
                close_ret = horizon_return[idx]
                max_ret = future_max_return[idx]
                min_ret = future_min_return[idx]

                directional_return = np.where(pred == 1, close_ret, -close_ret)
                path_best_return = np.where(pred == 1, max_ret, -min_ret)
                direction_correct = pred == actual
                up_mask = pred == 1
                down_mask = pred == 0
                threshold_hit_rate: Optional[float] = None
                if target_move > 0.0:
                    threshold_hit = np.where(pred == 1, max_ret >= target_move, min_ret <= -target_move)
                    threshold_hit_rate = self._safe_array_mean(threshold_hit.astype(np.float64))

                slices.append(
                    {
                        "bucket": f"top_{int(round(target_coverage * 100))}pct_{bucket_suffix}",
                        "ranking": ranking_name,
                        "target_coverage": target_coverage,
                        "coverage": float(slice_size / total_samples),
                        "samples": int(slice_size),
                        "valid_probability_samples": valid_samples,
                        "min_rank_score": _safe_metric_value(np.min(rank_score[idx])),
                        "mean_rank_score": _safe_metric_value(np.mean(rank_score[idx])),
                        "min_confidence": _safe_metric_value(np.min(confidence[idx])),
                        "mean_confidence": _safe_metric_value(np.mean(confidence[idx])),
                        "min_prob_up": _safe_metric_value(np.min(prob_values[idx])),
                        "mean_prob_up": _safe_metric_value(np.mean(prob_values[idx])),
                        "precision_direction": self._safe_array_mean(direction_correct.astype(np.float64)),
                        "actual_up_ratio": self._safe_array_mean(actual.astype(np.float64)),
                        "predicted_up_ratio": self._safe_array_mean(up_mask.astype(np.float64)),
                        "paper_return_mean": self._safe_array_mean(directional_return),
                        "paper_return_sum": self._safe_array_sum(directional_return),
                        "paper_win_rate": self._safe_array_mean((directional_return > 0.0).astype(np.float64)),
                        "path_best_return_mean": self._safe_array_mean(path_best_return),
                        "threshold_hit_rate": threshold_hit_rate,
                        "long_only_samples": int(np.sum(up_mask)),
                        "long_only_return_mean": self._safe_array_mean(close_ret[up_mask]),
                        "long_only_win_rate": self._safe_array_mean((close_ret[up_mask] > 0.0).astype(np.float64)),
                        "down_side_samples": int(np.sum(down_mask)),
                        "down_side_return_mean": self._safe_array_mean((-close_ret[down_mask])),
                        "down_side_win_rate": self._safe_array_mean((-close_ret[down_mask] > 0.0).astype(np.float64)),
                    }
                )

        return slices

    def _confidence_slice_metrics(
        self,
        y_dir_true: np.ndarray,
        y_sess_true: np.ndarray,
        y_dir_pred: np.ndarray,
        prob_up: np.ndarray,
        y_sess_pred: np.ndarray,
        default_pred: np.ndarray,
        direction_threshold: float,
        coverages: Optional[List[float]] = None,
    ) -> List[Dict[str, Any]]:
        if len(y_dir_true) == 0:
            return []

        prob_values = np.asarray(prob_up, dtype=np.float64)
        finite_mask = np.isfinite(prob_values)
        if not np.any(finite_mask):
            return []

        y_dir_true = np.asarray(y_dir_true)
        y_sess_true = np.asarray(y_sess_true)
        y_dir_pred = np.asarray(y_dir_pred)
        y_sess_pred = np.asarray(y_sess_pred)
        default_pred = np.asarray(default_pred)

        confidence = np.maximum(prob_values, 1.0 - prob_values)
        valid_indices = np.flatnonzero(finite_mask)
        ordered_indices = valid_indices[np.argsort(-confidence[valid_indices], kind="mergesort")]
        total_samples = int(len(y_dir_true))
        valid_samples = int(len(ordered_indices))
        selected_coverages = coverages or [0.1, 0.2, 0.3]
        slices: List[Dict[str, Any]] = []

        for coverage in selected_coverages:
            target_coverage = min(1.0, max(0.0, float(coverage)))
            if target_coverage <= 0.0:
                continue

            slice_size = min(valid_samples, max(1, int(np.ceil(valid_samples * target_coverage))))
            idx = ordered_indices[:slice_size]
            model_eval = self._evaluate_prediction_outputs(
                y_dir_true[idx],
                y_sess_true[idx],
                y_dir_pred[idx],
                prob_values[idx],
                y_sess_pred[idx],
                default_pred[idx],
                direction_threshold,
            )
            baseline_eval = self._baseline_backtest_metrics(y_dir_true[idx], y_sess_true[idx])
            accuracy = model_eval["metrics"].get("accuracy")
            majority_accuracy = baseline_eval.get("majority_accuracy")
            accuracy_lift = (
                None
                if accuracy is None or majority_accuracy is None
                else float(accuracy - majority_accuracy)
            )

            slice_confidence = confidence[idx]
            slice_prob = prob_values[idx]
            slices.append(
                {
                    "bucket": f"top_{int(round(target_coverage * 100))}pct_confidence",
                    "target_coverage": target_coverage,
                    "coverage": float(slice_size / total_samples),
                    "samples": int(slice_size),
                    "valid_probability_samples": valid_samples,
                    "probability_coverage": float(valid_samples / total_samples),
                    "min_confidence": _safe_metric_value(np.min(slice_confidence)),
                    "mean_confidence": _safe_metric_value(np.mean(slice_confidence)),
                    "max_confidence": _safe_metric_value(np.max(slice_confidence)),
                    "mean_prob_up": _safe_metric_value(np.mean(slice_prob)),
                    "actual_up_ratio": _safe_metric_value(np.mean(y_dir_true[idx])),
                    "predicted_up_ratio": _safe_metric_value(np.mean(y_dir_pred[idx])),
                    "accuracy_lift_vs_majority": accuracy_lift,
                    "model": model_eval["metrics"],
                    "scores": model_eval["scores"],
                    "baseline": baseline_eval,
                }
            )

        return slices

    def _rolling_overall_metrics(
        self,
        y_dir_true: List[np.ndarray],
        y_sess_true: List[np.ndarray],
        y_dir_pred: List[np.ndarray],
        prob_up: List[np.ndarray],
        y_sess_pred: List[np.ndarray],
        default_pred: List[np.ndarray],
        direction_thresholds: List[float],
        outcomes: Optional[List[Dict[str, np.ndarray]]] = None,
        direction_return_threshold: float = 0.0,
    ) -> Dict[str, Any]:
        if not y_dir_true:
            return {"metrics": {}, "scores": {}, "confidence_slices": [], "trade_slices": []}

        threshold = self._mean_metric(direction_thresholds) or 0.5
        y_dir_true_all = np.concatenate(y_dir_true)
        y_sess_true_all = np.concatenate(y_sess_true)
        y_dir_pred_all = np.concatenate(y_dir_pred)
        prob_up_all = np.concatenate(prob_up)
        y_sess_pred_all = np.concatenate(y_sess_pred)
        default_pred_all = np.concatenate(default_pred)

        result = self._evaluate_prediction_outputs(
            y_dir_true_all,
            y_sess_true_all,
            y_dir_pred_all,
            prob_up_all,
            y_sess_pred_all,
            default_pred_all,
            float(threshold),
        )
        result["confidence_slices"] = self._confidence_slice_metrics(
            y_dir_true_all,
            y_sess_true_all,
            y_dir_pred_all,
            prob_up_all,
            y_sess_pred_all,
            default_pred_all,
            float(threshold),
        )
        result["trade_slices"] = self._trade_slice_metrics(
            y_dir_true_all,
            y_dir_pred_all,
            prob_up_all,
            self._concat_outcomes(outcomes or []),
            direction_return_threshold=float(direction_return_threshold or 0.0),
        )
        return result

    def rolling_backtest(
        self,
        lookback_days: Optional[int] = None,
        max_rows: Optional[int] = None,
        horizon: Optional[int] = None,
        holdout_days: int = 5,
        min_train_days: int = 3,
        min_train_samples: Optional[int] = None,
        train_window_days: Optional[int] = None,
        max_events_per_symbol_day: Optional[int] = None,
        event_selection_strategy: Optional[str] = None,
        direction_return_threshold: Optional[float] = None,
        direction_neutral_policy: Optional[str] = None,
        direction_label_target: Optional[str] = None,
        min_cp_prob: Optional[float] = None,
        min_whale_score: Optional[float] = None,
        min_innovation_abs: Optional[float] = None,
    ) -> Dict[str, Any]:
        with self.train_lock:
            lookback = int(lookback_days or self.train_lookback_days)
            limit = int(max_rows or self.train_max_rows)
            max_h = max(2, int(horizon or self.max_forecast_horizon))
            days_to_eval = max(1, int(holdout_days))
            min_dates = max(1, int(min_train_days))
            min_samples = int(min_train_samples or self.global_min_train_samples)
            train_window = None if train_window_days is None else max(1, int(train_window_days))
            per_day_cap = max(
                1,
                int(max_events_per_symbol_day or self.train_max_events_per_symbol_day),
            )
            _, event_strategy = self._event_order_by_clause(
                event_selection_strategy or getattr(self, "train_event_selection_strategy", "latest")
            )
            label_config = self._resolve_direction_label_config(
                direction_return_threshold=direction_return_threshold,
                direction_neutral_policy=direction_neutral_policy,
                direction_label_target=direction_label_target,
            )
            event_filters = self._resolve_event_filter_config(
                min_cp_prob=min_cp_prob,
                min_whale_score=min_whale_score,
                min_innovation_abs=min_innovation_abs,
            )

            events = self._load_training_events(
                lookback,
                limit,
                max_events_per_symbol_day=per_day_cap,
                event_selection_strategy=event_strategy,
                **event_filters,
            )
            daily = self._load_daily_closes(lookback, max_h)
            X, y_dir, y_sessions, sample_groups, sample_dates, outcomes = self._build_training_dataset(
                events,
                daily,
                max_h,
                include_outcomes=True,
                **label_config,
            )
            if len(X) == 0:
                raise RuntimeError("No labeled samples available for rolling backtest.")

            dates = np.asarray(sample_dates, dtype=object)
            unique_dates = np.asarray(sorted({value for value in dates if value is not None}), dtype=object)
            if len(unique_dates) <= min_dates:
                raise RuntimeError(
                    f"Not enough labeled dates ({len(unique_dates)}) for rolling backtest with min_train_days={min_dates}."
                )

            candidate_dates = unique_dates[min_dates:]
            selected_dates = candidate_dates[-min(days_to_eval, len(candidate_dates)) :]
            daily_results: List[Dict[str, Any]] = []
            skipped: List[Dict[str, Any]] = []
            all_y_dir_true: List[np.ndarray] = []
            all_y_sess_true: List[np.ndarray] = []
            all_y_dir_pred: List[np.ndarray] = []
            all_prob_up: List[np.ndarray] = []
            all_y_sess_pred: List[np.ndarray] = []
            all_default_pred: List[np.ndarray] = []
            all_thresholds: List[float] = []
            all_outcomes: List[Dict[str, np.ndarray]] = []

            for current_date in selected_dates:
                train_mask = dates < current_date
                if train_window is not None:
                    min_ordinal = current_date.toordinal() - train_window
                    train_mask = train_mask & np.asarray(
                        [
                            value is not None and value.toordinal() >= min_ordinal
                            for value in dates
                        ],
                        dtype=bool,
                    )
                test_mask = dates == current_date
                train_idx = np.flatnonzero(train_mask)
                test_idx = np.flatnonzero(test_mask)
                train_dates = sorted({value for value in dates[train_idx] if value is not None})

                if len(test_idx) == 0:
                    skipped.append({"date": current_date.isoformat(), "reason": "no_test_samples"})
                    continue
                if len(train_dates) < min_dates or len(train_idx) < min_samples:
                    skipped.append(
                        {
                            "date": current_date.isoformat(),
                            "reason": "insufficient_train_data",
                            "train_date_count": int(len(train_dates)),
                            "train_size": int(len(train_idx)),
                        }
                    )
                    continue
                if len(np.unique(y_dir[train_idx])) < 2:
                    skipped.append({"date": current_date.isoformat(), "reason": "single_class_train"})
                    continue

                try:
                    train_weight, train_weight_meta = self._build_recency_sample_weights(dates[train_idx])
                    training_result = self._run_single_split_training_round(
                        X[train_idx],
                        y_dir[train_idx],
                        y_sessions[train_idx],
                        max_h,
                        sample_groups=sample_groups[train_idx],
                        sample_weight=train_weight,
                        sample_weight_meta=train_weight_meta,
                    )
                    meta = self._build_base_meta(
                        model_scope="rolling_backtest",
                        samples=len(train_idx),
                        lookback_days=lookback,
                        max_rows=limit,
                        horizon=max_h,
                        y_dir=y_dir[train_idx],
                        training_result=training_result,
                        direction_label_config=label_config,
                        event_filter_config=event_filters,
                    )
                    bundle = self._build_bundle(training_result, meta)
                    y_pred, prob_values, sess_pred, default_pred, threshold = self._predict_bundle_outputs(
                        bundle,
                        X[test_idx],
                        max_h,
                    )
                    model_eval = self._evaluate_prediction_outputs(
                        y_dir[test_idx],
                        y_sessions[test_idx],
                        y_pred,
                        prob_values,
                        sess_pred,
                        default_pred,
                        threshold,
                    )
                    test_outcomes = self._slice_outcomes(outcomes, test_idx)
                    trade_slices = self._trade_slice_metrics(
                        y_dir[test_idx],
                        y_pred,
                        prob_values,
                        test_outcomes,
                        direction_return_threshold=float(label_config["direction_return_threshold"]),
                    )
                except Exception as exc:
                    skipped.append(
                        {
                            "date": current_date.isoformat(),
                            "reason": "train_or_eval_failed",
                            "error": str(exc),
                        }
                    )
                    continue

                baseline_eval = self._baseline_backtest_metrics(y_dir[test_idx], y_sessions[test_idx])
                all_y_dir_true.append(y_dir[test_idx])
                all_y_sess_true.append(y_sessions[test_idx])
                all_y_dir_pred.append(y_pred)
                all_prob_up.append(prob_values)
                all_y_sess_pred.append(sess_pred)
                all_default_pred.append(default_pred)
                all_thresholds.append(float(threshold))
                all_outcomes.append(test_outcomes)

                daily_results.append(
                    {
                        "date": current_date.isoformat(),
                        "train_start_date": train_dates[0].isoformat(),
                        "train_end_date": train_dates[-1].isoformat(),
                        "train_date_count": int(len(train_dates)),
                        "train_size": int(len(train_idx)),
                        "test_size": int(len(test_idx)),
                        "up_ratio": float(np.mean(y_dir[test_idx])),
                        "selected_models": meta.get("selected_models", {}),
                        "direction_threshold": float(threshold),
                        "model": model_eval["metrics"],
                        "scores": model_eval["scores"],
                        "trade_slices": trade_slices,
                        "baseline": baseline_eval,
                    }
                )

            if not daily_results:
                raise RuntimeError(f"No rolling backtest dates could be evaluated: {skipped}")

            overall_eval = self._rolling_overall_metrics(
                all_y_dir_true,
                all_y_sess_true,
                all_y_dir_pred,
                all_prob_up,
                all_y_sess_pred,
                all_default_pred,
                all_thresholds,
                outcomes=all_outcomes,
                direction_return_threshold=float(label_config["direction_return_threshold"]),
            )
            overall_baseline = self._baseline_backtest_metrics(
                np.concatenate(all_y_dir_true),
                np.concatenate(all_y_sess_true),
            )

            def daily_mean(path: str) -> Optional[float]:
                values: List[Optional[float]] = []
                for row in daily_results:
                    cursor: Any = row
                    for part in path.split("."):
                        if not isinstance(cursor, dict):
                            cursor = None
                            break
                        cursor = cursor.get(part)
                    values.append(cursor)
                return self._mean_metric(values)

            return {
                "backtested_at": _utc_now().isoformat(),
                "params": {
                    "lookback_days": lookback,
                    "max_rows": limit,
                    "horizon_sessions": max_h,
                    "holdout_days": days_to_eval,
                    "min_train_days": min_dates,
                    "min_train_samples": min_samples,
                    "train_window_days": train_window,
                    "max_events_per_symbol_day": per_day_cap,
                    "event_selection_strategy": event_strategy,
                    "confidence_coverages": [0.1, 0.2, 0.3],
                    "trade_coverages": [0.1, 0.2, 0.3],
                    **label_config,
                    "event_filters": event_filters,
                },
                "samples": int(sum(len(values) for values in all_y_dir_true)),
                "loaded_samples": int(len(X)),
                "unique_date_count": int(len(unique_dates)),
                "evaluated_date_count": int(len(daily_results)),
                "skipped_date_count": int(len(skipped)),
                "date_start": daily_results[0]["date"],
                "date_end": daily_results[-1]["date"],
                "overall": {
                    "model": overall_eval["metrics"],
                    "scores": overall_eval["scores"],
                    "confidence_slices": overall_eval.get("confidence_slices", []),
                    "trade_slices": overall_eval.get("trade_slices", []),
                    "baseline": overall_baseline,
                },
                "daily_summary": {
                    "accuracy_mean": daily_mean("model.accuracy"),
                    "balanced_accuracy_mean": daily_mean("model.balanced_accuracy"),
                    "roc_auc_mean": daily_mean("model.roc_auc"),
                    "f1_direction_mean": daily_mean("model.f1_direction"),
                    "mae_sessions_mean": daily_mean("model.mae_sessions"),
                    "rmse_sessions_mean": daily_mean("model.rmse_sessions"),
                    "majority_accuracy_mean": daily_mean("baseline.majority_accuracy"),
                    "one_session_mae_mean": daily_mean("baseline.one_session_mae"),
                },
                "daily": daily_results,
                "skipped": skipped,
            }

    @staticmethod
    def _find_trade_slice(
        trade_slices: List[Dict[str, Any]],
        *,
        ranking: str,
        bucket: str,
    ) -> Optional[Dict[str, Any]]:
        for item in trade_slices:
            if str(item.get("ranking")) == ranking and str(item.get("bucket")) == bucket:
                return item
        return None

    def rolling_backtest_scan(
        self,
        lookback_days: Optional[int] = None,
        max_rows: Optional[int] = None,
        horizon: Optional[int] = None,
        holdout_days: int = 5,
        min_train_days: int = 3,
        min_train_samples: Optional[int] = None,
        train_window_days: Optional[int] = None,
        max_events_per_symbol_day: Optional[int] = None,
        event_selection_strategy: Optional[str] = None,
        direction_return_thresholds: Optional[List[float]] = None,
        max_events_per_symbol_day_options: Optional[List[int]] = None,
        direction_neutral_policy: Optional[str] = None,
        direction_label_target: Optional[str] = None,
        min_cp_prob: Optional[float] = None,
        min_whale_score: Optional[float] = None,
        min_innovation_abs: Optional[float] = None,
    ) -> Dict[str, Any]:
        threshold_values = direction_return_thresholds or [self.direction_return_threshold]
        thresholds = sorted({max(0.0, float(value)) for value in threshold_values})
        if not thresholds:
            thresholds = [max(0.0, float(self.direction_return_threshold))]

        cap_values = max_events_per_symbol_day_options or [max_events_per_symbol_day or self.train_max_events_per_symbol_day]
        caps = sorted({max(1, int(value)) for value in cap_values})

        scan_rows: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []

        for per_day_cap in caps:
            for threshold in thresholds:
                try:
                    result = self.rolling_backtest(
                        lookback_days=lookback_days,
                        max_rows=max_rows,
                        horizon=horizon,
                        holdout_days=holdout_days,
                        min_train_days=min_train_days,
                        min_train_samples=min_train_samples,
                        train_window_days=train_window_days,
                        max_events_per_symbol_day=per_day_cap,
                        event_selection_strategy=event_selection_strategy,
                        direction_return_threshold=threshold,
                        direction_neutral_policy=direction_neutral_policy,
                        direction_label_target=direction_label_target,
                        min_cp_prob=min_cp_prob,
                        min_whale_score=min_whale_score,
                        min_innovation_abs=min_innovation_abs,
                    )
                except Exception as exc:
                    failures.append(
                        {
                            "max_events_per_symbol_day": int(per_day_cap),
                            "direction_return_threshold": float(threshold),
                            "error": str(exc),
                        }
                    )
                    continue

                overall = dict(result.get("overall") or {})
                model_metrics = dict(overall.get("model") or {})
                trade_slices = list(overall.get("trade_slices") or [])
                top10_prob_up = self._find_trade_slice(
                    trade_slices,
                    ranking="prob_up_long",
                    bucket="top_10pct_prob_up",
                ) or {}
                top10_confidence = self._find_trade_slice(
                    trade_slices,
                    ranking="confidence",
                    bucket="top_10pct_confidence",
                ) or {}

                scan_rows.append(
                    {
                        "params": {
                            "max_events_per_symbol_day": int(per_day_cap),
                            "direction_return_threshold": float(threshold),
                        },
                        "samples": int(result.get("samples", 0)),
                        "loaded_samples": int(result.get("loaded_samples", 0)),
                        "evaluated_date_count": int(result.get("evaluated_date_count", 0)),
                        "overall_model": {
                            "accuracy": model_metrics.get("accuracy"),
                            "balanced_accuracy": model_metrics.get("balanced_accuracy"),
                            "roc_auc": model_metrics.get("roc_auc"),
                            "f1_direction": model_metrics.get("f1_direction"),
                        },
                        "top10_prob_up": {
                            "precision_direction": top10_prob_up.get("precision_direction"),
                            "paper_win_rate": top10_prob_up.get("paper_win_rate"),
                            "paper_return_mean": top10_prob_up.get("paper_return_mean"),
                            "threshold_hit_rate": top10_prob_up.get("threshold_hit_rate"),
                            "samples": top10_prob_up.get("samples"),
                            "mean_prob_up": top10_prob_up.get("mean_prob_up"),
                        },
                        "top10_confidence": {
                            "precision_direction": top10_confidence.get("precision_direction"),
                            "paper_win_rate": top10_confidence.get("paper_win_rate"),
                            "paper_return_mean": top10_confidence.get("paper_return_mean"),
                            "threshold_hit_rate": top10_confidence.get("threshold_hit_rate"),
                            "samples": top10_confidence.get("samples"),
                            "mean_confidence": top10_confidence.get("mean_confidence"),
                        },
                    }
                )

        if not scan_rows:
            raise RuntimeError(f"No valid rolling backtest scan result. Failures: {failures}")

        max_loaded = max(row.get("loaded_samples", 0) for row in scan_rows) or 1
        for row in scan_rows:
            row["loaded_sample_ratio_vs_max"] = float(row.get("loaded_samples", 0) / max_loaded)

        ranked_by_top10 = sorted(
            scan_rows,
            key=lambda row: (
                float(row.get("top10_prob_up", {}).get("precision_direction") or float("-inf")),
                float(row.get("top10_prob_up", {}).get("paper_win_rate") or float("-inf")),
                float(row.get("top10_prob_up", {}).get("paper_return_mean") or float("-inf")),
            ),
            reverse=True,
        )
        ranked_by_accuracy = sorted(
            scan_rows,
            key=lambda row: float(row.get("overall_model", {}).get("accuracy") or float("-inf")),
            reverse=True,
        )

        return {
            "scanned_at": _utc_now().isoformat(),
            "scan_space": {
                "direction_return_thresholds": thresholds,
                "max_events_per_symbol_day_options": caps,
                "lookback_days": lookback_days or self.train_lookback_days,
                "max_rows": max_rows or self.train_max_rows,
                "horizon": horizon or self.max_forecast_horizon,
                "holdout_days": holdout_days,
                "min_train_days": min_train_days,
                "min_train_samples": min_train_samples or self.global_min_train_samples,
                "train_window_days": train_window_days,
                "event_selection_strategy": event_selection_strategy or self.train_event_selection_strategy,
                "direction_neutral_policy": direction_neutral_policy or self.direction_neutral_policy,
                "direction_label_target": direction_label_target or self.direction_label_target,
                "min_cp_prob": min_cp_prob if min_cp_prob is not None else self.train_min_cp_prob,
                "min_whale_score": min_whale_score if min_whale_score is not None else self.train_min_whale_score,
                "min_innovation_abs": (
                    min_innovation_abs if min_innovation_abs is not None else self.train_min_innovation_abs
                ),
            },
            "best_by_top10_prob_up_precision": ranked_by_top10[0],
            "best_by_overall_accuracy": ranked_by_accuracy[0],
            "rows_sorted_by_top10_prob_up_precision": ranked_by_top10,
            "rows_sorted_by_overall_accuracy": ranked_by_accuracy,
            "failures": failures,
        }

    # MLflow registration helpers
    def _wait_until_model_version_ready(
        self,
        client: MlflowClient,
        model_name: str,
        model_version: str,
        timeout_seconds: int = 180,
    ):
        deadline = time.time() + float(timeout_seconds)
        while time.time() < deadline:
            info = client.get_model_version(model_name, model_version)
            status = str(info.status or "").upper()
            if status == "READY":
                return info
            if status in {"FAILED_REGISTRATION", "FAILED"}:
                raise RuntimeError(
                    f"Model version {model_name}/{model_version} failed with status={status}"
                )
            time.sleep(2)

        raise TimeoutError(
            f"Timed out waiting MLflow model version READY: {model_name}/{model_version}"
        )

    def _log_bundle_to_mlflow(
        self,
        bundle_path: Path,
        train_params: Dict[str, Any],
        metrics: Dict[str, Optional[float]],
        base_meta: Dict[str, Any],
        model_name: str,
        aliases_to_set: List[str],
        run_name_prefix: str,
    ) -> Dict[str, Any]:
        self._configure_mlflow()

        active_run = mlflow.active_run()
        if active_run is not None:
            logger.warning(
                "Closing stale active MLflow run before training: run_id=%s",
                active_run.info.run_id,
            )
            mlflow.end_run()

        run_name = f"{run_name_prefix}_{base_meta.get('version', 'unknown')}"
        with mlflow.start_run(run_name=run_name) as run:
            run_id = run.info.run_id

            mlflow.log_params({k: v for k, v in train_params.items() if v is not None})
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
            registered = mlflow.register_model(run_model_uri, model_name)
            client = MlflowClient()
            registered_info = self._wait_until_model_version_ready(
                client,
                model_name,
                str(registered.version),
            )
            model_version = str(registered_info.version)

            for alias in sorted({a.strip() for a in aliases_to_set if str(a or "").strip()}):
                client.set_registered_model_alias(model_name, alias, model_version)

            client.set_model_version_tag(model_name, model_version, "pipeline", "whale_ml_retrain_pipeline")

            registry_uri = f"models:/{model_name}/{model_version}"
            alias_uri = None
            if aliases_to_set:
                alias_uri = f"models:/{model_name}@{aliases_to_set[0]}"

            mlflow.set_tags(
                {
                    "registered_model_name": model_name,
                    "registered_model_version": model_version,
                    "registry_uri": registry_uri,
                    "model_scope": str(base_meta.get("model_scope", "global")),
                    "symbol": str(base_meta.get("symbol") or ""),
                }
            )

            result = {
                "mlflow_run_id": run_id,
                "model_name": model_name,
                "model_version": model_version,
                "model_uri": registry_uri,
                "model_aliases": aliases_to_set,
                "model_source": "registry",
            }
            if alias_uri:
                result["model_alias_uri"] = alias_uri
            if self.model_alias in aliases_to_set:
                result["model_alias"] = self.model_alias
            elif aliases_to_set:
                result["model_alias"] = aliases_to_set[0]
            return result

    # Training flows
    def _build_bundle(
        self,
        training_result: Dict[str, Any],
        meta: Dict[str, Any],
    ) -> Dict[str, Any]:
        best_classifier = training_result["best_classifier"]
        best_regressor = training_result["best_regressor"]
        return {
            "classifier": best_classifier["model"],
            "regressor": best_regressor["model"],
            "classifier_name": best_classifier["name"],
            "regressor_name": best_regressor["name"],
            "feature_names": list(FEATURE_NAMES),
            "meta": dict(meta),
        }

    def _build_base_meta(
        self,
        *,
        model_scope: str,
        samples: int,
        lookback_days: int,
        max_rows: int,
        horizon: int,
        y_dir: np.ndarray,
        training_result: Dict[str, Any],
        symbol: Optional[str] = None,
        direction_label_config: Optional[Dict[str, Any]] = None,
        event_filter_config: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        best_metrics = dict(training_result["best_metrics"])
        best_classifier = training_result["best_classifier"]
        best_regressor = training_result["best_regressor"]
        label_config = dict(direction_label_config or self._resolve_direction_label_config())
        event_filters = dict(event_filter_config or self._resolve_event_filter_config())

        meta = {
            "trained_at": _utc_now().isoformat(),
            "version": _utc_now().strftime("%Y%m%d%H%M%S"),
            "model_scope": model_scope,
            "samples": int(samples),
            "lookback_days": int(lookback_days),
            "max_rows": int(max_rows),
            "horizon_sessions": int(horizon),
            "up_ratio": float(np.mean(y_dir)),
            "direction_return_threshold": float(label_config["direction_return_threshold"]),
            "direction_neutral_policy": str(label_config["direction_neutral_policy"]),
            "direction_label_target": str(label_config["direction_label_target"]),
            "event_filters": event_filters,
            "feature_names": list(FEATURE_NAMES),
            "features_count": len(FEATURE_NAMES),
            "metrics": best_metrics,
            "selected_models": {
                "direction": best_classifier["name"],
                "sessions": best_regressor["name"],
            },
            "direction_threshold": float(best_classifier.get("direction_threshold") or 0.5),
            "direction_threshold_metric": str(
                best_classifier.get("direction_threshold_metric")
                or getattr(self, "direction_threshold_metric", "balanced_accuracy")
            ),
            "model_candidates": {
                "direction": training_result["classifier_leaderboard"],
                "sessions": training_result["regressor_leaderboard"],
            },
            "evaluation_split": dict(training_result.get("split_meta", {})),
        }
        if symbol:
            meta["symbol"] = symbol
        return meta

    def _compare_symbol_vs_global(
        self,
        challenger_bundle: Dict[str, Any],
        X_test: np.ndarray,
        y_dir_test: np.ndarray,
        y_sess_test: np.ndarray,
        horizon: int,
        force_promote: bool,
    ) -> Dict[str, Any]:
        challenger_eval = self._evaluate_bundle_on_holdout(
            challenger_bundle,
            X_test,
            y_dir_test,
            y_sess_test,
            horizon,
        )

        with self.lock:
            global_bundle = self.bundle

        if global_bundle is None:
            return {
                "baseline": "missing_global",
                "challenger": challenger_eval,
                "decision": {
                    "promote": bool(force_promote),
                    "reason": "global_model_not_ready",
                    "direction_delta": None,
                    "sessions_delta": None,
                    "min_direction_delta": self.symbol_min_direction_delta,
                    "min_sessions_delta": self.symbol_min_sessions_delta,
                    "require_both": self.symbol_require_both_deltas,
                },
            }

        champion_eval = self._evaluate_bundle_on_holdout(
            global_bundle,
            X_test,
            y_dir_test,
            y_sess_test,
            horizon,
        )

        challenger_direction = challenger_eval["scores"].get("direction")
        challenger_sessions = challenger_eval["scores"].get("sessions")
        champion_direction = champion_eval["scores"].get("direction")
        champion_sessions = champion_eval["scores"].get("sessions")

        direction_delta: Optional[float]
        sessions_delta: Optional[float]

        if challenger_direction is None or champion_direction is None:
            direction_delta = None
            passes_direction = False
        else:
            direction_delta = float(challenger_direction) - float(champion_direction)
            passes_direction = direction_delta >= float(self.symbol_min_direction_delta)

        if challenger_sessions is None or champion_sessions is None:
            sessions_delta = None
            passes_sessions = False
        else:
            sessions_delta = float(challenger_sessions) - float(champion_sessions)
            passes_sessions = sessions_delta >= float(self.symbol_min_sessions_delta)

        if force_promote:
            promote = True
            reason = "force_promote"
        elif self.symbol_require_both_deltas:
            promote = bool(passes_direction and passes_sessions)
            reason = "beat_global_on_both" if promote else "did_not_beat_global_on_both"
        else:
            promote = bool(passes_direction or passes_sessions)
            reason = "beat_global_on_either" if promote else "did_not_beat_global"

        return {
            "baseline": "global",
            "champion": champion_eval,
            "challenger": challenger_eval,
            "decision": {
                "promote": promote,
                "reason": reason,
                "direction_delta": direction_delta,
                "sessions_delta": sessions_delta,
                "min_direction_delta": self.symbol_min_direction_delta,
                "min_sessions_delta": self.symbol_min_sessions_delta,
                "require_both": self.symbol_require_both_deltas,
            },
        }

    def _compare_global_vs_champion(
        self,
        challenger_bundle: Dict[str, Any],
        X_test: np.ndarray,
        y_dir_test: np.ndarray,
        y_sess_test: np.ndarray,
        horizon: int,
    ) -> Dict[str, Any]:
        challenger_eval = self._evaluate_bundle_on_holdout(
            challenger_bundle,
            X_test,
            y_dir_test,
            y_sess_test,
            horizon,
        )

        with self.lock:
            champion_bundle = self.bundle

        if champion_bundle is None:
            return {
                "baseline": "missing_global",
                "challenger": challenger_eval,
                "decision": {
                    "promote": True,
                    "reason": "global_model_not_ready",
                    "direction_delta": None,
                    "sessions_delta": None,
                    "min_direction_delta": self.global_min_direction_delta,
                    "min_sessions_delta": self.global_min_sessions_delta,
                    "require_both": self.global_require_both_deltas,
                },
            }

        champion_eval = self._evaluate_bundle_on_holdout(
            champion_bundle,
            X_test,
            y_dir_test,
            y_sess_test,
            horizon,
        )

        challenger_direction = challenger_eval["scores"].get("direction")
        challenger_sessions = challenger_eval["scores"].get("sessions")
        champion_direction = champion_eval["scores"].get("direction")
        champion_sessions = champion_eval["scores"].get("sessions")

        direction_delta: Optional[float]
        sessions_delta: Optional[float]

        if challenger_direction is None or champion_direction is None:
            direction_delta = None
            passes_direction = False
        else:
            direction_delta = float(challenger_direction) - float(champion_direction)
            passes_direction = direction_delta >= float(self.global_min_direction_delta)

        if challenger_sessions is None or champion_sessions is None:
            sessions_delta = None
            passes_sessions = False
        else:
            sessions_delta = float(challenger_sessions) - float(champion_sessions)
            passes_sessions = sessions_delta >= float(self.global_min_sessions_delta)

        improves_any_metric = any(
            delta is not None and float(delta) > 0.0
            for delta in (direction_delta, sessions_delta)
        )
        if self.global_require_both_deltas:
            promote = bool(passes_direction and passes_sessions and improves_any_metric)
            reason = "beat_champion_on_both" if promote else "did_not_beat_champion_on_both"
        else:
            promote = bool((passes_direction or passes_sessions) and improves_any_metric)
            reason = "beat_champion_on_either" if promote else "did_not_beat_champion"

        return {
            "baseline": "global",
            "champion": champion_eval,
            "challenger": challenger_eval,
            "decision": {
                "promote": promote,
                "reason": reason,
                "direction_delta": direction_delta,
                "sessions_delta": sessions_delta,
                "min_direction_delta": self.global_min_direction_delta,
                "min_sessions_delta": self.global_min_sessions_delta,
                "require_both": self.global_require_both_deltas,
                "improves_any_metric": improves_any_metric,
            },
        }

    def train_experiment(
        self,
        lookback_days: Optional[int] = None,
        max_rows: Optional[int] = None,
        horizon: Optional[int] = None,
        direction_return_threshold: Optional[float] = None,
        direction_neutral_policy: Optional[str] = None,
        direction_label_target: Optional[str] = None,
        min_cp_prob: Optional[float] = None,
        min_whale_score: Optional[float] = None,
        min_innovation_abs: Optional[float] = None,
    ) -> Dict[str, Any]:
        with self.train_lock:
            lookback = int(lookback_days or self.train_lookback_days)
            limit = int(max_rows or self.train_max_rows)
            max_h = max(2, int(horizon or self.max_forecast_horizon))
            label_config = self._resolve_direction_label_config(
                direction_return_threshold=direction_return_threshold,
                direction_neutral_policy=direction_neutral_policy,
                direction_label_target=direction_label_target,
            )
            event_filters = self._resolve_event_filter_config(
                min_cp_prob=min_cp_prob,
                min_whale_score=min_whale_score,
                min_innovation_abs=min_innovation_abs,
            )

            events = self._load_training_events(
                lookback,
                limit,
                max_events_per_symbol_day=self.train_max_events_per_symbol_day,
                **event_filters,
            )
            daily = self._load_daily_closes(lookback, max_h)
            X, y_dir, y_sessions, sample_groups, sample_dates = self._build_training_dataset(
                events,
                daily,
                max_h,
                **label_config,
            )

            if len(X) < self.global_min_train_samples:
                raise RuntimeError(
                    f"Training data too small ({len(X)} rows). Need at least {self.global_min_train_samples} labeled samples."
                )

            training_result = self._run_training_round(
                X,
                y_dir,
                y_sessions,
                max_h,
                sample_groups=sample_groups,
                sample_dates=sample_dates,
            )
            meta = self._build_base_meta(
                model_scope="global_experiment",
                samples=len(X),
                lookback_days=lookback,
                max_rows=limit,
                horizon=max_h,
                y_dir=y_dir,
                training_result=training_result,
                direction_label_config=label_config,
                event_filter_config=event_filters,
            )
            bundle = self._build_bundle(training_result, meta)
            meta["compare_with_champion"] = self._compare_global_vs_champion(
                bundle,
                training_result["X_test"],
                training_result["y_dir_test"],
                training_result["y_sess_test"],
                max_h,
            )
            meta["experiment"] = True
            meta["promoted"] = False
            return meta

    def train(
        self,
        lookback_days: Optional[int] = None,
        max_rows: Optional[int] = None,
        horizon: Optional[int] = None,
        direction_return_threshold: Optional[float] = None,
        direction_neutral_policy: Optional[str] = None,
        direction_label_target: Optional[str] = None,
        min_cp_prob: Optional[float] = None,
        min_whale_score: Optional[float] = None,
        min_innovation_abs: Optional[float] = None,
    ) -> Dict[str, Any]:
        with self.train_lock:
            lookback = int(lookback_days or self.train_lookback_days)
            limit = int(max_rows or self.train_max_rows)
            max_h = max(2, int(horizon or self.max_forecast_horizon))
            label_config = self._resolve_direction_label_config(
                direction_return_threshold=direction_return_threshold,
                direction_neutral_policy=direction_neutral_policy,
                direction_label_target=direction_label_target,
            )
            event_filters = self._resolve_event_filter_config(
                min_cp_prob=min_cp_prob,
                min_whale_score=min_whale_score,
                min_innovation_abs=min_innovation_abs,
            )

            events = self._load_training_events(
                lookback,
                limit,
                max_events_per_symbol_day=self.train_max_events_per_symbol_day,
                **event_filters,
            )
            daily = self._load_daily_closes(lookback, max_h)
            X, y_dir, y_sessions, sample_groups, sample_dates = self._build_training_dataset(
                events,
                daily,
                max_h,
                **label_config,
            )

            if len(X) < self.global_min_train_samples:
                raise RuntimeError(
                    f"Training data too small ({len(X)} rows). Need at least {self.global_min_train_samples} labeled samples."
                )

            training_result = self._run_training_round(
                X,
                y_dir,
                y_sessions,
                max_h,
                sample_groups=sample_groups,
                sample_dates=sample_dates,
            )

            meta = self._build_base_meta(
                model_scope="global",
                samples=len(X),
                lookback_days=lookback,
                max_rows=limit,
                horizon=max_h,
                y_dir=y_dir,
                training_result=training_result,
                direction_label_config=label_config,
                event_filter_config=event_filters,
            )

            bundle = self._build_bundle(training_result, meta)
            compare = self._compare_global_vs_champion(
                bundle,
                training_result["X_test"],
                training_result["y_dir_test"],
                training_result["y_sess_test"],
                max_h,
            )
            decision = compare["decision"]
            promoted = bool(decision.get("promote"))
            meta["compare_with_champion"] = compare
            meta["promoted"] = promoted

            train_params = {
                "scope": "global",
                "lookback_days": lookback,
                "max_rows": limit,
                "max_events_per_symbol_day": self.train_max_events_per_symbol_day,
                "horizon_sessions": max_h,
                "features_count": len(FEATURE_NAMES),
                "samples": int(len(X)),
                **label_config,
                **event_filters,
                "recency_weight_enabled": bool(getattr(self, "recency_weight_enabled", False)),
                "recency_weight_half_life_days": float(getattr(self, "recency_weight_half_life_days", 10.0)),
                "recency_weight_min": float(getattr(self, "recency_weight_min", 0.25)),
                "direction_threshold_tuning_enabled": bool(
                    getattr(self, "direction_threshold_tuning_enabled", True)
                ),
                "direction_threshold_min": float(getattr(self, "direction_threshold_min", 0.35)),
                "direction_threshold_max": float(getattr(self, "direction_threshold_max", 0.65)),
                "direction_threshold_step": float(getattr(self, "direction_threshold_step", 0.01)),
                "direction_threshold_metric": str(
                    getattr(self, "direction_threshold_metric", "balanced_accuracy")
                ),
                "model_selection_std_penalty": float(getattr(self, "model_selection_std_penalty", 0.0)),
            }
            mlflow_metrics = dict(meta["metrics"])
            mlflow_metrics.update(
                self._flatten_candidate_metrics_for_mlflow(
                    prefix="classifier",
                    candidates=training_result["classifier_results"],
                )
            )
            mlflow_metrics.update(
                self._flatten_candidate_metrics_for_mlflow(
                    prefix="regressor",
                    candidates=training_result["regressor_results"],
                )
            )
            champion_scores = compare.get("champion", {}).get("scores", {})
            challenger_scores = compare.get("challenger", {}).get("scores", {})
            mlflow_metrics["baseline_direction_score"] = champion_scores.get("direction")
            mlflow_metrics["baseline_sessions_score"] = champion_scores.get("sessions")
            mlflow_metrics["challenger_direction_score"] = challenger_scores.get("direction")
            mlflow_metrics["challenger_sessions_score"] = challenger_scores.get("sessions")
            mlflow_metrics["direction_delta_vs_champion"] = decision.get("direction_delta")
            mlflow_metrics["sessions_delta_vs_champion"] = decision.get("sessions_delta")

            aliases_to_set = [self.global_candidate_alias]
            if promoted:
                aliases_to_set.append(self.model_alias)

            mlflow_meta: Dict[str, Any] = {}
            mlflow_error: Optional[str] = None
            with tempfile.TemporaryDirectory(prefix="whale_ml_train_global_") as temp_dir:
                temp_bundle_path = Path(temp_dir) / "whale_move_model.joblib"
                joblib.dump(bundle, temp_bundle_path)
                try:
                    mlflow_meta = self._log_bundle_to_mlflow(
                        temp_bundle_path,
                        train_params=train_params,
                        metrics=mlflow_metrics,
                        base_meta=meta,
                        model_name=self.registered_model_name,
                        aliases_to_set=aliases_to_set,
                        run_name_prefix="global_train",
                    )
                except Exception as exc:
                    mlflow_error = str(exc)
                    logger.error("MLflow registry registration failed for global model: %s", exc)
                    if self.mlflow_registry_required:
                        raise RuntimeError(f"MLflow registry registration failed: {exc}")

            if mlflow_meta:
                meta.update(mlflow_meta)
            else:
                meta.setdefault("model_source", "local")
                meta.setdefault("model_name", self.registered_model_name)
                meta.setdefault("model_alias", self.model_alias if promoted else self.global_candidate_alias)
                meta.setdefault("model_version", meta.get("version"))
                if mlflow_error:
                    meta["mlflow_error"] = mlflow_error

            bundle["meta"] = meta
            if promoted:
                with self.lock:
                    self.bundle = bundle
                    self.model_source = str(meta.get("model_source", "local"))
                self.save_bundle()
            else:
                logger.info(
                    "Global challenger was not promoted (reason=%s)",
                    decision.get("reason"),
                )
            return meta

    def train_symbol(
        self,
        symbol: str,
        lookback_days: Optional[int] = None,
        max_rows: Optional[int] = None,
        horizon: Optional[int] = None,
        force_promote: bool = False,
        direction_return_threshold: Optional[float] = None,
        direction_neutral_policy: Optional[str] = None,
        direction_label_target: Optional[str] = None,
        min_cp_prob: Optional[float] = None,
        min_whale_score: Optional[float] = None,
        min_innovation_abs: Optional[float] = None,
    ) -> Dict[str, Any]:
        if not self.symbol_enabled:
            raise RuntimeError("Symbol challenger training is disabled")

        sym = _sanitize_symbol(symbol)

        with self.train_lock:
            lookback = int(lookback_days or self.symbol_lookback_days)
            limit = int(max_rows or self.symbol_max_rows)
            max_h = max(2, int(horizon or self.max_forecast_horizon))
            label_config = self._resolve_direction_label_config(
                direction_return_threshold=direction_return_threshold,
                direction_neutral_policy=direction_neutral_policy,
                direction_label_target=direction_label_target,
            )
            event_filters = self._resolve_event_filter_config(
                min_cp_prob=min_cp_prob,
                min_whale_score=min_whale_score,
                min_innovation_abs=min_innovation_abs,
                symbol_scope=True,
            )

            events = self._load_training_events(
                lookback,
                limit,
                symbol=sym,
                max_events_per_symbol_day=self.symbol_max_events_per_symbol_day,
                **event_filters,
            )
            daily = self._load_daily_closes(lookback, max_h, symbol=sym)
            X, y_dir, y_sessions, sample_groups, sample_dates = self._build_training_dataset(
                events,
                daily,
                max_h,
                **label_config,
            )

            if len(X) < self.symbol_min_train_samples:
                raise RuntimeError(
                    f"Symbol {sym} data too small ({len(X)} rows). Need at least {self.symbol_min_train_samples} labeled samples."
                )

            training_result = self._run_training_round(
                X,
                y_dir,
                y_sessions,
                max_h,
                sample_groups=sample_groups,
                sample_dates=sample_dates,
            )

            meta = self._build_base_meta(
                model_scope="symbol",
                symbol=sym,
                samples=len(X),
                lookback_days=lookback,
                max_rows=limit,
                horizon=max_h,
                y_dir=y_dir,
                training_result=training_result,
                direction_label_config=label_config,
                event_filter_config=event_filters,
            )
            meta["model_name"] = self._model_name_for_symbol(sym)

            challenger_bundle = self._build_bundle(training_result, meta)
            compare = self._compare_symbol_vs_global(
                challenger_bundle,
                training_result["X_test"],
                training_result["y_dir_test"],
                training_result["y_sess_test"],
                max_h,
                force_promote=force_promote,
            )
            decision = compare["decision"]
            promoted = bool(decision.get("promote"))

            meta["compare_with_global"] = compare
            meta["promoted"] = promoted

            aliases_to_set = [self.symbol_candidate_alias]
            if promoted:
                aliases_to_set.append(self.model_alias)

            train_params = {
                "scope": "symbol",
                "symbol": sym,
                "lookback_days": lookback,
                "max_rows": limit,
                "max_events_per_symbol_day": self.symbol_max_events_per_symbol_day,
                "horizon_sessions": max_h,
                "features_count": len(FEATURE_NAMES),
                "samples": int(len(X)),
                "force_promote": bool(force_promote),
                **label_config,
                **event_filters,
                "recency_weight_enabled": bool(getattr(self, "recency_weight_enabled", False)),
                "recency_weight_half_life_days": float(getattr(self, "recency_weight_half_life_days", 10.0)),
                "recency_weight_min": float(getattr(self, "recency_weight_min", 0.25)),
                "direction_threshold_tuning_enabled": bool(
                    getattr(self, "direction_threshold_tuning_enabled", True)
                ),
                "direction_threshold_min": float(getattr(self, "direction_threshold_min", 0.35)),
                "direction_threshold_max": float(getattr(self, "direction_threshold_max", 0.65)),
                "direction_threshold_step": float(getattr(self, "direction_threshold_step", 0.01)),
                "direction_threshold_metric": str(
                    getattr(self, "direction_threshold_metric", "balanced_accuracy")
                ),
                "model_selection_std_penalty": float(getattr(self, "model_selection_std_penalty", 0.0)),
            }

            mlflow_metrics = dict(meta["metrics"])
            mlflow_metrics.update(
                self._flatten_candidate_metrics_for_mlflow(
                    prefix="classifier",
                    candidates=training_result["classifier_results"],
                )
            )
            mlflow_metrics.update(
                self._flatten_candidate_metrics_for_mlflow(
                    prefix="regressor",
                    candidates=training_result["regressor_results"],
                )
            )
            champion_scores = compare.get("champion", {}).get("scores", {})
            challenger_scores = compare.get("challenger", {}).get("scores", {})
            mlflow_metrics["baseline_direction_score"] = champion_scores.get("direction")
            mlflow_metrics["baseline_sessions_score"] = champion_scores.get("sessions")
            mlflow_metrics["challenger_direction_score"] = challenger_scores.get("direction")
            mlflow_metrics["challenger_sessions_score"] = challenger_scores.get("sessions")
            mlflow_metrics["direction_delta_vs_global"] = decision.get("direction_delta")
            mlflow_metrics["sessions_delta_vs_global"] = decision.get("sessions_delta")

            mlflow_meta: Dict[str, Any] = {}
            mlflow_error: Optional[str] = None
            with tempfile.TemporaryDirectory(prefix=f"whale_ml_train_symbol_{sym}_") as temp_dir:
                temp_bundle_path = Path(temp_dir) / f"whale_move_model_{sym}.joblib"
                joblib.dump(challenger_bundle, temp_bundle_path)
                try:
                    mlflow_meta = self._log_bundle_to_mlflow(
                        temp_bundle_path,
                        train_params=train_params,
                        metrics=mlflow_metrics,
                        base_meta=meta,
                        model_name=self._model_name_for_symbol(sym),
                        aliases_to_set=aliases_to_set,
                        run_name_prefix=f"symbol_train_{sym}",
                    )
                except Exception as exc:
                    mlflow_error = str(exc)
                    logger.error("MLflow registry registration failed for symbol %s: %s", sym, exc)
                    if self.symbol_registry_required:
                        raise RuntimeError(f"MLflow registry registration failed for symbol {sym}: {exc}")

            if mlflow_meta:
                meta.update(mlflow_meta)
            else:
                meta.setdefault("model_source", "local")
                meta.setdefault("model_version", meta.get("version"))
                meta.setdefault("model_alias", self.model_alias if promoted else self.symbol_candidate_alias)
                if mlflow_error:
                    meta["mlflow_error"] = mlflow_error

            challenger_bundle["meta"] = meta

            if promoted:
                artifact_path = self._symbol_artifact_path(sym)
                self._ensure_symbol_storage()
                joblib.dump(challenger_bundle, artifact_path)
                with self.lock:
                    self.symbol_bundles[sym] = challenger_bundle
                self._sync_symbol_index_entry(sym, meta, artifact_path)
            else:
                logger.info(
                    "Symbol challenger %s was not promoted (reason=%s)",
                    sym,
                    decision.get("reason"),
                )

            return meta

    # Event-driven symbol challenger trigger
    def _train_symbol_background(self, symbol: str):
        try:
            result = self.train_symbol(symbol=symbol)
            logger.info(
                "Symbol challenger train finished for %s (promoted=%s, version=%s)",
                symbol,
                result.get("promoted"),
                result.get("model_version"),
            )
        except Exception as exc:
            logger.warning("Symbol challenger train failed for %s: %s", symbol, exc)
        finally:
            with self.symbol_train_state_lock:
                self.symbol_training_in_progress.discard(symbol)
                self.symbol_last_train_at[symbol] = time.time()

    def trigger_symbol_training_from_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not self.symbol_enabled or not self.symbol_train_on_anomaly:
            return {
                "enabled": False,
                "queued": [],
                "skipped": [],
            }

        unique_symbols = []
        seen = set()
        for event in events:
            symbol = str(event.get("symbol") or "").strip().upper()
            if not symbol or symbol in seen:
                continue
            try:
                symbol = _sanitize_symbol(symbol)
            except Exception:
                continue
            seen.add(symbol)
            unique_symbols.append(symbol)

        queued: List[str] = []
        skipped: List[Dict[str, Any]] = []
        cooldown_seconds = max(self.symbol_train_cooldown_min, 0) * 60

        for symbol in unique_symbols:
            with self.symbol_train_state_lock:
                if symbol in self.symbol_training_in_progress:
                    skipped.append({"symbol": symbol, "reason": "in_progress"})
                    continue

                last_at = self.symbol_last_train_at.get(symbol)
                if cooldown_seconds > 0 and last_at is not None:
                    age = time.time() - float(last_at)
                    if age < cooldown_seconds:
                        skipped.append(
                            {
                                "symbol": symbol,
                                "reason": "cooldown",
                                "retry_after_seconds": int(max(1, cooldown_seconds - age)),
                            }
                        )
                        continue

                self.symbol_training_in_progress.add(symbol)

            thread = threading.Thread(
                target=self._train_symbol_background,
                args=(symbol,),
                daemon=True,
                name=f"symbol-train-{symbol}",
            )
            thread.start()
            queued.append(symbol)

        return {
            "enabled": True,
            "queued": queued,
            "skipped": skipped,
        }
