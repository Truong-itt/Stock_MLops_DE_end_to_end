import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import joblib
import mlflow
import numpy as np
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

from modeling_shared import (
    FEATURE_NAMES,
    WhaleBundlePyfuncModel,
    _metric_token,
    _safe_metric_value,
    _sanitize_symbol,
    _utc_now,
    logger,
)


class TrainingMixin:
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

        accuracy = _safe_metric_value(accuracy_score(y_test, y_pred))
        f1 = _safe_metric_value(f1_score(y_test, y_pred, zero_division=0))
        roc_auc: Optional[float] = None

        if hasattr(model, "predict_proba"):
            try:
                y_prob = model.predict_proba(X_test)
                classes = np.asarray(getattr(model, "classes_", []))
                up_positions = np.where(classes == 1)[0] if classes.size else np.asarray([])
                up_index = int(up_positions[0]) if len(up_positions) else (1 if y_prob.shape[1] > 1 else 0)
                roc_auc = _safe_metric_value(roc_auc_score(y_test, y_prob[:, up_index]))
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

        y_dir_pred = classifier.predict(X_test)
        accuracy = _safe_metric_value(accuracy_score(y_dir_test, y_dir_pred))
        f1 = _safe_metric_value(f1_score(y_dir_test, y_dir_pred, zero_division=0))

        roc_auc: Optional[float] = None
        if hasattr(classifier, "predict_proba"):
            try:
                y_prob = classifier.predict_proba(X_test)
                classes = np.asarray(getattr(classifier, "classes_", []))
                up_positions = np.where(classes == 1)[0] if classes.size else np.asarray([])
                up_index = int(up_positions[0]) if len(up_positions) else (1 if y_prob.shape[1] > 1 else 0)
                roc_auc = _safe_metric_value(roc_auc_score(y_dir_test, y_prob[:, up_index]))
            except Exception:
                roc_auc = None

        y_sess_pred = np.clip(regressor.predict(X_test), 1.0, float(horizon))
        mae = _safe_metric_value(mean_absolute_error(y_sess_test, y_sess_pred))
        rmse = _safe_metric_value(np.sqrt(mean_squared_error(y_sess_test, y_sess_pred)))

        direction_score = roc_auc if roc_auc is not None else accuracy
        sessions_score = None if mae is None else float(-mae)

        return {
            "metrics": {
                "accuracy": accuracy,
                "roc_auc": roc_auc,
                "f1_direction": f1,
                "mae_sessions": mae,
                "rmse_sessions": rmse,
            },
            "scores": {
                "direction": direction_score,
                "sessions": sessions_score,
            },
        }

    def _run_training_round(
        self,
        X: np.ndarray,
        y_dir: np.ndarray,
        y_sessions: np.ndarray,
        horizon: int,
    ) -> Dict[str, Any]:
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
                    horizon=horizon,
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

        best_metrics = {
            "accuracy": best_classifier["metrics"].get("accuracy"),
            "roc_auc": best_classifier["metrics"].get("roc_auc"),
            "f1_direction": best_classifier["metrics"].get("f1_direction"),
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
            "X_test": X_test,
            "y_dir_test": y_dir_test,
            "y_sess_test": y_sess_test,
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

    # -----------------------------
    # Training flows
    # -----------------------------
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
    ) -> Dict[str, Any]:
        best_metrics = dict(training_result["best_metrics"])
        best_classifier = training_result["best_classifier"]
        best_regressor = training_result["best_regressor"]

        meta = {
            "trained_at": _utc_now().isoformat(),
            "version": _utc_now().strftime("%Y%m%d%H%M%S"),
            "model_scope": model_scope,
            "samples": int(samples),
            "lookback_days": int(lookback_days),
            "max_rows": int(max_rows),
            "horizon_sessions": int(horizon),
            "up_ratio": float(np.mean(y_dir)),
            "metrics": best_metrics,
            "selected_models": {
                "direction": best_classifier["name"],
                "sessions": best_regressor["name"],
            },
            "model_candidates": {
                "direction": training_result["classifier_leaderboard"],
                "sessions": training_result["regressor_leaderboard"],
            },
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

    def train(
        self,
        lookback_days: Optional[int] = None,
        max_rows: Optional[int] = None,
        horizon: Optional[int] = None,
    ) -> Dict[str, Any]:
        with self.train_lock:
            lookback = int(lookback_days or self.train_lookback_days)
            limit = int(max_rows or self.train_max_rows)
            max_h = max(2, int(horizon or self.max_forecast_horizon))

            events = self._load_training_events(lookback, limit)
            daily = self._load_daily_closes(lookback, max_h)
            X, y_dir, y_sessions = self._build_training_dataset(events, daily, max_h)

            if len(X) < self.global_min_train_samples:
                raise RuntimeError(
                    f"Training data too small ({len(X)} rows). Need at least {self.global_min_train_samples} labeled samples."
                )

            training_result = self._run_training_round(X, y_dir, y_sessions, max_h)

            meta = self._build_base_meta(
                model_scope="global",
                samples=len(X),
                lookback_days=lookback,
                max_rows=limit,
                horizon=max_h,
                y_dir=y_dir,
                training_result=training_result,
            )

            bundle = self._build_bundle(training_result, meta)

            train_params = {
                "scope": "global",
                "lookback_days": lookback,
                "max_rows": limit,
                "horizon_sessions": max_h,
                "features_count": len(FEATURE_NAMES),
                "samples": int(len(X)),
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
                        aliases_to_set=[self.model_alias],
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

    def train_symbol(
        self,
        symbol: str,
        lookback_days: Optional[int] = None,
        max_rows: Optional[int] = None,
        horizon: Optional[int] = None,
        force_promote: bool = False,
    ) -> Dict[str, Any]:
        if not self.symbol_enabled:
            raise RuntimeError("Symbol challenger training is disabled")

        sym = _sanitize_symbol(symbol)

        with self.train_lock:
            lookback = int(lookback_days or self.symbol_lookback_days)
            limit = int(max_rows or self.symbol_max_rows)
            max_h = max(2, int(horizon or self.max_forecast_horizon))

            events = self._load_training_events(lookback, limit, symbol=sym)
            daily = self._load_daily_closes(lookback, max_h, symbol=sym)
            X, y_dir, y_sessions = self._build_training_dataset(events, daily, max_h)

            if len(X) < self.symbol_min_train_samples:
                raise RuntimeError(
                    f"Symbol {sym} data too small ({len(X)} rows). Need at least {self.symbol_min_train_samples} labeled samples."
                )

            training_result = self._run_training_round(X, y_dir, y_sessions, max_h)

            meta = self._build_base_meta(
                model_scope="symbol",
                symbol=sym,
                samples=len(X),
                lookback_days=lookback,
                max_rows=limit,
                horizon=max_h,
                y_dir=y_dir,
                training_result=training_result,
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
                "horizon_sessions": max_h,
                "features_count": len(FEATURE_NAMES),
                "samples": int(len(X)),
                "force_promote": bool(force_promote),
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

    # -----------------------------
    # Event-driven symbol challenger trigger
    # -----------------------------
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
