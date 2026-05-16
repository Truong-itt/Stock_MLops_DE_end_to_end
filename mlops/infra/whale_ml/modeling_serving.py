import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from modeling_shared import FEATURE_NAMES


class ServingMixin:
    def _default_trade_filter_config(self) -> Dict[str, Any]:
        return {
            "enabled": bool(getattr(self, "trade_filter_enabled", True)),
            "mode": "prob_up_long_top10",
            "min_prob_up": float(getattr(self, "trade_filter_default_min_prob_up", 0.88)),
            "source": "env_default",
            "calibrated_at": None,
            "calibration": {},
        }

    def _sanitize_trade_filter_config(self, payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        config = self._default_trade_filter_config()
        raw = payload if isinstance(payload, dict) else {}
        if "enabled" in raw:
            config["enabled"] = bool(raw.get("enabled"))
        if "mode" in raw:
            mode = str(raw.get("mode") or "").strip()
            if mode:
                config["mode"] = mode
        if "min_prob_up" in raw:
            try:
                value = float(raw.get("min_prob_up"))
                config["min_prob_up"] = min(1.0, max(0.0, value))
            except (TypeError, ValueError):
                pass
        if "source" in raw:
            source = str(raw.get("source") or "").strip()
            if source:
                config["source"] = source
        if "calibrated_at" in raw:
            calibrated_at = raw.get("calibrated_at")
            config["calibrated_at"] = str(calibrated_at) if calibrated_at else None
        if "calibration" in raw and isinstance(raw.get("calibration"), dict):
            config["calibration"] = dict(raw.get("calibration") or {})
        return config

    def _load_trade_filter_config(self) -> Dict[str, Any]:
        path = Path(getattr(self, "trade_filter_config_path", "/app/artifacts/trade_filter_config.json"))
        payload: Dict[str, Any] = {}
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
        config = self._sanitize_trade_filter_config(payload)
        with self.lock:
            self.trade_filter_config = config
        return dict(config)

    def _save_trade_filter_config(self) -> Dict[str, Any]:
        path = Path(getattr(self, "trade_filter_config_path", "/app/artifacts/trade_filter_config.json"))
        with self.lock:
            payload = self._sanitize_trade_filter_config(getattr(self, "trade_filter_config", {}))
            self.trade_filter_config = payload
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return dict(payload)

    def get_trade_filter_config(self) -> Dict[str, Any]:
        with self.lock:
            payload = dict(getattr(self, "trade_filter_config", {}) or {})
        if not payload:
            return self._load_trade_filter_config()
        return self._sanitize_trade_filter_config(payload)

    def update_trade_filter_config(
        self,
        *,
        min_prob_up: Optional[float] = None,
        enabled: Optional[bool] = None,
        source: Optional[str] = None,
        calibration: Optional[Dict[str, Any]] = None,
        calibrated_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        current = self.get_trade_filter_config()
        if min_prob_up is not None:
            current["min_prob_up"] = min(1.0, max(0.0, float(min_prob_up)))
        if enabled is not None:
            current["enabled"] = bool(enabled)
        if source:
            current["source"] = str(source)
        if calibration is not None:
            current["calibration"] = dict(calibration)
        if calibrated_at is not None:
            current["calibrated_at"] = str(calibrated_at)
        with self.lock:
            self.trade_filter_config = self._sanitize_trade_filter_config(current)
        return self._save_trade_filter_config()

    def calibrate_trade_filter(
        self,
        lookback_days: Optional[int] = 10,
        max_rows: Optional[int] = 40000,
        horizon: Optional[int] = 5,
        holdout_days: int = 5,
        min_train_days: int = 3,
        min_train_samples: Optional[int] = 300,
        train_window_days: Optional[int] = None,
        max_events_per_symbol_day: Optional[int] = 1,
        event_selection_strategy: Optional[str] = "strongest",
        direction_return_threshold: Optional[float] = 0.02,
        direction_neutral_policy: Optional[str] = "drop",
        direction_label_target: Optional[str] = "horizon_extreme",
        min_cp_prob: Optional[float] = 0.0,
        min_whale_score: Optional[float] = 0.0,
        min_innovation_abs: Optional[float] = 0.0,
        fallback_min_prob_up: float = 0.88,
        enable_filter: bool = True,
    ) -> Dict[str, Any]:
        result = self.rolling_backtest(
            lookback_days=lookback_days,
            max_rows=max_rows,
            horizon=horizon,
            holdout_days=holdout_days,
            min_train_days=min_train_days,
            min_train_samples=min_train_samples,
            train_window_days=train_window_days,
            max_events_per_symbol_day=max_events_per_symbol_day,
            event_selection_strategy=event_selection_strategy,
            direction_return_threshold=direction_return_threshold,
            direction_neutral_policy=direction_neutral_policy,
            direction_label_target=direction_label_target,
            min_cp_prob=min_cp_prob,
            min_whale_score=min_whale_score,
            min_innovation_abs=min_innovation_abs,
        )
        trade_slices = list((result.get("overall") or {}).get("trade_slices") or [])
        top10_prob_up = self._find_trade_slice(
            trade_slices,
            ranking="prob_up_long",
            bucket="top_10pct_prob_up",
        )
        threshold = top10_prob_up.get("min_prob_up") if isinstance(top10_prob_up, dict) else None
        if threshold is None:
            threshold = fallback_min_prob_up
        threshold = min(1.0, max(0.0, float(threshold)))
        calibrated_at = datetime.now(timezone.utc).isoformat()
        calibration_payload = {
            "method": "rolling_backtest.top_10pct_prob_up.min_prob_up",
            "params": result.get("params", {}),
            "samples": result.get("samples"),
            "loaded_samples": result.get("loaded_samples"),
            "evaluated_date_count": result.get("evaluated_date_count"),
            "top10_prob_up": top10_prob_up or {},
            "fallback_min_prob_up": float(fallback_min_prob_up),
        }
        trade_filter = self.update_trade_filter_config(
            min_prob_up=threshold,
            enabled=enable_filter,
            source="rolling_backtest_top10_prob_up",
            calibration=calibration_payload,
            calibrated_at=calibrated_at,
        )
        return {
            "trade_filter": trade_filter,
            "calibration_result": {
                "selected_min_prob_up": threshold,
                "calibrated_at": calibrated_at,
                "top10_prob_up": top10_prob_up or {},
            },
            "backtest": {
                "params": result.get("params", {}),
                "samples": result.get("samples"),
                "loaded_samples": result.get("loaded_samples"),
                "evaluated_date_count": result.get("evaluated_date_count"),
                "overall_model": (result.get("overall") or {}).get("model", {}),
            },
        }

    def is_ready(self) -> bool:
        with self.lock:
            return self.bundle is not None

    def list_symbol_models(self) -> Dict[str, Any]:
        with self.lock:
            symbols = []
            for sym in sorted(self.symbol_bundles.keys()):
                meta = dict(self.symbol_bundles[sym].get("meta", {}))
                symbols.append(
                    {
                        "symbol": sym,
                        "model_name": meta.get("model_name", self._model_name_for_symbol(sym)),
                        "model_version": meta.get("model_version", meta.get("version")),
                        "model_source": meta.get("model_source", "local"),
                        "trained_at": meta.get("trained_at"),
                        "samples": meta.get("samples"),
                        "selected_models": meta.get("selected_models", {}),
                    }
                )
        return {
            "enabled": self.symbol_enabled,
            "count": len(symbols),
            "items": symbols,
        }

    def model_info(self) -> Dict[str, Any]:
        with self.lock:
            if self.bundle is None:
                return {
                    "ready": False,
                    "symbol_models": self.list_symbol_models(),
                }

            meta = dict(self.bundle.get("meta", {}))
            selected_models = dict(meta.get("selected_models", {}))
            selected_models.setdefault("direction", self.bundle.get("classifier_name"))
            selected_models.setdefault("sessions", self.bundle.get("regressor_name"))
            if selected_models.get("direction") or selected_models.get("sessions"):
                meta["selected_models"] = selected_models

            meta["ready"] = True
            meta["feature_names"] = list(self.bundle.get("feature_names", FEATURE_NAMES))
            meta["model_source"] = meta.get("model_source", self.model_source or "local")
            meta["model_scope"] = meta.get("model_scope", "global")
            meta["model_name"] = meta.get("model_name", self.registered_model_name)
            if "model_version" not in meta:
                meta["model_version"] = meta.get("version")

            symbol_models = []
            for sym, sym_bundle in sorted(self.symbol_bundles.items()):
                sym_meta = dict(sym_bundle.get("meta", {}))
                symbol_models.append(
                    {
                        "symbol": sym,
                        "model_name": sym_meta.get("model_name", self._model_name_for_symbol(sym)),
                        "model_version": sym_meta.get("model_version", sym_meta.get("version")),
                        "model_source": sym_meta.get("model_source", "local"),
                        "trained_at": sym_meta.get("trained_at"),
                        "samples": sym_meta.get("samples"),
                    }
                )
            meta["symbol_models"] = {
                "enabled": self.symbol_enabled,
                "count": len(symbol_models),
                "items": symbol_models,
            }
            return meta

    def _predict_with_bundle(
        self,
        bundle: Dict[str, Any],
        event: Dict[str, Any],
    ) -> Dict[str, Any]:
        classifier = bundle["classifier"]
        regressor = bundle["regressor"]
        meta = dict(bundle.get("meta", {}))
        horizon = int(meta.get("horizon_sessions", self.max_forecast_horizon))
        direction_threshold = float(meta.get("direction_threshold", 0.5) or 0.5)
        feature_names = list(bundle.get("feature_names", FEATURE_NAMES))

        enriched_event = self._enrich_event_with_daily_technical_features(event, feature_names)
        features = self._build_feature_vector(enriched_event, feature_names=feature_names)
        row = features.reshape(1, -1)

        class_values = list(classifier.classes_)
        up_index = class_values.index(1) if 1 in class_values else (1 if len(class_values) > 1 else 0)

        probs = classifier.predict_proba(row)[0]
        prob_up = float(probs[up_index]) if len(probs) > up_index else float(probs[-1])
        prob_up = min(max(prob_up, 0.0), 1.0)
        prob_down = 1.0 - prob_up

        expected_sessions = float(regressor.predict(row)[0])
        expected_sessions = min(max(expected_sessions, 1.0), float(horizon))

        direction = "up" if prob_up >= direction_threshold else "down"
        confidence = max(prob_up, prob_down)
        trade_filter = self.get_trade_filter_config()
        filter_enabled = bool(trade_filter.get("enabled"))
        min_prob_up = float(trade_filter.get("min_prob_up", 0.88))
        is_trade_candidate = bool(filter_enabled and direction == "up" and prob_up >= min_prob_up)
        if not filter_enabled:
            candidate_reason = "filter_disabled"
        elif direction != "up":
            candidate_reason = "direction_not_up"
        elif prob_up < min_prob_up:
            candidate_reason = "prob_up_below_min"
        else:
            candidate_reason = "pass"

        text = (
            f"Du kien {expected_sessions:.1f} phien tang (P={prob_up * 100:.1f}%)"
            if direction == "up"
            else f"Du kien {expected_sessions:.1f} phien giam (P={prob_down * 100:.1f}%)"
        )

        return {
            "direction": direction,
            "prob_up": round(prob_up, 6),
            "prob_down": round(prob_down, 6),
            "direction_threshold": round(direction_threshold, 6),
            "expected_sessions": round(expected_sessions, 4),
            "confidence": round(float(confidence), 6),
            "is_trade_candidate": is_trade_candidate,
            "trade_filter": {
                "enabled": filter_enabled,
                "mode": str(trade_filter.get("mode") or "prob_up_long_top10"),
                "min_prob_up": round(min_prob_up, 6),
                "reason": candidate_reason,
                "source": trade_filter.get("source"),
                "calibrated_at": trade_filter.get("calibrated_at"),
            },
            "feature_count": len(feature_names),
            "text": text,
            "model_scope": meta.get("model_scope", "global"),
            "model_name": meta.get("model_name", self.registered_model_name),
            "model_version": meta.get("model_version", meta.get("version")),
            "model_source": meta.get("model_source", self.model_source or "local"),
        }

    def predict_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        with self.lock:
            global_bundle = self.bundle

        if global_bundle is None:
            raise RuntimeError("Global model is not trained yet")

        if not events:
            return {
                "predictions": [],
                "model": self.model_info(),
            }

        trigger_info = self.trigger_symbol_training_from_events(events)

        predictions = []
        for event in events:
            event_key = str(event.get("event_key") or "").strip()
            symbol = str(event.get("symbol") or "").upper().strip()
            if not symbol:
                continue

            with self.lock:
                active_bundle = self.symbol_bundles.get(symbol, self.bundle)
            if active_bundle is None:
                continue

            payload = self._predict_with_bundle(active_bundle, event)
            payload.update(
                {
                    "event_key": event_key or f"{symbol}|{event.get('event_time') or ''}",
                    "symbol": symbol,
                    "event_time": event.get("event_time"),
                }
            )
            predictions.append(payload)

        self.write_prediction_audit(predictions)

        return {
            "predictions": predictions,
            "model": self.model_info(),
            "symbol_train_trigger": trigger_info,
        }
