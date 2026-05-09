from typing import Any, Dict, List

from modeling_shared import FEATURE_NAMES


class ServingMixin:
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

        features = self._build_feature_vector(event)
        row = features.reshape(1, -1)

        class_values = list(classifier.classes_)
        up_index = class_values.index(1) if 1 in class_values else (1 if len(class_values) > 1 else 0)

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

        return {
            "direction": direction,
            "prob_up": round(prob_up, 6),
            "prob_down": round(prob_down, 6),
            "expected_sessions": round(expected_sessions, 4),
            "confidence": round(float(confidence), 6),
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
                }
            )
            predictions.append(payload)

        return {
            "predictions": predictions,
            "model": self.model_info(),
            "symbol_train_trigger": trigger_info,
        }
