import json
import os
from pathlib import Path
from typing import Any, Dict

import clickhouse_connect
import joblib
import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient

from modeling_shared import _utc_now, logger

class ModelInfraMixin:
    def _configure_mlflow(self):
        if not self.mlflow_tracking_uri:
            raise RuntimeError("MLFLOW_TRACKING_URI is empty")
        # Keep startup and load flow responsive when tracking server is unreachable.
        os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "5")
        os.environ.setdefault("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "1")
        os.environ.setdefault("MLFLOW_HTTP_REQUEST_BACKOFF_FACTOR", "1")
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
        logger.info(
            "Connected ClickHouse for whale ML at %s:%s",
            self.clickhouse_host,
            self.clickhouse_port,
        )

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

    def _model_name_for_symbol(self, symbol: str) -> str:
        return self.symbol_model_name_template.format(base=self.registered_model_name, symbol=symbol)

    def _symbol_artifact_path(self, symbol: str) -> Path:
        return self.symbol_artifact_dir / f"{symbol}.joblib"

    def _ensure_symbol_storage(self):
        self.symbol_artifact_dir.mkdir(parents=True, exist_ok=True)
        self.symbol_index_path.parent.mkdir(parents=True, exist_ok=True)

    def _load_symbol_index(self) -> Dict[str, Any]:
        self._ensure_symbol_storage()
        if not self.symbol_index_path.exists():
            return {"symbols": {}}
        try:
            payload = json.loads(self.symbol_index_path.read_text(encoding="utf-8"))
            symbols = payload.get("symbols")
            if not isinstance(symbols, dict):
                return {"symbols": {}}
            return {"symbols": symbols}
        except Exception as exc:
            logger.warning("Cannot read symbol model index %s: %s", self.symbol_index_path, exc)
            return {"symbols": {}}

    def _save_symbol_index(self):
        self._ensure_symbol_storage()
        payload = {
            "updated_at": _utc_now().isoformat(),
            "symbols": self.symbol_index.get("symbols", {}),
        }
        self.symbol_index_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _sync_symbol_index_entry(self, symbol: str, meta: Dict[str, Any], artifact_path: Path):
        symbols = self.symbol_index.setdefault("symbols", {})
        symbols[symbol] = {
            "artifact_path": str(artifact_path),
            "trained_at": meta.get("trained_at"),
            "samples": meta.get("samples"),
            "model_name": meta.get("model_name"),
            "model_version": meta.get("model_version"),
            "model_alias": meta.get("model_alias", self.model_alias),
            "model_source": meta.get("model_source", "local"),
            "selected_models": meta.get("selected_models", {}),
        }
        self._save_symbol_index()

    def _load_symbol_bundles_from_local(self):
        if not self.symbol_enabled:
            return

        self.symbol_index = self._load_symbol_index()
        symbols = dict(self.symbol_index.get("symbols", {}))

        loaded_count = 0
        for symbol, row in symbols.items():
            sym = str(symbol or "").strip().upper()
            if not sym:
                continue
            artifact_path = Path(row.get("artifact_path") or self._symbol_artifact_path(sym))
            if not artifact_path.exists():
                continue
            try:
                bundle = joblib.load(artifact_path)
            except Exception as exc:
                logger.warning("Cannot load symbol bundle %s (%s): %s", sym, artifact_path, exc)
                continue

            meta = dict(bundle.get("meta", {}))
            meta.setdefault("model_scope", "symbol")
            meta.setdefault("symbol", sym)
            meta.setdefault("model_source", "local")
            meta.setdefault("model_name", self._model_name_for_symbol(sym))
            if "model_version" not in meta:
                meta["model_version"] = meta.get("version")
            bundle["meta"] = meta

            with self.lock:
                self.symbol_bundles[sym] = bundle
            loaded_count += 1

        if loaded_count:
            logger.info("Loaded %d promoted symbol models from local storage", loaded_count)

    def _download_bundle_from_registry(self, registered_model_name: str, alias: str) -> Dict[str, Any]:
        self._configure_mlflow()
        client = MlflowClient()
        version_info = client.get_model_version_by_alias(registered_model_name, alias)
        model_version = str(version_info.version)
        model_uri = f"models:/{registered_model_name}@{alias}"

        model_dir = Path(mlflow.artifacts.download_artifacts(artifact_uri=model_uri))
        candidates = sorted(model_dir.rglob("*.joblib"))
        if not candidates:
            raise RuntimeError(f"No .joblib artifact found under {model_dir}")

        bundle = joblib.load(candidates[0])
        meta = dict(bundle.get("meta", {}))
        meta.update(
            {
                "model_source": "registry",
                "model_name": registered_model_name,
                "model_alias": alias,
                "model_version": model_version,
                "model_uri": f"models:/{registered_model_name}/{model_version}",
            }
        )
        if version_info.run_id:
            meta["mlflow_run_id"] = version_info.run_id
        bundle["meta"] = meta
        return bundle

    def _load_global_bundle_from_local(self) -> bool:
        if not self.model_artifact_path.exists():
            return False
        try:
            bundle = joblib.load(self.model_artifact_path)
        except Exception as exc:
            logger.warning("Cannot load existing local global model bundle: %s", exc)
            return False

        meta = dict(bundle.get("meta", {}))
        meta.setdefault("model_source", "local")
        meta.setdefault("model_scope", "global")
        meta.setdefault("model_name", self.registered_model_name)
        if "model_version" not in meta:
            meta["model_version"] = meta.get("version")
        bundle["meta"] = meta

        with self.lock:
            self.bundle = bundle
            self.model_source = "local"
        return True

    def _load_global_bundle_from_registry(self) -> bool:
        try:
            bundle = self._download_bundle_from_registry(self.registered_model_name, self.model_alias)
            meta = dict(bundle.get("meta", {}))
            meta.setdefault("model_scope", "global")
            bundle["meta"] = meta

            with self.lock:
                self.bundle = bundle
                self.model_source = "registry"

            # Keep global local fallback in sync with current registry production.
            self.model_artifact_path.parent.mkdir(parents=True, exist_ok=True)
            joblib.dump(bundle, self.model_artifact_path)
            return True
        except Exception as exc:
            logger.warning(
                "Cannot load global model from MLflow Registry (%s@%s): %s",
                self.registered_model_name,
                self.model_alias,
                exc,
            )
            return False

    def load_bundle(self) -> bool:
        loaded = False
        if self.prefer_registry:
            loaded = self._load_global_bundle_from_registry() or self._load_global_bundle_from_local()
        else:
            loaded = self._load_global_bundle_from_local() or self._load_global_bundle_from_registry()

        self._load_symbol_bundles_from_local()
        return loaded

    def save_bundle(self) -> None:
        with self.lock:
            if self.bundle is None:
                return
            self.model_artifact_path.parent.mkdir(parents=True, exist_ok=True)
            joblib.dump(self.bundle, self.model_artifact_path)
