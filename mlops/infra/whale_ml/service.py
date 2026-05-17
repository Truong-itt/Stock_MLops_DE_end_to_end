import asyncio
import os
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from logging_setup import get_logger
from modeling import WhaleMoveForecaster

logger = get_logger(
    logs_dir="/var/log/whale_ml",
    log_filename="whale_ml.log",
    keep_days=30,
)

AUTO_TRAIN_ON_STARTUP = os.getenv("AUTO_TRAIN_ON_STARTUP", "1").strip().lower() in {
    "1",
    "true",
    "yes",
}
# AUTO_RETRAIN_INTERVAL_MIN = int(os.getenv("AUTO_RETRAIN_INTERVAL_MIN", "0"))

forecaster = WhaleMoveForecaster()
# retrain_task: Optional[asyncio.Task] = None


def ok(data):
    return {"status": "ok", "data": data}

# async def _retrain_loop():
#     interval_seconds = max(AUTO_RETRAIN_INTERVAL_MIN, 1) * 60
#     while True:
#         await asyncio.sleep(interval_seconds)
#         try:
#             logger.info("Auto retrain triggered")
#             meta = await asyncio.to_thread(forecaster.train)
#             logger.info("Auto retrain completed (samples=%s)", meta.get("samples"))
#         except asyncio.CancelledError:
#             raise
#         except Exception as exc:
#             logger.warning("Auto retrain failed: %s", exc)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # global retrain_task
    try:
        forecaster.connect()
    except Exception as exc:
        logger.warning("ClickHouse connect failed on startup: %s", exc)
    loaded = forecaster.load_bundle()
    if loaded:
        logger.info("Loaded existing model bundle (source=%s)", forecaster.model_info().get("model_source"))

    if AUTO_TRAIN_ON_STARTUP and not forecaster.is_ready():
        try:
            meta = await asyncio.to_thread(forecaster.train)
            logger.info("Initial training complete (samples=%s)", meta.get("samples"))
        except Exception as exc:
            logger.warning("Initial training failed: %s", exc)

    # if AUTO_RETRAIN_INTERVAL_MIN > 0:
    #     retrain_task = asyncio.create_task(_retrain_loop())
    #     logger.warning(
    #         "Emergency auto retrain is enabled in service (interval=%d minutes). "
    #         "Recommended production mode is Airflow-driven retrain with AUTO_RETRAIN_INTERVAL_MIN=0.",
    #         AUTO_RETRAIN_INTERVAL_MIN,
    #     )

    yield

    # if retrain_task:
    #     retrain_task.cancel()
    #     try:
    #         await retrain_task
    #     except asyncio.CancelledError:
    #         pass
    forecaster.close()


app = FastAPI(
    title="Whale Move Forecast Service",
    version="1.0.0",
    lifespan=lifespan,
)

class TrainRequest(BaseModel):
    lookback_days: Optional[int] = Field(default=None, ge=5, le=720)
    max_rows: Optional[int] = Field(default=None, ge=2000, le=800000)
    horizon: Optional[int] = Field(default=None, ge=2, le=20)
    direction_return_threshold: Optional[float] = Field(default=None, ge=0.0, le=0.2)
    direction_neutral_policy: Optional[str] = Field(default=None, pattern="^(drop|sign)$")
    direction_label_target: Optional[str] = Field(default=None, pattern="^(next_close|horizon_extreme)$")
    min_cp_prob: Optional[float] = Field(default=None, ge=0.0)
    min_whale_score: Optional[float] = Field(default=None, ge=0.0)
    min_innovation_abs: Optional[float] = Field(default=None, ge=0.0)
    

class TrainSymbolRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    lookback_days: Optional[int] = Field(default=None, ge=5, le=720)
    max_rows: Optional[int] = Field(default=None, ge=500, le=200000)
    horizon: Optional[int] = Field(default=None, ge=2, le=20)
    force_promote: bool = False
    direction_return_threshold: Optional[float] = Field(default=None, ge=0.0, le=0.2)
    direction_neutral_policy: Optional[str] = Field(default=None, pattern="^(drop|sign)$")
    direction_label_target: Optional[str] = Field(default=None, pattern="^(next_close|horizon_extreme)$")
    min_cp_prob: Optional[float] = Field(default=None, ge=0.0)
    min_whale_score: Optional[float] = Field(default=None, ge=0.0)
    min_innovation_abs: Optional[float] = Field(default=None, ge=0.0)


class BacktestRequest(BaseModel):
    lookback_days: Optional[int] = Field(default=None, ge=5, le=720)
    max_rows: Optional[int] = Field(default=None, ge=2000, le=800000)
    horizon: Optional[int] = Field(default=None, ge=2, le=20)
    holdout_days: int = Field(default=10, ge=1, le=120)
    direction_return_threshold: Optional[float] = Field(default=None, ge=0.0, le=0.2)
    direction_neutral_policy: Optional[str] = Field(default=None, pattern="^(drop|sign)$")
    direction_label_target: Optional[str] = Field(default=None, pattern="^(next_close|horizon_extreme)$")
    min_cp_prob: Optional[float] = Field(default=None, ge=0.0)
    min_whale_score: Optional[float] = Field(default=None, ge=0.0)
    min_innovation_abs: Optional[float] = Field(default=None, ge=0.0)


class RollingBacktestRequest(BacktestRequest):
    holdout_days: int = Field(default=5, ge=1, le=30)
    min_train_days: int = Field(default=3, ge=1, le=240)
    min_train_samples: Optional[int] = Field(default=None, ge=100, le=800000)
    train_window_days: Optional[int] = Field(default=None, ge=1, le=720)
    max_events_per_symbol_day: Optional[int] = Field(default=None, ge=1, le=200)
    event_selection_strategy: Optional[str] = Field(
        default=None,
        pattern="^(latest|cp_prob|whale_score|innovation_abs|strongest)$",
    )
    trade_rule_min_prob_up: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class RollingBacktestScanRequest(RollingBacktestRequest):
    direction_return_thresholds: List[float] = Field(default_factory=lambda: [0.005, 0.008, 0.01, 0.012, 0.015, 0.02])
    max_events_per_symbol_day_options: List[int] = Field(default_factory=lambda: [1, 2, 3])


class ForecastEvent(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    event_time: Optional[str] = None
    cp_prob: Optional[float] = None
    whale_score: Optional[float] = None
    innovation_zscore: Optional[float] = None
    expected_run_length: Optional[float] = None
    map_run_length: Optional[float] = None
    predictive_volatility: Optional[float] = None
    return_value: Optional[float] = None
    hazard: Optional[float] = None
    evidence: Optional[float] = None
    price: Optional[float] = None
    event_key: Optional[str] = None


class BatchPredictRequest(BaseModel):
    events: List[ForecastEvent] = Field(default_factory=list)


class TradeFilterCalibrateRequest(BaseModel):
    lookback_days: Optional[int] = Field(default=10, ge=5, le=720)
    max_rows: Optional[int] = Field(default=40000, ge=2000, le=800000)
    horizon: Optional[int] = Field(default=5, ge=2, le=20)
    holdout_days: int = Field(default=5, ge=1, le=30)
    min_train_days: int = Field(default=3, ge=1, le=240)
    min_train_samples: Optional[int] = Field(default=300, ge=100, le=800000)
    train_window_days: Optional[int] = Field(default=None, ge=1, le=720)
    max_events_per_symbol_day: Optional[int] = Field(default=1, ge=1, le=200)
    event_selection_strategy: Optional[str] = Field(
        default="strongest",
        pattern="^(latest|cp_prob|whale_score|innovation_abs|strongest)$",
    )
    direction_return_threshold: Optional[float] = Field(default=0.02, ge=0.0, le=0.2)
    direction_neutral_policy: Optional[str] = Field(default="drop", pattern="^(drop|sign)$")
    direction_label_target: Optional[str] = Field(default="horizon_extreme", pattern="^(next_close|horizon_extreme)$")
    min_cp_prob: Optional[float] = Field(default=0.0, ge=0.0)
    min_whale_score: Optional[float] = Field(default=0.0, ge=0.0)
    min_innovation_abs: Optional[float] = Field(default=0.0, ge=0.0)
    fallback_min_prob_up: float = Field(default=0.88, ge=0.0, le=1.0)
    enable_filter: bool = True
    min_top10_samples: int = Field(default=30, ge=1, le=1000000)
    min_top10_precision: float = Field(default=0.7, ge=0.0, le=1.0)
    min_top10_win_rate: float = Field(default=0.55, ge=0.0, le=1.0)
    min_evaluated_days: int = Field(default=3, ge=1, le=3650)


class TradeFilterUpdateRequest(BaseModel):
    enabled: Optional[bool] = None
    min_prob_up: Optional[float] = Field(default=None, ge=0.0, le=1.0)


@app.get("/health")
async def health():
    info = forecaster.model_info()
    symbol_models = info.get("symbol_models") or {}
    return ok(
        {
            "service": "whale-ml",
            "model_ready": bool(info.get("ready")),
            "model_source": info.get("model_source"),
            "model_name": info.get("model_name"),
            "model_version": info.get("model_version", info.get("version")),
            "selected_models": info.get("selected_models"),
            "mlflow_run_id": info.get("mlflow_run_id"),
            "trained_at": info.get("trained_at"),
            "samples": info.get("samples"),
            "symbol_model_count": symbol_models.get("count", 0),
        }
    )


@app.get("/model/info")
async def model_info():
    return ok(forecaster.model_info())


@app.get("/model/symbols")
async def model_symbols():
    return ok(forecaster.list_symbol_models())


@app.get("/trade-filter")
async def trade_filter_info():
    return ok(forecaster.get_trade_filter_config())


@app.post("/trade-filter")
async def trade_filter_update(payload: TradeFilterUpdateRequest):
    if payload.enabled is None and payload.min_prob_up is None:
        raise HTTPException(status_code=400, detail="At least one field required: enabled or min_prob_up")
    try:
        result = await asyncio.to_thread(
            forecaster.update_trade_filter_config,
            min_prob_up=payload.min_prob_up,
            enabled=payload.enabled,
            source="manual_api",
        )
        return ok(result)
    except Exception as exc:
        logger.error("Trade filter update failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/trade-filter/calibrate")
async def trade_filter_calibrate(payload: TradeFilterCalibrateRequest):
    try:
        result = await asyncio.to_thread(
            forecaster.calibrate_trade_filter,
            payload.lookback_days,
            payload.max_rows,
            payload.horizon,
            payload.holdout_days,
            payload.min_train_days,
            payload.min_train_samples,
            payload.train_window_days,
            payload.max_events_per_symbol_day,
            payload.event_selection_strategy,
            payload.direction_return_threshold,
            payload.direction_neutral_policy,
            payload.direction_label_target,
            payload.min_cp_prob,
            payload.min_whale_score,
            payload.min_innovation_abs,
            payload.fallback_min_prob_up,
            payload.enable_filter,
            payload.min_top10_samples,
            payload.min_top10_precision,
            payload.min_top10_win_rate,
            payload.min_evaluated_days,
        )
        return ok(result)
    except Exception as exc:
        logger.error("Trade filter calibration failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/train")
async def train_model(payload: TrainRequest):
    try:
        meta = await asyncio.to_thread(
            forecaster.train,
            payload.lookback_days,
            payload.max_rows,
            payload.horizon,
            payload.direction_return_threshold,
            payload.direction_neutral_policy,
            payload.direction_label_target,
            payload.min_cp_prob,
            payload.min_whale_score,
            payload.min_innovation_abs,
        )
        return ok(meta)
    except Exception as exc:
        logger.error("Train failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/train-experiment")
async def train_experiment(payload: TrainRequest):
    try:
        meta = await asyncio.to_thread(
            forecaster.train_experiment,
            payload.lookback_days,
            payload.max_rows,
            payload.horizon,
            payload.direction_return_threshold,
            payload.direction_neutral_policy,
            payload.direction_label_target,
            payload.min_cp_prob,
            payload.min_whale_score,
            payload.min_innovation_abs,
        )
        return ok(meta)
    except Exception as exc:
        logger.error("Train experiment failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/train-symbol")
async def train_symbol_model(payload: TrainSymbolRequest):
    try:
        meta = await asyncio.to_thread(
            forecaster.train_symbol,
            payload.symbol,
            payload.lookback_days,
            payload.max_rows,
            payload.horizon,
            payload.force_promote,
            payload.direction_return_threshold,
            payload.direction_neutral_policy,
            payload.direction_label_target,
            payload.min_cp_prob,
            payload.min_whale_score,
            payload.min_innovation_abs,
        )
        return ok(meta)
    except Exception as exc:
        logger.error("Train symbol failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/backtest")
async def backtest_model(payload: BacktestRequest):
    try:
        result = await asyncio.to_thread(
            forecaster.backtest_current_model,
            payload.lookback_days,
            payload.max_rows,
            payload.horizon,
            payload.holdout_days,
            payload.direction_return_threshold,
            payload.direction_neutral_policy,
            payload.direction_label_target,
            payload.min_cp_prob,
            payload.min_whale_score,
            payload.min_innovation_abs,
        )
        return ok(result)
    except Exception as exc:
        logger.error("Backtest failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/rolling-backtest")
async def rolling_backtest(payload: RollingBacktestRequest):
    try:
        result = await asyncio.to_thread(
            forecaster.rolling_backtest,
            payload.lookback_days,
            payload.max_rows,
            payload.horizon,
            payload.holdout_days,
            payload.min_train_days,
            payload.min_train_samples,
            payload.train_window_days,
            payload.max_events_per_symbol_day,
            payload.event_selection_strategy,
            payload.direction_return_threshold,
            payload.direction_neutral_policy,
            payload.direction_label_target,
            payload.min_cp_prob,
            payload.min_whale_score,
            payload.min_innovation_abs,
            payload.trade_rule_min_prob_up,
        )
        return ok(result)
    except Exception as exc:
        logger.error("Rolling backtest failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/rolling-backtest-scan")
async def rolling_backtest_scan(payload: RollingBacktestScanRequest):
    try:
        result = await asyncio.to_thread(
            forecaster.rolling_backtest_scan,
            payload.lookback_days,
            payload.max_rows,
            payload.horizon,
            payload.holdout_days,
            payload.min_train_days,
            payload.min_train_samples,
            payload.train_window_days,
            payload.max_events_per_symbol_day,
            payload.event_selection_strategy,
            payload.direction_return_thresholds,
            payload.max_events_per_symbol_day_options,
            payload.direction_neutral_policy,
            payload.direction_label_target,
            payload.min_cp_prob,
            payload.min_whale_score,
            payload.min_innovation_abs,
        )
        return ok(result)
    except Exception as exc:
        logger.error("Rolling backtest scan failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/predict-event")
async def predict_event(payload: ForecastEvent):
    try:
        result = await asyncio.to_thread(forecaster.predict_events, [payload.dict()])
        predictions = result.get("predictions", [])
        if not predictions:
            raise HTTPException(status_code=400, detail="No prediction output for event")
        return ok({"prediction": predictions[0], "model": result.get("model", {})})
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("predict-event failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/predict-batch")
async def predict_batch(payload: BatchPredictRequest):
    if not payload.events:
        return ok({"predictions": [], "model": forecaster.model_info()})
    try:
        events = [event.dict() for event in payload.events]
        result = await asyncio.to_thread(forecaster.predict_events, events)
        return ok(result)
    except Exception as exc:
        logger.error("predict-batch failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
