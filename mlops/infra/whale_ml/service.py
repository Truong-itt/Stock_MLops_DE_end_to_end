import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from modeling import WhaleMoveForecaster


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("whale_ml.service")

AUTO_TRAIN_ON_STARTUP = os.getenv("AUTO_TRAIN_ON_STARTUP", "1").strip().lower() in {
    "1",
    "true",
    "yes",
}
AUTO_RETRAIN_INTERVAL_MIN = int(os.getenv("AUTO_RETRAIN_INTERVAL_MIN", "0"))

forecaster = WhaleMoveForecaster()
retrain_task: Optional[asyncio.Task] = None


def ok(data):
    return {"status": "ok", "data": data}


class TrainRequest(BaseModel):
    lookback_days: Optional[int] = Field(default=None, ge=30, le=720)
    max_rows: Optional[int] = Field(default=None, ge=2000, le=800000)
    horizon: Optional[int] = Field(default=None, ge=2, le=20)


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


async def _retrain_loop():
    interval_seconds = max(AUTO_RETRAIN_INTERVAL_MIN, 1) * 60
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            logger.info("Auto retrain triggered")
            meta = await asyncio.to_thread(forecaster.train)
            logger.info("Auto retrain completed (samples=%s)", meta.get("samples"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Auto retrain failed: %s", exc)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global retrain_task
    forecaster.connect()
    loaded = forecaster.load_bundle()
    if loaded:
        logger.info("Loaded existing model bundle (source=%s)", forecaster.model_info().get("model_source"))

    if AUTO_TRAIN_ON_STARTUP and not forecaster.is_ready():
        try:
            meta = await asyncio.to_thread(forecaster.train)
            logger.info("Initial training complete (samples=%s)", meta.get("samples"))
        except Exception as exc:
            logger.warning("Initial training failed: %s", exc)

    if AUTO_RETRAIN_INTERVAL_MIN > 0:
        retrain_task = asyncio.create_task(_retrain_loop())
        logger.warning(
            "Emergency auto retrain is enabled in service (interval=%d minutes). "
            "Recommended production mode is Airflow-driven retrain with AUTO_RETRAIN_INTERVAL_MIN=0.",
            AUTO_RETRAIN_INTERVAL_MIN,
        )

    yield

    if retrain_task:
        retrain_task.cancel()
        try:
            await retrain_task
        except asyncio.CancelledError:
            pass
    forecaster.close()


app = FastAPI(
    title="Whale Move Forecast Service",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    info = forecaster.model_info()
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
        }
    )


@app.get("/model/info")
async def model_info():
    return ok(forecaster.model_info())


@app.post("/train")
async def train_model(payload: TrainRequest):
    try:
        meta = await asyncio.to_thread(
            forecaster.train,
            payload.lookback_days,
            payload.max_rows,
            payload.horizon,
        )
        return ok(meta)
    except Exception as exc:
        logger.error("Train failed: %s", exc)
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
