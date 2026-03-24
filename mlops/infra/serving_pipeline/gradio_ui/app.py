import os
from datetime import datetime, timezone

import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field


PREDICT_API_URL = os.getenv("PREDICT_API_URL", "http://whale-ml-service:8090/predict-event")
HEALTH_API_URL = os.getenv("HEALTH_API_URL", "http://whale-ml-service:8090/health")

app = FastAPI(title="Whale ML UI", version="1.0.0")


class PredictInput(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    cp_prob: float
    whale_score: float
    innovation_zscore: float
    expected_run_length: float
    return_value: float


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Whale ML Forecast</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 900px; margin: 24px auto; padding: 0 12px; }
    h1 { margin-bottom: 8px; }
    .row { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin-bottom: 12px; }
    label { display: block; font-size: 14px; margin-bottom: 4px; }
    input { width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 6px; }
    button { padding: 10px 14px; border: 0; border-radius: 6px; background: #0b5fff; color: #fff; cursor: pointer; }
    pre { background: #f5f5f5; border-radius: 8px; padding: 12px; overflow: auto; }
    .actions { display: flex; gap: 8px; margin: 10px 0 14px; }
  </style>
</head>
<body>
  <h1>Whale ML Forecast UI</h1>
  <p>Nhap signal de du doan huong tang/giam va so phien ky vong.</p>

  <div class="actions">
    <button onclick="checkHealth()">Kiem tra health</button>
    <button onclick="predict()">Du doan</button>
  </div>

  <div id="health"></div>
  <div class="row">
    <div><label>Symbol</label><input id="symbol" value="AAPL"/></div>
    <div><label>CP Prob</label><input id="cp_prob" type="number" step="any" value="0.22"/></div>
  </div>
  <div class="row">
    <div><label>Whale Score</label><input id="whale_score" type="number" step="any" value="0.18"/></div>
    <div><label>Innovation z</label><input id="innovation_zscore" type="number" step="any" value="2.4"/></div>
  </div>
  <div class="row">
    <div><label>Expected run length</label><input id="expected_run_length" type="number" step="any" value="3.0"/></div>
    <div><label>Return value</label><input id="return_value" type="number" step="any" value="0.004"/></div>
  </div>

  <h3>Result</h3>
  <pre id="result">{}</pre>

  <script>
    async function checkHealth() {
      const el = document.getElementById("health");
      try {
        const resp = await fetch("/health-upstream");
        const data = await resp.json();
        el.innerText = "Health: " + JSON.stringify(data);
      } catch (e) {
        el.innerText = "Health error: " + e;
      }
    }

    async function predict() {
      const payload = {
        symbol: (document.getElementById("symbol").value || "").trim().toUpperCase(),
        cp_prob: Number(document.getElementById("cp_prob").value),
        whale_score: Number(document.getElementById("whale_score").value),
        innovation_zscore: Number(document.getElementById("innovation_zscore").value),
        expected_run_length: Number(document.getElementById("expected_run_length").value),
        return_value: Number(document.getElementById("return_value").value),
      };
      const out = document.getElementById("result");
      out.innerText = "Loading...";
      try {
        const resp = await fetch("/predict", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const body = await resp.json();
        out.innerText = JSON.stringify(body, null, 2);
      } catch (e) {
        out.innerText = JSON.stringify({ error: String(e) }, null, 2);
      }
    }
  </script>
</body>
</html>
"""


@app.get("/health")
def health():
    return {"status": "ok", "service": "whale-ml-ui"}


@app.get("/health-upstream")
def health_upstream():
    try:
        resp = requests.get(HEALTH_API_URL, timeout=8)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Upstream health failed: {exc}")


@app.post("/predict")
def predict(payload: PredictInput):
    request_payload = {
        "symbol": payload.symbol.strip().upper(),
        "event_time": datetime.now(tz=timezone.utc).isoformat(),
        "cp_prob": payload.cp_prob,
        "whale_score": payload.whale_score,
        "innovation_zscore": payload.innovation_zscore,
        "expected_run_length": payload.expected_run_length,
        "return_value": payload.return_value,
    }
    try:
        resp = requests.post(PREDICT_API_URL, json=request_payload, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Predict upstream failed: {exc}")


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=7860, reload=False)
