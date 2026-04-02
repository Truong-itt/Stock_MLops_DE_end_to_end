# Whale ML Service

Service `whale-ml-service` dự báo hậu bất thường BOCPD cho từng mã:

- hướng dự kiến: `up/down`
- xác suất `P(up)`, `P(down)`
- số phiên liên tiếp kỳ vọng theo hướng đó

## 1) Dữ liệu train

Nguồn chính từ ClickHouse:

- `stock_warehouse.stock_changepoint_events`
- `stock_warehouse.v_ohlcv_daily`

Ý tưởng: mỗi event BOCPD tại thời điểm `t` sẽ gắn nhãn theo diễn biến đóng cửa trong các phiên kế tiếp.

## 2) Công thức toán đang dùng

### 2.1 Return phiên kế tiếp

Với close tại ngày event `d` là `C_d`, phiên sau là `C_{d+1}`:

$$
R_{d+1}=\frac{C_{d+1}}{C_d}-1
$$

### 2.2 Nhãn phân loại hướng

$$
y_{\text{dir}}=
\begin{cases}
1 & \text{nếu } R_{d+1}\ge 0\\
0 & \text{nếu } R_{d+1}<0
\end{cases}
$$

### 2.3 Nhãn số phiên liên tiếp

Với horizon `H`, số phiên liên tiếp cùng dấu với `R_{d+1}`:

$$
y_{\text{sess}}=\min\left(H,\; \max k \text{ sao cho } \operatorname{sign}(R_{d+i})=\operatorname{sign}(R_{d+1}),\; i=1..k\right)
$$

### 2.4 Feature chính

Từ BOCPD event + ngữ cảnh thời gian/market:

- `cp_prob`, `whale_score`, `innovation_zscore`
- `expected_run_length`, `map_run_length`
- `predictive_volatility`, `return_value`
- `hazard`, `evidence`, `log_price`
- `hour_sin`, `hour_cos`, `dow_sin`, `dow_cos`
- `is_vn`, `is_world`

### 2.5 Mô hình và hàm dự báo

- Bài toán hướng `up/down` train 3 classifier:
  - `logistic_regression`
  - `random_forest_classifier`
  - `gradient_boosting_classifier`
- Bài toán số phiên kỳ vọng train 3 regressor:
  - `random_forest_regressor`
  - `extra_trees_regressor`
  - `gradient_boosting_regressor`
- Chọn model tốt nhất tự động:
  - Direction score: ưu tiên `ROC-AUC`, fallback `Accuracy` nếu không tính được AUC
  - Sessions score: `-MAE` (tương đương MAE càng thấp càng tốt)
  - Khi hòa điểm, direction ưu tiên `Accuracy` cao hơn; sessions ưu tiên `MAE` thấp hơn
- Pair winner (1 classifier + 1 regressor) được đóng gói để serving và đưa lên alias `production`.

- Quy tắc output:

$$
\text{direction}=
\begin{cases}
\text{up} & \text{nếu } P(\text{up})\ge 0.5\\
\text{down} & \text{ngược lại}
\end{cases}
$$

$$
\text{confidence}=\max(P(\text{up}),P(\text{down}))
$$

### 2.6 Metrics train

- Accuracy cho hướng
- ROC-AUC cho xác suất hướng
- F1 cho hướng
- MAE và RMSE cho số phiên:

$$
\text{MAE}=\frac{1}{n}\sum_{i=1}^{n}|y_i-\hat{y}_i|
$$

## 3) MLOps flow (đã triển khai)

Mỗi lần `POST /train`:

1. Train 3 classifier + 3 regressor từ ClickHouse.
2. Chấm điểm trên tập test và tự chọn winner cho từng task.
3. Log MLflow run (params/metrics/artifacts + leaderboard candidate qua các metric `classifier_*`, `regressor_*`).
4. Đăng ký model vào MLflow Registry:
   - model name mặc định: `whale_move_forecaster`
   - alias phục vụ: `production` (được cập nhật tự động sang winner mới)
5. Cập nhật metadata trong model bundle:
   - `selected_models`
   - `model_candidates`
   - `mlflow_run_id`
   - `model_name`
   - `model_version`
   - `model_uri`
   - `model_alias_uri`
   - `model_source`
6. Load model cho serving theo thứ tự:
   - ưu tiên `MLflow Registry` (`models:/...@production`)
   - fallback `joblib` local `/app/artifacts/whale_move_model.joblib`

## 4) API

- `GET /health`
- `GET /model/info`
- `POST /train`
- `POST /predict-event`
- `POST /predict-batch`

### `POST /train` request

```json
{
  "lookback_days": 240,
  "max_rows": 120000,
  "horizon": 5
}
```

### `POST /train` response (rút gọn)

```json
{
  "status": "ok",
  "data": {
    "samples": 12345,
    "metrics": {
      "accuracy": 0.62,
      "roc_auc": 0.67,
      "f1_direction": 0.61,
      "mae_sessions": 0.88,
      "rmse_sessions": 1.12,
      "classifier_score": 0.67,
      "regressor_score": -0.88
    },
    "selected_models": {
      "direction": "random_forest_classifier",
      "sessions": "extra_trees_regressor"
    },
    "mlflow_run_id": "....",
    "model_name": "whale_move_forecaster",
    "model_version": "17",
    "model_uri": "models:/whale_move_forecaster/17",
    "model_source": "registry"
  }
}
```

## 5) Chạy service

Từ thư mục `mlops/infra`:

```bash
docker compose up -d --build whale-ml-service
```

Kiểm tra:

```bash
curl -s http://localhost:8090/health | jq
curl -s http://localhost:8090/model/info | jq
```

Kiểm tra winner đã lên production:

```bash
curl -s http://localhost:8090/model/info \
  | jq '.data | {selected_models, model_version, model_source, mlflow_run_id}'
```

## 6) Biến môi trường quan trọng

- ClickHouse:
  - `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB`
- Registry/model:
  - `MLFLOW_TRACKING_URI`
  - `WHALE_ML_MODEL_NAME` (default `whale_move_forecaster`)
  - `WHALE_ML_MODEL_ALIAS` (default `production`)
  - `PREFER_MLFLOW_REGISTRY` (default `1`)
  - `MLFLOW_REGISTRY_REQUIRED` (default `1`)
- Train policy:
  - `TRAIN_LOOKBACK_DAYS` (default `240`)
  - `TRAIN_MAX_ROWS` (default `120000`)
  - `MAX_FORECAST_HORIZON` (default `5`)
  - `AUTO_TRAIN_ON_STARTUP`
  - `AUTO_RETRAIN_INTERVAL_MIN` (production nên để `0`, Airflow điều phối retrain)
- Fallback artifact:
  - `MODEL_ARTIFACT_PATH` (default `/app/artifacts/whale_move_model.joblib`)

## 7) Airflow retrain integration

DAG: `mlops/infra/dags/whale_ml_retrain_pipeline.py`

Chuẩn `dag_run.conf`:

- `lookback_days` (int)
- `max_rows` (int)
- `horizon` (int)
- `timeout_seconds` (int)

DAG verify bắt buộc sau train:

- `mlflow_run_id` có giá trị
- `model_version` có giá trị
- `selected_models.direction` có giá trị
- `selected_models.sessions` có giá trị
- model ở trạng thái ready cho serving
