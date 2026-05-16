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

### 3.1 Champion-Challenger theo mã (symbol)

- Champion: model global (`whale_move_forecaster@production`).
- Challenger: model train riêng theo mã khi có event bất thường đi vào `predict-event/predict-batch`.
- Challenger được train nền (background), có cooldown theo mã để tránh train dồn dập.
- Trước khi promote, challenger bắt buộc so sánh với champion global trên holdout của chính mã đó:
  - `direction_delta = challenger_direction_score - global_direction_score`
  - `sessions_delta = challenger_sessions_score - global_sessions_score`
- Chỉ promote lên alias `production` của model theo mã khi qua ngưỡng cấu hình.
- Dù không promote, challenger vẫn được log MLflow (alias `candidate`) để theo dõi.

## 4) API

- `GET /health`
- `GET /model/info`
- `GET /model/symbols`
- `GET /trade-filter`
- `POST /trade-filter`
- `POST /trade-filter/calibrate`
- `POST /train`
- `POST /train-experiment`
- `POST /train-symbol`
- `POST /rolling-backtest`
- `POST /rolling-backtest-scan`
- `POST /predict-event`
- `POST /predict-batch`

### `POST /train` request

```json
{
  "lookback_days": 240,
  "max_rows": 120000,
  "horizon": 5,
  "direction_return_threshold": 0.005,
  "direction_neutral_policy": "drop",
  "direction_label_target": "horizon_extreme",
  "min_cp_prob": 0.0,
  "min_whale_score": 0.0,
  "min_innovation_abs": 0.0
}
```

### `POST /train-experiment`

Chạy cùng logic train nhưng không ghi MLflow Registry, không lưu artifact và không promote production. Dùng endpoint này để thử label/filter trước khi retrain thật:

```bash
curl -s -X POST http://localhost:8090/train-experiment \
  -H 'Content-Type: application/json' \
  -d '{"lookback_days":10,"max_rows":120000,"horizon":5,"direction_return_threshold":0.005,"direction_neutral_policy":"drop","direction_label_target":"horizon_extreme"}' \
  | jq '.data | {samples, selected_models, metrics, compare_with_champion: .compare_with_champion.decision}'
```

### `POST /rolling-backtest`

Rolling backtest train lại model theo từng ngày, chỉ dùng dữ liệu trước ngày test. Endpoint này dùng để phát hiện leakage và thử dedupe event mà không ảnh hưởng production. Response có thêm:

- `overall.confidence_slices`: top 10/20/30% tín hiệu tự tin nhất có lift so với majority baseline hay không
- `overall.trade_slices`: `precision_direction`, `paper_return_mean`, `paper_win_rate`, `long_only_return_mean`, `threshold_hit_rate` cho top 10/20/30% theo `confidence` và theo `prob_up_long`

```bash
curl -s -X POST http://localhost:8090/rolling-backtest \
  -H 'Content-Type: application/json' \
  -d '{"lookback_days":10,"max_rows":40000,"horizon":5,"holdout_days":3,"min_train_days":3,"min_train_samples":300,"max_events_per_symbol_day":3,"event_selection_strategy":"strongest","direction_return_threshold":0.005,"direction_neutral_policy":"drop","direction_label_target":"horizon_extreme"}' \
  | jq '.data | {overall: {model: .overall.model, trade_slices: .overall.trade_slices, confidence_slices: .overall.confidence_slices, baseline: .overall.baseline}, daily_summary, params}'
```

### `POST /rolling-backtest-scan`

Quét nhiều cấu hình lọc nhiễu trong một lệnh để so sánh nhanh `samples` và `accuracy/precision`:

```bash
curl -s -X POST http://localhost:8090/rolling-backtest-scan \
  -H 'Content-Type: application/json' \
  -d '{"lookback_days":10,"max_rows":40000,"horizon":5,"holdout_days":3,"min_train_days":3,"min_train_samples":300,"event_selection_strategy":"strongest","direction_return_thresholds":[0.005,0.008,0.01,0.012,0.015,0.02],"max_events_per_symbol_day_options":[1,2,3],"direction_neutral_policy":"drop","direction_label_target":"horizon_extreme"}' \
  | jq '.data | {best_by_top10_prob_up_precision, best_by_overall_accuracy, rows_sorted_by_top10_prob_up_precision: .rows_sorted_by_top10_prob_up_precision[0:5]}'
```

### `POST /trade-filter/calibrate`

Tự chạy rolling-backtest và lấy ngưỡng `min_prob_up` từ bucket `top_10pct_prob_up` để bật lọc trade live:

```bash
curl -s -X POST http://localhost:8090/trade-filter/calibrate \
  -H 'Content-Type: application/json' \
  -d '{"lookback_days":10,"max_rows":40000,"horizon":5,"holdout_days":5,"min_train_days":3,"min_train_samples":300,"max_events_per_symbol_day":1,"event_selection_strategy":"strongest","direction_return_threshold":0.02,"direction_neutral_policy":"drop","direction_label_target":"horizon_extreme","fallback_min_prob_up":0.88,"enable_filter":true}' \
  | jq '.data'
```

### `GET /trade-filter`

Xem rule filter trade đang áp dụng (`enabled`, `min_prob_up`, `source`, `calibrated_at`).

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

### `POST /train-symbol` request

```json
{
  "symbol": "AAPL",
  "lookback_days": 240,
  "max_rows": 20000,
  "horizon": 5,
  "force_promote": false
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

Rolling backtest production model trên nhiều ngày đã có label:

```bash
curl -s -X POST http://localhost:8090/backtest \
  -H 'Content-Type: application/json' \
  -d '{"lookback_days":30,"max_rows":120000,"horizon":5,"holdout_days":10}' \
  | jq '.data.overall, .data.daily_summary'
```

Online predictions được ghi vào ClickHouse table `stock_warehouse.whale_ml_prediction_audit`.
Table này lưu `event_key`, `symbol`, `event_time`, model version, xác suất direction, expected sessions và payload JSON.
Các cột `actual_*` được để sẵn cho job reconcile sau này khi dữ liệu tương lai đủ label.

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
  - `TRAIN_MAX_EVENTS_PER_SYMBOL_DAY` (default `40`; giữ độ phủ ngày rộng hơn khi event dày)
  - `TRAIN_EVENT_SELECTION_STRATEGY` (`latest`, `cp_prob`, `whale_score`, `innovation_abs`, `strongest`; dùng cùng `TRAIN_MAX_EVENTS_PER_SYMBOL_DAY=1..3` để dedupe theo mã/ngày)
  - `MAX_FORECAST_HORIZON` (default `5`)
  - `WALK_FORWARD_ENABLED` (default `1`)
  - `WALK_FORWARD_FOLDS` (default `4`)
  - `WALK_FORWARD_MIN_TRAIN_DATES` (default `60`)
  - `WALK_FORWARD_TEST_DATES` (default `20`)
  - `RECENCY_WEIGHT_ENABLED` (default `1`; ưu tiên sample mới hơn khi fit model)
  - `RECENCY_WEIGHT_HALF_LIFE_DAYS` (default `10`; sample cũ hơn mỗi 10 ngày giảm nửa trọng số trước khi chuẩn hoá)
  - `RECENCY_WEIGHT_MIN` (default `0.25`; sàn trọng số thô cho sample cũ)
  - `DIRECTION_THRESHOLD_TUNING_ENABLED` (default `1`; tune ngưỡng `prob_up` cho direction thay vì cố định `0.5`)
  - `DIRECTION_THRESHOLD_MIN`, `DIRECTION_THRESHOLD_MAX`, `DIRECTION_THRESHOLD_STEP`
  - `DIRECTION_THRESHOLD_METRIC` (default `balanced_accuracy`; tránh ngưỡng đoán một chiều để lấy accuracy ảo)
  - `DIRECTION_RETURN_THRESHOLD` (default `0.0`; nếu > 0 thì bỏ/giữ vùng return nhỏ theo `DIRECTION_NEUTRAL_POLICY`)
  - `DIRECTION_NEUTRAL_POLICY` (`drop` hoặc `sign`; `drop` bỏ mẫu có biên return dưới threshold)
  - `DIRECTION_LABEL_TARGET` (`next_close` giữ label cũ; `horizon_extreme` dùng max/min close trong horizon để label cú move đáng trade)
  - `TRAIN_MIN_CP_PROB`, `TRAIN_MIN_WHALE_SCORE`, `TRAIN_MIN_INNOVATION_ABS` (default `0.0`; lọc event train yếu/nhiễu)
  - `MODEL_SELECTION_STD_PENALTY` (default `0.5`; trừ điểm model có CV biến động cao để giảm chọn nhầm model may mắn theo fold)
  - `PREDICTION_AUDIT_ENABLED` (default `1`; ghi prediction online vào ClickHouse)
  - `PREDICTION_AUDIT_TABLE` (default `whale_ml_prediction_audit`)
  - Direction model dùng thêm technical features từ `v_ohlcv_daily`: return 1/3/5 ngày, volatility 5 ngày, daily range, close position, volume z-score, `change_percent`.
  - Direction candidate pool có thêm `extra_trees_classifier` và `hist_gradient_boosting_classifier` để bắt nonlinear signal tốt hơn.
  - Serving giữ backward compatibility bằng cách build vector theo `feature_names` lưu trong bundle; model production cũ vẫn dùng đúng bộ feature cũ.
  - Sessions model có thêm baseline candidates (`one_session_baseline`, `median_sessions_baseline`) để không over-predict khi thị trường hiện tại chỉ giữ hướng 1 phiên.
  - Nếu dữ liệu có ít ngày hơn cấu hình chuẩn, walk-forward tự thu nhỏ cửa sổ và đánh dấu `adaptive=true` trong `evaluation_split`.
  - `GLOBAL_PROMOTION_REQUIRE_BOTH` (`1`: global challenger phải thắng cả direction và sessions)
  - `GLOBAL_PROMOTION_MIN_DIRECTION_DELTA`, `GLOBAL_PROMOTION_MIN_SESSIONS_DELTA`
  - `WHALE_ML_GLOBAL_CANDIDATE_ALIAS` (default `candidate`)
  - Global challenger chỉ lên `production` khi qua ngưỡng cấu hình và cải thiện ít nhất một metric so với champion hiện tại.
  - `AUTO_TRAIN_ON_STARTUP`
  - `AUTO_RETRAIN_INTERVAL_MIN` (production nên để `0`, Airflow điều phối retrain)
- Symbol challenger policy:
  - `SYMBOL_CHALLENGER_ENABLED` (bật/tắt train riêng theo mã)
  - `SYMBOL_TRAIN_ON_ANOMALY` (trigger train khi có event bất thường đi vào API predict)
  - `SYMBOL_TRAIN_COOLDOWN_MIN` (cooldown train lại theo mã)
  - `SYMBOL_TRAIN_LOOKBACK_DAYS`, `SYMBOL_TRAIN_MAX_ROWS`, `SYMBOL_MIN_TRAIN_SAMPLES`
  - `SYMBOL_PROMOTION_REQUIRE_BOTH` (`1`: challenger phải thắng cả direction và sessions)
  - `SYMBOL_PROMOTION_MIN_DIRECTION_DELTA`, `SYMBOL_PROMOTION_MIN_SESSIONS_DELTA`
  - `WHALE_ML_SYMBOL_CANDIDATE_ALIAS` (default `candidate`)
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



model global

TRAIN_LOOKBACK_DAYS có thể nhìn lại giá trị 240 ngày 

TRAIN_MAX_ROWS số dòng nhận ban đầu có thể đat được tối đa 120000 dòng

MAX_FORECAST_HORIZON có thệ dự đoán tối đa số phiên mặc định 5 

GLOBAL_MIN_TRAIN_SAMPLES  sau khi lọc dữ liệu thì cần tôi thiểu mặc định 800

SYMBOL_TRAIN_LOOKBACK_DAYS số ngày  có thể  được tối đa cho train riêng từng mã 240 ngày

SYMBOL_TRAIN_MAX_ROWS số lượng tối đa dòng khi thu thập ban đầu 20000

SYMBOL_MIN_TRAIN_SAMPLES số lượng mẫu min sau khi thực hiện lọc 350


SYMBOL_TRAIN_COOLDOWN_MIN từ khi nhận tính hiệu bát thươngf cho đến mặc định 180 phút tiếp theo dù có nhận tính hiệu bất thường cũng không train lại model


các ngưỡng để pass lên production
GLOBAL_PROMOTION_REQUIRE_BOTH: "1"
GLOBAL_PROMOTION_MIN_DIRECTION_DELTA: "0.0"
GLOBAL_PROMOTION_MIN_SESSIONS_DELTA: "0.0"
WALK_FORWARD_ENABLED: "1"
WALK_FORWARD_FOLDS: "4"
WALK_FORWARD_MIN_TRAIN_DATES: "60"
WALK_FORWARD_TEST_DATES: "20"
RECENCY_WEIGHT_ENABLED: "1"
RECENCY_WEIGHT_HALF_LIFE_DAYS: "10"
RECENCY_WEIGHT_MIN: "0.25"
DIRECTION_THRESHOLD_TUNING_ENABLED: "1"
DIRECTION_THRESHOLD_MIN: "0.35"
DIRECTION_THRESHOLD_MAX: "0.65"
DIRECTION_THRESHOLD_STEP: "0.01"
DIRECTION_THRESHOLD_METRIC: "balanced_accuracy"
DIRECTION_RETURN_THRESHOLD: "0.0"
DIRECTION_NEUTRAL_POLICY: "drop"
DIRECTION_LABEL_TARGET: "next_close"
TRAIN_EVENT_SELECTION_STRATEGY: "latest"
TRAIN_MIN_CP_PROB: "0.0"
TRAIN_MIN_WHALE_SCORE: "0.0"
TRAIN_MIN_INNOVATION_ABS: "0.0"
MODEL_SELECTION_STD_PENALTY: "0.5"
SYMBOL_PROMOTION_REQUIRE_BOTH: "1"
SYMBOL_PROMOTION_MIN_DIRECTION_DELTA: "0.0"
SYMBOL_PROMOTION_MIN_SESSIONS_DELTA: "0.0"
