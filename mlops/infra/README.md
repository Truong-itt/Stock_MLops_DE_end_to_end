# MLOps Infra (MLflow + Airflow + Whale ML)

Thư mục `mlops/infra/` là cụm MLOps chạy cho bài toán phát hiện và dự báo biến động bất thường.

## Thành phần chính

- `minio`: artifact/object store cho MLflow
- `postgres`: metadata DB cho Airflow/infra
- `mlflow`: tracking + registry
- `airflow-webserver`, `airflow-scheduler`: điều phối retrain
- `whale-ml-service`: train + serve model dự báo hậu bất thường
- `gradio`: UI test nhanh endpoint dự báo

## Khởi động full cụm

Từ thư mục `mlops/infra`:

```bash
docker compose up -d --build
```

## URL mặc định

- Airflow: `http://localhost:8080` (user/pass mặc định: `airflow/airflow`)
- MLflow: `http://localhost:5000`
- MinIO API: `http://localhost:9100`
- MinIO Console: `http://localhost:9101`
- Whale ML Service: `http://localhost:8090`
- Gradio: `http://localhost:7860`

## Luồng retrain chuẩn

1. Airflow DAG gọi `GET /health` của whale-ml-service.
2. Gọi `POST /train` với tham số từ `dag_run.conf` (hoặc Airflow Variables).
3. Service train 3 classifier + 3 regressor, chọn winner theo score và auto promote alias `production`.
4. Sau train, verify `GET /model/info`:
   - `ready=true`
   - có `mlflow_run_id`
   - có `model_version`
   - có `selected_models.direction` và `selected_models.sessions`
5. Web backend gọi `POST /predict-batch` để enrich cảnh báo BOCPD bằng forecast ML.

## DAG retrain

- DAG ID: `whale_ml_retrain_pipeline`
- File: `dags/whale_ml_retrain_pipeline.py`
- Schedule mặc định: `0 */6 * * *` (6 giờ/lần)

### `dag_run.conf` chuẩn

```json
{
  "lookback_days": 240,
  "max_rows": 120000,
  "horizon": 5,
  "timeout_seconds": 900
}
```

### Test DAG thủ công

```bash
docker compose exec airflow-webserver \
  airflow dags test whale_ml_retrain_pipeline 2026-03-24
```

## Chính sách train/serve khuyến nghị

- Retrain chính qua Airflow.
- `AUTO_RETRAIN_INTERVAL_MIN=0` để tránh train chồng trong service.
- Serving ưu tiên MLflow Registry alias `production`.
- Giữ local artifact làm fallback an toàn khi registry lỗi tạm thời.

## Tiêu chí chọn winner production

- Direction winner: score cao nhất theo `ROC-AUC`; nếu candidate không tính được ROC-AUC thì fallback theo `Accuracy`.
- Sessions winner: score cao nhất theo `-MAE` (tức là MAE nhỏ nhất sẽ thắng).
- Sau khi chọn winner, service tự register model version mới và tự set alias `production` sang version này.

## Kiểm tra nhanh sau retrain

Từ thư mục `mlops/infra`:

```bash
curl -s http://localhost:8090/health | jq '.data | {model_name, model_version, selected_models, mlflow_run_id}'
curl -s http://localhost:8090/model/info | jq '.data | {ready, selected_models, metrics, model_version, model_source}'
```

## Tài liệu chi tiết

- `whale_ml/README.md`: công thức toán, train logic, API.
- `../search/README.md`: công thức BOCPD và cách sinh tín hiệu bất thường đầu vào cho Whale ML.
