# Search Module (BOCPD Worker)

Module `search` chạy phát hiện changepoint theo thời gian thực cho giá cổ phiếu bằng BOCPD, sau đó ghi kết quả ra ScyllaDB và ClickHouse để phục vụ monitoring và pipeline ML.

## 1. Thành phần chính

- `bocpd.py`: lõi BOCPD cho chuỗi return, giả định Gaussian mean ~ 0 và variance thay đổi theo regime.
- `changepoint_worker.py`: worker chính, kết nối DB, bootstrap dữ liệu lịch sử, xử lý live, ghi kết quả.
- `symbol_registry.py`: đọc danh sách mã theo file JSON, hỗ trợ reload khi file thay đổi.
- `docker-compose.yml` và `Dockerfile`: cấu hình chạy container.

## 2. Input và output dữ liệu

### Input từ ScyllaDB

Worker đọc dữ liệu từ keyspace `stock_data`:

- `stock_prices`: dùng cho bootstrap lịch sử.
- `stock_latest_prices`: dùng cho polling realtime.

Các cột đầu vào cần có:

- `symbol`
- `price`
- `timestamp`

### Output ra ScyllaDB (tự tạo nếu chưa có)

- `stock_changepoint_latest`
- `stock_changepoint_history`

### Output ra ClickHouse (tự tạo nếu chưa có)

- `stock_changepoint_events` (source-of-truth cho event stream BOCPD)
- `v_changepoint_latest` (view bản ghi mới nhất theo symbol)

Các trường kết quả chính:

- `cp_prob`: xác suất vừa xảy ra changepoint tại tick hiện tại (`P(r_t = 0 | x_1:t)`). Càng gần `1` thì khả năng đổi chế độ càng cao.
- `expected_run_length`: kỳ vọng số bước đã đi từ changepoint gần nhất. Giá trị thấp thường nghĩa là vừa có dấu hiệu reset regime.
- `map_run_length`: run-length có xác suất lớn nhất (ước lượng “khả dĩ nhất” thay vì trung bình như `expected_run_length`).
- `predictive_volatility`: độ biến động dự đoán của return ở trạng thái hiện tại; cao nghĩa là thị trường đang nhiễu/biến động hơn.
- `innovation_zscore`: độ bất thường của return mới so với biến động kỳ vọng trước cập nhật (`|R_t| / sigma_pred_before`). Thường `> 2` là bắt đầu đáng chú ý, `> 3` là khá bất thường.
- `whale_score`: điểm cảnh báo nghiệp vụ nội bộ (kết hợp `cp_prob` và `innovation_zscore`), chuẩn hóa trong `[0, 1]`; cao hơn nghĩa là tín hiệu “watch” mạnh hơn.
- `hazard`: xác suất nền để chuyển regime ở bước hiện tại theo hazard function của BOCPD (chịu ảnh hưởng trực tiếp bởi cấu hình `hazard_lambda`).
- `evidence`: hằng số chuẩn hóa posterior tại bước hiện tại; phản ánh mức “phù hợp” tổng thể của quan sát mới với các giả thuyết hiện có.
- `regime_label`: nhãn diễn giải vận hành từ model gồm `stable`, `transition`, `whale-watch`.
- `source`: nguồn phát sinh event, gồm `bootstrap` (từ dữ liệu lịch sử) hoặc `live` (từ polling realtime).

## 3. Luồng xử lý

1. Worker khởi động, kết nối ScyllaDB và ClickHouse (có retry).
2. Tạo schema output nếu chưa tồn tại.
3. Đồng bộ symbol registry và bootstrap cho symbol mới.
4. Vào vòng lặp vô hạn.
5. Reload registry nếu file đổi.
6. Poll `stock_latest_prices`, tính return mới, cập nhật BOCPD theo từng symbol.
7. Ghi kết quả vào ClickHouse, đồng thời ghi sang Scylla cho serving.

Chi tiết bootstrap:

- Query lịch sử `stock_prices` theo `BOCPD_BOOTSTRAP_LIMIT`.
- Sắp xếp theo thời gian tăng dần, tính return `R_t = p_t / p_{t-1} - 1`.
- Gọi `model.update(return)` cho từng bước.
- Luôn ghi ClickHouse; Scylla chỉ giữ phần gần nhất theo `BOCPD_BOOTSTRAP_HISTORY_LIMIT`.
- Nếu không có lịch sử, fallback lấy mốc từ `stock_latest_prices`.

## 4. Mô hình BOCPD trong module

Đầu vào cho BOCPD là return:

- `R_t = p_t / p_{t-1} - 1`

Cấu hình BOCPD:

- `alpha0`, `beta0`: prior Gamma cho precision.
- `hazard_lambda`: điều khiển mức nhạy changepoint.
- `hazard_prior`: kiểu prior hazard (`poisson` mặc định trong `BOCPDConfig`; worker hiện không expose env riêng).
- `max_run_length`: giới hạn số trạng thái run-length.
- `tail_mass_threshold`: ngưỡng cắt đuôi posterior.

Nhãn regime đang dùng:

- `stable`
- `transition`
- `whale-watch`

## 5. Biến môi trường

Các biến chính (đọc trực tiếp từ code):

- `SCYLLA_CONTACT_POINTS` (mặc định: `scylla-node1,scylla-node2,scylla-node3`)
- `SCYLLA_PORT` (mặc định: `9042`)
- `SCYLLA_KEYSPACE` (mặc định: `stock_data`)
- `CLICKHOUSE_HOST` (mặc định: `clickhouse`)
- `CLICKHOUSE_PORT` (mặc định: `8123`)
- `CLICKHOUSE_USER` (mặc định: `default`)
- `CLICKHOUSE_PASSWORD` (mặc định: `truongittstock`)
- `CLICKHOUSE_DB` (mặc định: `stock_warehouse`)
- `SYMBOL_REGISTRY_PATH` (mặc định: `/app/config/symbol_registry.json`)
- `BOCPD_ALPHA0` (mặc định: `1.0`)
- `BOCPD_BETA0` (mặc định: `0.0001`)
- `BOCPD_HAZARD_LAMBDA` (mặc định: `90`)
- `BOCPD_MAX_RUN_LENGTH` (mặc định: `180`)
- `BOCPD_TAIL_MASS_THRESHOLD` (mặc định: `0.000001`)
- `BOCPD_BOOTSTRAP_LIMIT` (mặc định: `120`)
- `BOCPD_BOOTSTRAP_HISTORY_LIMIT` (mặc định: `80`)
- `BOCPD_POLL_INTERVAL` (mặc định: `2.0` giây)

## 6. Cấu trúc symbol registry

File registry mẫu đang dùng là `config/symbol_registry.json`, gồm 2 market `vn` và `world`.

Ví dụ tối thiểu:

```json
{
  "version": 1,
  "updated_at": "2026-01-01T00:00:00+00:00",
  "markets": {
    "vn": {
      "label": "Vietnam",
      "topic": "stock_price_vn",
      "symbols": ["VCB", "FPT"]
    },
    "world": {
      "label": "World",
      "topic": "stock_price_dif",
      "symbols": ["AAPL", "NVDA"]
    }
  }
}
```

Lưu ý:

- Worker tự reload file khi thay đổi mtime, không cần restart để thêm/bớt symbol.
- Symbol được normalize về uppercase và loại trùng.

## 7. Chạy module

### Chạy bằng Docker Compose

Từ thư mục `search`:

```bash
docker network create stock-network || true
docker compose up -d --build
docker compose logs -f changepoint-worker
```

`docker-compose.yml` mount `../config` vào `/app/config`, nên cần đảm bảo file `symbol_registry.json` có sẵn ở thư mục `config` của project.

### Chạy local bằng Python

```bash
cd search
pip install -r requirements.txt
python -u changepoint_worker.py
```

## 8. Checklist vận hành nhanh

1. Kiểm tra worker connect được ScyllaDB và ClickHouse.
2. Kiểm tra log có dòng `Bootstrapped <symbol> with ...`.
3. Kiểm tra ClickHouse có dữ liệu mới trong `stock_changepoint_events`.
4. Kiểm tra Scylla có dữ liệu ở `stock_changepoint_latest`.
5. Theo dõi tần suất `whale-watch` để điều chỉnh `BOCPD_HAZARD_LAMBDA` nếu cần.

## 9. Tài liệu liên quan trong module

- `README_CORE_BOCPD.md`: giải thích toán BOCPD chi tiết.
- `README_WORKFLOW.md`: mô tả workflow đầu-cuối của pipeline.
