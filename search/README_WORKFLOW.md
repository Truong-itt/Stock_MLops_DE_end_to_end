# Search Workflow (BOCPD)

---

## 1) module search

- `bocpd.py`: lõi toán BOCPD online.
- `changepoint_worker.py`: worker chạy liên tục, đọc dữ liệu giá, gọi BOCPD, ghi kết quả.
- `symbol_registry.py`: đọc danh sách mã theo file cấu hình.

Luồng giá vào search đi qua upstream (Kafka/Flink) rồi đã được ghi vào Scylla.

---

### 2.1 Luồng upstream trước khi tới search

Luồng thực tế theo code trong Flink:

1. Kafka topic vào:
   - `stock_price_vn`
   - `stock_price_dif`
2. Flink job hợp nhất 2 stream.
3. Flink ghi vào Scylla:
   - `stock_prices` (tick raw)
   - `stock_latest_prices` (1 bản ghi mới nhất mỗi mã)
4. Search worker đọc 2 bảng này để chạy BOCPD.

Điểm quan trọng về timestamp:

- `stock_prices.timestamp` là text (thường là epoch ms dạng chuỗi).
- `stock_latest_prices.timestamp` là kiểu timestamp.

### 2.2 Hợp đồng dữ liệu đầu vào mà search dùng

Search chỉ cần 3 trường cốt lõi từ Scylla:

- `symbol`
- `price`
- `timestamp`

Nguồn bootstrap:

- Đọc từ `stock_prices` theo từng mã.

Nguồn realtime:

- Đọc từ `stock_latest_prices` toàn bảng mỗi chu kỳ polling.

### 2.3 Danh sách mã được theo dõi

Search không hard-code mã, mà đọc từ file registry:

- `config/symbol_registry.json`

Registry có hai nhóm thị trường (`vn`, `world`) và mỗi nhóm có mảng `symbols`.
Worker reload registry khi file đổi, nên thêm/bớt mã không cần restart service.

---

## 3) Worker khởi động ra sao (startup flow)

Khi chạy `python changepoint_worker.py`, luồng startup là:

1. Tạo `ChangepointWorker`.
2. Tạo cấu hình BOCPD từ biến môi trường:
   - `BOCPD_ALPHA0`, `BOCPD_BETA0`
   - `BOCPD_HAZARD_LAMBDA`
   - `BOCPD_MAX_RUN_LENGTH`
   - `BOCPD_TAIL_MASS_THRESHOLD`
3. `connect()` tới Scylla (retry tối đa 30 lần).
4. `connect_clickhouse()` tới ClickHouse (retry tối đa 30 lần).
5. `ensure_tables()` tạo bảng Scylla output nếu chưa có.
6. `ensure_clickhouse_schema()` tạo bảng + view ClickHouse output.
7. `sync_registry()` để bootstrap state cho các mã mới.
8. Vào vòng lặp vô hạn:
   - đồng bộ registry
   - xử lý dữ liệu live
   - sleep theo `BOCPD_POLL_INTERVAL`

---

## 4) Mô hình trạng thái trong RAM

Mỗi mã có một `SymbolState` gồm:

- `model`: một instance BOCPD độc lập cho mã đó.
- `last_price`: giá gần nhất đã xử lý.
- `last_event_time`: timestamp gần nhất đã xử lý.
- `bootstrapped`: đã chạy bootstrap hay chưa.

Ý nghĩa:

- Không trộn state giữa các mã.
- Có thể bỏ qua bản ghi trùng (`same price + same timestamp`).

---

## 5) Step-by-step bootstrap lịch sử

Mục đích bootstrap:

- Làm nóng posterior của BOCPD bằng dữ liệu lịch sử trước khi xử lý realtime.

Flow bootstrap cho một mã:

1. Query lịch sử từ Scylla:
   - `SELECT timestamp, price FROM stock_prices WHERE symbol = ? LIMIT ?`
2. Parse timestamp bằng `parse_event_time()`.
3. Bỏ bản ghi thiếu timestamp hoặc thiếu price.
4. Sort tăng dần theo thời gian.
5. Duyệt cặp liên tiếp `(p_{t-1}, p_t)` để tính return:

$$
R_t = \frac{p_t}{p_{t-1}} - 1
$$

6. Gọi BOCPD update với `R_t`.
7. Ghi output:
   - Luôn ghi đầy đủ vào ClickHouse (`stock_changepoint_events`).
   - Chỉ ghi phần lịch sử gần nhất vào Scylla nếu còn trong cửa sổ `BOCPD_BOOTSTRAP_HISTORY_LIMIT`.
8. Nếu không có lịch sử trong `stock_prices`, fallback lấy 1 dòng từ `stock_latest_prices` để set mốc `last_price/last_event_time`.
9. Đánh dấu `bootstrapped = True`.

---

## 6) Step-by-step xử lý realtime

Mỗi chu kỳ polling:

1. Query toàn bộ latest rows:
   - `SELECT symbol, price, timestamp FROM stock_latest_prices`
2. Với từng row:
   - Bỏ qua nếu symbol không thuộc registry.
   - Bỏ qua nếu thiếu price hoặc timestamp.
   - Parse timestamp về UTC datetime.
3. Kiểm tra trùng:
   - Nếu trùng `last_event_time` và `last_price` thì skip.
4. Nếu chưa có `last_price`, chỉ cập nhật mốc và chờ tick kế tiếp.
5. Tính return mới:

$$
R_t = \frac{p_t}{p_{t-1}} - 1
$$

6. Gọi `model.update(R_t)`.
7. Ghi kết quả vào ClickHouse + Scylla qua `upsert_result(..., source="live")`.
8. Cập nhật lại `last_price`, `last_event_time`.

---

## 7) Lỗi BOCPD

### 7.1 Ký hiệu

- $x_t$: quan sát tại thời điểm $t$ (ở đây là return $R_t$).
- $r_t$: run-length, số bước kể từ changepoint gần nhất.
- $P(r_t \mid x_{1:t})$: posterior run-length.

### 7.2 Hazard function

Dùng hazard hằng số:

$$
H(\tau)=\frac{1}{\lambda_{gap}}
$$

Trong code:

- `hazard_lambda` tương ứng $\lambda_{gap}$.

### 7.3 Mô hình xác suất của dữ liệu

Giả định cho return:

$$
x_t \sim \mathcal{N}(0, \tau^{-1}), \quad \tau \sim \mathrm{Gamma}(\alpha,\beta)
$$

Predictive density (dạng đóng sau khi tích phân theo $\tau$):

$$
p(x_t\mid\alpha,\beta)=
\frac{\Gamma(\alpha+\tfrac12)}{\Gamma(\alpha)}
\frac{\beta^{\alpha}}{\sqrt{2\pi}}
\left(\beta+\frac{x_t^2}{2}\right)^{-(\alpha+\tfrac12)}
$$

Code tính ở log-space để ổn định số học:

$$
\log p =
\log\Gamma(\alpha+\tfrac12)-\log\Gamma(\alpha)
+ \alpha\log\beta
- \tfrac12\log(2\pi)
- (\alpha+\tfrac12)\log\left(\beta+\frac{x_t^2}{2}\right)
$$

### 7.4 Nhánh growth và changepoint

Với mỗi giả thuyết run-length cũ $r$:

$$
\text{growth}_r = P(r_{t-1}=r\mid x_{1:t-1})\, p(x_t\mid r)\, (1-H(r+1))
$$

Nhánh reset (changepoint):

$$
\text{cp} = \left[\sum_r P(r_{t-1}=r\mid x_{1:t-1}) H(r+1)\right] p(x_t\mid \text{prior})
$$

### 7.5 Evidence và posterior mới

Evidence:

$$
\mathcal{Z}_t = \text{cp} + \sum_r \text{growth}_r
$$

Posterior mới:

$$
P(r_t=0\mid x_{1:t}) = \frac{\text{cp}}{\mathcal{Z}_t}
$$

$$
P(r_t=r+1\mid x_{1:t}) = \frac{\text{growth}_r}{\mathcal{Z}_t}
$$

### 7.6 Cập nhật sufficient statistics

Với mỗi nhánh kéo dài:

$$
\alpha' = \alpha + \frac12
$$

$$
\beta' = \beta + \frac{x_t^2}{2}
$$

Nhánh reset dùng prior $(\alpha_0,\beta_0)$ rồi cộng đóng góp quan sát mới.

### 7.7 Predictive volatility hỗn hợp

Code dùng xấp xỉ thực dụng:

$$
\sigma_{pred}^2 \approx \sum_r P(r_t=r\mid x_{1:t})\frac{\beta_r}{\alpha_r}
$$

$$
\sigma_{pred}=\sqrt{\max(\sigma_{pred}^2,\varepsilon)}
$$

### 7.8 Innovation và điểm cảnh báo

Điểm lệch chuẩn hóa:

$$
innovation\_zscore = \frac{|x_t|}{\max(\sigma_{pred\_before}, 10^{-8})}
$$

Điểm cảnh báo nghiệp vụ:

$$
whale\_score=
\min\left(1,\;cp\_prob\cdot\frac{\min(innovation\_zscore,4)}{4}\right)
$$

Luật gán nhãn:

- `whale-watch` nếu `cp_prob >= 0.35` và `whale_score >= 0.15`
- `transition` nếu `cp_prob >= 0.15`
- còn lại là `stable`

---

## 8) Output ghi đi đâu và vì sao

### 8.1 ClickHouse (nguồn chuẩn cho phân tích/train)

Bảng:

- `stock_changepoint_events`

Đặc điểm:

- Giữ toàn bộ event stream BOCPD.
- Partition theo tháng từ `event_time`.
- TTL 2 năm.

View latest:

- `v_changepoint_latest`

### 8.2 Scylla (phục vụ đọc nhanh cho app)

Bảng:

- `stock_changepoint_latest`: trạng thái mới nhất theo mã.
- `stock_changepoint_history`: lịch sử theo ngày để vẽ chart.

Lưu ý bootstrap:

- ClickHouse nhận toàn bộ bootstrap.
- Scylla chỉ giữ phần bootstrap gần nhất (bounded) để tránh phình dữ liệu phục vụ.

---

## 9) Danh sách trường output BOCPD

Mỗi lần cập nhật tạo các trường:

- `cp_prob`
- `expected_run_length`
- `map_run_length`
- `predictive_volatility`
- `innovation_zscore`
- `whale_score`
- `hazard`
- `evidence`
- `regime_label`
- `num_updates`

Ý nghĩa vận hành ngắn gọn:

- `cp_prob` cao: khả năng cao vừa đổi chế độ.
- `expected_run_length` lớn: chế độ hiện tại đã kéo dài tương đối lâu.
- `innovation_zscore` cao: return mới bất thường so với mức biến động kỳ vọng.

---

## 10) Biến môi trường quan trọng

Kết nối:

- `SCYLLA_CONTACT_POINTS`
- `SCYLLA_PORT`
- `SCYLLA_KEYSPACE`
- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DB`

BOCPD:

- `BOCPD_ALPHA0`
- `BOCPD_BETA0`
- `BOCPD_HAZARD_LAMBDA`
- `BOCPD_MAX_RUN_LENGTH`
- `BOCPD_TAIL_MASS_THRESHOLD`

Workflow:

- `BOCPD_BOOTSTRAP_LIMIT`
- `BOCPD_BOOTSTRAP_HISTORY_LIMIT`
- `BOCPD_POLL_INTERVAL`

Registry:

- `SYMBOL_REGISTRY_PATH`

---

## 11) Chạy thực tế step-by-step (runbook)

### Bước 1: Đảm bảo hạ tầng upstream đã có dữ liệu

Bạn cần các thành phần đã chạy:

- Kafka + Schema Registry
- Flink job ghi giá
- Scylla
- ClickHouse

Nếu upstream chưa chạy, search worker vẫn chạy nhưng không có dữ liệu để tính.

### Bước 2: Kiểm tra registry mã

Mở file:

- `config/symbol_registry.json`

Đảm bảo danh sách `symbols` có mã bạn muốn theo dõi.

### Bước 3: Chạy search worker

Tại thư mục `search/`:

```bash
docker compose up -d --build
```

### Bước 4: Kiểm tra log

Kỳ vọng thấy các mốc:

1. Kết nối Scylla thành công.
2. Kết nối ClickHouse thành công.
3. Tạo schema/table xong.
4. Bootstrapped từng mã.
5. Worker vào vòng lặp live.


