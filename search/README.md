# BOCPD Search Module

Module `search/` hiện thực hóa paper:

- `Bayesian Online Changepoint Detection`
- Ryan Prescott Adams, David J.C. MacKay (2007)

Mục tiêu trong project:

- chạy online liên tục cho toàn bộ danh sách mã
- phát hiện thời điểm đổi regime nhanh
- tạo tín hiệu bất thường để đưa sang web và Whale ML
- lưu event stream vào ClickHouse làm nguồn train/backfill

## Bài toán và ký hiệu

- Giá tại thời điểm `t`: `p_t`
- Return đầu vào BOCPD:

$$
R_t = \frac{p_t}{p_{t-1}} - 1
$$

- `x_t := R_t`
- `r_t`: run-length (số bước kể từ changepoint gần nhất)

Module đang dùng biến thể phù hợp bài toán cổ phiếu:

- zero-mean Gaussian
- variance thay đổi theo regime
- prior Gamma trên precision

## Công thức toán đang dùng trong code

### (14) Chuyển giá sang return

$$
R_t = \frac{p_t}{p_{t-1}} - 1
$$

### (5) Hazard function

Với hazard hằng số:

$$
H(\tau)=\frac{1}{\lambda_{\text{gap}}}
$$

Trong code: `BOCPD_HAZARD_LAMBDA` (mặc định `90`).

### (11) Predictive density

Với `x_t ~ N(0, \tau^{-1})`, `\tau ~ Gamma(\alpha,\beta)`, predictive density:

$$
p(x_t \mid r_{t-1}) \propto
\frac{\Gamma(\alpha+\tfrac12)}{\Gamma(\alpha)}
\beta^{\alpha}
\left(\beta+\frac{x_t^2}{2}\right)^{-(\alpha+\tfrac12)}
$$

Code dùng dạng log-density ổn định số học trong `bocpd.py::_predictive_pdf`.

### (4) Growth / Changepoint transition

Với mỗi giả thuyết run-length trước đó:

$$
\text{growth}_r = P(r_{t-1}=r \mid x_{1:t-1}) \cdot p(x_t \mid r) \cdot (1-H(r+1))
$$

$$
\text{cp} = \sum_r P(r_{t-1}=r \mid x_{1:t-1}) \cdot H(r+1) \cdot p(x_t \mid \text{prior})
$$

### (3) Evidence (joint update)

$$
p(x_{1:t}) = \text{cp} + \sum_r \text{growth}_r
$$

### (2) Posterior run-length

$$
P(r_t=0 \mid x_{1:t}) = \frac{\text{cp}}{p(x_{1:t})}
$$

$$
P(r_t=r+1 \mid x_{1:t}) = \frac{\text{growth}_r}{p(x_{1:t})}
$$

### (12), (13) Cập nhật sufficient statistics

Với quan sát mới `x_t`:

$$
\alpha' = \alpha + \frac12
$$

$$
\beta' = \beta + \frac{x_t^2}{2}
$$

Nhánh reset dùng prior `(\alpha_0,\beta_0)`.

### (1) Mixture predictive volatility (triển khai thực dụng)

Module dùng xấp xỉ:

$$
\sigma_{\text{pred}}^2 \approx \sum_r P(r_t=r \mid x_{1:t}) \cdot \frac{\beta_r}{\alpha_r}
$$

$$
\sigma_{\text{pred}} = \sqrt{\max(\sigma_{\text{pred}}^2,\varepsilon)}
$$

## Chỉ số nghiệp vụ sinh ra

Mỗi event BOCPD sinh các trường chính:

- `cp_prob`
- `expected_run_length`
- `map_run_length`
- `predictive_volatility`
- `innovation_zscore = |R_t| / sigma_pred`
- `hazard`
- `evidence`

Heuristic phục vụ quan sát bất thường:

$$
\text{whale\_score} =
\min\left(1,\; cp\_prob \cdot \frac{\min(innovation\_zscore,4)}{4}\right)
$$

`regime_label`:

- `stable`
- `transition`
- `whale-watch`

Lưu ý: `whale_score` là tín hiệu cảnh báo ưu tiên theo dõi, không phải kết luận thao túng chắc chắn.

## Luồng dữ liệu module

- Input:
  - Scylla `stock_prices` (bootstrap)
  - Scylla `stock_latest_prices` (online updates)
- Output:
  - ClickHouse `stock_changepoint_events` (source-of-truth cho ML/backtest)
  - ClickHouse `v_changepoint_latest`
  - Scylla `stock_changepoint_latest` (cache)
  - Scylla `stock_changepoint_history` (phục vụ chart UI)

## Cấu trúc thư mục

- `bocpd.py`: lõi toán học BOCPD
- `changepoint_worker.py`: online worker
- `symbol_registry.py`: đọc danh sách mã dùng chung
- `Dockerfile`
- `docker-compose.yml`

## Chạy module

Từ thư mục `search/`:

```bash
docker compose up -d --build
```

Service sẽ:

1. kết nối Scylla + ClickHouse
2. tạo schema changepoint nếu chưa có
3. bootstrap state từ lịch sử giá
4. theo dõi stream latest price và cập nhật BOCPD online

## Biến môi trường chính

- `SCYLLA_CONTACT_POINTS` (default: `scylla-node1,scylla-node2,scylla-node3`)
- `SCYLLA_PORT` (default: `9042`)
- `SCYLLA_KEYSPACE` (default: `stock_data`)
- `CLICKHOUSE_HOST` (default: `clickhouse`)
- `CLICKHOUSE_PORT` (default: `8123`)
- `CLICKHOUSE_DB` (default: `stock_warehouse`)
- `BOCPD_ALPHA0` (default: `1.0`)
- `BOCPD_BETA0` (default: `0.0001`)
- `BOCPD_HAZARD_LAMBDA` (default: `90`)
- `BOCPD_MAX_RUN_LENGTH` (default: `180`)
- `BOCPD_TAIL_MASS_THRESHOLD` (default: `0.000001`)
- `BOCPD_BOOTSTRAP_LIMIT` (default: `120`)
- `BOCPD_POLL_INTERVAL` (default: `2.0`)

## Tích hợp web

Backend web đọc từ:

- `GET /api/changepoint/latest`
- `GET /api/changepoint/abnormal`
- `GET /api/changepoint/{symbol}`
- `GET /api/changepoint/{symbol}/history`

Frontend hiển thị:

- trạng thái BOCPD trên bảng giá
- chart `r_t` + `cp_prob` trong drawer
- bảng nghi vấn bất thường và forecast ML cho từng mã
