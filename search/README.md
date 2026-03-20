# BOCPD Search Module

Module trong thư mục `search/` hiện thực hóa paper:

- `Bayesian Online Changepoint Detection`
- Ryan Prescott Adams, David J.C. MacKay

Mục tiêu thực tế trong project này không phải dùng toàn bộ paper một cách chung chung, mà là chọn phần phù hợp nhất để:

- theo dõi toàn bộ mã cổ phiếu liên tục
- phát hiện thời điểm thị trường đổi regime rất nhanh
- sinh tín hiệu nghi ngờ "cá mập vào lệnh / dòng tiền lớn can thiệp"
- đưa tín hiệu đó lên web dưới dạng thông tin + biểu đồ `r_t`

## Công thức được chọn

### Công thức dùng trực tiếp

- `(14)` `R_t = p_t / p_(t-1) - 1`
  - chuyển chuỗi giá thành chuỗi return
  - đây là đầu vào phù hợp nhất cho stock và cho việc bắt regime shift

- `(3)` cập nhật joint distribution online
  - dùng để cập nhật `P(r_t, x_1:t)`

- `(4)` chuyển trạng thái run length
  - hoặc segment tiếp tục, hoặc xảy ra changepoint mới

- `(5)` hazard function
  - ở module này dùng hazard hằng số `H(tau) = 1 / lambda_gap`

- `(2)` chuẩn hóa posterior
  - lấy `P(r_t | x_1:t)` từ joint distribution

- `(11)` predictive distribution
  - dùng predictive density để chấm điểm return mới dưới từng giả thuyết run length

- `(12)` và `(13)` cập nhật sufficient statistics
  - được hiện thực bằng posterior Gamma trên precision của Gaussian variance model

### Công thức dùng làm nền tảng lý thuyết

- `(1)` mixture prediction theo posterior run length
- `(8)`, `(9)`, `(10)` dạng conjugate-exponential tổng quát

Các công thức này vẫn được phản ánh trong code, nhưng không phơi toàn bộ dạng tổng quát ra business logic.

## Vì sao chọn mô hình này

Paper có 3 ví dụ chính:

- well-log: Gaussian mean shift
- Dow Jones returns: zero-mean Gaussian với variance thay đổi theo regime
- coal mine disasters: Poisson process

Cho bài toán stock của project này, module chọn ví dụ **Dow Jones returns** vì:

- dữ liệu hiện có là price tick / latest price
- thao túng hoặc dòng tiền lớn thường làm:
  - return đổi đột ngột
  - volatility tăng mạnh
  - run length reset nhanh

Mô hình zero-mean Gaussian với variance thay đổi theo regime phù hợp hơn:

- mean shift model của well-log
- Poisson event model của coal-disaster

## Tín hiệu được tạo ra

Service ghi ra 2 bảng Scylla:

- `stock_changepoint_latest`
  - trạng thái mới nhất của từng mã
- `stock_changepoint_history`
  - lịch sử changepoint để web vẽ biểu đồ `r_t`

Mỗi điểm cập nhật có:

- `cp_prob`
  - xác suất vừa xảy ra changepoint
- `expected_run_length`
  - kỳ vọng `E[r_t]`
- `map_run_length`
  - run length có posterior lớn nhất
- `return_value`
  - return dùng từ công thức `(14)`
- `predictive_volatility`
  - độ biến động dự báo từ mixture posterior
- `innovation_zscore`
  - độ lớn return chuẩn hóa theo predictive volatility
- `whale_score`
  - heuristic score phục vụ quan sát dòng tiền lớn
- `regime_label`
  - `stable`, `transition`, `whale-watch`

Lưu ý:

- `whale_score` là heuristic triển khai trong project này
- nó không phải là kết luận thao túng chắc chắn
- nó chỉ là tín hiệu ưu tiên để theo dõi

## Cấu trúc thư mục

- `bocpd.py`
  - lõi toán học BOCPD
- `changepoint_worker.py`
  - worker chạy online, đọc Scylla, tính BOCPD, ghi kết quả
- `symbol_registry.py`
  - đọc registry mã dùng chung với project
- `Dockerfile`
  - image cho service BOCPD
- `docker-compose.yml`
  - compose riêng cho module này

## Chạy module

Từ thư mục `search/`:

```bash
docker compose up -d --build
```

Service sẽ:

1. kết nối Scylla
2. tạo bảng changepoint nếu chưa có
3. bootstrap trạng thái từ `stock_prices`
4. theo dõi `stock_latest_prices` liên tục
5. cập nhật BOCPD cho toàn bộ mã trong registry

## Các biến môi trường chính

- `SCYLLA_CONTACT_POINTS`
  - mặc định: `scylla-node1,scylla-node2,scylla-node3`
- `SCYLLA_PORT`
  - mặc định: `9042`
- `SCYLLA_KEYSPACE`
  - mặc định: `stock_data`
- `BOCPD_HAZARD_LAMBDA`
  - mặc định: `90`
- `BOCPD_MAX_RUN_LENGTH`
  - mặc định: `180`
- `BOCPD_BOOTSTRAP_LIMIT`
  - mặc định: `120`
- `BOCPD_POLL_INTERVAL`
  - mặc định: `2.0`

## Nối với web

Backend web đọc dữ liệu từ:

- `GET /api/changepoint/{symbol}`
- `GET /api/changepoint/{symbol}/history`

Frontend hiển thị:

- info changepoint trong drawer
- biểu đồ `r_t` theo thời gian
- changepoint probability chồng lên biểu đồ run length

## Ghi chú triển khai

- Thuật toán trong code có chú thích công thức `(1)` đến `(13)` và dùng `(14)` để tạo return.
- Module ưu tiên tính online ổn định cho nhiều mã hơn là theo đuổi bản triển khai hàn lâm nặng nề.
- Tail posterior được cắt theo `max_run_length` để kiểm soát chi phí tính toán.
