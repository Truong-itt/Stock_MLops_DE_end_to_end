# BOCPD Core

## 1. Bài toán mà core đang giải

Đầu vào:

$$
R_t = \frac{p_t}{p_{t-1}} - 1
$$

Trong code, đầu vào cho BOCPD là:

- `x_t := R_t`

BOCPD sẽ ước lượng online:

- Xác suất vừa xảy ra điểm đổi chế độ ở thời điểm hiện tại (`cp_prob`).
- Độ dài chế độ hiện tại (`run_length`).

Ở dự án này, mô hình giả định:

- Mean của return xấp xỉ 0.
- Chế độ thay đổi chủ yếu thể hiện qua variance (độ biến động), không phải qua mean.

Vì vậy core dùng biến thể:

- Zero-mean Gaussian.
- Prior Gamma cho precision (nghịch đảo variance).

---

## 2. Cấu trúc chính trong bocpd.py

### 2.1 BOCPDConfig

Các tham số cấu hình:

- `alpha0`, `beta0`: prior Gamma ban đầu cho precision.
- `hazard_lambda`: điều khiển xác suất đổi chế độ nền.
- `max_run_length`: giới hạn số trạng thái run-length để tránh phình bộ nhớ.
- `tail_mass_threshold`: ngưỡng cho phép cắt đuôi phân phối xác suất.

Ý nghĩa nhanh:

- `hazard_lambda` càng nhỏ -> mô hình càng nhạy với changepoint.
- `hazard_lambda` càng lớn -> mô hình càng bảo thủ, ít báo đổi chế độ.

### 2.2 ZeroMeanGaussianVarianceBOCPD

Trạng thái nội bộ quan trọng:

- `run_length_posterior[r] = P(r_t = r | x_1:t)`
- `alpha_params[r]`, `beta_params[r]`: thống kê hậu nghiệm tương ứng cho từng giả thuyết run-length.
- `num_updates`: số lần đã cập nhật.

Điều kiện khởi tạo:

- `P(r_0 = 0) = 1`
- Chỉ có 1 trạng thái ban đầu, dùng prior.

---

## 3. Map công thức paper vào hàm trong code

### 3.1 Hazard - hàm _hazard

Core dùng hazard hằng số (memoryless):

$$
H(\tau) = \frac{1}{\lambda_{gap}}
$$

Trong code:

- `hazard_lambda` được chặn tối thiểu 1.0.
- Giá trị hazard được chặn tối đa 1.0.

### 3.2 Predictive density - hàm _predictive_pdf

Giả định:

- $x_t \sim \mathcal{N}(0, \tau^{-1})$
- $\tau \sim \text{Gamma}(\alpha, \beta)$

Khi lấy tích phân theo $\tau$, nhận được predictive density dạng Student-t (dạng đóng).
Code tính bằng log-space (`math.lgamma`, `math.log`) để ổn định số học, sau đó `exp` ra pdf.

### 3.3 Predictive volatility hỗn hợp - hàm _predictive_volatility_from_state

Dùng xấp xỉ thực dụng:

$$
\sigma_{pred}^2 \approx \sum_r P(r_t=r\mid x_{1:t}) \cdot \frac{\beta_r}{\alpha_r}
$$

Sau đó lấy căn bậc hai để ra `predictive_volatility`.

### 3.4 Cắt đuôi trạng thái - hàm _truncate_tail

Đây là tối ưu runtime:

- Chỉ giữ tối đa `max_run_length + 1` trạng thái.
- Nếu cắt mất khối lượng xác suất đáng kể, phần bị cắt được dồn vào trạng thái cuối để tránh giật phân phối.

Kết quả:

- Giữ ổn định bộ nhớ và CPU khi worker chạy liên tục nhiều mã.

### 3.5 Cập nhật thống kê đủ - hàm _update_sufficient_statistics

Với quan sát mới $x_t$:

$$
\alpha' = \alpha + \frac12,
\quad
\beta' = \beta + \frac{x_t^2}{2}
$$

Nhánh reset dùng prior (`alpha0`, `beta0`) cộng đóng góp của $x_t$.

### 3.6 Gán nhãn nghiệp vụ - hàm _classify_regime

Sau khi có tín hiệu xác suất, code thêm heuristic phục vụ giám sát:

$$
\text{whale\_score} =
\min\left(1,\; cp\_prob \cdot \frac{\min(innovation\_zscore,4)}{4}\right)
$$

Nhãn:

- `whale-watch` nếu tín hiệu đổi chế độ mạnh và độ bất thường đủ cao.
- `transition` nếu đang có dấu hiệu chuyển pha.
- `stable` nếu còn ổn định.

Lưu ý:

- Đây là nhãn vận hành, không phải kết luận pháp lý về thao túng.

---

## 4. Luồng đầy đủ của update(observation)

Hàm `update` chạy theo thứ tự:

1. Đổi đầu vào sang `float`, tính `predictive_volatility_before_update`.
2. Tính predictive prob cho mọi giả thuyết run-length hiện có.
3. Tính nhánh changepoint (reset) bằng prior predictive.
4. Dùng hazard để tách 2 nhánh:
   - growth: tiếp tục chế độ cũ.
   - cp: bắt đầu chế độ mới.
5. Tính evidence để chuẩn hóa.
6. Chuẩn hóa thành posterior mới của run-length.
7. Trích xuất chỉ số chính: `cp_prob`, `expected_run_length`, `map_run_length`, `innovation_zscore`.
8. Cập nhật sufficient statistics cho vòng kế tiếp.
9. Cắt đuôi trạng thái dài.
10. Tính lại `predictive_volatility` sau cập nhật.
11. Tính `whale_score` và `regime_label`.
12. Trả dict kết quả.

---

## 5. Ý nghĩa các trường output

Mỗi lần gọi `update` trả ra:

- `cp_prob`: xác suất xảy ra changepoint ngay hiện tại.
- `expected_run_length`: kỳ vọng số bước từ changepoint gần nhất.
- `map_run_length`: run-length có xác suất cao nhất.
- `predictive_volatility`: độ biến động dự đoán tại thời điểm đó.
- `innovation_zscore`: độ lệch chuẩn hóa của return mới.
- `whale_score`: điểm bất thường nghiệp vụ.
- `hazard`: xác suất đổi chế độ nền (hằng số).
- `evidence`: hằng số chuẩn hóa posterior.
- `regime_label`: nhãn diễn giải (`stable/transition/whale-watch`).
- `num_updates`: số bản ghi đã xử lý bởi model.

---

## 6. Ví dụ dùng nhanh

```python
from bocpd import BOCPDConfig, ZeroMeanGaussianVarianceBOCPD

cfg = BOCPDConfig(
    alpha0=1.0,
    beta0=1e-4,
    hazard_lambda=90,
    max_run_length=180,
    tail_mass_threshold=1e-6,
)

model = ZeroMeanGaussianVarianceBOCPD(cfg)

returns = [0.0012, -0.0007, 0.0021, -0.0135, 0.0009]
for r in returns:
    result = model.update(r)
    print(result["cp_prob"], result["regime_label"], result["whale_score"])
```

Kỳ vọng thực tế:

- Khi xuất hiện return đột biến lớn, `cp_prob` và `innovation_zscore` thường tăng.
- `regime_label` có thể chuyển từ `stable` sang `transition` hoặc `whale-watch`.

---

## 7. Tuning tham số trong production

### 7.1 Hazard lambda

- Dữ liệu nhiễu mạnh, nhiều spike giả: tăng `hazard_lambda`.
- Cần bắt regime change nhanh hơn: giảm `hazard_lambda`.

Gợi ý thực nghiệm:

- Bắt đầu từ 90 (mặc định).
- Thử dải 45, 60, 90, 120.
- So sánh precision/recall trên tập sự kiện đã gán nhãn nội bộ.

### 7.2 Prior alpha0, beta0

- Ảnh hưởng độ ổn định giai đoạn đầu khi dữ liệu còn ít.
- Nếu đầu phiên dao động quá mạnh do khởi tạo, cân nhắc prior thông tin hơn (tăng nhẹ alpha0/beta0 theo lịch sử).

### 7.3 max_run_length và tail_mass_threshold

- `max_run_length` thấp: tiết kiệm tài nguyên nhưng có thể mất thông tin chế độ dài.
- `tail_mass_threshold` quá chặt: ít mất mát xác suất hơn nhưng tốn tài nguyên hơn.

---

## 8. Độ phức tạp và ổn định số

- Mỗi bước update có độ phức tạp xấp xỉ O(S), với S là số trạng thái run-length đang giữ.
- Có chặn EPS để tránh chia 0 hoặc log(0).
- Có truncate tail để tránh tăng trạng thái vô hạn theo thời gian chạy.

---

## 9. Khi nào nên tin hoặc không nên tin tín hiệu

Nên tin hơn khi đồng thời có:

- `cp_prob` tăng rõ rệt,
- `innovation_zscore` cao,
- biến động tăng liên tục ở vài bước kế tiếp.

Nên thận trọng khi:

- chỉ có một điểm spike đơn lẻ,
- thanh khoản thấp gây nhiễu giá,
- dữ liệu đầu vào thiếu hoặc sai timestamp.

Khuyến nghị vận hành:

- Dùng BOCPD như lớp phát hiện sớm.
- Kết hợp thêm volume, order-book, tin tức, và model ML phía sau trước khi đưa ra cảnh báo mức cao.

---
