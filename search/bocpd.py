"""
Loi BOCPD cho chuoi loi suat co phieu.

File nay bam sat ky hieu cua bai bao de de doi chieu khi doc:

- Cong thuc (2): posterior run length P(r_t | x_1:t).
- Cong thuc (3): cap nhat joint theo de quy.
- Cong thuc (4): prior changepoint / chuyen trang thai tang-vs-reset.
- Cong thuc (5): ham hazard H(tau).
- Cong thuc (8), (9), (10): nen tang ho conjugate-exponential.
- Cong thuc (11): phan phoi du doan cho quan sat moi.
- Cong thuc (12), (13): cap nhat du lieu du cho thong ke day du.

- x_t la loi suat R_t theo cong thuc (14)
- trung binh loi suat xem nhu xap xi 0
- thay doi che do the hien ro nhat o do bien dong (variance)
- posterior cua precision Gaussian dung prior Gamma
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List
EPS = 1e-12

@dataclass
class BOCPDConfig:
    alpha0: float = 1.0
    beta0: float = 1e-4
    hazard_lambda: float = 90.0
    hazard_prior: str = "poisson"
    max_run_length: int = 180
    tail_mass_threshold: float = 1e-6

class ZeroMeanGaussianVarianceBOCPD:
    """
    BOCPD chuyen biet cho loi suat Gaussian trung binh 0 va variance theo tung doan.

    Thiet lap nay phu hop du an hien tai vi cac hanh vi whale/manipulation
    thuong xuat hien truoc o dang dot bien bien dong gia thay vi doi dich
    ben vung cua muc gia trung binh.
    """

    def __init__(self, config: BOCPDConfig | None = None):
        self.config = config or BOCPDConfig()
        self.reset()

    def reset(self) -> None:
        # tại thời điểm t=0 xác xuất để độ dài chu kỳ r_0 = 0 là 1 tương ứng 100% khả năng bắt đầu một chu kỳ mới tại thời điểm khởi tạo. Điều này phản ánh giả định rằng trước khi có quan sát nào, chúng ta đang ở trong một chu kỳ mới.
        # Dieu kien bien: P(r_0 = 0) = 1 cho khoi tao online don gian.
        self.run_length_posterior: List[float] = [1.0]
        self.alpha_params: List[float] = [self.config.alpha0]
        self.beta_params: List[float] = [self.config.beta0]
        self.num_updates = 0

    def _gap_pmf(self, tau: int) -> float:
        """
        P_gap(g=tau) cho cong thuc (5).

        - geometric: truong hop memoryless => hazard hang so.
        - poisson (mac dinh): shifted-Poisson de hazard phu thuoc run length.
        """
        tau = max(int(tau), 1)
        prior = (self.config.hazard_prior or "poisson").strip().lower()
        lam = max(float(self.config.hazard_lambda), 1.0)

        if prior == "geometric":
            p = min(1.0, 1.0 / lam)
            return max(p * ((1.0 - p) ** (tau - 1)), EPS)

        # shifted-Poisson: g = 1 + k, k ~ Poisson(lam)
        k = tau - 1
        log_p = -lam + k * math.log(lam) - math.lgamma(k + 1.0)
        return max(math.exp(log_p), EPS)

    def _gap_survival(self, tau: int) -> float:
        """
        S_gap(tau) = P(g >= tau) = sum_{t=tau..inf} P_gap(g=t).
        """
        tau = max(int(tau), 1)
        prior = (self.config.hazard_prior or "poisson").strip().lower()
        lam = max(float(self.config.hazard_lambda), 1.0)

        if tau <= 1:
            return 1.0

        if prior == "geometric":
            p = min(1.0, 1.0 / lam)
            return max((1.0 - p) ** (tau - 1), EPS)

        # shifted-Poisson survival via Poisson CDF: P(g>=tau)=P(k>=tau-1), k=g-1
        max_k = tau - 2
        term = math.exp(-lam)  # k=0
        cdf = term
        for k in range(1, max_k + 1):
            term *= lam / k
            cdf += term
        return max(1.0 - min(cdf, 1.0), EPS)

    def _hazard(self, run_length: int) -> float:
        """
        Cong thuc (5): H(tau) = P_gap(g=tau) / sum_{t=tau..inf} P_gap(g=t).
        """
        tau = max(int(run_length), 1)
        gap_prob = self._gap_pmf(tau)
        survival = self._gap_survival(tau)
        hazard = gap_prob / max(survival, EPS)
        return min(1.0, max(EPS, hazard))

    def _predictive_pdf(self, x: float, alpha: float, beta: float) -> float:
        """
        Cong thuc (11): phan phoi du doan P(x_{t+1} | r_t).
        Gia dinh x_t ~ N(0, precision^-1) va precision ~ Gamma(alpha, beta).
        """
        beta = max(beta, EPS)
        log_pdf = (
            math.lgamma(alpha + 0.5)
            - math.lgamma(alpha)
            + alpha * math.log(beta)
            - 0.5 * math.log(2.0 * math.pi)
            - (alpha + 0.5) * math.log(beta + 0.5 * x * x)
        )
        return max(math.exp(log_pdf), EPS)

    def _predictive_volatility_from_state(self) -> float:
        """
        độ lêch chuẩn tổng thể do bien dong du doan dang hon hop tren moi gia thuyet run-length.
        """
        mixture_variance = 0.0
        for weight, alpha, beta in zip(self.run_length_posterior, self.alpha_params, self.beta_params):
            variance_proxy = max(beta / max(alpha, EPS), EPS)
            mixture_variance += weight * variance_proxy
        return math.sqrt(max(mixture_variance, EPS))

    def _truncate_tail(self) -> None:
        """
        Toi uu thuc tien cho bai toan chi phi tinh toan trong bai bao.
        Giu lai cac run-length ngan co xac suat lon va cat xac suat nho
        """
        max_states = max(2, self.config.max_run_length + 1)
        if len(self.run_length_posterior) <= max_states:
            return
        kept_probs = self.run_length_posterior[:max_states]
        kept_alpha = self.alpha_params[:max_states]
        kept_beta = self.beta_params[:max_states]
        mass = sum(kept_probs)
        if mass < 1.0 - self.config.tail_mass_threshold:
            # Don xac suat duoi bi cat vao trang thai cuoi cung de tranh mat dot ngot.
            kept_probs[-1] += 1.0 - mass
            mass = sum(kept_probs)
        self.run_length_posterior = [p / mass for p in kept_probs]
        self.alpha_params = kept_alpha
        self.beta_params = kept_beta

    def _update_sufficient_statistics(self, x_t: float) -> None:
        """
        Cong thuc (12), (13): cap nhat alpha/beta cho vong lap tiep theo.
        """
        x_sq_half = 0.5 * x_t * x_t
        self.alpha_params = [self.config.alpha0 + 0.5] + [alpha + 0.5 for alpha in self.alpha_params]
        self.beta_params = [self.config.beta0 + x_sq_half] + [beta + x_sq_half for beta in self.beta_params]

    def _classify_regime(self, cp_prob: float, innovation_zscore: float) -> tuple[float, str]:
        whale_score = min(1.0, cp_prob * min(innovation_zscore, 4.0) / 4.0)
        if cp_prob >= 0.35 and whale_score >= 0.15:
            return whale_score, "whale-watch"
        if cp_prob >= 0.15:
            return whale_score, "transition"
        return whale_score, "stable"

    def update(self, observation: float) -> Dict[str, float]:
        x_t = float(observation)
        # độ lệch chuẩn du doan truoc khi cap nhat thong so du lieu moi
        # du doan hon hop tren cac gia thuyet run-length hien co.
        predictive_volatility_before_update = self._predictive_volatility_from_state()

        # Cong thuc (11): xac suat du doan cho tung gia thuyet run-length hien co.
        growth_predictive_probs = [
            self._predictive_pdf(x_t, alpha, beta)
            for alpha, beta in zip(self.alpha_params, self.beta_params)
        ]
        # Neu xay ra changepoint tai thoi diem t, x_t bat dau doan moi,
        # nhanh reset duoc tinh theo prior predictive.
        cp_predictive_prob = self._predictive_pdf(
            x_t, self.config.alpha0, self.config.beta0
        )

        # Cong thuc (4): mo hinh chuyen trang thai dua vao hazard:
        # - changepoint / reset ve r_t = 0
        # - growth den r_t = r_{t-1} + 1
        hazard_values = [
            self._hazard(run_length + 1)
            for run_length in range(len(self.run_length_posterior))
        ]
        growth_joint = [
            posterior * pred * (1.0 - hazard)
            for posterior, pred, hazard in zip(
                self.run_length_posterior, growth_predictive_probs, hazard_values
            )
        ]
        cp_joint = sum(
            posterior * hazard
            for posterior, hazard in zip(self.run_length_posterior, hazard_values)
        ) * cp_predictive_prob

        # Cong thuc (3) va buoc 6 cua thuat toan: evidence.
        evidence = max(cp_joint + sum(growth_joint), EPS)

        # Cong thuc (2): posterior tren run length.
        new_posterior = [cp_joint / evidence]
        new_posterior.extend(value / evidence for value in growth_joint)
        self.run_length_posterior = new_posterior

        expected_run_length = sum(
            run_length * weight
            for run_length, weight in enumerate(self.run_length_posterior)
        )
        map_run_length = max(
            range(len(self.run_length_posterior)), key=lambda idx: self.run_length_posterior[idx],
        )
        cp_prob = self.run_length_posterior[0]
        innovation_zscore = abs(x_t) / max(predictive_volatility_before_update, 1e-8)
        self._update_sufficient_statistics(x_t)

        self._truncate_tail()
        predictive_volatility = self._predictive_volatility_from_state()
        self.num_updates += 1

        whale_score, regime_label = self._classify_regime(cp_prob, innovation_zscore)

        return {
            "cp_prob": round(cp_prob, 6),
            "expected_run_length": round(expected_run_length, 4),
            "map_run_length": int(map_run_length),
            "predictive_volatility": round(predictive_volatility, 8),
            "innovation_zscore": round(innovation_zscore, 6),
            "whale_score": round(whale_score, 6),
            "hazard": round(self._hazard(map_run_length + 1), 8),
            "evidence": round(evidence, 12),
            "regime_label": regime_label,
            "num_updates": self.num_updates,
        }
