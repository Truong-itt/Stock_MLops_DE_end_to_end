"""
BOCPD core for stock returns.

The implementation intentionally maps the paper's notation to executable code:

- Formula (1): mixture prediction over run-length posterior.
- Formula (2): posterior over run length P(r_t | x_1:t).
- Formula (3): recursive joint update.
- Formula (4): changepoint prior / growth-vs-reset transition.
- Formula (5): hazard function H(tau).
- Formula (8), (9), (10): conjugate-exponential family background.
- Formula (11): predictive distribution for a new observation.
- Formula (12), (13): sufficient-statistics updates.

For the stock problem we use the paper's Dow-Jones-style choice:

- x_t is return R_t from formula (14)
- return mean is assumed approximately zero
- regime changes are expressed through changing variance
- posterior over Gaussian precision uses a Gamma prior
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List


EPS = 1e-12


@dataclass
class BOCPDConfig:
    # Paper section 3.2 uses a Gamma prior on inverse variance.
    alpha0: float = 1.0
    beta0: float = 1e-4
    hazard_lambda: float = 90.0
    max_run_length: int = 180
    tail_mass_threshold: float = 1e-6


class ZeroMeanGaussianVarianceBOCPD:
    """
    BOCPD specialised for zero-mean Gaussian returns with piecewise-constant variance.

    This is the best-fit choice from the paper for the current project because
    abrupt whale / manipulation-like activity is usually seen first as a jump in
    return volatility rather than as a stable shift in the mean level of price.
    """

    def __init__(self, config: BOCPDConfig | None = None):
        self.config = config or BOCPDConfig()
        self.reset()

    def reset(self) -> None:
        # Boundary condition: P(r_0 = 0) = 1, matching the simple online start.
        self.run_length_posterior: List[float] = [1.0]
        self.alpha_params: List[float] = [self.config.alpha0]
        self.beta_params: List[float] = [self.config.beta0]
        self.num_updates = 0

    def _hazard(self, run_length: int) -> float:
        """
        Formula (5): hazard H(tau).

        We use the paper's memoryless discrete-exponential case, so hazard is constant:
            H(tau) = 1 / lambda_gap
        """
        lam = max(self.config.hazard_lambda, 1.0)
        return min(1.0, 1.0 / lam)

    def _predictive_pdf(self, x: float, alpha: float, beta: float) -> float:
        """
        Formula (11): predictive distribution P(x_{t+1} | r_t).

        Here x_t ~ N(0, precision^-1) and precision ~ Gamma(alpha, beta).
        Integrating out precision yields the closed-form Student-t-like predictive density.
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
        Formula (1): mixture-style predictive volatility over all run-length hypotheses.

        We use a stable proxy based on the posterior Gamma mean of variance scale:
            scale^2 ~= beta / alpha
        and weight it by the run-length posterior.
        """
        mixture_variance = 0.0
        for weight, alpha, beta in zip(
            self.run_length_posterior, self.alpha_params, self.beta_params
        ):
            variance_proxy = max(beta / max(alpha, EPS), EPS)
            mixture_variance += weight * variance_proxy
        return math.sqrt(max(mixture_variance, EPS))

    def _truncate_tail(self) -> None:
        """
        Practical optimisation mentioned under computational cost in the paper.

        We keep short run lengths with high mass and cut very long tiny-mass tails
        to keep the worker stable when it runs forever for many symbols.
        """
        max_states = max(2, self.config.max_run_length + 1)
        if len(self.run_length_posterior) <= max_states:
            return

        kept_probs = self.run_length_posterior[:max_states]
        kept_alpha = self.alpha_params[:max_states]
        kept_beta = self.beta_params[:max_states]

        mass = sum(kept_probs)
        if mass < 1.0 - self.config.tail_mass_threshold:
            # Fold the discarded tail into the last kept state instead of dropping it abruptly.
            kept_probs[-1] += 1.0 - mass
            mass = sum(kept_probs)

        self.run_length_posterior = [p / mass for p in kept_probs]
        self.alpha_params = kept_alpha
        self.beta_params = kept_beta

    def update(self, observation: float) -> Dict[str, float]:
        """
        Run one BOCPD online update for a new return observation x_t.
        """
        x_t = float(observation)
        predictive_volatility_before_update = self._predictive_volatility_from_state()

        # Formula (11): predictive probability for each existing run-length hypothesis.
        growth_predictive_probs = [
            self._predictive_pdf(x_t, alpha, beta)
            for alpha, beta in zip(self.alpha_params, self.beta_params)
        ]
        # If a changepoint occurs at time t then x_t starts a fresh segment,
        # so the reset branch must be scored under the prior predictive.
        cp_predictive_prob = self._predictive_pdf(
            x_t, self.config.alpha0, self.config.beta0
        )

        # Formula (4): transition model using hazard for
        # - changepoint / reset to r_t = 0
        # - growth to r_t = r_{t-1} + 1
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

        # Formula (3) and algorithm step 6: evidence.
        evidence = max(cp_joint + sum(growth_joint), EPS)

        # Formula (2): posterior over run length.
        new_posterior = [cp_joint / evidence]
        new_posterior.extend(value / evidence for value in growth_joint)
        self.run_length_posterior = new_posterior

        expected_run_length = sum(
            run_length * weight
            for run_length, weight in enumerate(self.run_length_posterior)
        )
        map_run_length = max(
            range(len(self.run_length_posterior)),
            key=lambda idx: self.run_length_posterior[idx],
        )
        cp_prob = self.run_length_posterior[0]
        innovation_zscore = abs(x_t) / max(predictive_volatility_before_update, 1e-8)

        # Formula (12) and (13): update sufficient statistics for next iteration.
        x_sq_half = 0.5 * x_t * x_t
        self.alpha_params = [self.config.alpha0 + 0.5] + [
            alpha + 0.5 for alpha in self.alpha_params
        ]
        self.beta_params = [self.config.beta0 + x_sq_half] + [
            beta + x_sq_half for beta in self.beta_params
        ]

        self._truncate_tail()
        predictive_volatility = self._predictive_volatility_from_state()
        self.num_updates += 1

        # Project-level heuristic layered on top of BOCPD outputs.
        whale_score = min(1.0, cp_prob * min(innovation_zscore, 4.0) / 4.0)
        if cp_prob >= 0.35 and whale_score >= 0.15:
            regime_label = "whale-watch"
        elif cp_prob >= 0.15:
            regime_label = "transition"
        else:
            regime_label = "stable"

        return {
            "cp_prob": round(cp_prob, 6),
            "expected_run_length": round(expected_run_length, 4),
            "map_run_length": int(map_run_length),
            "predictive_volatility": round(predictive_volatility, 8),
            "innovation_zscore": round(innovation_zscore, 6),
            "whale_score": round(whale_score, 6),
            "hazard": round(self._hazard(1), 8),
            "evidence": round(evidence, 12),
            "regime_label": regime_label,
            "num_updates": self.num_updates,
        }
