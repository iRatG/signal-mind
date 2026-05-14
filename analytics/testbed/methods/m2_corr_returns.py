"""M2 - Pearson r on RETURNS with Newey-West HAC standard errors.

Same correlation idea as m1, but on the right unit (returns, not levels)
and with autocorrelation-consistent p-values. This is the minimum viable
"correct" correlation method and the first comparison point against the
v1-style m1.

Expected behaviour on testbed:
  - S1 pure noise:           FPR ~ 5%  (p-value calibrated)
  - S2 spurious trend:       FPR ~ 5%  (no longer trapped — that's the point)
  - S3 known signal lag=14:  TPR high at lag 14, ~0 at other lags
  - S4 regime-dependent:     TPR high in first-half, ~0 in second
  - S5 multi-signal:         topic_A and topic_B confirmed, topic_C rejected
  - S6 confounder:           FPR low-to-moderate (no causal discipline yet)

m2 still triggers M008 (no walk-forward inside one dataset). All other
mistakes of the v1 family are avoided by construction.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats

from .base import Hypothesis, MethodVerdict, shift_forward
from .evaluator import GateThresholds, evaluate


M2_CONFIG: dict = {
    "method": "pearson_corr",
    "target_transform": "log_returns",          # NOT levels — sidesteps M001
    "p_value_method": "HAC",                    # sidesteps M002
    "n_correction": "HAC",                      # sidesteps M007
    "confirmed_decision": "deterministic_threshold",  # sidesteps M004
    "subsetting_in_sql": False,                 # sidesteps M005
    "regime_split_check": "same_sign_required", # sidesteps M003
    "coverage_check": True,                     # sidesteps M006
    "out_of_sample_validation": False,          # M008 still fires inside one dataset
}

R_MIN = 0.40
N_MIN = 150
P_MAX = 0.01


def _hac_maxlag(n: int) -> int:
    """Newey-West rule-of-thumb: maxlags = floor(4 * (n/100)^(2/9))."""
    return max(1, int(np.floor(4.0 * (n / 100.0) ** (2.0 / 9.0))))


def _zscore(arr: np.ndarray) -> np.ndarray:
    s = arr.std()
    if s == 0 or not np.isfinite(s):
        return arr - arr.mean()
    return (arr - arr.mean()) / s


@dataclass(frozen=True)
class M2CorrReturns:
    name: str = "M2_corr_returns_hac"
    config: dict = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        object.__setattr__(self, "config", dict(M2_CONFIG))

    def evaluate(self, df: pd.DataFrame, hyp: Hypothesis) -> MethodVerdict:
        # Resolve the target as RETURNS regardless of what the hypothesis
        # named: this is the structural protection against M001.
        if "market_return" in df.columns:
            target = df["market_return"].to_numpy(dtype=np.float64)
        else:
            # Derive log-returns from market_close if needed.
            close = df["market_close"].to_numpy(dtype=np.float64)
            target = np.empty_like(close)
            target[:] = np.nan
            target[1:] = np.diff(np.log(close))

        news = df[hyp.news_field].to_numpy(dtype=np.float64)
        news_lagged = shift_forward(news, hyp.lag_days)

        # Regime slicing (same convention as m1)
        if hyp.regime_filter == "first_half":
            half = len(df) // 2
            news_lagged = news_lagged[:half]
            target = target[:half]
        elif hyp.regime_filter == "second_half":
            half = len(df) // 2
            news_lagged = news_lagged[half:]
            target = target[half:]

        mask = ~np.isnan(news_lagged) & ~np.isnan(target)
        n = int(mask.sum())
        if n < 30:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n,
                extra={"reason": "insufficient_data"},
            )

        x = news_lagged[mask]
        y = target[mask]

        x_z = _zscore(x)
        y_z = _zscore(y)

        # OLS y_z = a + b * x_z, with Newey-West HAC standard errors.
        # When both sides are standardised, b == Pearson r.
        X = sm.add_constant(x_z)
        try:
            res = sm.OLS(y_z, X).fit(
                cov_type="HAC",
                cov_kwds={"maxlags": _hac_maxlag(n)},
            )
        except Exception as exc:  # pragma: no cover — defensive
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n,
                extra={"reason": f"ols_failed: {exc!r}"},
            )

        beta = float(res.params[1])
        se = float(res.bse[1])
        if se <= 0 or not np.isfinite(se):
            return MethodVerdict(
                confirmed=False, score=beta, p_value=None, n=n,
                extra={"reason": "zero_or_nan_se"},
            )

        t_stat = beta / se
        df_t = max(1, n - 2)
        p_two_sided = float(2.0 * (1.0 - stats.t.cdf(abs(t_stat), df_t)))
        # Numeric edge: p can come back as exactly 0.0 from cdf saturation;
        # clamp to a tiny positive number so the gate's strict < still works.
        if p_two_sided <= 0.0:
            p_two_sided = 1e-300

        gate = evaluate(
            r=beta, p_value=p_two_sided, n=n,
            thresholds=GateThresholds(r_min=R_MIN, n_min=N_MIN, p_max=P_MAX),
        )
        return MethodVerdict(
            confirmed=gate.confirmed,
            score=beta,
            p_value=p_two_sided,
            n=n,
            extra={
                "r_min": R_MIN, "n_min": N_MIN, "p_max": P_MAX,
                "target_used": "market_return (log-returns)",
                "hac_maxlag": _hac_maxlag(n),
                "se_hac": se,
                "t_stat": t_stat,
                "gate_reason": gate.reason,
            },
        )


def build() -> M2CorrReturns:
    return M2CorrReturns()
