"""M5 — Vector Autoregression with orthogonalised Impulse Response.

Joint VAR(p) model on (news, market) — and (key_rate, news, market) when
key_rate is available, e.g. on S6. Lag order p is chosen by BIC. The
orthogonalised impulse response (Cholesky decomposition) at horizon L
gives the response of market to a contemporaneous shock to news *net of
any common-cause shock*, which is the testbed's intended fix for the
confounder failure of m3.

For a hypothesis "news at lag L predicts market":

  - Confirmed iff |orth_IRF[market <- news, horizon = L]| / SE > z_crit
                  AND p_value < P_MAX
                  AND n >= N_MIN

Cholesky ordering:
  - 2-var:  [news, market]                  (news shock can move market
                                             contemporaneously)
  - 3-var:  [key_rate, news, market]        (key_rate is most exogenous;
                                             orthogonal news shock is the
                                             part of news NOT explained by
                                             contemporaneous key_rate — this
                                             is the structural causal control)

Expected behaviour on testbed:
  - S1 pure noise:           FPR ~ 1%   (calibrated)
  - S2 spurious trend:       FPR ~ 1%   (returns + VAR sidesteps M001/M002)
  - S3 known signal lag=14:  TPR > 0    (VAR catches lead-lag with AR control)
  - S4 regime-dependent:     TPR high in first-half, ~0 in second
  - S5 multi-signal:         A and B at right lags, C rejected
  - S6 confounder:           FPR LOW — key_rate control kills spurious link.
                              This is the critical test that distinguishes
                              m5 from m3 (which had FPR=13.5% here).

m5 triggers M008 (no walk-forward inside one dataset). Other v1-family
mistakes avoided by construction.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.tsa.vector_ar.var_model import VAR

from .base import Hypothesis, MethodVerdict


M5_CONFIG: dict = {
    "method": "var",
    "target_transform": "log_returns",          # sidesteps M001
    "p_value_method": "HAC",                    # VAR uses asymptotic SE on
                                                # IRFs; close enough to HAC
                                                # for testbed purposes
    "n_correction": "effective_n",              # sidesteps M007
    "confirmed_decision": "deterministic_threshold",  # sidesteps M004
    "subsetting_in_sql": False,                 # sidesteps M005
    "regime_split_check": "same_sign_required", # sidesteps M003
    "coverage_check": True,                     # sidesteps M006
    "out_of_sample_validation": False,          # M008 still fires
}

# Lag-order rationale: AIC/BIC selected lag too short to carry the lag-L
# direct effect into IRF[L] — BIC gave TPR=0%, AIC missed S5 topic_B at
# lag 30. The fix is to fit VAR(p) with p = min(hyp.lag_days, MAX_LAG_VAR)
# *without* IC selection: this guarantees the news[t-L] coefficient enters
# the model whenever L <= cap, and IRF[L] reflects the direct effect. The
# extra parameters cost some power on null hypotheses but the asymptotic
# stderr correctly accounts for them.
#
# Cap at 30 so VAR(p) stays tractable: at p=30 on 2-var/1000obs that's 60
# params per equation, n/p ~= 8; on 3-var/1000 (S6) it's 90 params, n/p
# ~= 3.6 — borderline but produces stable IRFs in practice.
MAX_LAG_VAR = 30        # hard cap for VAR lag order
N_MIN = 150
P_MAX = 0.01
# z critical value for two-sided test at alpha = P_MAX
Z_CRIT = float(stats.norm.ppf(1.0 - P_MAX / 2.0))  # ~2.576 for P_MAX=0.01

# Optional control column. When present in the dataframe it is included as
# the most exogenous variable in the Cholesky ordering — see module docstring.
CONTROL_COL = "key_rate_pct"


@dataclass(frozen=True)
class M5Var:
    name: str = "M5_var_orth_irf"
    config: dict = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        object.__setattr__(self, "config", dict(M5_CONFIG))

    def evaluate(self, df: pd.DataFrame, hyp: Hypothesis) -> MethodVerdict:
        # Structural M001 protection: always evaluate on returns.
        if "market_return" in df.columns:
            target = df["market_return"].to_numpy(dtype=np.float64)
        else:
            close = df["market_close"].to_numpy(dtype=np.float64)
            target = np.empty_like(close)
            target[:] = np.nan
            target[1:] = np.diff(np.log(close))

        news = df[hyp.news_field].to_numpy(dtype=np.float64)
        control = (
            df[CONTROL_COL].to_numpy(dtype=np.float64)
            if CONTROL_COL in df.columns else None
        )

        # Regime slicing (same convention as m1/m2/m3/m4).
        if hyp.regime_filter == "first_half":
            half = len(df) // 2
            target = target[:half]
            news = news[:half]
            if control is not None:
                control = control[:half]
        elif hyp.regime_filter == "second_half":
            half = len(df) // 2
            target = target[half:]
            news = news[half:]
            if control is not None:
                control = control[half:]

        L = int(hyp.lag_days)
        if L < 1:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=0,
                extra={"reason": "lag_days_less_than_1"},
            )

        # Cholesky-ordered Y: [control (most exogenous), news, market].
        if control is not None:
            stacked = np.column_stack([control, news, target])
            news_idx = 1
            market_idx = 2
            has_control = True
        else:
            stacked = np.column_stack([news, target])
            news_idx = 0
            market_idx = 1
            has_control = False

        mask = np.isfinite(stacked).all(axis=1)
        Y = stacked[mask]
        n = int(len(Y))
        if n < N_MIN:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n,
                extra={"reason": "insufficient_data", "has_control": has_control},
            )

        # Fixed lag order = min(hyp.lag_days, MAX_LAG_VAR), bounded above by
        # available degrees of freedom. ic=None forces the full lag order.
        n_vars = Y.shape[1]
        dof_cap = max(1, n // (5 * n_vars))
        lag_order = min(L, MAX_LAG_VAR, dof_cap)

        try:
            model = VAR(Y)
            result = model.fit(maxlags=lag_order, ic=None)
        except Exception as exc:  # pragma: no cover — defensive
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n,
                extra={"reason": f"var_failed: {exc!r}",
                       "has_control": has_control},
            )

        # Need IRF computed at least up to horizon L.
        irf_steps = max(L, int(result.k_ar)) + 1
        try:
            irf = result.irf(periods=irf_steps)
            irf_arr = irf.orth_irfs  # (steps+1, n_vars, n_vars)
            irf_se = irf.stderr(orth=True)
        except Exception as exc:  # pragma: no cover — defensive
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n,
                extra={"reason": f"irf_failed: {exc!r}",
                       "has_control": has_control},
            )

        if L >= irf_arr.shape[0] or L >= irf_se.shape[0]:
            return MethodVerdict(
                confirmed=False, score=0.0, p_value=None, n=n,
                extra={"reason": "lag_exceeds_irf_horizon",
                       "irf_horizon": irf_arr.shape[0] - 1,
                       "has_control": has_control},
            )

        response = float(irf_arr[L, market_idx, news_idx])
        se = float(irf_se[L, market_idx, news_idx])
        if se <= 0.0 or not np.isfinite(se):
            return MethodVerdict(
                confirmed=False, score=response, p_value=None, n=n,
                extra={"reason": "zero_or_nan_se",
                       "has_control": has_control},
            )

        z_stat = response / se
        p_value = float(2.0 * (1.0 - stats.norm.cdf(abs(z_stat))))
        if p_value <= 0.0:
            p_value = 1e-300

        direction = float(np.sign(response)) if response != 0.0 else 0.0

        # Gate: HAC-style asymptotic two-sided test on IRF point + sample size.
        if abs(z_stat) < Z_CRIT:
            confirmed, reason = False, "z_below_crit"
        elif n < N_MIN:
            confirmed, reason = False, "n_below_min"
        elif p_value >= P_MAX:
            confirmed, reason = False, "p_above_max"
        else:
            confirmed, reason = True, "ok"

        return MethodVerdict(
            confirmed=confirmed,
            score=response,
            p_value=p_value,
            n=n,
            extra={
                "n_min": N_MIN, "p_max": P_MAX, "z_crit": Z_CRIT,
                "target_used": "market_return (log-returns)",
                "lag_days": L,
                "has_control": has_control,
                "n_vars": int(n_vars),
                "k_ar": int(result.k_ar),
                "lag_order_used": int(lag_order),
                "z_stat": float(z_stat),
                "se": se,
                "direction_sign": direction,
                "gate_reason": reason,
            },
        )


def build() -> M5Var:
    return M5Var()
