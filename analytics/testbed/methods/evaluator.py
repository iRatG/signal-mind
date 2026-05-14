"""Deterministic gate — decides confirmed/rejected from (r, p, n[, regimes]).

Takes the LLM out of the confirmed/rejected decision (mistake M004). Every
method in the testbed funnels its raw statistics through this single gate,
so the decision rule is one place, testable, and consistent across methods.

The gate is conservative by default: any of the thresholds being violated
means rejection, with a machine-readable `reason` for diagnostics.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class GateThresholds:
    """Numeric thresholds. Defaults match analytics/experiment_v1.md."""
    r_min: float = 0.40
    n_min: int = 150
    p_max: float = 0.01
    require_same_sign_across_regimes: bool = True


@dataclass(frozen=True)
class Evaluation:
    """The gate's verdict for one (r, p, n[, regimes]) tuple."""
    confirmed: bool
    reason: str  # short tag for diagnostics; "ok" iff confirmed

    @property
    def rejected(self) -> bool:
        return not self.confirmed


REASON_OK = "ok"
REASON_INVALID_INPUT = "invalid_input"
REASON_R_BELOW_MIN = "r_below_min"
REASON_N_BELOW_MIN = "n_below_min"
REASON_P_ABOVE_MAX = "p_above_max"
REASON_MIXED_SIGNS = "mixed_regime_signs"
REASON_ANY_REGIME_BELOW = "any_regime_below_threshold"


def evaluate(
    r: Optional[float],
    p_value: Optional[float],
    n: int,
    regime_results: Optional[list[dict]] = None,
    thresholds: GateThresholds = GateThresholds(),
) -> Evaluation:
    """Apply the gate.

    Args:
        r: Pearson r (or analogous signed effect size). May be None when the
           method has no scalar r (e.g. pure Granger F).
        p_value: significance under the method's null. May be None if the
           method declines to produce one — in which case we conservatively
           reject (you can't "confirm" without a p, by policy).
        n: effective sample size used.
        regime_results: list of {"r": float, "n": int, "p": float} per regime
           when the hypothesis carries a multi-regime split. If supplied,
           ALL regimes must individually pass AND share the same sign of r
           (mistake M003 — opposite-sign confirmations are rejected here).
        thresholds: GateThresholds instance with the numeric cutoffs.

    Returns:
        Evaluation with `confirmed: bool` and `reason: str`.
    """
    # Bad inputs reject conservatively (and audibly).
    if r is None or p_value is None:
        return Evaluation(False, REASON_INVALID_INPUT)
    if not (math.isfinite(r) and math.isfinite(p_value)):
        return Evaluation(False, REASON_INVALID_INPUT)
    if n < 0:
        return Evaluation(False, REASON_INVALID_INPUT)

    # Multi-regime path: every regime must pass AND signs must agree.
    if regime_results:
        signs = set()
        for rr in regime_results:
            r_i = rr.get("r")
            p_i = rr.get("p")
            n_i = int(rr.get("n", 0))
            if r_i is None or p_i is None:
                return Evaluation(False, REASON_INVALID_INPUT)
            if abs(r_i) < thresholds.r_min:
                return Evaluation(False, REASON_ANY_REGIME_BELOW)
            if n_i < thresholds.n_min:
                return Evaluation(False, REASON_ANY_REGIME_BELOW)
            if p_i >= thresholds.p_max:
                return Evaluation(False, REASON_ANY_REGIME_BELOW)
            signs.add(1 if r_i > 0 else -1)
        if thresholds.require_same_sign_across_regimes and len(signs) > 1:
            return Evaluation(False, REASON_MIXED_SIGNS)
        return Evaluation(True, REASON_OK)

    # Single-result path.
    if abs(r) < thresholds.r_min:
        return Evaluation(False, REASON_R_BELOW_MIN)
    if n < thresholds.n_min:
        return Evaluation(False, REASON_N_BELOW_MIN)
    if p_value >= thresholds.p_max:
        return Evaluation(False, REASON_P_ABOVE_MAX)
    return Evaluation(True, REASON_OK)
