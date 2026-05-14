"""Unit tests for the deterministic evaluator gate."""
from __future__ import annotations

import math

from analytics.testbed.methods.evaluator import (
    Evaluation,
    GateThresholds,
    REASON_OK,
    REASON_R_BELOW_MIN,
    REASON_N_BELOW_MIN,
    REASON_P_ABOVE_MAX,
    REASON_MIXED_SIGNS,
    REASON_INVALID_INPUT,
    REASON_ANY_REGIME_BELOW,
    evaluate,
)


# ---------------------------------------------------------------------------
# Single-result path
# ---------------------------------------------------------------------------

def test_confirms_clear_strong_signal() -> None:
    v = evaluate(r=-0.55, p_value=1e-5, n=200)
    assert v.confirmed is True
    assert v.reason == REASON_OK


def test_rejects_weak_r() -> None:
    v = evaluate(r=0.39, p_value=1e-5, n=300)
    assert v.confirmed is False
    assert v.reason == REASON_R_BELOW_MIN


def test_rejects_low_n() -> None:
    v = evaluate(r=-0.5, p_value=1e-3, n=149)
    assert v.confirmed is False
    assert v.reason == REASON_N_BELOW_MIN


def test_rejects_p_too_high() -> None:
    v = evaluate(r=-0.5, p_value=0.011, n=200)
    assert v.confirmed is False
    assert v.reason == REASON_P_ABOVE_MAX


def test_r_min_is_inclusive_on_violation_side() -> None:
    """r exactly at threshold MUST pass; r just below MUST fail."""
    assert evaluate(r=0.40, p_value=1e-5, n=200).confirmed is True
    v = evaluate(r=0.3999, p_value=1e-5, n=200)
    assert v.confirmed is False
    assert v.reason == REASON_R_BELOW_MIN


def test_invalid_input_none() -> None:
    v = evaluate(r=None, p_value=1e-5, n=200)
    assert v.confirmed is False
    assert v.reason == REASON_INVALID_INPUT
    v2 = evaluate(r=-0.5, p_value=None, n=200)
    assert v2.confirmed is False
    assert v2.reason == REASON_INVALID_INPUT


def test_invalid_input_nan() -> None:
    v = evaluate(r=float("nan"), p_value=1e-5, n=200)
    assert v.confirmed is False
    assert v.reason == REASON_INVALID_INPUT
    v2 = evaluate(r=-0.5, p_value=math.inf, n=200)
    assert v2.confirmed is False
    assert v2.reason == REASON_INVALID_INPUT


def test_negative_n_is_invalid() -> None:
    v = evaluate(r=-0.5, p_value=1e-5, n=-1)
    assert v.confirmed is False
    assert v.reason == REASON_INVALID_INPUT


# ---------------------------------------------------------------------------
# Multi-regime path
# ---------------------------------------------------------------------------

def test_multi_regime_same_sign_confirms() -> None:
    rr = [
        {"r": -0.50, "n": 200, "p": 1e-5},
        {"r": -0.45, "n": 180, "p": 5e-4},
    ]
    v = evaluate(r=-0.475, p_value=1e-5, n=380, regime_results=rr)
    assert v.confirmed is True


def test_multi_regime_mixed_signs_rejects_M003() -> None:
    """The exact failure mode of mistake M003 — opposite-sign regimes that
    BOTH individually pass thresholds. The smoke v1 example (r=-0.34 + r=+0.58)
    actually rejects at the threshold step first; here we use stronger values
    so the gate gets to evaluate the sign-mismatch rule itself."""
    rr = [
        {"r": -0.50, "n": 200, "p": 1e-5},
        {"r": +0.55, "n": 180, "p": 1e-6},
    ]
    v = evaluate(r=0.0, p_value=1e-5, n=380, regime_results=rr)
    assert v.confirmed is False
    assert v.reason == REASON_MIXED_SIGNS


def test_multi_regime_smoke_v1_inflation_case_rejects_at_threshold() -> None:
    """The actual smoke-v1 case rejects BEFORE the sign rule, because the
    first regime's |r|=0.34 is already below threshold. Documents real
    project history."""
    rr = [
        {"r": -0.34, "n": 194, "p": 1e-3},
        {"r": +0.58, "n": 165, "p": 1e-7},
    ]
    v = evaluate(r=0.12, p_value=1e-3, n=359, regime_results=rr)
    assert v.confirmed is False
    assert v.reason == REASON_ANY_REGIME_BELOW


def test_multi_regime_any_below_threshold_rejects() -> None:
    rr = [
        {"r": -0.50, "n": 200, "p": 1e-5},
        {"r": -0.20, "n": 200, "p": 1e-5},   # second regime |r|<0.4
    ]
    v = evaluate(r=-0.35, p_value=1e-5, n=400, regime_results=rr)
    assert v.confirmed is False
    assert v.reason == REASON_ANY_REGIME_BELOW


def test_can_disable_same_sign_requirement() -> None:
    """If user explicitly opts out, mixed signs no longer reject — they
    pass through to the per-regime threshold check."""
    th = GateThresholds(require_same_sign_across_regimes=False)
    rr = [
        {"r": -0.50, "n": 200, "p": 1e-5},
        {"r": +0.50, "n": 200, "p": 1e-5},
    ]
    v = evaluate(r=0.0, p_value=1e-5, n=400, regime_results=rr, thresholds=th)
    assert v.confirmed is True


def test_evaluation_dataclass_exposes_rejected_helper() -> None:
    v = Evaluation(False, REASON_R_BELOW_MIN)
    assert v.rejected is True
    assert v.confirmed is False
