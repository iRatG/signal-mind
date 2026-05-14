"""Unit tests for analytics.testbed.methods.base helpers."""
from __future__ import annotations

import math

import numpy as np

from analytics.testbed.methods.base import (
    Hypothesis,
    MethodVerdict,
    shift_forward,
)


def test_shift_forward_basic() -> None:
    arr = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    out = shift_forward(arr, 2)
    assert math.isnan(out[0]) and math.isnan(out[1])
    assert (out[2:] == arr[:3]).all()


def test_shift_forward_zero_lag_identity() -> None:
    arr = np.array([10.0, 20.0, 30.0])
    out = shift_forward(arr, 0)
    assert (out == arr).all()


def test_shift_forward_lag_equal_length_all_nan() -> None:
    arr = np.array([1.0, 2.0, 3.0])
    out = shift_forward(arr, 3)
    assert np.isnan(out).all()


def test_shift_forward_lag_exceeds_length_all_nan() -> None:
    arr = np.array([1.0, 2.0])
    out = shift_forward(arr, 5)
    assert np.isnan(out).all()


def test_shift_forward_preserves_dtype_to_float() -> None:
    """Integer input must come out as float (because of NaN padding)."""
    arr = np.array([1, 2, 3], dtype=np.int32)
    out = shift_forward(arr, 1)
    assert out.dtype.kind == "f"


def test_hypothesis_is_immutable_dataclass() -> None:
    h = Hypothesis(news_field="x", target_field="y", lag_days=7)
    try:
        h.lag_days = 14  # type: ignore[misc]
    except Exception:
        return
    raise AssertionError("Hypothesis should be frozen")


def test_method_verdict_carries_extra_dict() -> None:
    v = MethodVerdict(confirmed=True, score=0.5, p_value=1e-3, n=200,
                      extra={"note": "ok"})
    assert v.extra["note"] == "ok"
