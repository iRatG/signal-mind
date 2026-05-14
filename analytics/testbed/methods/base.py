"""Common interface for all candidate methods.

Every method (M1..M5) implements `evaluate()` returning a `MethodVerdict`.
The harness uses only this interface — methods are interchangeable plugins.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, Optional

import numpy as np


@dataclass(frozen=True)
class Hypothesis:
    """One concrete hypothesis to test on a dataset seed."""
    news_field: str           # column name in seed parquet
    target_field: str         # e.g. "market_return" or "market_close"
    lag_days: int             # how many days news leads market
    direction: Optional[str] = None  # "positive" | "negative" | None
    regime_filter: Optional[str] = None  # SQL-like filter or None


@dataclass(frozen=True)
class MethodVerdict:
    """One method's verdict on one (seed, hypothesis) pair."""
    confirmed: bool
    score: float              # method-specific magnitude (r, F-stat, IC, ...)
    p_value: Optional[float]  # None if method doesn't produce one
    n: int                    # effective sample size used
    extra: dict = field(default_factory=dict)


class Method(Protocol):
    """Plugin contract for methods. Read-only — no side effects on inputs."""
    name: str
    config: dict   # used by mistakes catalogue's compute_penalty

    def evaluate(self, df, hyp: Hypothesis) -> MethodVerdict:
        """Apply the method to one dataframe (one seed) and one hypothesis."""
        ...


def shift_forward(arr: np.ndarray, k: int) -> np.ndarray:
    """Return `arr` shifted forward by `k` positions (NaN-padded at start).

    Used so that `target[t]` aligns with `news[t-k]` after dropping NaNs.
    """
    out = np.empty_like(arr, dtype=np.float64)
    out[:] = np.nan
    if k < len(arr):
        out[k:] = arr[: len(arr) - k]
    return out
