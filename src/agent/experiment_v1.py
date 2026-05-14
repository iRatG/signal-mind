"""Experiment v1 — runtime mode selector.

A single module that any agent component consults to figure out which split
window it is operating in. Activated via the EXPERIMENT_MODE env var:

    EXPERIMENT_MODE=v1_train  -> Train window, agent only sees 2022-01..2023-09
    EXPERIMENT_MODE=v1_val    -> Validation window (used by validation_pass.py)
    EXPERIMENT_MODE=v1_test   -> Test window (used by test_oneshot.py, one-shot)
    (unset)                   -> legacy full-data mode (pre-experiment behaviour)

Frozen at commit da6801a — see analytics/experiment_v1.md.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal, Optional

Mode = Literal["v1_train", "v1_val", "v1_test"]


@dataclass(frozen=True)
class Window:
    name: str            # 'train' / 'val' / 'test'
    mode: str            # 'v1_train' / 'v1_val' / 'v1_test'
    start: str           # ISO YYYY-MM-DD
    end: str             # ISO YYYY-MM-DD
    ctx_view: str        # e.g. 'v_train_ctx'
    sectors_view: str    # e.g. 'v_train_sectors'
    news_view: str       # e.g. 'v_train_news'
    disqualified_topics: tuple[str, ...]
    # Window-typical regime — used to keep regime injection consistent with the
    # data the agent actually sees. Values are window medians from data_audit_v1.
    regime_key_rate_pct: float = 0.0
    regime_usd_rub: float = 0.0


_WINDOWS: dict[str, Window] = {
    "v1_train": Window(
        name="train",
        mode="v1_train",
        start="2022-01-01",
        end="2023-09-30",
        ctx_view="v_train_ctx",
        sectors_view="v_train_sectors",
        news_view="v_train_news",
        disqualified_topics=("ruble",),
        regime_key_rate_pct=8.0,
        regime_usd_rub=74.94,
    ),
    "v1_val": Window(
        name="val",
        mode="v1_val",
        start="2024-01-01",
        end="2025-04-30",
        ctx_view="v_val_ctx",
        sectors_view="v_val_sectors",
        news_view="v_val_news",
        disqualified_topics=("ruble", "sanctions"),
        regime_key_rate_pct=18.5,
        regime_usd_rub=91.26,
    ),
    "v1_test": Window(
        name="test",
        mode="v1_test",
        start="2025-09-01",
        end="2026-04-29",
        ctx_view="v_test_ctx",
        sectors_view="v_test_sectors",
        news_view="v_test_news",
        disqualified_topics=(),
        regime_key_rate_pct=16.0,
        regime_usd_rub=79.08,
    ),
}


def get_mode() -> Optional[Mode]:
    """Return EXPERIMENT_MODE env var if it is one of the known modes, else None."""
    raw = os.environ.get("EXPERIMENT_MODE", "").strip().lower()
    if raw in _WINDOWS:
        return raw  # type: ignore[return-value]
    return None


def get_window() -> Optional[Window]:
    """Active window object, or None if no experiment mode is active."""
    mode = get_mode()
    if mode is None:
        return None
    return _WINDOWS[mode]


def is_active() -> bool:
    return get_mode() is not None


def banner() -> str:
    """Short human-readable banner — printed at agent start when experiment is on."""
    w = get_window()
    if w is None:
        return ""
    return (
        f"=== EXPERIMENT v1 / {w.mode.upper()} active ===\n"
        f"  window     : {w.start} -> {w.end}\n"
        f"  views      : {w.ctx_view}, {w.sectors_view}, {w.news_view}\n"
        f"  disqualified topics: {', '.join(w.disqualified_topics) if w.disqualified_topics else '(none)'}\n"
        f"  any data outside this window MUST NOT be referenced.\n"
    )
