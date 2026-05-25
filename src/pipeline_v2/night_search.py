"""Adaptive Night Search — overnight signal discovery with multiple strategies.

Architecture:
  Phase 1 — Quick Sweep (M6-only, fast):
    Test all feature types × all targets with M6 only.
    Identify which (feature, target) combinations show the most IC signal.

  Phase 2 — Deep Dive (M5+M6/AND, strict):
    Apply the full ensemble to the top-N configs from Phase 1.
    Find signals that pass both structural (M5) and predictive (M6) gates.

  Phase 3 — Regime Split (adaptive):
    Take the best config from Phase 2 (or Phase 1 if Phase 2 found nothing).
    Split Train into 2022 (crisis) and 2023 (normalisation) sub-periods.
    Test within each regime — find regime-specific signals.

  Report — Ranked summary of all found signals across all phases.

Each phase runs until its time budget is exhausted or hypotheses are done.
Results are saved incrementally — safe to interrupt.

Usage:
    .venv/Scripts/python -m src.pipeline_v2.night_search
    .venv/Scripts/python -m src.pipeline_v2.night_search --phase-budget 7200
    .venv/Scripts/python -m src.pipeline_v2.night_search --phases 1,2
"""
from __future__ import annotations

import argparse
import importlib
import io
import sys
import time
import warnings
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

ROOT    = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "db" / "signal_mind.duckdb"
OUT_DIR = ROOT / "analytics" / "phase_b" / "night_search"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Import shared infrastructure from big_scanner
sys.path.insert(0, str(ROOT))
from src.pipeline_v2.big_scanner import (
    MARKET_INSTRUMENTS, MOEX_SECTORS,
    load_market_split, load_moex_split,
)
from src.pipeline_v2.feature_transformer import FeatureTransformer, FEATURE_TYPES, TARGETS

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

LAGS_FULL   = [1, 7, 14, 30, 60, 90]
LAGS_SHORT  = [1, 7, 14, 30]          # faster, more data per fold

DEFAULT_PHASE_BUDGET = 2 * 3600       # 2 hours per phase


@dataclass
class SearchConfig:
    feature_type: str     # one of FEATURE_TYPES
    target: str           # one of TARGETS
    ensemble: str         # "M6_only" | "M5_AND_M6" | "M5_only"
    m5_p_max: float       # M5 p-value gate
    m6_ic_min: float      # M6 IC gate
    m6_bootstrap_min: float  # M6 bootstrap 5th-pct gate (0 = off)
    lags: list[int]       # lag days to test
    label: str            # human-readable label for reports

    @property
    def id(self) -> str:
        return (f"{self.feature_type}__{self.target}__{self.ensemble}"
                f"__p{self.m5_p_max}_ic{self.m6_ic_min}")


@dataclass
class SignalHit:
    instrument: str
    topic: str
    lag: int
    m5_pvalue: float
    m5_score: float
    m6_ic: float
    m6_confirmed: bool
    m5_confirmed: bool
    ensemble_pass: bool
    config_id: str
    split: str
    regime: str = ""     # "2022" | "2023" | "" = full


@dataclass
class PhaseResult:
    phase: int
    config: SearchConfig
    n_train_pass: int
    n_val_pass: int
    mean_ic: float
    max_ic: float
    ic_above_05: int      # hypotheses with IC >= 0.05
    hits: list[SignalHit] = field(default_factory=list)
    elapsed_s: float = 0.0

    @property
    def score(self) -> float:
        """Ranking score for adaptive selection."""
        return self.n_train_pass * 10 + self.ic_above_05 + self.mean_ic * 100


# ──────────────────────────────────────────────────────────────────────────────
# Phase 1 config matrix
# ──────────────────────────────────────────────────────────────────────────────

def phase1_configs() -> list[SearchConfig]:
    """All combinations for Phase 1 sweep (M6-only, fast)."""
    configs = []
    for feat in FEATURE_TYPES:
        for tgt in TARGETS:
            configs.append(SearchConfig(
                feature_type=feat,
                target=tgt,
                ensemble="M6_only",
                m5_p_max=0.05,
                m6_ic_min=0.03,
                m6_bootstrap_min=0.0,
                lags=LAGS_SHORT,
                label=f"sweep_{feat}_{tgt}",
            ))
    return configs   # 5 features × 3 targets = 15 configs


def phase2_configs(top_results: list[PhaseResult]) -> list[SearchConfig]:
    """Deep-dive configs from Phase 1 winners (top-3 by score)."""
    sorted_results = sorted(top_results, key=lambda r: r.score, reverse=True)[:3]
    configs = []
    for r in sorted_results:
        # AND ensemble, full lags, tighter IC
        configs.append(SearchConfig(
            feature_type=r.config.feature_type,
            target=r.config.target,
            ensemble="M5_AND_M6",
            m5_p_max=0.05,
            m6_ic_min=0.01,
            m6_bootstrap_min=0.0,
            lags=LAGS_FULL,
            label=f"deep_{r.config.feature_type}_{r.config.target}",
        ))
        # Also try M6-only with stricter IC
        configs.append(SearchConfig(
            feature_type=r.config.feature_type,
            target=r.config.target,
            ensemble="M6_only",
            m5_p_max=0.05,
            m6_ic_min=0.05,
            m6_bootstrap_min=0.01,
            lags=LAGS_FULL,
            label=f"deep_m6strict_{r.config.feature_type}_{r.config.target}",
        ))
    return configs


def phase3_configs(best_result: PhaseResult) -> list[tuple[SearchConfig, str, str, str]]:
    """Regime split: (config, split_start, split_end, regime_label)."""
    cfg = best_result.config
    regime_configs = []
    for (start, end, label) in [
        ("2022-01-01", "2022-12-31", "regime_2022"),
        ("2023-01-01", "2023-09-30", "regime_2023"),
    ]:
        regime_configs.append((
            SearchConfig(
                feature_type=cfg.feature_type,
                target=cfg.target,
                ensemble=cfg.ensemble,
                m5_p_max=cfg.m5_p_max,
                m6_ic_min=cfg.m6_ic_min,
                m6_bootstrap_min=cfg.m6_bootstrap_min,
                lags=LAGS_FULL,
                label=f"{label}_{cfg.feature_type}_{cfg.target}",
            ),
            start, end, label
        ))
    return regime_configs


# ──────────────────────────────────────────────────────────────────────────────
# Core scan logic
# ──────────────────────────────────────────────────────────────────────────────

def evaluate_hypothesis(df: pd.DataFrame, topic: str, target: str,
                        lag: int, config: SearchConfig,
                        m5, m6) -> dict:
    """Run M5+M6 on one hypothesis. Returns raw stats + custom gate verdict."""
    from analytics.testbed.methods.base import Hypothesis

    hyp = Hypothesis(news_field=topic, target_field=target, lag_days=lag)

    # M5
    try:
        v5 = m5.evaluate(df, hyp)
        m5_p = v5.p_value or 1.0
        m5_score = float(v5.score)
        m5_n = v5.n
        m5_raw_confirmed = v5.confirmed
    except Exception as e:
        return {"error": f"m5: {e}", "confirmed": False,
                "m5_confirmed": False, "m6_confirmed": False,
                "m5_pvalue": 1.0, "m5_score": 0.0,
                "m6_ic": 0.0, "m6_n": 0}

    # M6
    try:
        v6 = m6.evaluate(df, hyp)
        m6_ic = float(v6.score)
        m6_n = v6.n
        m6_ci_lo = v6.extra.get("ic_bootstrap_lo", float('nan'))
    except Exception as e:
        return {"error": f"m6: {e}", "confirmed": False,
                "m5_confirmed": False, "m6_confirmed": False,
                "m5_pvalue": m5_p, "m5_score": m5_score,
                "m6_ic": 0.0, "m6_n": 0}

    # Custom gates based on config
    m5_pass = (m5_n >= 100) and (m5_p < config.m5_p_max)
    m6_pass = (m6_n >= 100) and (m6_ic >= config.m6_ic_min)
    if config.m6_bootstrap_min > 0 and not np.isnan(m6_ci_lo):
        m6_pass = m6_pass and (m6_ci_lo > config.m6_bootstrap_min)

    if config.ensemble == "M5_AND_M6":
        confirmed = m5_pass and m6_pass
    elif config.ensemble == "M6_only":
        confirmed = m6_pass
    elif config.ensemble == "M5_only":
        confirmed = m5_pass
    else:
        confirmed = False

    return {
        "confirmed": confirmed,
        "m5_confirmed": m5_pass,
        "m6_confirmed": m6_pass,
        "m5_pvalue": round(m5_p, 6),
        "m5_score":  round(m5_score, 6),
        "m6_ic":     round(m6_ic, 6),
        "m6_n":      m6_n,
        "error": "",
    }


def scan_split(split_dfs: dict[str, pd.DataFrame | None],
               config: SearchConfig,
               m5, m6,
               instruments: list[str],
               topics: list[str],
               time_budget: float,
               log_fn,
               regime_label: str = "") -> list[SignalHit]:
    """Scan one split with given config. Returns list of confirmed signals."""
    hits: list[SignalHit] = []
    t0 = time.time()
    done = confirmed = 0
    total = len(instruments) * len(topics) * len(config.lags)

    for inst in instruments:
        df_raw = split_dfs.get(inst)
        if df_raw is None:
            continue
        # Apply feature transform
        try:
            df = FeatureTransformer.apply(df_raw, config.feature_type, config.target)
        except ValueError:
            continue

        for topic in topics:
            if topic not in df.columns:
                continue
            for lag in config.lags:
                if time.time() - t0 > time_budget:
                    log_fn(f"  Budget exhausted at {done}/{total}")
                    return hits

                r = evaluate_hypothesis(df, topic, config.target, lag,
                                        config, m5, m6)
                done += 1
                if r.get("confirmed"):
                    confirmed += 1
                    hits.append(SignalHit(
                        instrument=inst, topic=topic, lag=lag,
                        m5_pvalue=r["m5_pvalue"], m5_score=r["m5_score"],
                        m6_ic=r["m6_ic"],
                        m5_confirmed=r["m5_confirmed"],
                        m6_confirmed=r["m6_confirmed"],
                        ensemble_pass=True,
                        config_id=config.id,
                        split="train",
                        regime=regime_label,
                    ))
                    log_fn(f"  HIT  {inst:18s} {topic:16s} lag={lag:3d} "
                           f"m5_p={r['m5_pvalue']:.4f}  m6_ic={r['m6_ic']:.4f}")

        if done % 50 == 0 and done > 0:
            elapsed = time.time() - t0
            log_fn(f"  progress {done}/{total}  confirmed={confirmed}  "
                   f"elapsed={elapsed:.0f}s")

    return hits


# ──────────────────────────────────────────────────────────────────────────────
# Data loading with feature transform
# ──────────────────────────────────────────────────────────────────────────────

def load_arbitrary_window(date_start: str, date_end: str,
                           log_fn) -> dict[str, pd.DataFrame | None]:
    """
    Load data for ANY date range by querying across all split views.
    Used for rolling window training — not limited to fixed splits.
    """
    import duckdb

    def _load(inst: str, is_moex: bool, col: str = "") -> pd.DataFrame | None:
        try:
            con = duckdb.connect(str(DB_PATH), read_only=True)

            # Load market from ALL splits combined
            if is_moex:
                parts = []
                for split in ["train", "val", "test"]:
                    try:
                        p = con.execute(
                            f"SELECT trade_date AS date, {col} AS close "
                            f"FROM v_{split}_sectors WHERE {col} IS NOT NULL"
                        ).fetchdf()
                        parts.append(p)
                    except Exception:
                        pass
            else:
                parts = []
                for split in ["train", "val", "test"]:
                    try:
                        p = con.execute(
                            f"SELECT trade_date AS date, close "
                            f"FROM v_{split}_market_data WHERE instrument='{inst}'"
                        ).fetchdf()
                        parts.append(p)
                    except Exception:
                        pass

            if not parts:
                return None
            mkt = pd.concat(parts).drop_duplicates("date").sort_values("date")
            mkt["date"] = pd.to_datetime(mkt["date"])
            mkt = mkt[(mkt["date"] >= date_start) & (mkt["date"] <= date_end)]
            if len(mkt) < 50:
                return None
            mkt = mkt.sort_values("date").reset_index(drop=True)
            mkt["market_return"] = np.log(mkt["close"]).diff()

            kr = con.execute(
                "SELECT period_date AS date, rate_pct AS key_rate_pct "
                "FROM v_key_rate_daily"
            ).fetchdf()
            kr["date"] = pd.to_datetime(kr["date"])
            mkt = pd.merge_asof(mkt, kr.sort_values("date"),
                                on="date", direction="backward")

            # News from all splits
            news_parts = []
            for split in ["train", "val", "test"]:
                try:
                    np_ = con.execute(f"SELECT * FROM v_{split}_news").fetchdf()
                    np_ = np_.rename(columns={"news_date": "date"})
                    news_parts.append(np_)
                except Exception:
                    pass
            if not news_parts:
                return None
            news = pd.concat(news_parts).drop_duplicates("date")
            news["date"] = pd.to_datetime(news["date"])
            news = news[(news["date"] >= date_start) & (news["date"] <= date_end)]

            df = pd.merge(mkt, news, on="date", how="inner")
            return df.sort_values("date").reset_index(drop=True) if len(df) >= 50 else None
        except Exception:
            return None
        finally:
            try:
                con.close()
            except Exception:
                pass

    dfs = {}
    for inst in MARKET_INSTRUMENTS:
        dfs[inst] = _load(inst, is_moex=False)
    for name, col in MOEX_SECTORS.items():
        dfs[name] = _load(name, is_moex=True, col=col)
    n_ok = sum(1 for v in dfs.values() if v is not None)
    log_fn(f"  Window {date_start}→{date_end}: {n_ok} instruments")
    return dfs


def load_all_splits(log_fn) -> tuple[dict, dict, dict]:
    """Pre-load all instruments for train/val/test. Returns (train, val, test)."""
    all_insts = MARKET_INSTRUMENTS + list(MOEX_SECTORS.keys())

    def load_split(split: str) -> dict:
        log_fn(f"Loading {split} data...")
        dfs = {}
        for inst in MARKET_INSTRUMENTS:
            dfs[inst] = load_market_split(split, inst)
        for name, col in MOEX_SECTORS.items():
            dfs[name] = load_moex_split(split, col)
        n_ok = sum(1 for v in dfs.values() if v is not None)
        log_fn(f"  {split}: {n_ok}/{len(all_insts)} instruments loaded")
        return dfs

    return (load_split("train"), load_split("val"), load_split("test"))


def load_regime_split(regime_start: str, regime_end: str,
                      log_fn) -> dict[str, pd.DataFrame | None]:
    """Load train data filtered to a specific date range (regime)."""
    import duckdb

    def _load_inst(query: str) -> pd.DataFrame | None:
        try:
            con = duckdb.connect(str(DB_PATH), read_only=True)
            news_df = con.execute("SELECT * FROM v_train_news").fetchdf()
            news_df = news_df.rename(columns={"news_date": "date"})
            news_df["date"] = pd.to_datetime(news_df["date"])
            news_df = news_df[(news_df["date"] >= regime_start) &
                              (news_df["date"] <= regime_end)]

            mkt_df = con.execute(query).fetchdf()
            mkt_df["date"] = pd.to_datetime(mkt_df["trade_date"])
            mkt_df = mkt_df[(mkt_df["date"] >= regime_start) &
                            (mkt_df["date"] <= regime_end)]
            if len(mkt_df) < 50:
                return None
            mkt_df["market_return"] = np.log(mkt_df["close"]).diff()

            kr_df = con.execute(
                "SELECT period_date AS date, rate_pct AS key_rate_pct "
                "FROM v_key_rate_daily ORDER BY period_date"
            ).fetchdf()
            kr_df["date"] = pd.to_datetime(kr_df["date"])
            mkt_df = pd.merge_asof(
                mkt_df.sort_values("date"), kr_df.sort_values("date"),
                on="date", direction="backward"
            )
            df = pd.merge(mkt_df, news_df, on="date", how="inner")
            return df.sort_values("date").reset_index(drop=True) if len(df) >= 50 else None
        except Exception:
            return None
        finally:
            try:
                con.close()
            except Exception:
                pass

    log_fn(f"  Loading regime {regime_start}..{regime_end}")
    dfs = {}
    for inst in MARKET_INSTRUMENTS:
        q = (f"SELECT trade_date, close FROM v_train_market_data "
             f"WHERE instrument='{inst}' ORDER BY trade_date")
        dfs[inst] = _load_inst(q)
    for name, col in MOEX_SECTORS.items():
        q = (f"SELECT trade_date, {col} AS close FROM v_train_sectors "
             f"WHERE {col} IS NOT NULL ORDER BY trade_date")
        dfs[name] = _load_inst(q)

    n_ok = sum(1 for v in dfs.values() if v is not None)
    log_fn(f"  Regime loaded: {n_ok} instruments")
    return dfs


# ──────────────────────────────────────────────────────────────────────────────
# Search Knowledge — accumulated learning across rounds
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class SearchKnowledge:
    """
    Accumulated knowledge from all search rounds.

    Updated after every round. Used to generate smarter configs for the next.
    This is the learning/feedback mechanism — the system gets better with each round.
    """

    # Per-entity IC history (all rounds)
    instrument_ic:  dict = field(default_factory=dict)  # inst  -> [ic, ...]
    topic_ic:       dict = field(default_factory=dict)  # topic -> [ic, ...]
    lag_ic:         dict = field(default_factory=dict)  # lag   -> [ic, ...]
    feat_ic:        dict = field(default_factory=dict)  # feat  -> [ic, ...]

    # Signal stability: how many rounds each (inst, topic, lag) appeared
    signal_count:    dict = field(default_factory=dict)  # key -> count
    signal_val_pass: dict = field(default_factory=dict)  # key -> val_pass_count
    signal_best_ic:  dict = field(default_factory=dict)  # key -> max ic seen

    # Round-level stats for trend analysis
    round_train_hits: list = field(default_factory=list)
    round_val_hits:   list = field(default_factory=list)
    round_ic_means:   list = field(default_factory=list)

    # All hits (for stable signal deep-dive)
    all_hits_flat: list = field(default_factory=list)

    # Tried config IDs (to avoid exact repeats)
    tried_config_ids: set = field(default_factory=set)

    # Round counter
    rounds_done: int = 0

    def update(self, round_hits: list, val_hits: list, config_id: str) -> None:
        """Update knowledge from one config's results."""
        val_keys = {(h.instrument, h.topic, h.lag) for h in val_hits}
        train_hits = [h for h in round_hits if h.split == "train"]

        for h in train_hits:
            key = (h.instrument, h.topic, h.lag)
            feat = h.config_id.split("__")[0]

            self.instrument_ic.setdefault(h.instrument, []).append(h.m6_ic)
            self.topic_ic.setdefault(h.topic, []).append(h.m6_ic)
            self.lag_ic.setdefault(h.lag, []).append(h.m6_ic)
            self.feat_ic.setdefault(feat, []).append(h.m6_ic)

            self.signal_count[key] = self.signal_count.get(key, 0) + 1
            if key in val_keys:
                self.signal_val_pass[key] = self.signal_val_pass.get(key, 0) + 1
            self.signal_best_ic[key] = max(self.signal_best_ic.get(key, 0.0), h.m6_ic)

        self.all_hits_flat.extend(train_hits)
        self.tried_config_ids.add(config_id)

    def end_round(self, n_train: int, n_val: int, mean_ic: float) -> None:
        self.round_train_hits.append(n_train)
        self.round_val_hits.append(n_val)
        self.round_ic_means.append(mean_ic)
        self.rounds_done += 1

    # ─── derived stats ─────────────────────────────────────────────────────────

    @property
    def total_train(self) -> int:
        return sum(self.round_train_hits)

    @property
    def total_val(self) -> int:
        return sum(self.round_val_hits)

    @property
    def val_pass_rate(self) -> float:
        return self.total_val / max(self.total_train, 1)

    @property
    def ic_trend(self) -> str:
        """Is average IC improving, stable, or declining?"""
        if len(self.round_ic_means) < 3:
            return "unknown"
        recent = np.mean(self.round_ic_means[-3:])
        earlier = np.mean(self.round_ic_means[:-3]) if len(self.round_ic_means) > 3 else recent
        if recent > earlier * 1.05:
            return "improving"
        elif recent < earlier * 0.95:
            return "declining"
        return "stable"

    def top_instruments(self, n: int = 6) -> list:
        if not self.instrument_ic:
            return MARKET_INSTRUMENTS + list(MOEX_SECTORS.keys())
        scored = {k: np.mean(v) for k, v in self.instrument_ic.items()}
        return sorted(scored, key=scored.__getitem__, reverse=True)[:n]

    def top_topics(self, n: int = 4) -> list:
        from src.pipeline_v2.feature_transformer import KEYWORD_TOPICS
        if not self.topic_ic:
            return KEYWORD_TOPICS
        scored = {k: np.mean(v) for k, v in self.topic_ic.items()}
        return sorted(scored, key=scored.__getitem__, reverse=True)[:n]

    def best_feature_type(self) -> str:
        if not self.feat_ic:
            return "keyword_z90"
        # Prefer feature types with val-confirmed signals
        val_feat_ics: dict = {}
        for h in self.all_hits_flat:
            key = (h.instrument, h.topic, h.lag)
            if self.signal_val_pass.get(key, 0) > 0:
                feat = h.config_id.split("__")[0]
                val_feat_ics.setdefault(feat, []).append(h.m6_ic)
        if val_feat_ics:
            return max(val_feat_ics, key=lambda f: np.mean(val_feat_ics[f]))
        # Fallback: highest mean IC
        scored = {k: np.mean(v) for k, v in self.feat_ic.items()}
        return max(scored, key=scored.__getitem__)

    def best_lags(self, n: int = 4) -> list:
        if not self.lag_ic:
            return [1, 7, 14, 30]
        scored = {k: np.mean(v) for k, v in self.lag_ic.items()}
        top = sorted(scored, key=scored.__getitem__, reverse=True)[:n]
        return sorted(top)

    def stable_signals(self, min_count: int = 2) -> list:
        """Signals that appeared in multiple rounds — high stability."""
        return sorted(
            [(k, v) for k, v in self.signal_count.items() if v >= min_count],
            key=lambda x: (-x[1], -self.signal_best_ic.get(x[0], 0))
        )

    # ─── adaptive gate decisions ───────────────────────────────────────────────

    def adaptive_ic_gate(self) -> float:
        """
        Dynamically adjust IC threshold based on what we're finding.
        Core learning mechanism: too many signals → tighten, too few → loosen.
        """
        vpr = self.val_pass_rate
        recent_train = sum(self.round_train_hits[-3:]) if self.round_train_hits else 0

        # Lots of val-confirmed → we're finding real things, can be stricter
        if vpr > 0.50:
            return 0.08
        # Good val rate → tighten slightly
        if vpr > 0.30:
            return 0.05
        # Lots of train but no val → possible overfitting → tighten
        if recent_train > 150 and vpr < 0.05 and self.rounds_done >= 2:
            return 0.05
        # Finding almost nothing → loosen
        if recent_train < 10:
            return 0.01
        # Declining trend → loosen to explore more
        if self.ic_trend == "declining":
            return 0.01
        return 0.03

    def adaptive_ensemble(self) -> str:
        """
        Choose ensemble based on what pattern we see.
        If M6 finds lots but AND finds nothing → M5 is the bottleneck.
        Try M6-only strict to at least confirm predictive signals.
        """
        if self.total_train > 200 and self.total_val == 0 and self.rounds_done >= 2:
            return "M6_only"
        return "M5_AND_M6"

    def summary_lines(self) -> list:
        return [
            f"  Knowledge after {self.rounds_done} rounds:",
            f"    train_total={self.total_train}  val_total={self.total_val}  "
            f"val_pass_rate={self.val_pass_rate:.1%}",
            f"    stable_signals={len(self.stable_signals())}  "
            f"ic_trend={self.ic_trend}",
            f"    top_instruments={self.top_instruments(3)}",
            f"    top_topics={self.top_topics(3)}",
            f"    best_feat={self.best_feature_type()}  "
            f"→ next_ic_gate={self.adaptive_ic_gate():.3f}  "
            f"ensemble={self.adaptive_ensemble()}",
        ]


def generate_next_configs(knowledge: SearchKnowledge, round_num: int) -> list:
    """
    Generate configs for the next round using accumulated knowledge.
    This is the core of the Ouroboros loop:
      - knowledge tells us what worked → exploit
      - we also explore what we haven't tried yet
    """
    ic_gate  = knowledge.adaptive_ic_gate()
    ensemble = knowledge.adaptive_ensemble()
    best_feat = knowledge.best_feature_type()
    best_lags = knowledge.best_lags(n=4)
    top_insts = knowledge.top_instruments(n=min(8, 4 + round_num))
    top_topics = knowledge.top_topics(n=min(7, 3 + round_num))

    configs = []

    # ── 1. Primary: exploit best known combo ────────────────────────────────
    configs.append(SearchConfig(
        feature_type=best_feat,
        target="market_return",
        ensemble=ensemble,
        m5_p_max=0.05,
        m6_ic_min=ic_gate,
        m6_bootstrap_min=0.0,
        lags=sorted(set(best_lags + [1, 7, 30, 60, 90])),
        label=f"r{round_num}_primary_{best_feat}",
    ))

    # ── 2. Alternate feature types not yet tried ─────────────────────────────
    from src.pipeline_v2.feature_transformer import FEATURE_TYPES
    feat_scores = {f: np.mean(v) for f, v in knowledge.feat_ic.items()} if knowledge.feat_ic else {}
    tried_feats = {c.split("_primary_")[1] for c in knowledge.tried_config_ids
                   if "_primary_" in c}
    for feat in sorted(FEATURE_TYPES, key=lambda f: -feat_scores.get(f, 0)):
        if feat != best_feat and feat not in tried_feats:
            configs.append(SearchConfig(
                feature_type=feat,
                target="market_return",
                ensemble=ensemble,
                m5_p_max=0.05,
                m6_ic_min=ic_gate,
                m6_bootstrap_min=0.0,
                lags=best_lags,
                label=f"r{round_num}_alt_{feat}",
            ))
            if len(configs) >= 4:
                break

    # ── 3. Stable signals deep-dive ──────────────────────────────────────────
    stable = knowledge.stable_signals(min_count=2)
    for (inst, topic, lag), count in stable[:4]:
        # Explore neighboring lags around this stable signal
        neighbor_lags = sorted({max(1, lag - 7), lag, lag + 7, lag * 2})[:4]
        label = f"r{round_num}_stable_{inst[:6]}_{topic[:6]}"
        if label not in knowledge.tried_config_ids:
            configs.append(SearchConfig(
                feature_type=best_feat,
                target="market_return",
                ensemble="M5_AND_M6",  # always AND for stable signals
                m5_p_max=0.05,
                m6_ic_min=max(0.01, ic_gate - 0.02),
                m6_bootstrap_min=0.0,
                lags=neighbor_lags,
                label=label,
            ))

    # ── 4. If ensemble is AND and still 0 val → try M6-strict ───────────────
    if knowledge.total_val == 0 and knowledge.rounds_done >= 2:
        label = f"r{round_num}_m6strict_{best_feat}"
        if label not in knowledge.tried_config_ids:
            configs.append(SearchConfig(
                feature_type=best_feat,
                target="market_return",
                ensemble="M6_only",
                m5_p_max=0.05,
                m6_ic_min=max(0.05, ic_gate),
                m6_bootstrap_min=0.01,
                lags=LAGS_FULL,
                label=label,
            ))

    # ── 5. Regime: every 3rd round ───────────────────────────────────────────
    if round_num % 3 == 0:
        for (start, end, rlabel) in [
            ("2022-01-01", "2022-12-31", "2022"),
            ("2023-01-01", "2023-09-30", "2023"),
        ]:
            label = f"r{round_num}_regime{rlabel}_{best_feat}"
            if label not in knowledge.tried_config_ids:
                configs.append(SearchConfig(
                    feature_type=best_feat,
                    target="market_return",
                    ensemble=ensemble,
                    m5_p_max=0.05,
                    m6_ic_min=max(0.01, ic_gate - 0.01),
                    m6_bootstrap_min=0.0,
                    lags=best_lags,
                    label=label,
                ))

    # Deduplicate and exclude already tried
    seen: set = set()
    unique = []
    for c in configs:
        if c.label not in seen and c.label not in knowledge.tried_config_ids:
            seen.add(c.label)
            unique.append(c)

    return unique


# ──────────────────────────────────────────────────────────────────────────────
# Night Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

class NightOrchestrator:

    def __init__(self, phase_budget: float, phases_to_run: list[int],
                 log_fh, ts: str):
        self.phase_budget = phase_budget
        self.phases_to_run = phases_to_run
        self.log_fh = log_fh
        self.ts = ts
        self.all_results: list[PhaseResult] = []
        self.all_hits: list[SignalHit] = []

        # Load methods once
        self.m5 = importlib.import_module("analytics.testbed.methods.m5_var").build()
        self.m6 = importlib.import_module("analytics.testbed.methods.m6_lgbm").build()

    def log(self, msg: str) -> None:
        t = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = f"[{t}] {msg}"
        print(line)
        self.log_fh.write(line + "\n")
        self.log_fh.flush()

    def run_config(self, config: SearchConfig, train_dfs: dict,
                   val_dfs: dict, phase: int,
                   regime_label: str = "",
                   custom_train_dfs: dict | None = None) -> PhaseResult:
        """Run one config on train (and optionally val). Returns PhaseResult."""
        t0 = time.time()
        topics = FeatureTransformer.topic_columns(config.feature_type)
        instruments = MARKET_INSTRUMENTS + list(MOEX_SECTORS.keys())

        self.log(f"  Config: {config.label}")
        self.log(f"    feature={config.feature_type}  target={config.target}  "
                 f"ensemble={config.ensemble}  ic_min={config.m6_ic_min}")

        # Train scan
        use_train = custom_train_dfs if custom_train_dfs is not None else train_dfs
        train_hits = scan_split(
            use_train, config, self.m5, self.m6, instruments, topics,
            time_budget=self.phase_budget * 0.7,
            log_fn=self.log,
            regime_label=regime_label,
        )

        n_train = len(train_hits)
        self.log(f"    Train: {n_train} signals")

        # Val holdout on train-confirmed signals
        n_val = 0
        val_hits = []
        if n_train > 0 and val_dfs:
            self.log(f"    Val holdout ({n_train} candidates)...")
            for hit in train_hits:
                df_raw = val_dfs.get(hit.instrument)
                if df_raw is None:
                    continue
                try:
                    df = FeatureTransformer.apply(df_raw, config.feature_type,
                                                  config.target)
                except ValueError:
                    continue
                if hit.topic not in df.columns:
                    continue
                r = evaluate_hypothesis(df, hit.topic, config.target, hit.lag,
                                        config, self.m5, self.m6)
                if r.get("confirmed"):
                    n_val += 1
                    val_hits.append(SignalHit(
                        instrument=hit.instrument, topic=hit.topic, lag=hit.lag,
                        m5_pvalue=r["m5_pvalue"], m5_score=r["m5_score"],
                        m6_ic=r["m6_ic"],
                        m5_confirmed=r["m5_confirmed"],
                        m6_confirmed=r["m6_confirmed"],
                        ensemble_pass=True,
                        config_id=config.id,
                        split="val",
                        regime=regime_label,
                    ))
                    self.log(f"    VAL PASS  {hit.instrument:18s} {hit.topic:16s} "
                             f"lag={hit.lag:3d}  m6_ic={r['m6_ic']:.4f}")

            self.log(f"    Val: {n_val}/{n_train} held")

        all_ics = [h.m6_ic for h in train_hits] if train_hits else [0.0]
        result = PhaseResult(
            phase=phase,
            config=config,
            n_train_pass=n_train,
            n_val_pass=n_val,
            mean_ic=float(np.mean(all_ics)),
            max_ic=float(np.max(all_ics)),
            ic_above_05=sum(1 for ic in all_ics if ic >= 0.05),
            hits=train_hits + val_hits,
            elapsed_s=time.time() - t0,
        )
        self.all_results.append(result)
        self.all_hits.extend(train_hits + val_hits)
        return result

    # ─── phases ───────────────────────────────────────────────────────────────

    def run_phase1(self, train_dfs: dict, val_dfs: dict) -> list[PhaseResult]:
        self.log("=" * 60)
        self.log("PHASE 1: Feature sweep (M6-only, fast)")
        self.log("=" * 60)
        configs = phase1_configs()
        self.log(f"Configs: {len(configs)}")
        results = []
        for i, cfg in enumerate(configs):
            self.log(f"\n[{i+1}/{len(configs)}] {cfg.label}")
            r = self.run_config(cfg, train_dfs, val_dfs, phase=1)
            results.append(r)
            self.log(f"  Score: {r.score:.1f}  "
                     f"train={r.n_train_pass}  ic_max={r.max_ic:.4f}")
        return results

    def run_phase2(self, train_dfs: dict, val_dfs: dict,
                   phase1_results: list[PhaseResult]) -> list[PhaseResult]:
        self.log("\n" + "=" * 60)
        self.log("PHASE 2: Deep dive — top configs with M5+M6/AND")
        self.log("=" * 60)
        configs = phase2_configs(phase1_results)
        self.log(f"Configs (top Phase-1 winners): {len(configs)}")
        results = []
        for i, cfg in enumerate(configs):
            self.log(f"\n[{i+1}/{len(configs)}] {cfg.label}")
            r = self.run_config(cfg, train_dfs, val_dfs, phase=2)
            results.append(r)
            self.log(f"  Score: {r.score:.1f}  "
                     f"train={r.n_train_pass}  val={r.n_val_pass}")
        return results

    def run_phase3(self, val_dfs: dict,
                   phase1_results: list[PhaseResult],
                   phase2_results: list[PhaseResult]) -> list[PhaseResult]:
        self.log("\n" + "=" * 60)
        self.log("PHASE 3: Regime detection (2022 vs 2023)")
        self.log("=" * 60)

        # Best config = top score from phase2, or phase1 if phase2 empty
        pool = phase2_results or phase1_results
        if not pool:
            self.log("  No results to base regime detection on — skipping")
            return []
        best = max(pool, key=lambda r: r.score)
        self.log(f"  Base config: {best.config.label}")

        regime_cfgs = phase3_configs(best)
        results = []
        for (cfg, start, end, label) in regime_cfgs:
            self.log(f"\nRegime: {label}  ({start} .. {end})")
            regime_dfs = load_regime_split(start, end, self.log)
            r = self.run_config(cfg, {}, val_dfs, phase=3,
                                regime_label=label,
                                custom_train_dfs=regime_dfs)
            results.append(r)
            self.log(f"  {label}: {r.n_train_pass} signals, "
                     f"max_ic={r.max_ic:.4f}")
        return results

    # ─── report ───────────────────────────────────────────────────────────────

    def write_report(self) -> Path:
        self.log("\n" + "=" * 60)
        self.log("FINAL REPORT")
        self.log("=" * 60)

        # Rank all results by score
        ranked = sorted(self.all_results, key=lambda r: r.score, reverse=True)

        # Collect all train+val confirmed signals
        train_hits = [h for h in self.all_hits if h.split == "train"]
        val_hits   = [h for h in self.all_hits if h.split == "val"]

        lines = [
            "# Night Search Report",
            f"Run: {self.ts}",
            "",
            "## Summary by Phase",
            "",
            "| Phase | Config | Train | Val | Max IC | Score |",
            "|---|---|---|---|---|---|",
        ]
        for r in ranked:
            lines.append(
                f"| {r.phase} | {r.config.label} "
                f"| {r.n_train_pass} | {r.n_val_pass} "
                f"| {r.max_ic:.4f} | {r.score:.1f} |"
            )

        lines += ["", "## Best Configs Ranking", ""]
        if ranked:
            winner = ranked[0]
            lines.append(f"**Winner: {winner.config.label}**")
            lines.append(f"- Feature: `{winner.config.feature_type}`")
            lines.append(f"- Target: `{winner.config.target}`")
            lines.append(f"- Ensemble: `{winner.config.ensemble}`")
            lines.append(f"- IC_min: `{winner.config.m6_ic_min}`")
            lines.append(f"- Train signals: {winner.n_train_pass}")
            lines.append(f"- Val signals: {winner.n_val_pass}")
            lines.append(f"- Max IC: {winner.max_ic:.4f}")

        lines += ["", "## All Train-Confirmed Signals", ""]
        if train_hits:
            lines += [
                "| Instrument | Topic | Lag | M5 p-val | M6 IC | Val | Regime | Config |",
                "|---|---|---|---|---|---|---|---|",
            ]
            for h in sorted(train_hits, key=lambda x: -x.m6_ic):
                val_pass = any(
                    v.instrument == h.instrument and v.topic == h.topic
                    and v.lag == h.lag and v.split == "val"
                    for v in val_hits
                )
                lines.append(
                    f"| {h.instrument} | {h.topic} | {h.lag} "
                    f"| {h.m5_pvalue:.4f} | {h.m6_ic:.4f} "
                    f"| {'PASS' if val_pass else 'fail'} "
                    f"| {h.regime or 'full'} | {h.config_id} |"
                )
        else:
            lines.append("*(no confirmed train signals)*")

        lines += ["", "## Val-Confirmed (Train+Val) Signals", ""]
        double_confirmed = [
            h for h in train_hits
            if any(v.instrument == h.instrument and v.topic == h.topic
                   and v.lag == h.lag for v in val_hits)
        ]
        if double_confirmed:
            lines.append("**These passed both Train and Val — highest confidence:**")
            lines += [
                "", "| Instrument | Topic | Lag | M6 IC | Config |",
                "|---|---|---|---|---|",
            ]
            for h in double_confirmed:
                lines.append(f"| {h.instrument} | {h.topic} | {h.lag} "
                              f"| {h.m6_ic:.4f} | {h.config_id} |")
        else:
            lines.append("*(no signals passed both Train and Val)*")

        lines += [
            "",
            "## Feature Type Ranking (by total train signals)",
            "",
        ]
        feat_counts: dict[str, int] = {}
        for h in train_hits:
            ft = h.config_id.split("__")[0]
            feat_counts[ft] = feat_counts.get(ft, 0) + 1
        for ft, cnt in sorted(feat_counts.items(), key=lambda x: -x[1]):
            lines.append(f"- `{ft}`: {cnt} train signals")

        lines += [
            "",
            "## Recommendation for Next Session",
            "",
        ]
        if double_confirmed:
            lines.append("Production-grade signals found. Next: run full big_scanner "
                         "with winning config, then Phase B real-train run.")
        elif train_hits:
            lines.append("Train signals found but none held on Val. "
                         "Try: OR rule, expand data, or investigate regime stability.")
        else:
            lines.append("No signals found in any configuration. "
                         "Options: (A) event-based features, (B) accept weak signals, "
                         "(C) expand data coverage beyond 2022-2025.")

        md_path = OUT_DIR / f"night_report_{self.ts}.md"
        md_path.write_text("\n".join(lines), encoding="utf-8")
        self.log(f"Report: {md_path}")

        # Also save all hits as CSV
        if self.all_hits:
            csv_path = OUT_DIR / f"night_hits_{self.ts}.csv"
            pd.DataFrame([asdict(h) for h in self.all_hits]).to_csv(csv_path, index=False)
            self.log(f"Hits CSV: {csv_path}")

        return md_path

    def run(self, phases: list[int]) -> None:
        t_total = time.time()
        self.log(f"Night Search started — phases={phases}  "
                 f"budget={self.phase_budget/3600:.1f}h/phase")

        train_dfs, val_dfs, test_dfs = load_all_splits(self.log)

        p1_results, p2_results, p3_results = [], [], []

        if 1 in phases:
            p1_results = self.run_phase1(train_dfs, val_dfs)
            self._log_phase_summary(1, p1_results)

        if 2 in phases:
            p2_results = self.run_phase2(train_dfs, val_dfs, p1_results)
            self._log_phase_summary(2, p2_results)

        if 3 in phases:
            p3_results = self.run_phase3(val_dfs, p1_results, p2_results)
            self._log_phase_summary(3, p3_results)

        self.write_report()
        elapsed = (time.time() - t_total) / 3600
        self.log(f"Total elapsed: {elapsed:.2f}h")

    def _log_phase_summary(self, phase: int, results: list[PhaseResult]) -> None:
        self.log(f"\n--- Phase {phase} summary ---")
        total_train = sum(r.n_train_pass for r in results)
        total_val   = sum(r.n_val_pass   for r in results)
        best = max(results, key=lambda r: r.score) if results else None
        self.log(f"  Total Train signals: {total_train}")
        self.log(f"  Total Val signals:   {total_val}")
        if best:
            self.log(f"  Best config: {best.config.label}  "
                     f"(score={best.score:.1f}, ic_max={best.max_ic:.4f})")

    # ─── adaptive config generation ───────────────────────────────────────────

    def _adaptive_configs(self, round_num: int) -> list[SearchConfig]:
        """
        Generate next-round configs based on accumulated hits.

        Round 1: full sweep (standard Phase 1)
        Round 2: tighten IC, focus on best (feature, target, instrument) triples
        Round 3: AND ensemble on round-2 winners; vary window sizes
        Round 4+: vary lags, explore near-miss pairs from previous rounds
        """
        from src.pipeline_v2.feature_transformer import FEATURE_TYPES, TARGETS

        if round_num == 1:
            return phase1_configs()

        # Gather stats from accumulated hits
        train_hits = [h for h in self.all_hits if h.split == "train"]
        if not train_hits:
            self.log("  No hits yet — repeating wide sweep")
            return phase1_configs()

        import pandas as pd
        df_hits = pd.DataFrame([asdict(h) for h in train_hits])
        df_hits['feat'] = df_hits.config_id.str.split('__').str[0]
        df_hits['tgt']  = df_hits.config_id.str.split('__').str[1]

        # Top instruments by mean IC
        top_insts = (df_hits.groupby('instrument')['m6_ic']
                     .mean().nlargest(6).index.tolist())
        # Top topics by mean IC
        top_topics_raw = (df_hits.groupby('topic')['m6_ic']
                          .mean().nlargest(4).index.tolist())
        # Best (feat, tgt) combo
        best_feat_tgt = (df_hits.groupby(['feat','tgt'])['m6_ic']
                         .mean().idxmax())
        best_feat, best_tgt = best_feat_tgt

        self.log(f"  Adaptive round {round_num}: "
                 f"top_insts={top_insts[:3]}, best_feat={best_feat}, best_tgt={best_tgt}")

        configs: list[SearchConfig] = []

        if round_num == 2:
            # Stricter M6 (IC≥0.05), best feature types, focused instruments
            for feat in [best_feat, "keyword_z90", "embedding_z90"]:
                for tgt in [best_tgt, "market_return"]:
                    configs.append(SearchConfig(
                        feature_type=feat, target=tgt,
                        ensemble="M6_only",
                        m5_p_max=0.05, m6_ic_min=0.05,
                        m6_bootstrap_min=0.0,
                        lags=LAGS_FULL,
                        label=f"r2_m6strict_{feat}_{tgt}",
                    ))
            # AND ensemble on best combo
            configs.append(SearchConfig(
                feature_type=best_feat, target=best_tgt,
                ensemble="M5_AND_M6",
                m5_p_max=0.05, m6_ic_min=0.01,
                m6_bootstrap_min=0.0,   # disable bootstrap gate
                lags=LAGS_FULL,
                label=f"r2_and_{best_feat}_{best_tgt}",
            ))

        elif round_num == 3:
            # Focus on top instruments, ALL feature types × best target
            for feat in FEATURE_TYPES:
                configs.append(SearchConfig(
                    feature_type=feat, target=best_tgt,
                    ensemble="M5_AND_M6",
                    m5_p_max=0.05, m6_ic_min=0.01,
                    m6_bootstrap_min=0.0,
                    lags=LAGS_FULL,
                    label=f"r3_and_{feat}_{best_tgt}",
                ))
            # M6 with very strict IC (production quality)
            configs.append(SearchConfig(
                feature_type=best_feat, target=best_tgt,
                ensemble="M6_only",
                m5_p_max=0.05, m6_ic_min=0.08,
                m6_bootstrap_min=0.01,
                lags=LAGS_FULL,
                label=f"r3_m6prod_{best_feat}_{best_tgt}",
            ))

        else:
            # Round 4+: near-miss exploration — pairs where M5 p<0.1 OR M6 IC > 0.10
            near_m5 = df_hits[df_hits.m5_pvalue < 0.10]
            near_m6 = df_hits[df_hits.m6_ic > 0.10]
            near_all = pd.concat([near_m5, near_m6]).drop_duplicates(
                subset=['feat', 'tgt'])

            for _, row in near_all.iterrows():
                configs.append(SearchConfig(
                    feature_type=row['feat'], target=row['tgt'],
                    ensemble="M5_AND_M6",
                    m5_p_max=0.05, m6_ic_min=0.01,
                    m6_bootstrap_min=0.0,
                    lags=LAGS_FULL,
                    label=f"r{round_num}_nearmiss_{row['feat']}_{row['tgt']}",
                ))
                if len(configs) >= 6:
                    break

            if not configs:
                # Fallback: random perturbation of best config
                configs.append(SearchConfig(
                    feature_type=best_feat, target=best_tgt,
                    ensemble="M5_AND_M6",
                    m5_p_max=0.03,  # tighter
                    m6_ic_min=0.01,
                    m6_bootstrap_min=0.0,
                    lags=LAGS_FULL,
                    label=f"r{round_num}_tight_{best_feat}_{best_tgt}",
                ))

        # Deduplicate configs by label
        seen = set()
        unique_configs = []
        for c in configs:
            if c.label not in seen:
                seen.add(c.label)
                unique_configs.append(c)
        return unique_configs

    # ─── loop mode ────────────────────────────────────────────────────────────

    def _write_ledger(self, session_id: str, session_type: str,
                      t_start: float, knowledge: "SearchKnowledge") -> None:
        """Write session results to the scientific ledger."""
        try:
            from src.pipeline_v2.session_ledger import (
                log_session_end, check_session_integrity, generate_summary, Flag
            )
            train_hits = [h for h in self.all_hits if h.split == "train"]
            val_hits   = [h for h in self.all_hits if h.split == "val"]
            ic_vals    = [h.m6_ic for h in train_hits]
            val_dicts  = [
                {
                    "m6_ic_train": next(
                        (t.m6_ic for t in train_hits
                         if t.instrument == v.instrument
                         and t.topic == v.topic and t.lag == v.lag), 0.0
                    ),
                    "m6_ic_val":   v.m6_ic,
                    "sign_flip":   False,  # would need raw prediction to compute
                    "val_pass":    v.ensemble_pass,
                }
                for v in val_hits
            ]

            n_hyp = sum(
                len(r.config.lags) * len(
                    FeatureTransformer.topic_columns(r.config.feature_type)
                ) * len(MARKET_INSTRUMENTS + list(MOEX_SECTORS.keys()))
                for r in self.all_results
            )

            flags, enrich = check_session_integrity(
                n_hypotheses=n_hyp,
                n_train=len(train_hits),
                n_val=len(val_hits),
                ic_values_train=ic_vals,
                val_results=val_dicts,
                regime_labels=[h.regime for h in train_hits],
            )

            stats = {
                "hypotheses_tested":  n_hyp,
                "train_signals":      len(train_hits),
                "val_confirmed":      len(val_hits),
                "mean_ic_train":      round(float(np.mean(ic_vals)), 4) if ic_vals else 0.0,
                "max_ic_train":       round(float(np.max(ic_vals)), 4)  if ic_vals else 0.0,
                "duration_hours":     round((time.time() - t_start) / 3600, 2),
                "enrichment_factor":  enrich["enrichment_factor"],
                "expected_by_chance": enrich["expected_by_chance"],
                "val_rate":           enrich["val_rate"],
            }

            log_session_end(session_id, stats, flags)

            # Print integrity summary
            self.log("\n" + "=" * 60)
            self.log("INTEGRITY CHECK")
            self.log("=" * 60)
            fail_n = sum(1 for f in flags if f["level"] == Flag.FAIL)
            warn_n = sum(1 for f in flags if f["level"] == Flag.WARN)
            self.log(f"  Enrichment vs random: {enrich['enrichment_factor']:.2f}x "
                     f"(tested={n_hyp}, expected_fp={enrich['expected_by_chance']:.1f}, "
                     f"actual={len(train_hits)})")
            self.log(f"  FAIL flags: {fail_n}  WARN flags: {warn_n}")
            for f in flags:
                if f["level"] in (Flag.FAIL, Flag.WARN):
                    self.log(f"  [{f['level']}] {f['code']}: {f['message'][:100]}")

            generate_summary()
            self.log(f"  Ledger updated.")
        except Exception as e:
            self.log(f"[ledger] Error writing ledger: {e}")

    def run_loop(self, max_hours: float, phases: list[int],
                 seed_knowledge: "SearchKnowledge | None" = None) -> None:
        """
        Ouroboros loop — self-improving signal search.

        Each round:
          1. Generate configs using SearchKnowledge (learned from all previous rounds)
          2. Run scan, collect hits
          3. Update knowledge (which instruments/topics/lags/features are strongest)
          4. Knowledge adapts: IC gate, ensemble choice, instrument focus
          5. Repeat → each round is smarter than the last

        seed_knowledge: pre-seeded from inter_session_analyzer.
          Allows knowledge to carry over across separate night/day sessions.

        The loop continues until max_hours is exhausted.
        Interim report written after every config — safe to interrupt.
        """
        t_start = time.time()
        knowledge = seed_knowledge if seed_knowledge is not None else SearchKnowledge()
        if seed_knowledge is not None:
            self.log(f"[session] Starting with pre-seeded knowledge: "
                     f"rounds={knowledge.rounds_done}  "
                     f"stable={len(knowledge.stable_signals())}  "
                     f"ic_gate→{knowledge.adaptive_ic_gate():.3f}  "
                     f"ensemble→{knowledge.adaptive_ensemble()}")
        train_dfs, val_dfs, _ = load_all_splits(self.log)

        self.log(f"OUROBOROS LOOP: max={max_hours:.1f}h")
        self.log("Each round learns from the previous. Gates adapt. Focus narrows.")

        round_num = 0

        while True:
            elapsed_h = (time.time() - t_start) / 3600
            remaining_h = max_hours - elapsed_h
            if remaining_h < 0.08:   # stop with 5 min to spare for final report
                self.log(f"Time budget exhausted ({elapsed_h:.2f}h).")
                break

            round_num += 1
            self.log(f"\n{'='*60}")
            self.log(f"ROUND {round_num}  |  elapsed={elapsed_h:.2f}h  "
                     f"remaining={remaining_h:.2f}h")
            self.log(f"{'='*60}")

            # ── Print what we know so far ──────────────────────────────────
            if knowledge.rounds_done > 0:
                for line in knowledge.summary_lines():
                    self.log(line)

            # ── Generate configs for this round ───────────────────────────
            if round_num == 1:
                # First round: always a full sweep
                configs = phase1_configs()
            else:
                configs = generate_next_configs(knowledge, round_num)

            self.log(f"Configs this round: {len(configs)}")
            if not configs:
                self.log("No new configs to try. Loop complete.")
                break

            # ── Run each config ────────────────────────────────────────────
            round_train = round_val = 0
            round_ics: list[float] = []

            for i, cfg in enumerate(configs):
                remaining_now = max_hours - (time.time() - t_start) / 3600
                if remaining_now < 0.08:
                    break

                # Budget: split remaining time equally among remaining configs
                n_left = len(configs) - i
                per_cfg_budget = max(
                    300.0,  # min 5 min per config
                    (remaining_now * 3600) / n_left
                )

                self.log(f"\n  [{i+1}/{len(configs)}] {cfg.label}  "
                         f"budget={per_cfg_budget/60:.0f}min  "
                         f"ic_gate={cfg.m6_ic_min}  ens={cfg.ensemble}")

                orig_budget = self.phase_budget
                self.phase_budget = per_cfg_budget

                # Handle regime configs (label contains "regime2022"/"regime2023")
                custom_train = None
                if "regime2022" in cfg.label:
                    custom_train = load_regime_split(
                        "2022-01-01", "2022-12-31", self.log)
                elif "regime2023" in cfg.label:
                    custom_train = load_regime_split(
                        "2023-01-01", "2023-09-30", self.log)

                r = self.run_config(cfg, train_dfs, val_dfs, phase=round_num,
                                    custom_train_dfs=custom_train)
                self.phase_budget = orig_budget

                # Update knowledge from this config's results
                train_hits = [h for h in r.hits if h.split == "train"]
                val_hits   = [h for h in r.hits if h.split == "val"]
                knowledge.update(train_hits + val_hits, val_hits, cfg.id)

                round_train += r.n_train_pass
                round_val   += r.n_val_pass
                round_ics.extend(h.m6_ic for h in train_hits)

                self.log(f"  → train={r.n_train_pass}  val={r.n_val_pass}  "
                         f"max_ic={r.max_ic:.4f}")

                # Interim report after each config
                self.write_report()

            # ── End of round ──────────────────────────────────────────────
            mean_ic = float(np.mean(round_ics)) if round_ics else 0.0
            knowledge.end_round(round_train, round_val, mean_ic)

            self.log(f"\n{'─'*40}")
            self.log(f"Round {round_num} done: "
                     f"train={round_train}  val={round_val}  mean_ic={mean_ic:.4f}")
            self.log(f"Cumulative: train={knowledge.total_train}  "
                     f"val={knowledge.total_val}  "
                     f"val_rate={knowledge.val_pass_rate:.1%}  "
                     f"stable={len(knowledge.stable_signals())}")

        # Final report
        self.write_report()
        elapsed_h = (time.time() - t_start) / 3600
        self.log(f"\nOuroboros complete: {round_num} rounds in {elapsed_h:.2f}h  "
                 f"total_train={knowledge.total_train}  "
                 f"total_val={knowledge.total_val}  "
                 f"stable_signals={len(knowledge.stable_signals())}")

        # Write to scientific ledger
        self._write_ledger(self.ts, "loop", t_start, knowledge)

    def run_rolling_loop(self, max_hours: float) -> None:
        """
        Rolling window Ouroboros — learn from RECENT data.

        Instead of fixed Train 2022-2023, uses rolling 12-month windows
        starting from the most recent data. Each window is trained with the
        Ouroboros approach. Signals stable across multiple windows are the
        most reliable.

        Window sequence (most recent first, step backward by 6 months):
          window 1: 2024-05-01 → 2025-04-30  (latest 12m, val=2025-05)
          window 2: 2023-11-01 → 2024-10-31  (12m offset by 6m)
          window 3: 2023-05-01 → 2024-04-30  (12m offset by 12m)
          window 4: 2022-11-01 → 2023-10-31  (12m offset by 18m)
        """
        t_start = time.time()
        knowledge = SearchKnowledge()  # shared across all windows

        # Windows: (train_start, train_end, val_end) — val = next 2 months
        windows = [
            ("2024-05-01", "2025-04-30", "2025-06-30", "window1_recent"),
            ("2023-11-01", "2024-10-31", "2024-12-31", "window2_2024"),
            ("2023-05-01", "2024-04-30", "2024-06-30", "window3_2023-24"),
            ("2022-11-01", "2023-10-31", "2023-12-31", "window4_2022-23"),
        ]

        self.log(f"ROLLING WINDOW OUROBOROS: {len(windows)} windows, max={max_hours:.1f}h")
        self.log("Shared knowledge accumulates across all windows.")
        self.log("Signals stable in 2+ windows = highest confidence.")

        window_signal_counts: dict[tuple, list] = {}  # signal -> [window_labels where found]

        for win_idx, (wstart, wend, vend, wlabel) in enumerate(windows):
            elapsed_h = (time.time() - t_start) / 3600
            if elapsed_h >= max_hours - 0.1:
                self.log(f"Time budget reached after {win_idx} windows.")
                break

            remaining_h = max_hours - elapsed_h
            window_budget_h = remaining_h / max(1, len(windows) - win_idx)

            self.log(f"\n{'='*60}")
            self.log(f"WINDOW {win_idx+1}/{len(windows)}: {wlabel}")
            self.log(f"  Train: {wstart} → {wend}  |  Val: {wend} → {vend}")
            self.log(f"  Budget: {window_budget_h:.1f}h  |  Elapsed: {elapsed_h:.2f}h")
            self.log(f"{'='*60}")

            # Load this window's data
            self.log("  Loading train window...")
            train_w = load_arbitrary_window(wstart, wend, self.log)
            self.log("  Loading val window...")
            val_w   = load_arbitrary_window(wend,  vend,  self.log)

            # Run Ouroboros rounds within this window's budget
            round_num = 0
            t_window = time.time()

            while True:
                elapsed_window = (time.time() - t_window) / 3600
                elapsed_total  = (time.time() - t_start)  / 3600
                if elapsed_window >= window_budget_h or elapsed_total >= max_hours - 0.08:
                    break

                round_num += 1
                self.log(f"\n  [{wlabel}] Round {round_num}  "
                         f"elapsed_win={elapsed_window:.2f}h")

                if knowledge.rounds_done > 0:
                    for line in knowledge.summary_lines():
                        self.log(line)

                configs = phase1_configs() if round_num == 1 else \
                          generate_next_configs(knowledge, round_num)

                if not configs:
                    self.log("  No new configs. Window done.")
                    break

                round_train = round_val = 0
                round_ics: list[float] = []

                for i, cfg in enumerate(configs):
                    t_remaining = (max_hours - (time.time()-t_start)/3600) * 3600
                    if t_remaining < 300:
                        break
                    n_left = len(configs) - i
                    per_cfg = max(180.0, t_remaining / max(n_left, 1))

                    orig_b = self.phase_budget
                    self.phase_budget = per_cfg
                    r = self.run_config(cfg, train_w, val_w, phase=round_num)
                    self.phase_budget = orig_b

                    train_hits = [h for h in r.hits if h.split == "train"]
                    val_hits   = [h for h in r.hits if h.split == "val"]
                    knowledge.update(train_hits + val_hits, val_hits, cfg.id)

                    # Track which windows each signal appeared in
                    val_keys = {(h.instrument, h.topic, h.lag) for h in val_hits}
                    for h in train_hits:
                        key = (h.instrument, h.topic, h.lag)
                        if key in val_keys:
                            if key not in window_signal_counts:
                                window_signal_counts[key] = []
                            if wlabel not in window_signal_counts[key]:
                                window_signal_counts[key].append(wlabel)

                    round_train += r.n_train_pass
                    round_val   += r.n_val_pass
                    round_ics.extend(h.m6_ic for h in train_hits)
                    self.write_report()

                mean_ic = float(np.mean(round_ics)) if round_ics else 0.0
                knowledge.end_round(round_train, round_val, mean_ic)

                self.log(f"  [{wlabel}] Round {round_num}: "
                         f"train={round_train} val={round_val} ic={mean_ic:.4f}")

        # ── Cross-window stability report ──────────────────────────────────────
        self.log(f"\n{'='*60}")
        self.log("CROSS-WINDOW STABILITY")
        self.log(f"{'='*60}")

        # Signals found in 2+ windows
        multi_window = {k: v for k, v in window_signal_counts.items() if len(v) >= 2}
        multi_sorted = sorted(multi_window.items(), key=lambda x: -len(x[1]))

        self.log(f"Signals stable in 2+ windows: {len(multi_window)}")
        for (inst, topic, lag), wins in multi_sorted[:20]:
            best_ic = self.knowledge_best_ic(inst, topic, lag)
            self.log(f"  {inst:18s} {topic:16s} lag={lag:2d}  "
                     f"windows={len(wins)} {wins}  best_ic={best_ic:.4f}")

        # Save stability data to CSV
        rows = []
        for (inst, topic, lag), wins in multi_sorted:
            rows.append({
                "instrument": inst, "topic": topic, "lag": lag,
                "n_windows": len(wins), "windows": "|".join(wins),
                "best_ic": self.knowledge_best_ic(inst, topic, lag),
            })
        if rows:
            stab_path = OUT_DIR / f"rolling_stable_{self.ts}.csv"
            pd.DataFrame(rows).to_csv(stab_path, index=False)
            self.log(f"Stability CSV: {stab_path}")

        self.write_report()
        elapsed_h = (time.time() - t_start) / 3600
        self.log(f"\nRolling Ouroboros complete: {elapsed_h:.2f}h  "
                 f"stable_across_windows={len(multi_window)}")

    def knowledge_best_ic(self, inst: str, topic: str, lag: int) -> float:
        """Get best IC ever seen for this signal across all hits."""
        key = (inst, topic, lag)
        return next(
            (h.m6_ic for h in sorted(self.all_hits, key=lambda x: -x.m6_ic)
             if h.instrument == inst and h.topic == topic and h.lag == lag),
            0.0
        )


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def load_session_knowledge(config_path: str) -> "SearchKnowledge | None":
    """
    Pre-seed SearchKnowledge from inter-session config JSON.
    Allows the system to start each session smarter than the last.
    """
    import json
    p = Path(config_path)
    if not p.exists():
        return None
    try:
        cfg = json.loads(p.read_text(encoding="utf-8"))
        k = SearchKnowledge()

        # Pre-fill IC history from stable signals
        for sig in cfg.get("stable_signals", []):
            inst  = sig["instrument"]
            topic = sig["topic"]
            lag   = sig["lag"]
            ic    = sig.get("best_ic", 0.05)
            n_s   = sig.get("n_sessions", 1)
            n_v   = int(sig.get("n_val", 0))
            key   = (inst, topic, lag)

            k.instrument_ic.setdefault(inst, []).extend([ic] * n_s)
            k.topic_ic.setdefault(topic, []).extend([ic] * n_s)
            k.lag_ic.setdefault(lag, []).extend([ic] * n_s)
            k.signal_count[key]    = n_s
            k.signal_val_pass[key] = n_v
            k.signal_best_ic[key]  = ic

        # Pre-fill instrument/topic IC from top lists
        for inst in cfg.get("top_instruments", []):
            k.instrument_ic.setdefault(inst, []).append(0.04)
        for topic in cfg.get("top_topics", []):
            k.topic_ic.setdefault(topic, []).append(0.04)

        # Pre-fill feature IC
        best_feat = cfg.get("best_feature", "keyword_z90")
        k.feat_ic[best_feat] = [cfg.get("ic_gate", 0.03) * 2]

        # Simulate N rounds so adaptive logic kicks in correctly
        sessions = cfg.get("sessions_analyzed", 0)
        val_rate = cfg.get("val_pass_rate", 0.0)
        for _ in range(min(sessions, 5)):
            k.end_round(
                n_train=max(1, int(cfg.get("n_train_total", 0) / max(sessions, 1))),
                n_val=max(0, int(cfg.get("n_val_total", 0) / max(sessions, 1))),
                mean_ic=cfg.get("ic_gate", 0.03),
            )

        return k
    except Exception as e:
        print(f"[session_config] Could not load {config_path}: {e}")
        return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Adaptive overnight signal search")
    parser.add_argument(
        "--phase-budget", type=int, default=DEFAULT_PHASE_BUDGET,
        help="Max seconds per phase in single-run mode (default: 7200 = 2h)")
    parser.add_argument(
        "--phases", type=str, default="1,2,3",
        help="Comma-separated phases for single-run mode (default: 1,2,3)")
    parser.add_argument(
        "--loop-hours", type=float, default=0.0,
        help="Adaptive loop for N hours (each round learns from previous)")
    parser.add_argument(
        "--rolling", type=float, default=0.0,
        help="Rolling window Ouroboros for N hours. Uses 4 sequential 12-month "
             "windows (most recent first). Finds signals stable across windows.")
    parser.add_argument(
        "--session-config", type=str, default="",
        help="Path to session_config.json from inter_session_analyzer. "
             "Pre-seeds SearchKnowledge so each session starts smarter.")
    args = parser.parse_args()

    import logging
    logging.basicConfig(level=logging.WARNING, format="%(message)s")

    # Load inter-session knowledge if available
    session_knowledge = None
    cfg_path = args.session_config or str(
        ROOT / "analytics" / "phase_b" / "session_config.json"
    )
    if Path(cfg_path).exists():
        session_knowledge = load_session_knowledge(cfg_path)
        if session_knowledge:
            print(f"[session_config] Loaded accumulated knowledge from {cfg_path}")
            print(f"  stable_signals={len(session_knowledge.stable_signals())}  "
                  f"rounds_simulated={session_knowledge.rounds_done}")
        else:
            print(f"[session_config] Could not load {cfg_path} — starting fresh")
    else:
        print("[session_config] No session_config.json found — starting fresh")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = OUT_DIR / f"night_search_{ts}.log"

    with open(log_path, "w", encoding="utf-8", buffering=1) as log_fh:
        orchestrator = NightOrchestrator(
            phase_budget=args.phase_budget,
            phases_to_run=[],
            log_fh=log_fh,
            ts=ts,
        )
        if args.rolling > 0:
            orchestrator.run_rolling_loop(max_hours=args.rolling)
        elif args.loop_hours > 0:
            orchestrator.run_loop(
                max_hours=args.loop_hours,
                phases=[1, 2, 3],
                seed_knowledge=session_knowledge,
            )
        else:
            phases = [int(p.strip()) for p in args.phases.split(",")]
            orchestrator.run(phases)

    print(f"\nLog: {log_path}")
    print(f"Results: {OUT_DIR}")


if __name__ == "__main__":
    main()
