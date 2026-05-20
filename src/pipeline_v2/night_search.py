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

    def run_loop(self, max_hours: float, phases: list[int]) -> None:
        """
        Run multiple rounds until max_hours is exhausted.
        Each round adapts configs based on findings of previous rounds.
        """
        t_start = time.time()
        round_num = 0
        train_dfs, val_dfs, _ = load_all_splits(self.log)

        self.log(f"LOOP MODE: max={max_hours:.1f}h  phases={phases}")

        while True:
            elapsed_h = (time.time() - t_start) / 3600
            remaining_h = max_hours - elapsed_h
            if remaining_h < 0.1:
                self.log(f"Time budget exhausted ({elapsed_h:.2f}h). Stopping.")
                break

            round_num += 1
            self.log(f"\n{'='*60}")
            self.log(f"ROUND {round_num}  |  elapsed={elapsed_h:.2f}h  "
                     f"remaining={remaining_h:.2f}h")
            self.log(f"{'='*60}")

            # Generate adaptive configs for this round
            configs = self._adaptive_configs(round_num)
            self.log(f"Configs for round {round_num}: {len(configs)}")

            round_results = []
            for i, cfg in enumerate(configs):
                # Per-config time budget = remaining / configs left (min 10 min)
                per_cfg_budget = max(
                    600.0,
                    (max_hours * 3600 - (time.time() - t_start)) / max(len(configs) - i, 1)
                )
                self.log(f"\n[{i+1}/{len(configs)}] {cfg.label}  "
                         f"(budget={per_cfg_budget/60:.0f}min)")

                # Temporarily override phase_budget
                orig_budget = self.phase_budget
                self.phase_budget = per_cfg_budget

                r = self.run_config(cfg, train_dfs, val_dfs, phase=round_num)
                round_results.append(r)
                self.phase_budget = orig_budget

                self.log(f"  → train={r.n_train_pass}  val={r.n_val_pass}  "
                         f"max_ic={r.max_ic:.4f}  score={r.score:.1f}")

                # Write interim report after each config
                self.write_report()

                # Check time
                if (time.time() - t_start) / 3600 >= max_hours:
                    break

            # Summary for this round
            best = max(round_results, key=lambda r: r.score) if round_results else None
            total_hits = sum(r.n_train_pass for r in round_results)
            max_ic_str = f"{best.max_ic:.4f}" if best else "0.0000"
            best_label = best.config.label if best else "none"
            self.log(f"\n--- Round {round_num} done: {total_hits} train signals  "
                     f"best={best_label}  max_ic={max_ic_str}")

        # Final report
        self.write_report()
        elapsed_h = (time.time() - t_start) / 3600
        self.log(f"\nLoop ended after {round_num} rounds, {elapsed_h:.2f}h")


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

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
        help="Run in adaptive loop mode for N hours (e.g. --loop-hours 8). "
             "Each round adapts configs based on findings. Overrides --phases.")
    args = parser.parse_args()

    import logging
    logging.basicConfig(level=logging.WARNING, format="%(message)s")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = OUT_DIR / f"night_search_{ts}.log"

    with open(log_path, "w", encoding="utf-8", buffering=1) as log_fh:
        orchestrator = NightOrchestrator(
            phase_budget=args.phase_budget,
            phases_to_run=[],
            log_fh=log_fh,
            ts=ts,
        )
        if args.loop_hours > 0:
            orchestrator.run_loop(max_hours=args.loop_hours, phases=[1, 2, 3])
        else:
            phases = [int(p.strip()) for p in args.phases.split(",")]
            orchestrator.run(phases)

    print(f"\nLog: {log_path}")
    print(f"Results: {OUT_DIR}")


if __name__ == "__main__":
    main()
