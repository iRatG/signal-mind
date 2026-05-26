"""Inter-session Analyzer — Ouroboros на уровне сессий.

Читает все past CSV из analytics/phase_b/night_search/night_hits_*.csv,
агрегирует знания, пишет session_config.json + analysis report.

Каждый раз когда запускается — учитывает ВСЁ что было до этого.
Config становится умнее с каждой сессией.

Usage:
    .venv/Scripts/python -m src.pipeline_v2.inter_session_analyzer
"""
from __future__ import annotations

import json
import sys
import io
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

ROOT     = Path(__file__).resolve().parents[2]
HITS_DIR = ROOT / "analytics" / "phase_b" / "night_search"
OUT_DIR  = ROOT / "analytics" / "phase_b" / "analysis"
CONFIG_PATH = ROOT / "analytics" / "phase_b" / "session_config.json"
OUT_DIR.mkdir(parents=True, exist_ok=True)

FEATURE_TYPES = ["keyword_raw", "keyword_z90", "keyword_z30",
                 "embedding_raw", "embedding_z90"]
ALL_INSTRUMENTS_DEFAULT = [
    "SP500", "BRENT", "GOLD", "SILVER", "COPPER", "NATGAS", "WHEAT",
    "USDRUB", "EURUSD", "CNYRUB",
    "IMOEX", "RGBI",
    "MOEXOG", "MOEXMM", "MOEXFN", "MOEXIT",
]
ALL_TOPICS_DEFAULT = ["oil", "rate", "ruble", "sanctions",
                      "inflation", "banking", "gold"]
ALL_LAGS_DEFAULT   = [1, 7, 14, 30, 60, 90]


# ──────────────────────────────────────────────────────────────────────────────
# Load all past hits
# ──────────────────────────────────────────────────────────────────────────────

def load_all_hits() -> pd.DataFrame:
    """Concat all night_hits_*.csv into one DataFrame."""
    files = sorted(HITS_DIR.glob("night_hits_*.csv"))
    if not files:
        return pd.DataFrame()

    parts = []
    for f in files:
        try:
            df = pd.read_csv(f)
            # Extract session timestamp from filename
            ts_str = f.stem.replace("night_hits_", "")
            df["session"] = ts_str
            parts.append(df)
        except Exception:
            pass

    if not parts:
        return pd.DataFrame()

    all_hits = pd.concat(parts, ignore_index=True)

    # Normalize: extract feature type from config_id
    if "config_id" in all_hits.columns:
        all_hits["feat"] = all_hits["config_id"].str.split("__").str[0]
    else:
        all_hits["feat"] = "unknown"

    return all_hits


# ──────────────────────────────────────────────────────────────────────────────
# Analysis logic
# ──────────────────────────────────────────────────────────────────────────────

def analyze(all_hits: pd.DataFrame) -> dict:
    """
    Derive adaptive config from accumulated hits.
    Returns dict with recommendations for next session.
    """
    if all_hits.empty:
        return _default_config("no_data")

    train_hits = all_hits[all_hits["split"] == "train"].copy()
    val_hits   = all_hits[all_hits["split"] == "val"].copy()

    n_sessions = all_hits["session"].nunique()
    n_train    = len(train_hits)
    n_val      = len(val_hits)
    val_rate   = n_val / max(n_train, 1)

    # ── Per-entity IC stats ──────────────────────────────────────────────────

    def top_by_ic(df: pd.DataFrame, col: str, n: int, default: list) -> list:
        if df.empty or col not in df.columns:
            return default
        ranked = df.groupby(col)["m6_ic"].mean().nlargest(n)
        return ranked.index.tolist()

    top_instruments = top_by_ic(train_hits, "instrument", 8, ALL_INSTRUMENTS_DEFAULT)
    top_topics      = top_by_ic(train_hits, "topic",      5, ALL_TOPICS_DEFAULT)
    best_feat       = (
        train_hits.groupby("feat")["m6_ic"].mean().idxmax()
        if not train_hits.empty and "feat" in train_hits.columns
        else "keyword_z90"
    )

    # Best lags (top-4 by mean IC)
    if not train_hits.empty and "lag" in train_hits.columns:
        lag_ic = train_hits.groupby("lag")["m6_ic"].mean().nlargest(4)
        best_lags = sorted(lag_ic.index.tolist())
    else:
        best_lags = [1, 7, 14, 30]

    # ── Adaptive IC gate ─────────────────────────────────────────────────────
    # Too many train but no val → tighten  (overfitting)
    # Too few train → loosen  (missing real signals)
    # Good val rate → keep or tighten
    if val_rate > 0.40:
        ic_gate  = 0.08
        ensemble = "M5_AND_M6"
        reason   = "high val_rate → tighten"
    elif val_rate > 0.20:
        ic_gate  = 0.05
        ensemble = "M5_AND_M6"
        reason   = "moderate val_rate → standard"
    elif n_train > 200 and val_rate < 0.05:
        ic_gate  = 0.05
        ensemble = "M6_only"
        reason   = "M5 bottleneck — switching to M6_only"
    elif n_train < 20:
        ic_gate  = 0.01
        ensemble = "M5_AND_M6"
        reason   = "too few train signals → loosen IC"
    else:
        ic_gate  = 0.03
        ensemble = "M5_AND_M6"
        reason   = "default"

    # ── Stable signals (appeared in 2+ sessions) ──────────────────────────────
    if not train_hits.empty:
        signal_counts = (
            train_hits.groupby(["instrument", "topic", "lag"])["session"]
            .nunique()
            .reset_index()
            .rename(columns={"session": "n_sessions"})
        )
        # Val-confirmed stability
        if not val_hits.empty:
            val_counts = (
                val_hits.groupby(["instrument", "topic", "lag"])
                .size()
                .reset_index(name="n_val")
            )
            signal_counts = signal_counts.merge(
                val_counts, on=["instrument", "topic", "lag"], how="left"
            ).fillna({"n_val": 0})
        else:
            signal_counts["n_val"] = 0

        # Best IC per signal
        best_ic = (
            train_hits.groupby(["instrument", "topic", "lag"])["m6_ic"]
            .max()
            .reset_index()
            .rename(columns={"m6_ic": "best_ic"})
        )
        signal_counts = signal_counts.merge(best_ic, on=["instrument", "topic", "lag"])
        stable = signal_counts[signal_counts["n_sessions"] >= 2].sort_values(
            ["n_sessions", "best_ic"], ascending=False
        )
        stable_signals = stable.head(10).to_dict("records")
    else:
        stable_signals = []

    # ── Anti-patterns: (inst, topic, lag) that appear in train 3+ times but never val ─
    deprioritize = []
    if not train_hits.empty:
        val_sig_keys: set = set()
        if not val_hits.empty:
            val_sig_keys = set(
                zip(val_hits["instrument"], val_hits["topic"], val_hits["lag"])
            )
        # Count train appearances per signal
        train_counts = (
            train_hits.groupby(["instrument", "topic", "lag"])
            .size()
            .reset_index(name="n_train")
        )
        # Keep only those never confirmed on val AND appeared 3+ times
        chronic = train_counts[
            train_counts.apply(
                lambda r: (r["instrument"], r["topic"], r["lag"]) not in val_sig_keys,
                axis=1
            ) & (train_counts["n_train"] >= 3)
        ]
        deprioritize = [
            {"instrument": str(r["instrument"]),
             "topic":      str(r["topic"]),
             "lag":        int(r["lag"])}
            for _, r in chronic.iterrows()
        ][:20]

    # ── Session trend ─────────────────────────────────────────────────────────
    sess_stats = (
        train_hits.groupby("session")["m6_ic"]
        .agg(["mean", "count"])
        .reset_index()
    ) if not train_hits.empty else pd.DataFrame()

    ic_trend = "unknown"
    if len(sess_stats) >= 3:
        recent  = sess_stats["mean"].iloc[-2:].mean()
        earlier = sess_stats["mean"].iloc[:-2].mean()
        if recent > earlier * 1.05:
            ic_trend = "improving"
        elif recent < earlier * 0.95:
            ic_trend = "declining"
        else:
            ic_trend = "stable"

    return {
        "generated_at":       datetime.now(timezone.utc).isoformat(),
        "sessions_analyzed":  n_sessions,
        "n_train_total":      n_train,
        "n_val_total":        n_val,
        "val_pass_rate":      round(val_rate, 4),
        "ic_trend":           ic_trend,

        # Adaptive search params
        "ic_gate":            ic_gate,
        "ensemble":           ensemble,
        "ic_gate_reason":     reason,

        # Focus lists (ordered by performance)
        "top_instruments":    top_instruments,
        "top_topics":         top_topics,
        "best_feature":       best_feat,
        "best_lags":          best_lags,

        # Accumulated intelligence
        "stable_signals":     stable_signals,
        "deprioritize":       deprioritize,
    }


def _default_config(reason: str) -> dict:
    return {
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "sessions_analyzed": 0,
        "n_train_total":     0,
        "n_val_total":       0,
        "val_pass_rate":     0.0,
        "ic_trend":          "unknown",
        "ic_gate":           0.03,
        "ensemble":          "M5_AND_M6",
        "ic_gate_reason":    reason,
        "top_instruments":   ALL_INSTRUMENTS_DEFAULT,
        "top_topics":        ALL_TOPICS_DEFAULT,
        "best_feature":      "keyword_z90",
        "best_lags":         [1, 7, 14, 30],
        "stable_signals":    [],
        "deprioritize":      [],
    }


# ──────────────────────────────────────────────────────────────────────────────
# Report generation
# ──────────────────────────────────────────────────────────────────────────────

def write_report(cfg: dict, all_hits: pd.DataFrame) -> Path:
    ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    md  = OUT_DIR / f"analysis_{ts}.md"

    train_hits = all_hits[all_hits["split"] == "train"] if not all_hits.empty else pd.DataFrame()
    val_hits   = all_hits[all_hits["split"] == "val"]   if not all_hits.empty else pd.DataFrame()

    lines = [
        f"# Inter-Session Analysis — {ts}",
        "",
        "## Сводка по всем сессиям",
        "",
        f"- Сессий проанализировано: **{cfg['sessions_analyzed']}**",
        f"- Train сигналов (всего): **{cfg['n_train_total']}**",
        f"- Val-confirmed (всего):  **{cfg['n_val_total']}**",
        f"- Val pass rate:          **{cfg['val_pass_rate']:.1%}**",
        f"- Тренд IC:               **{cfg['ic_trend']}**",
        "",
        "## Решение для следующей сессии",
        "",
        f"| Параметр | Значение | Причина |",
        f"|---|---|---|",
        f"| IC gate      | `{cfg['ic_gate']}`   | {cfg['ic_gate_reason']} |",
        f"| Ensemble     | `{cfg['ensemble']}`  | — |",
        f"| Feature type | `{cfg['best_feature']}` | max mean IC по истории |",
        f"| Лаги         | `{cfg['best_lags']}` | топ-4 по IC |",
        "",
        "## Фокус: топ-инструменты",
        "",
    ]
    for i, inst in enumerate(cfg["top_instruments"], 1):
        if not train_hits.empty and "instrument" in train_hits.columns:
            ic_mean = train_hits[train_hits["instrument"] == inst]["m6_ic"].mean()
            n       = train_hits[train_hits["instrument"] == inst].shape[0]
            lines.append(f"{i}. **{inst}** — mean IC={ic_mean:.4f}  n={n}")
        else:
            lines.append(f"{i}. {inst}")

    lines += ["", "## Фокус: топ-темы", ""]
    for i, t in enumerate(cfg["top_topics"], 1):
        if not train_hits.empty and "topic" in train_hits.columns:
            ic_mean = train_hits[train_hits["topic"] == t]["m6_ic"].mean()
            lines.append(f"{i}. **{t}** — mean IC={ic_mean:.4f}")
        else:
            lines.append(f"{i}. {t}")

    if cfg["stable_signals"]:
        lines += ["", "## Стабильные сигналы (2+ сессий)", "",
                  "| Инструмент | Тема | Lag | N сессий | N val | Best IC |",
                  "|---|---|---|---|---|---|"]
        for s in cfg["stable_signals"]:
            lines.append(
                f"| {s['instrument']} | {s['topic']} | {s['lag']} "
                f"| {s['n_sessions']} | {int(s.get('n_val', 0))} "
                f"| {s.get('best_ic', 0):.4f} |"
            )

    if cfg["deprioritize"]:
        lines += ["", "## Анти-паттерны (не проходят val 3+ раз)", "",
                  "| Инструмент | Тема | Lag |",
                  "|---|---|---|"]
        for d in cfg["deprioritize"][:10]:
            lines.append(f"| {d['instrument']} | {d['topic']} | {d['lag']} |")

    # Per-session stats table
    if not train_hits.empty and "session" in train_hits.columns:
        lines += ["", "## История по сессиям", "",
                  "| Сессия | Train | Val | Mean IC |",
                  "|---|---|---|---|"]
        for sess, grp in train_hits.groupby("session"):
            n_val_s = val_hits[val_hits["session"] == sess].shape[0] if not val_hits.empty else 0
            lines.append(
                f"| {sess} | {len(grp)} | {n_val_s} | {grp['m6_ic'].mean():.4f} |"
            )

    md.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report: {md}")
    return md


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("INTER-SESSION ANALYZER")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    print("\nLoading all past hits...")
    all_hits = load_all_hits()

    if all_hits.empty:
        print("No hits found yet — writing default config")
        cfg = _default_config("first_run")
    else:
        n_sessions = all_hits["session"].nunique()
        print(f"Found {len(all_hits)} hits across {n_sessions} sessions")
        cfg = analyze(all_hits)

    # Save config
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False),
                           encoding="utf-8")
    print(f"\nConfig saved: {CONFIG_PATH}")

    # Print summary
    print(f"\n{'─'*40}")
    print(f"Sessions analyzed : {cfg['sessions_analyzed']}")
    print(f"Val pass rate     : {cfg['val_pass_rate']:.1%}")
    print(f"IC trend          : {cfg['ic_trend']}")
    print(f"Next IC gate      : {cfg['ic_gate']}  ({cfg['ic_gate_reason']})")
    print(f"Next ensemble     : {cfg['ensemble']}")
    print(f"Best feature      : {cfg['best_feature']}")
    print(f"Top instruments   : {cfg['top_instruments'][:4]}")
    print(f"Top topics        : {cfg['top_topics'][:3]}")
    print(f"Best lags         : {cfg['best_lags']}")
    print(f"Stable signals    : {len(cfg['stable_signals'])}")
    print(f"Anti-patterns     : {len(cfg['deprioritize'])}")
    print(f"{'─'*40}\n")

    # Write human-readable report
    write_report(cfg, all_hits)

    # Log config change to scientific ledger
    try:
        from src.pipeline_v2.session_ledger import (
            log_config_change, log_analysis, read_sessions,
            check_cross_session_integrity, generate_summary
        )

        # Load previous config if exists
        prev_cfg: dict = {}
        if CONFIG_PATH.exists():
            try:
                prev_cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            except Exception:
                pass

        changed_keys = [
            k for k in ("ic_gate", "ensemble", "best_feature", "best_lags")
            if prev_cfg.get(k) != cfg.get(k)
        ]
        if changed_keys:
            reasons = [cfg.get("ic_gate_reason", "analyzer decision")]
            from_map = {k: prev_cfg.get(k) for k in changed_keys}
            to_map   = {k: cfg.get(k)      for k in changed_keys}
            log_config_change(from_map, to_map, reasons)

        # Cross-session integrity
        sessions = read_sessions()
        cross_flags = check_cross_session_integrity(sessions)

        decisions = [
            {"parameter": k, "old": prev_cfg.get(k), "new": cfg.get(k),
             "reason": cfg.get("ic_gate_reason", "")}
            for k in changed_keys
        ]
        log_analysis(
            summary={
                "sessions_analyzed": cfg["sessions_analyzed"],
                "val_pass_rate":     cfg["val_pass_rate"],
                "ic_trend":          cfg["ic_trend"],
                "stable_signals":    len(cfg["stable_signals"]),
                "anti_patterns":     len(cfg["deprioritize"]),
            },
            flags=cross_flags,
            decisions=decisions,
        )

        if cross_flags:
            print("\nCross-session integrity flags:")
            for f in cross_flags:
                print(f"  [{f['level']}] {f['code']}: {f['message'][:100]}")

        generate_summary()
        print(f"\nLedger summary updated.")
    except Exception as e:
        print(f"[ledger] Warning: {e}")

    print("\nDone. Next session will use this config automatically.")


if __name__ == "__main__":
    main()
