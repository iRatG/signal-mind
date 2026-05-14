"""Universal harness — run ONE method on ALL six testbed datasets.

Reads seeds from analytics/testbed/datasets/S*/, applies the method to each
seed under the dataset's ground-truth hypotheses, and aggregates FPR/TPR/F1
per dataset.

Output:
  - analytics/testbed/results/<method_name>_summary.csv  (per-dataset table)
  - analytics/testbed/results/<method_name>_raw.csv      (per-seed verdicts)
  - analytics/testbed/results/<method_name>.md           (human report)

Read-only on data. Does NOT touch real project DBs.
"""
from __future__ import annotations

import argparse
import importlib
import io
import json
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# Force UTF-8 stdout on Windows (cp1251 default).
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

from analytics.testbed.methods.base import Hypothesis  # noqa: E402
from analytics.testbed.mistakes.catalog import compute_penalty  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DATASETS_DIR = ROOT / "datasets"
RESULTS_DIR = ROOT / "results"


# ---------------------------------------------------------------------------
# Hypothesis schedule — what to test on each dataset.
#
# Each dataset declares ground-truth signals in its ground_truth.json. The
# harness schedules a SUPERSET of hypotheses per dataset: the true ones plus
# off-target lags / topics, so we measure both TPR (on true signals) and
# FPR (on no-signal hypotheses).
# ---------------------------------------------------------------------------

# Standard lag sweep used on every dataset
LAG_SWEEP = (1, 7, 14, 30, 60, 90)


def _hyps_for_S1(_gt) -> list[tuple[Hypothesis, bool]]:
    # No real signal. Every hypothesis at every lag is a no-signal case.
    return [
        (Hypothesis(news_field="news_mentions",
                    target_field="market_return", lag_days=L), False)
        for L in LAG_SWEEP
    ]


def _hyps_for_S2(_gt) -> list[tuple[Hypothesis, bool]]:
    # Also no real signal — the trend is the trap.
    return [
        (Hypothesis(news_field="news_mentions",
                    target_field="market_return", lag_days=L), False)
        for L in LAG_SWEEP
    ]


def _hyps_for_S3(gt) -> list[tuple[Hypothesis, bool]]:
    true_lag = gt["true_signals"][0]["lag_days"]
    return [
        (Hypothesis(news_field="news_mentions",
                    target_field="market_return", lag_days=L),
         L == true_lag)
        for L in LAG_SWEEP
    ]


def _hyps_for_S4(gt) -> list[tuple[Hypothesis, bool]]:
    """S4 has the signal in the FIRST HALF only. We test:
      - lag=7 first_half (true signal)
      - lag=7 second_half (no signal — low_rate regime)
      - lag=14 first_half (wrong lag, no signal)
    """
    true_lag = gt["true_signals"][0]["lag_days"]
    out: list[tuple[Hypothesis, bool]] = []
    for L in LAG_SWEEP:
        out.append((
            Hypothesis(news_field="news_mentions",
                       target_field="market_return", lag_days=L,
                       regime_filter="first_half"),
            L == true_lag,
        ))
        out.append((
            Hypothesis(news_field="news_mentions",
                       target_field="market_return", lag_days=L,
                       regime_filter="second_half"),
            False,
        ))
    return out


def _hyps_for_S5(gt) -> list[tuple[Hypothesis, bool]]:
    true_set = {(s["news_field"], s["lag_days"]) for s in gt["true_signals"]}
    topics = ["topic_A", "topic_B", "topic_C"]
    out: list[tuple[Hypothesis, bool]] = []
    for t in topics:
        for L in LAG_SWEEP:
            out.append((
                Hypothesis(news_field=t, target_field="market_return", lag_days=L),
                (t, L) in true_set,
            ))
    return out


def _hyps_for_S6(_gt) -> list[tuple[Hypothesis, bool]]:
    # No DIRECT news->market causal link. All hypotheses are false-positives.
    return [
        (Hypothesis(news_field="news_mentions",
                    target_field="market_return", lag_days=L), False)
        for L in LAG_SWEEP
    ]


HYPOTHESIS_FACTORIES = {
    "S1": _hyps_for_S1, "S2": _hyps_for_S2, "S3": _hyps_for_S3,
    "S4": _hyps_for_S4, "S5": _hyps_for_S5, "S6": _hyps_for_S6,
}


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_seeds(dataset: str) -> list[tuple[int, pd.DataFrame]]:
    ddir = DATASETS_DIR / dataset
    seeds = []
    for p in sorted(ddir.glob("seed_*.parquet")):
        idx = int(p.stem.split("_")[1])
        seeds.append((idx, pd.read_parquet(p)))
    return seeds


def load_ground_truth(dataset: str) -> dict:
    return json.loads((DATASETS_DIR / dataset / "ground_truth.json").read_text())


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_method(method_module: str) -> dict:
    mod = importlib.import_module(method_module)
    method = mod.build()

    pre_penalty, pre_triggered, _ = compute_penalty(method.config)

    raw_rows: list[dict] = []
    summary_rows: list[dict] = []

    for dataset in ("S1", "S2", "S3", "S4", "S5", "S6"):
        gt = load_ground_truth(dataset)
        hyps = HYPOTHESIS_FACTORIES[dataset](gt)
        seeds = load_seeds(dataset)

        n_tp = n_fp = n_tn = n_fn = 0
        for seed_idx, df in seeds:
            for hyp, is_true in hyps:
                v = method.evaluate(df, hyp)
                if v.confirmed and is_true:
                    n_tp += 1
                elif v.confirmed and not is_true:
                    n_fp += 1
                elif not v.confirmed and not is_true:
                    n_tn += 1
                else:
                    n_fn += 1
                raw_rows.append({
                    "dataset": dataset, "seed": seed_idx,
                    "news_field": hyp.news_field,
                    "lag": hyp.lag_days,
                    "regime_filter": hyp.regime_filter or "",
                    "is_true_signal": is_true,
                    "confirmed": v.confirmed,
                    "score": v.score, "p_value": v.p_value, "n": v.n,
                })

        total_pos = n_tp + n_fn   # ground-truth positives
        total_neg = n_fp + n_tn   # ground-truth negatives
        tpr = n_tp / total_pos if total_pos else float("nan")
        fpr = n_fp / total_neg if total_neg else float("nan")
        precision = n_tp / (n_tp + n_fp) if (n_tp + n_fp) else float("nan")
        f1 = (2 * precision * tpr / (precision + tpr)
              if (not np.isnan(precision) and not np.isnan(tpr)
                  and (precision + tpr) > 0)
              else float("nan"))

        summary_rows.append({
            "dataset": dataset,
            "n_tp": n_tp, "n_fp": n_fp, "n_tn": n_tn, "n_fn": n_fn,
            "tpr": round(tpr, 4) if not np.isnan(tpr) else None,
            "fpr": round(fpr, 4) if not np.isnan(fpr) else None,
            "precision": round(precision, 4) if not np.isnan(precision) else None,
            "f1": round(f1, 4) if not np.isnan(f1) else None,
        })

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    raw_csv = RESULTS_DIR / f"{method.name}_raw.csv"
    summary_csv = RESULTS_DIR / f"{method.name}_summary.csv"
    md_path = RESULTS_DIR / f"{method.name}.md"

    pd.DataFrame(raw_rows).to_csv(raw_csv, index=False)
    pd.DataFrame(summary_rows).to_csv(summary_csv, index=False)

    # Post-measurement penalty: take max FPR across no-signal datasets
    # (S1, S2, S6 are the FPR-relevant ones) and feed it back to the catalogue
    # so M009 ("pipeline gives >10% confirmed on shuffle") can fire.
    fpr_observed = [
        r["fpr"] for r in summary_rows
        if r["dataset"] in {"S1", "S2", "S6"} and r["fpr"] is not None
    ]
    max_fpr = max(fpr_observed) if fpr_observed else None
    post_config = dict(method.config, measured_fpr_on_shuffle=max_fpr)
    post_penalty, post_triggered, _ = compute_penalty(post_config)

    # Markdown report
    lines = [
        f"# {method.name} on testbed",
        "",
        f"Run timestamp: {datetime.now(timezone.utc).isoformat()}",
        f"Source: {method_module}",
        "",
        "## Penalty from mistakes catalogue",
        "",
        f"- **Pre-measurement penalty:** {pre_penalty:.1f}  triggers `{pre_triggered}`",
        f"- **Post-measurement penalty:** {post_penalty:.1f}  triggers `{post_triggered}`",
        f"- max FPR observed (S1/S2/S6) = {max_fpr}",
        "",
        "## Per-dataset metrics",
        "",
        "| Dataset | n_TP | n_FP | n_TN | n_FN | TPR | FPR | Precision | F1 |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in summary_rows:
        lines.append(
            f"| {row['dataset']} | {row['n_tp']} | {row['n_fp']} | "
            f"{row['n_tn']} | {row['n_fn']} | {row['tpr']} | {row['fpr']} | "
            f"{row['precision']} | {row['f1']} |"
        )
    lines += [
        "",
        "## Diagnostic notes",
        "",
        "- High FPR on S1: method gives false positives on pure noise.",
        "- High FPR on S2: method falls into the spurious-trend trap (M001).",
        "- TPR on S3 measures statistical power at the true lag.",
        "- High FPR on S6: method confuses confounded correlation with causation.",
        "- TPR=0 on S3/S4/S5 means the method is blind to genuine signals.",
        "",
        f"Raw verdicts: `{raw_csv.name}` (one row per seed × hypothesis)",
        f"Summary CSV : `{summary_csv.name}`",
    ]
    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"[harness] method={method.name}")
    print(f"[harness] pre-measurement penalty  = {pre_penalty:.1f} {pre_triggered}")
    print(f"[harness] max FPR (S1/S2/S6)       = {max_fpr}")
    print(f"[harness] post-measurement penalty = {post_penalty:.1f} {post_triggered}")
    print(f"[harness] wrote: {summary_csv}")
    print(f"[harness] wrote: {raw_csv}")
    print(f"[harness] wrote: {md_path}")
    return {
        "summary": summary_rows,
        "pre_penalty": pre_penalty, "pre_triggered": pre_triggered,
        "post_penalty": post_penalty, "post_triggered": post_triggered,
        "max_fpr": max_fpr,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--method",
        default="analytics.testbed.methods.m1_corr_levels",
        help="Dotted module path of method to run (must expose build()).",
    )
    args = ap.parse_args()
    run_method(args.method)


if __name__ == "__main__":
    main()
