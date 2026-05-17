"""Phase A.3b — Ensemble configuration search.

Loads per-method raw verdict CSVs (M2..M6), merges them into a joint table,
then enumerates ~100 candidate voting configurations (single methods, 2-of-N,
K-of-N, etc.) and computes Loss for each:

    Loss(config) = alpha * avg_FPR(S1,S2,S6)
                 + beta  * (1 - avg_TPR(S3,S4,S5))
                 + gamma * penalty(config, mistakes_catalog)
                 + delta * complexity(config)

    alpha = 1.0, beta = 1.0, gamma = 2.0, delta = 0.05

Outputs:
    results/ensemble_search.csv  — full ranking
    results/ensemble_search.md   — human-readable top-20

The top-1 config by min Loss is the candidate for Phase A.4 freeze.

Run:
    python -m analytics.testbed.ensemble.search
"""
from __future__ import annotations

import io
import itertools
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"

# ---------------------------------------------------------------------------
# Loss hyper-parameters
# ---------------------------------------------------------------------------
ALPHA = 1.0    # FPR weight
BETA = 1.0     # (1-TPR) weight
GAMMA = 2.0    # mistakes-penalty multiplier
DELTA = 0.05   # complexity penalty per extra method

# No-signal datasets (contribute to FPR); signal datasets (contribute to TPR)
FPR_DATASETS = {"S1", "S2", "S6"}
TPR_DATASETS = {"S3", "S4", "S5"}


# ---------------------------------------------------------------------------
# Load raw verdicts for each method
# ---------------------------------------------------------------------------

def _load_method_verdicts(method_name: str) -> pd.DataFrame | None:
    """Read *_raw.csv for a method, return with 'confirmed_<method>' column."""
    csv = RESULTS / f"{method_name}_raw.csv"
    if not csv.exists():
        return None
    df = pd.read_csv(csv)
    df = df.rename(columns={"confirmed": f"confirmed_{method_name}"})
    return df


JOIN_KEYS = ["dataset", "seed", "news_field", "lag",
             "regime_filter", "is_true_signal"]

METHODS = {
    "M2_corr_returns_hac": {
        "out_of_sample_validation": False,
    },
    "M3_granger_hac": {
        "out_of_sample_validation": False,
    },
    "M4_event_study_hac": {
        "out_of_sample_validation": False,
    },
    "M5_var_orth_irf": {
        "out_of_sample_validation": False,
    },
    "M6_lgbm_walkforward": {
        "out_of_sample_validation": True,   # walk-forward CV built-in
    },
}


def load_joint() -> tuple[pd.DataFrame, list[str]]:
    """Merge all method verdicts. Returns (df, method_col_names)."""
    base = None
    method_cols: list[str] = []
    for m_name in METHODS:
        df = _load_method_verdicts(m_name)
        if df is None:
            print(f"[search] WARNING: {m_name}_raw.csv not found, skipping")
            continue
        col = f"confirmed_{m_name}"
        method_cols.append(col)
        keep = JOIN_KEYS + [col]
        df = df[keep]
        if base is None:
            base = df
        else:
            base = base.merge(df, on=JOIN_KEYS, how="inner")
    if base is None or len(method_cols) == 0:
        raise RuntimeError("No method raw CSVs found in results/")
    return base, method_cols


# ---------------------------------------------------------------------------
# Voting rule application
# ---------------------------------------------------------------------------

def apply_voting(df: pd.DataFrame, method_cols: list[str], k: int) -> pd.Series:
    """Return a boolean series: confirmed = (sum of confirmed flags >= k)."""
    total = df[method_cols].sum(axis=1)
    return total >= k


# ---------------------------------------------------------------------------
# FPR / TPR from joint df + confirmed series
# ---------------------------------------------------------------------------

def compute_metrics(
    df: pd.DataFrame, confirmed: pd.Series
) -> dict[str, float]:
    """Return avg_FPR (over no-signal datasets) and avg_TPR (signal datasets)."""
    fprs: list[float] = []
    tprs: list[float] = []
    for ds, grp in df.groupby("dataset"):
        c = confirmed.loc[grp.index]
        is_true = grp["is_true_signal"].astype(bool)
        tp = int((c & is_true).sum())
        fp = int((c & ~is_true).sum())
        tn = int((~c & ~is_true).sum())
        fn = int((~c & is_true).sum())
        total_neg = fp + tn
        total_pos = tp + fn
        if ds in FPR_DATASETS and total_neg > 0:
            fprs.append(fp / total_neg)
        if ds in TPR_DATASETS and total_pos > 0:
            tprs.append(tp / total_pos)
    avg_fpr = float(np.mean(fprs)) if fprs else 0.0
    avg_tpr = float(np.mean(tprs)) if tprs else 0.0
    return {"avg_fpr": avg_fpr, "avg_tpr": avg_tpr,
            "fprs": fprs, "tprs": tprs}


# ---------------------------------------------------------------------------
# Penalty from mistakes catalog
# ---------------------------------------------------------------------------

def _ensemble_penalty(method_names: list[str]) -> tuple[float, list[str]]:
    """Compute mistakes penalty for a voting ensemble.

    Rules:
    - method = "ensemble" -> M001 (corr-on-levels) never fires.
    - M002 (no HAC): all component methods use HAC -> never fires.
    - M003, M004, M005, M006, M007: all avoided by all components.
    - M008 (no walk-forward): fires ONLY if NONE of the components
      have out_of_sample_validation=True.
    - M009: fires if measured_fpr > 0.10 (computed after evaluation).

    Returns (pre_penalty, triggered_ids).
    """
    has_oos = any(METHODS[m].get("out_of_sample_validation", False)
                  for m in method_names if m in METHODS)
    triggered: list[str] = []
    total = 0.0
    if not has_oos:
        triggered.append("M008")
        total += 0.9
    return total, triggered


def _post_penalty(pre_penalty: float, pre_triggered: list[str],
                  max_fpr: float) -> tuple[float, list[str]]:
    total = pre_penalty
    triggered = list(pre_triggered)
    if max_fpr > 0.10:
        triggered.append("M009")
        total += 1.0
    return total, triggered


# ---------------------------------------------------------------------------
# Complexity
# ---------------------------------------------------------------------------

def complexity(n_methods: int) -> float:
    return float(n_methods)


# ---------------------------------------------------------------------------
# Loss function
# ---------------------------------------------------------------------------

def compute_loss(avg_fpr: float, avg_tpr: float,
                 pre_penalty: float, n_methods: int) -> float:
    return (ALPHA * avg_fpr
            + BETA * (1.0 - avg_tpr)
            + GAMMA * pre_penalty
            + DELTA * complexity(n_methods))


# ---------------------------------------------------------------------------
# Candidate configuration generator
# ---------------------------------------------------------------------------

def generate_candidates(method_cols: list[str]) -> list[dict]:
    """Generate all single, 2-method, 3-method, 4-method, 5-method configs."""
    n = len(method_cols)
    candidates: list[dict] = []

    for size in range(1, n + 1):
        for subset in itertools.combinations(range(n), size):
            cols = [method_cols[i] for i in subset]
            names = [c.replace("confirmed_", "") for c in cols]
            # Voting thresholds K=1 (OR), K=ceil(size/2) (MAJ), K=size (AND)
            voting_rules: list[tuple[str, int]] = []
            if size == 1:
                voting_rules = [("SINGLE", 1)]
            elif size == 2:
                voting_rules = [("OR", 1), ("AND", 2)]
            else:
                mid = (size + 1) // 2
                voting_rules = [("OR", 1), (f"MAJ_{mid}of{size}", mid),
                                 ("AND", size)]
            for rule_name, k in voting_rules:
                candidates.append({
                    "label": f"{'+'.join(n.split('_')[0] for n in names)}/{rule_name}",
                    "method_names": names,
                    "method_cols": cols,
                    "k": k,
                    "size": size,
                    "rule": rule_name,
                })
    return candidates


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("[search] loading raw verdicts ...")
    joint, method_cols = load_joint()
    print(f"[search] joint table: {len(joint)} rows, "
          f"{len(method_cols)} methods: {[c.replace('confirmed_','') for c in method_cols]}")

    candidates = generate_candidates(method_cols)
    print(f"[search] {len(candidates)} candidate configs")

    rows: list[dict] = []
    for cand in candidates:
        confirmed = apply_voting(joint, cand["method_cols"], cand["k"])
        metrics = compute_metrics(joint, confirmed)
        avg_fpr = metrics["avg_fpr"]
        avg_tpr = metrics["avg_tpr"]

        pre_penalty, pre_triggered = _ensemble_penalty(cand["method_names"])
        max_fpr_no_signal = float(np.max(metrics["fprs"])) if metrics["fprs"] else 0.0
        post_penalty, post_triggered = _post_penalty(
            pre_penalty, pre_triggered, max_fpr_no_signal)

        loss = compute_loss(avg_fpr, avg_tpr, pre_penalty, cand["size"])

        rows.append({
            "label": cand["label"],
            "methods": ",".join(cand["method_names"]),
            "k": cand["k"],
            "n_methods": cand["size"],
            "rule": cand["rule"],
            "avg_fpr": round(avg_fpr, 4),
            "avg_tpr": round(avg_tpr, 4),
            "max_fpr_S1S2S6": round(max_fpr_no_signal, 4),
            "pre_penalty": round(pre_penalty, 1),
            "post_penalty": round(post_penalty, 1),
            "triggered_post": ",".join(post_triggered) if post_triggered else "",
            "loss": round(loss, 4),
        })

    result_df = pd.DataFrame(rows).sort_values("loss").reset_index(drop=True)
    result_df.index += 1

    csv_path = RESULTS / "ensemble_search.csv"
    md_path = RESULTS / "ensemble_search.md"
    result_df.to_csv(csv_path, index_label="rank")

    # Markdown report
    lines: list[str] = []
    lines.append("# Ensemble configuration search")
    lines.append("")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Loss weights: alpha={ALPHA}, beta={BETA}, "
                 f"gamma={GAMMA}, delta={DELTA}")
    lines.append("")
    lines.append("## Top-20 configurations by Loss")
    lines.append("")
    top20 = result_df.head(20)
    lines.append("| Rank | Label | avg_FPR | avg_TPR | max_FPR_noSignal"
                 " | pre-penalty | post-penalty | Loss |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for rank, row in top20.iterrows():
        lines.append(
            f"| {rank} | `{row['label']}` | {row['avg_fpr']:.4f} "
            f"| {row['avg_tpr']:.4f} | {row['max_fpr_S1S2S6']:.4f} "
            f"| {row['pre_penalty']} | {row['post_penalty']} "
            f"| **{row['loss']:.4f}** |"
        )
    lines.append("")
    lines.append("## Winner (rank 1)")
    lines.append("")
    winner = result_df.iloc[0]
    lines.append(f"**Config:** `{winner['label']}`")
    lines.append(f"- Methods: {winner['methods']}")
    lines.append(f"- Voting rule: {winner['rule']} (K={winner['k']} of "
                 f"{winner['n_methods']})")
    lines.append(f"- avg_FPR: {winner['avg_fpr']:.4f}")
    lines.append(f"- avg_TPR: {winner['avg_tpr']:.4f}")
    lines.append(f"- max FPR (no-signal): {winner['max_fpr_S1S2S6']:.4f}")
    lines.append(f"- Pre-measurement penalty: {winner['pre_penalty']}")
    lines.append(f"- Post-measurement penalty: {winner['post_penalty']}")
    lines.append(f"  (triggered: {winner['triggered_post'] or 'none'})")
    lines.append(f"- **Loss: {winner['loss']:.4f}**")
    lines.append("")
    lines.append(f"Full ranking: `{csv_path.name}`")

    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"[search] wrote: {csv_path}")
    print(f"[search] wrote: {md_path}")

    # Print top-5 to stdout for quick review
    print("\n[search] Top-5 by Loss:")
    for i, row in result_df.head(5).iterrows():
        print(f"  #{i:2d}  Loss={row['loss']:.4f}  "
              f"FPR={row['avg_fpr']:.4f}  TPR={row['avg_tpr']:.4f}  "
              f"maxFPR={row['max_fpr_S1S2S6']:.4f}  "
              f"pen={row['post_penalty']}  {row['label']}")

    print(f"\n[search] Winner: {winner['label']} "
          f"(Loss={winner['loss']:.4f})")


if __name__ == "__main__":
    main()
