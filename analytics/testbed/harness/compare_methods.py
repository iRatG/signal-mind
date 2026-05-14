"""Aggregate per-method summaries into a comparison table.

Reads every `*_summary.csv` in analytics/testbed/results/ and emits:
  - results/comparison.csv  (long-format: method, dataset, metric, value)
  - results/comparison.md   (compact tables per metric, side-by-side)

Also pulls each method's pre- and post-measurement penalty from its own
report (re-runs compute_penalty against the method's config + observed FPR).

Read-only. No LLM. Works on however many methods are currently present.
"""
from __future__ import annotations

import importlib
import io
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

from analytics.testbed.mistakes.catalog import compute_penalty  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
METHODS_DIR = ROOT / "methods"


def discover_method_modules() -> dict[str, str]:
    """Map method.name (from build()) -> dotted module path."""
    mapping: dict[str, str] = {}
    for py in sorted(METHODS_DIR.glob("m*.py")):
        mod_name = f"analytics.testbed.methods.{py.stem}"
        try:
            mod = importlib.import_module(mod_name)
            if hasattr(mod, "build"):
                m = mod.build()
                mapping[m.name] = mod_name
        except Exception:
            continue
    return mapping


def load_summaries() -> dict[str, pd.DataFrame]:
    """Return {method_name: per-dataset summary df}."""
    out: dict[str, pd.DataFrame] = {}
    for p in sorted(RESULTS.glob("*_summary.csv")):
        name = p.stem.removesuffix("_summary")
        out[name] = pd.read_csv(p)
    return out


def main() -> None:
    summaries = load_summaries()
    if not summaries:
        print("[compare] no *_summary.csv files found")
        return

    module_map = discover_method_modules()

    # Long-format CSV
    long_rows: list[dict] = []
    for method_name, df in summaries.items():
        for _, row in df.iterrows():
            for metric in ("tpr", "fpr", "precision", "f1"):
                long_rows.append({
                    "method": method_name,
                    "dataset": row["dataset"],
                    "metric": metric,
                    "value": row[metric],
                })
    long_df = pd.DataFrame(long_rows)
    long_csv = RESULTS / "comparison.csv"
    long_df.to_csv(long_csv, index=False)

    # Compact pivots per metric
    datasets = ("S1", "S2", "S3", "S4", "S5", "S6")
    methods_order = sorted(summaries.keys())

    def pivot(metric: str) -> pd.DataFrame:
        rows = []
        for ds in datasets:
            row = {"dataset": ds}
            for m in methods_order:
                df = summaries[m]
                cell = df.loc[df["dataset"] == ds, metric]
                row[m] = (
                    float(cell.iloc[0]) if not cell.empty and pd.notna(cell.iloc[0])
                    else None
                )
            rows.append(row)
        return pd.DataFrame(rows)

    tpr_pivot = pivot("tpr")
    fpr_pivot = pivot("fpr")
    f1_pivot = pivot("f1")

    # Penalty per method
    penalty_rows: list[dict] = []
    for m_name in methods_order:
        mod_path = module_map.get(m_name)
        config = None
        if mod_path:
            try:
                mod = importlib.import_module(mod_path)
                config = mod.build().config
            except Exception:
                config = None
        if config is None:
            penalty_rows.append({
                "method": m_name, "pre_penalty": None,
                "max_fpr_no_signal": None, "post_penalty": None,
                "triggered_post": "?",
            })
            continue
        pre, pre_tr, _ = compute_penalty(config)
        df = summaries[m_name]
        fprs = df.loc[df["dataset"].isin({"S1", "S2", "S6"}), "fpr"]
        max_fpr = float(fprs.max()) if not fprs.empty else None
        post_cfg = dict(config, measured_fpr_on_shuffle=max_fpr)
        post, post_tr, _ = compute_penalty(post_cfg)
        penalty_rows.append({
            "method": m_name, "pre_penalty": pre,
            "max_fpr_no_signal": max_fpr,
            "post_penalty": post,
            "triggered_post": ",".join(post_tr),
        })

    # Markdown
    lines: list[str] = []
    lines.append("# Methods comparison on testbed")
    lines.append("")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Methods compared: {len(methods_order)}")
    lines.append("")

    lines.append("## Penalty (mistakes catalogue)")
    lines.append("")
    lines.append("| Method | Pre-penalty | Max FPR (no-signal) | Post-penalty | Triggered |")
    lines.append("|---|---|---|---|---|")
    for r in penalty_rows:
        lines.append(
            f"| {r['method']} | {r['pre_penalty']} | "
            f"{r['max_fpr_no_signal']} | {r['post_penalty']} | "
            f"{r['triggered_post']} |"
        )
    lines.append("")

    def render(metric_name: str, pivot_df: pd.DataFrame) -> list[str]:
        out = [f"## {metric_name} per dataset", ""]
        header = "| dataset | " + " | ".join(methods_order) + " |"
        sep = "|---|" + "|".join(["---"] * len(methods_order)) + "|"
        out.append(header)
        out.append(sep)
        for _, row in pivot_df.iterrows():
            cells = [str(row["dataset"])]
            for m in methods_order:
                v = row[m]
                cells.append("—" if v is None else f"{v:.4f}")
            out.append("| " + " | ".join(cells) + " |")
        out.append("")
        return out

    lines += render("TPR", tpr_pivot)
    lines += render("FPR", fpr_pivot)
    lines += render("F1", f1_pivot)

    lines.append("## Interpretation hints")
    lines.append("")
    lines.append("- **S1 (pure noise)**: FPR should be ~5% for any sound method.")
    lines.append("- **S2 (spurious trend)**: FPR >> 5% signals M001 contamination.")
    lines.append("- **S3 (r=-0.3 at lag=14)**: weak signal, may be below |r|>=0.4 gate.")
    lines.append("- **S4 (r=-0.5 in first half)**: strong signal, expect high TPR.")
    lines.append("- **S5 (multi-signal)**: tests selectivity (A,B real; C noise).")
    lines.append("- **S6 (confounder)**: FPR > 5% signals lack of causal discipline.")
    lines.append("")
    lines.append("Long-format data: `comparison.csv`")

    md_path = RESULTS / "comparison.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"[compare] {len(methods_order)} methods aggregated: {methods_order}")
    print(f"[compare] wrote: {long_csv}")
    print(f"[compare] wrote: {md_path}")


if __name__ == "__main__":
    main()
