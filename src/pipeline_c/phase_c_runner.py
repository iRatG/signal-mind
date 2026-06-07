"""Phase C Runner — оркестратор.

Полный цикл одной сессии:
  1. Загрузить существующие гипотезы ИЛИ запустить RAG extractor
  2. Тестировать все гипотезы на Train + Val (M5+M6)
  3. Записать в ledger_c.jsonl
  4. Обновить session_config_c.json (Ouroboros: какие компании дают сигналы)
  5. Сгенерировать отчёт

Usage:
    python -m src.pipeline_c.phase_c_runner
    python -m src.pipeline_c.phase_c_runner --extract          # re-extract from RAG
    python -m src.pipeline_c.phase_c_runner --company sberbank --year 2023
    python -m src.pipeline_c.phase_c_runner --dry-run          # no writes
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

from src.pipeline_c.hypothesis_schema import RagHypothesis, HypothesisResult
from src.pipeline_c.rag_extractor import (
    extract_hypotheses, load_all_hypotheses, load_hypotheses,
    filter_known_signals, COMPANY_INSTRUMENT_MAP, YEARS,
)
from src.pipeline_c.hypothesis_tester import test_batch

OUT_DIR     = ROOT / "analytics" / "phase_c"
LEDGER_PATH = OUT_DIR / "ledger_c.jsonl"
CONFIG_PATH = OUT_DIR / "session_config_c.json"
FINDINGS_PATH = OUT_DIR / "FINDINGS.md"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# Ledger (append-only)
# ──────────────────────────────────────────────────────────────────────────────

def _ledger_append(record: dict) -> None:
    record["_ts"] = datetime.now(timezone.utc).isoformat()
    with open(LEDGER_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _ledger_read() -> list[dict]:
    if not LEDGER_PATH.exists():
        return []
    records = []
    with open(LEDGER_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


# ──────────────────────────────────────────────────────────────────────────────
# Session config — Ouroboros knowledge about companies/topics
# ──────────────────────────────────────────────────────────────────────────────

def _load_config() -> dict:
    defaults = {
        "session_count":        0,
        "company_scores":       {},   # company → {signals: int, val_pass: int}
        "topic_scores":         {},   # topic → {signals: int, val_pass: int}
        "best_pairs":           [],   # [{instrument, topic, lag, ic, sessions}]
        "skip_companies":       [],   # companies that never produce signals
        "preferred_years":      [],   # years that produce most signals
        "notes":                [],   # free-form Ouroboros observations
    }
    if CONFIG_PATH.exists():
        try:
            saved = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            defaults.update(saved)
        except Exception:
            pass
    return defaults


def _save_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(
        json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _update_config(cfg: dict, results: list[HypothesisResult]) -> dict:
    """Update session_config_c.json based on this session's results."""
    # Count signals per company (via source_label)
    for r in results:
        if r.split == "train" and r.ensemble_pass:
            company = r.source_label.split("/")[0] if r.source_label else "unknown"
            if company not in cfg["company_scores"]:
                cfg["company_scores"][company] = {"signals": 0, "val_pass": 0}
            cfg["company_scores"][company]["signals"] += 1

            if r.topic not in cfg["topic_scores"]:
                cfg["topic_scores"][r.topic] = {"signals": 0, "val_pass": 0}
            cfg["topic_scores"][r.topic]["signals"] += 1

        if r.split == "val" and r.ensemble_pass:
            company = r.source_label.split("/")[0] if r.source_label else "unknown"
            if company in cfg["company_scores"]:
                cfg["company_scores"][company]["val_pass"] += 1
            if r.topic in cfg["topic_scores"]:
                cfg["topic_scores"][r.topic]["val_pass"] += 1

    # Update best pairs list
    val_pass_set = {
        (r.instrument, r.topic, r.lag)
        for r in results
        if r.split == "val" and r.ensemble_pass
    }
    existing_pairs = {
        (p["instrument"], p["topic"], p["lag"])
        for p in cfg["best_pairs"]
    }
    for r in results:
        key = (r.instrument, r.topic, r.lag)
        if key in val_pass_set and key not in existing_pairs:
            cfg["best_pairs"].append({
                "instrument": r.instrument,
                "topic":      r.topic,
                "lag":        r.lag,
                "ic":         r.m6_ic,
                "source":     r.source_label,
                "sessions":   1,
            })
            existing_pairs.add(key)
        elif key in val_pass_set:
            for p in cfg["best_pairs"]:
                if (p["instrument"], p["topic"], p["lag"]) == key:
                    p["sessions"] = p.get("sessions", 1) + 1
                    p["ic"] = max(p["ic"], r.m6_ic)

    cfg["session_count"] = cfg.get("session_count", 0) + 1
    return cfg


# ──────────────────────────────────────────────────────────────────────────────
# Integrity checks for Phase C
# ──────────────────────────────────────────────────────────────────────────────

def _integrity_check(
    n_hypotheses: int,
    n_train_pass: int,
    n_val_pass: int,
    results: list[HypothesisResult],
) -> list[dict]:
    flags = []
    p_thresh = 0.05
    expected = n_hypotheses * p_thresh
    enrichment = n_train_pass / max(expected, 0.01)

    flags.append({
        "level": "INFO",
        "code":  "ENRICHMENT_STATS",
        "message": (
            f"Hypotheses={n_hypotheses}  expected_by_chance={expected:.1f}  "
            f"train={n_train_pass}  val={n_val_pass}  enrichment={enrichment:.2f}x"
        ),
    })

    if n_hypotheses > 20 and enrichment < 2.0:
        flags.append({
            "level": "WARN",
            "code":  "NEAR_CHANCE",
            "message": f"Enrichment={enrichment:.2f}x barely above random",
        })

    val_rate = n_val_pass / max(n_train_pass, 1)
    if val_rate > 0.60 and n_train_pass >= 5:
        flags.append({
            "level": "FAIL",
            "code":  "VAL_RATE_TOO_HIGH",
            "message": f"Val pass rate {val_rate:.1%} — suspicious, check data leakage",
        })

    # Sign flips
    sign_flips = [r for r in results if r.split == "val" and r.sign_flip]
    if sign_flips and len(sign_flips) / max(n_val_pass, 1) > 0.4:
        flags.append({
            "level": "WARN",
            "code":  "SIGN_FLIPS",
            "message": f"{len(sign_flips)} sign flips on val — regime dependency",
        })

    if n_train_pass == 0:
        flags.append({
            "level": "INFO",
            "code":  "ZERO_SIGNALS",
            "message": "No train signals — documents may not contain predictive info",
        })

    return flags


# ──────────────────────────────────────────────────────────────────────────────
# Report generator
# ──────────────────────────────────────────────────────────────────────────────

def _generate_report(
    session_id: str,
    hypotheses: list[RagHypothesis],
    results: list[HypothesisResult],
    flags: list[dict],
    elapsed: float,
    cfg: dict,
) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    train_pass = [r for r in results if r.split == "train" and r.ensemble_pass]
    val_pass   = [r for r in results if r.split == "val"   and r.ensemble_pass]
    errors     = [r for r in results if r.error]

    lines = [
        f"# Phase C Session Report — {session_id}",
        f"Generated: {ts}  |  Elapsed: {elapsed:.0f}s",
        "",
        "## Summary",
        "",
        f"| Метрика | Значение |",
        f"|---|---|",
        f"| Гипотез извлечено | {len(hypotheses)} |",
        f"| Train подтверждено | {len(train_pass)} |",
        f"| Val подтверждено | {len(val_pass)} |",
        f"| Ошибок при тесте | {len(errors)} |",
        f"| Elapsed | {elapsed:.0f}s |",
        "",
    ]

    # Integrity flags
    fail_flags = [f for f in flags if f["level"] == "FAIL"]
    warn_flags = [f for f in flags if f["level"] == "WARN"]
    info_flags = [f for f in flags if f["level"] == "INFO"]

    lines += ["## Integrity Flags", ""]
    for f in info_flags:
        lines.append(f"- INFO  [{f['code']}] {f['message']}")
    for f in warn_flags:
        lines.append(f"- WARN  [{f['code']}] {f['message']}")
    for f in fail_flags:
        lines.append(f"- FAIL  [{f['code']}] {f['message']}")
    lines.append("")

    # Val-confirmed signals
    if val_pass:
        lines += [
            "## Val-Confirmed Signals",
            "",
            "| Инструмент | Тема | Lag | M5 p | M6 IC | Источник | Обоснование |",
            "|---|---|---|---|---|---|---|",
        ]
        for r in sorted(val_pass, key=lambda x: -x.m6_ic):
            # Find rationale
            hyp = next(
                (h for h in hypotheses if h.hypothesis_id == r.hypothesis_id),
                None,
            )
            rationale = (hyp.rationale[:80] + "...") if hyp and hyp.rationale else "—"
            lines.append(
                f"| {r.instrument} | {r.topic} | {r.lag} "
                f"| {r.m5_pvalue:.4f} | {r.m6_ic:.4f} "
                f"| {r.source_label} | {rationale} |"
            )
        lines.append("")
    else:
        lines += ["## No Val-Confirmed Signals", ""]

    # All train signals
    if train_pass:
        lines += [
            "## Train Signals (not yet val-confirmed)",
            "",
            "| Инструмент | Тема | Lag | M5 p | M6 IC | Val | Источник |",
            "|---|---|---|---|---|---|---|",
        ]
        val_keys = {(r.instrument, r.topic, r.lag) for r in val_pass}
        for r in sorted(train_pass, key=lambda x: -x.m6_ic):
            key = (r.instrument, r.topic, r.lag)
            val_status = "HOLD" if key in val_keys else "fail"
            lines.append(
                f"| {r.instrument} | {r.topic} | {r.lag} "
                f"| {r.m5_pvalue:.4f} | {r.m6_ic:.4f} "
                f"| {val_status} | {r.source_label} |"
            )
        lines.append("")

    # Company leaderboard from config
    company_scores = cfg.get("company_scores", {})
    if company_scores:
        lines += [
            "## Company Leaderboard (cumulative)",
            "",
            "| Компания | Train signals | Val pass |",
            "|---|---|---|",
        ]
        for company, scores in sorted(
            company_scores.items(), key=lambda x: -x[1].get("val_pass", 0)
        ):
            lines.append(
                f"| {company} | {scores.get('signals', 0)} | {scores.get('val_pass', 0)} |"
            )
        lines.append("")

    lines += [
        "---",
        f"Session: {session_id}  |  Phase C  |  RAG+M5+M6",
    ]

    return "\n".join(lines)


def _update_findings(
    val_pass: list[HypothesisResult],
    hypotheses: list[RagHypothesis],
    session_id: str,
) -> None:
    """Append new val-confirmed signals to FINDINGS.md."""
    if not val_pass:
        return

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines = []
    if FINDINGS_PATH.exists():
        existing = FINDINGS_PATH.read_text(encoding="utf-8")
        # Replace the "пусто" placeholder if still there
        existing = existing.replace(
            "*(пусто — Phase C ещё не запускалась)*", ""
        )
        lines.append(existing.rstrip())
        lines.append("")

    lines.append(f"## Сессия {session_id} ({ts})")
    lines.append("")
    for r in sorted(val_pass, key=lambda x: -x.m6_ic):
        hyp = next(
            (h for h in hypotheses if h.hypothesis_id == r.hypothesis_id), None
        )
        rationale = hyp.rationale if hyp else "—"
        lines.append(
            f"- **{r.instrument} / {r.topic} / lag={r.lag}**  "
            f"IC={r.m6_ic:.4f}  M5_p={r.m5_pvalue:.4f}  "
            f"src={r.source_label}  "
            f"rationale: {rationale}"
        )
    lines.append("")
    FINDINGS_PATH.write_text("\n".join(lines), encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────────
# Main session
# ──────────────────────────────────────────────────────────────────────────────

def run_session(
    company: str = "",
    year: int = 0,
    extract: bool = False,
    dry_run: bool = False,
    filter_vault: bool = True,
    verbose: bool = True,
) -> dict:
    """Run one Phase C session. Returns stats dict."""
    session_id = datetime.now(timezone.utc).strftime("C_%Y%m%d_%H%M%S")
    start_time = time.time()

    def log(msg: str) -> None:
        if verbose:
            t = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"[{t}] {msg}")

    log(f"=== Phase C Session {session_id} ===")
    log(f"dry_run={dry_run}  extract={extract}  filter_vault={filter_vault}")

    cfg = _load_config()
    log(f"Session #{cfg['session_count'] + 1}  "
        f"best_pairs so far: {len(cfg['best_pairs'])}")

    # ── 1. Load or extract hypotheses ─────────────────────────────────────────
    if extract or not list((OUT_DIR / "hypotheses").glob("*.json")):
        log("Extracting hypotheses via RAG ...")
        companies = [company] if company else list(COMPANY_INSTRUMENT_MAP.keys())
        years     = [year]    if year     else YEARS
        hypotheses: list[RagHypothesis] = []
        for c in companies:
            for y in years:
                log(f"  {c}/{y} ...")
                hyps = extract_hypotheses(c, y, dry_run=dry_run, verbose=verbose)
                hypotheses.extend(hyps)
        log(f"Total extracted: {len(hypotheses)}")
    else:
        if company and year:
            hypotheses = load_hypotheses(company, year)
        else:
            hypotheses = load_all_hypotheses()
        log(f"Loaded {len(hypotheses)} existing hypotheses")

    if not hypotheses:
        log("No hypotheses — abort")
        return {"session_id": session_id, "hypotheses": 0, "train_pass": 0, "val_pass": 0}

    # ── 2. Obsidian vault filter ──────────────────────────────────────────────
    if filter_vault:
        before = len(hypotheses)
        hypotheses = filter_known_signals(hypotheses)
        log(f"Vault filter: {before} → {len(hypotheses)}")

    # Deduplicate by hypothesis_id
    seen_ids: set = set()
    unique_hyps: list[RagHypothesis] = []
    for h in hypotheses:
        if h.hypothesis_id not in seen_ids:
            seen_ids.add(h.hypothesis_id)
            unique_hyps.append(h)
    hypotheses = unique_hyps
    log(f"After dedup: {len(hypotheses)} unique hypotheses")

    # Log session start
    if not dry_run:
        _ledger_append({
            "type":       "session_start",
            "session_id": session_id,
            "n_hypotheses": len(hypotheses),
            "companies":  list({h.source_company for h in hypotheses}),
            "years":      sorted({h.source_year for h in hypotheses}),
        })

    # ── 3. Test all hypotheses ────────────────────────────────────────────────
    log(f"Testing {len(hypotheses)} hypotheses on Train + Val ...")
    results = test_batch(hypotheses, splits=("train", "val"), verbose=verbose)
    log(f"Testing complete: {len(results)} result records")

    # ── 4. Compute stats ──────────────────────────────────────────────────────
    train_pass = [r for r in results if r.split == "train" and r.ensemble_pass]
    val_pass   = [r for r in results if r.split == "val"   and r.ensemble_pass]

    log(f"Train confirmed: {len(train_pass)}")
    log(f"Val confirmed:   {len(val_pass)}")

    flags = _integrity_check(
        n_hypotheses = len(hypotheses) * len(hypotheses[0].lag_days) if hypotheses else 0,
        n_train_pass = len(train_pass),
        n_val_pass   = len(val_pass),
        results      = results,
    )

    elapsed = time.time() - start_time

    # ── 5. Write ledger ───────────────────────────────────────────────────────
    if not dry_run:
        # Log each result
        for r in results:
            _ledger_append({
                "type":          "result",
                "session_id":    session_id,
                **r.to_dict(),
            })

        # Log session end
        _ledger_append({
            "type":          "session_end",
            "session_id":    session_id,
            "n_hypotheses":  len(hypotheses),
            "n_results":     len(results),
            "n_train_pass":  len(train_pass),
            "n_val_pass":    len(val_pass),
            "elapsed_s":     round(elapsed, 1),
            "integrity_flags": flags,
        })

        # Update session config (Ouroboros)
        cfg = _update_config(cfg, results)
        _save_config(cfg)
        log("Session config updated")

        # Update FINDINGS.md
        _update_findings(val_pass, hypotheses, session_id)

    # ── 6. Generate report ────────────────────────────────────────────────────
    report = _generate_report(session_id, hypotheses, results, flags, elapsed, cfg)

    report_dir = OUT_DIR / "reports"
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / f"report_{session_id}.md"

    if not dry_run:
        report_path.write_text(report, encoding="utf-8")
        log(f"Report: {report_path}")

    print()
    print(report)

    stats = {
        "session_id":   session_id,
        "hypotheses":   len(hypotheses),
        "train_pass":   len(train_pass),
        "val_pass":     len(val_pass),
        "elapsed_s":    round(elapsed, 1),
        "flags":        flags,
    }

    log(f"=== Session {session_id} complete in {elapsed:.0f}s ===")
    return stats


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Phase C Runner")
    parser.add_argument("--extract",      action="store_true",
                        help="Re-extract hypotheses from RAG (default: load saved)")
    parser.add_argument("--company",      default="",  help="Single company")
    parser.add_argument("--year",         type=int, default=0, help="Single year")
    parser.add_argument("--dry-run",      action="store_true", help="No writes")
    parser.add_argument("--no-vault",     action="store_true",
                        help="Disable Obsidian vault filter")
    parser.add_argument("--quiet",        action="store_true", help="Less output")
    args = parser.parse_args()

    run_session(
        company      = args.company,
        year         = args.year,
        extract      = args.extract,
        dry_run      = args.dry_run,
        filter_vault = not args.no_vault,
        verbose      = not args.quiet,
    )


if __name__ == "__main__":
    main()
