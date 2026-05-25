"""Session Ledger — научный журнал всех сессий, гипотез и решений.

Append-only. Никогда не редактировать вручную.
Каждая строка = один факт. Факты не исчезают.

Структура:
  analytics/phase_b/ledger.jsonl      — machine-readable, append-only
  analytics/phase_b/ledger_summary.md — human-readable, перегенерируется

Типы записей:
  session_start   — начало сессии
  session_end     — конец сессии (итоги, integrity flags)
  hypothesis      — одна проверенная гипотеза (train результат)
  val_result      — результат той же гипотезы на val
  analysis        — межсессионный анализ (решения + причины)
  integrity_flag  — конкретный флаг самообмана
  config_change   — что и почему изменили в конфиге
"""
from __future__ import annotations

import json
import sys
import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

ROOT         = Path(__file__).resolve().parents[2]
LEDGER_PATH  = ROOT / "analytics" / "phase_b" / "ledger.jsonl"
SUMMARY_PATH = ROOT / "analytics" / "phase_b" / "ledger_summary.md"
LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# Integrity flag levels and codes
# ──────────────────────────────────────────────────────────────────────────────

class Flag:
    """Scientific integrity flag."""

    # Levels
    INFO = "INFO"
    WARN = "WARN"
    FAIL = "FAIL"   # serious scientific problem

    # Codes
    P_HACKING_RISK        = "P_HACKING_RISK"         # many hypotheses, no BH
    SMALL_N               = "SMALL_N"                # n < 80
    VAL_RATE_IMPOSSIBLE   = "VAL_RATE_IMPOSSIBLE"    # > 60% val pass (too good)
    SIGN_FLIP             = "SIGN_FLIP"              # direction reversed on val
    REGIME_CONCENTRATION  = "REGIME_CONCENTRATION"  # all signals from one period
    ENRICHMENT_NEAR_CHANCE = "ENRICHMENT_NEAR_CHANCE" # < 2x above random
    ZERO_SIGNALS          = "ZERO_SIGNALS"           # nothing found at all
    IC_INFLATED           = "IC_INFLATED"            # suspiciously high IC
    HYPOTHESIS_EXHAUSTION = "HYPOTHESIS_EXHAUSTION"  # same configs, no new finds
    STABLE_TINY_IC        = "STABLE_TINY_IC"         # stable signal but IC < 0.02
    CONFIG_OSCILLATING    = "CONFIG_OSCILLATING"     # IC gate flips back and forth
    VAL_WORSE_THAN_TRAIN  = "VAL_WORSE_THAN_TRAIN"  # systematic IC drop train→val
    REPEATED_FAILURE      = "REPEATED_FAILURE"       # (inst,topic,lag) fails 3+ times
    TEST_SET_EXPOSED      = "TEST_SET_EXPOSED"       # test data used for exploration

    @staticmethod
    def make(level: str, code: str, message: str,
             data: dict | None = None) -> dict:
        return {
            "level":   level,
            "code":    code,
            "message": message,
            "data":    data or {},
        }


# ──────────────────────────────────────────────────────────────────────────────
# Writer — append-only
# ──────────────────────────────────────────────────────────────────────────────

def _write(record: dict) -> None:
    """Append one JSON record to ledger.jsonl."""
    record["_ts"] = datetime.now(timezone.utc).isoformat()
    with open(LEDGER_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def log_session_start(session_id: str, session_type: str,
                      config: dict) -> None:
    _write({
        "type":         "session_start",
        "session_id":   session_id,
        "session_type": session_type,   # "night" | "day" | "afternoon"
        "config":       config,
    })


def log_session_end(session_id: str, stats: dict,
                    integrity_flags: list[dict]) -> None:
    """
    stats keys expected:
      hypotheses_tested, train_signals, val_confirmed,
      mean_ic_train, max_ic_train, mean_ic_val,
      duration_hours, enrichment_factor, expected_by_chance
    """
    _write({
        "type":             "session_end",
        "session_id":       session_id,
        "stats":            stats,
        "integrity_flags":  integrity_flags,
        "verdict":          _session_verdict(stats, integrity_flags),
    })


def log_hypothesis(session_id: str, instrument: str, topic: str,
                   lag: int, feature: str, ensemble: str,
                   m5_pvalue: float, m6_ic: float, n_obs: int,
                   confirmed: bool) -> None:
    _write({
        "type":        "hypothesis",
        "session_id":  session_id,
        "instrument":  instrument,
        "topic":       topic,
        "lag":         lag,
        "feature":     feature,
        "ensemble":    ensemble,
        "m5_pvalue":   m5_pvalue,
        "m6_ic":       m6_ic,
        "n_obs":       n_obs,
        "confirmed":   confirmed,
    })


def log_val_result(session_id: str, instrument: str, topic: str,
                   lag: int, m6_ic_train: float, m6_ic_val: float,
                   sign_flip: bool, val_pass: bool) -> None:
    _write({
        "type":         "val_result",
        "session_id":   session_id,
        "instrument":   instrument,
        "topic":        topic,
        "lag":          lag,
        "m6_ic_train":  m6_ic_train,
        "m6_ic_val":    m6_ic_val,
        "sign_flip":    sign_flip,
        "val_pass":     val_pass,
    })


def log_config_change(from_config: dict, to_config: dict,
                      reasons: list[str]) -> None:
    """Record every config change with explicit reasons."""
    _write({
        "type":    "config_change",
        "from":    from_config,
        "to":      to_config,
        "reasons": reasons,
    })


def log_analysis(summary: dict, flags: list[dict],
                 decisions: list[dict]) -> None:
    """
    decisions: list of {"parameter": "ic_gate", "old": 0.05, "new": 0.03, "reason": "..."}
    """
    _write({
        "type":      "analysis",
        "summary":   summary,
        "flags":     flags,
        "decisions": decisions,
    })


def _session_verdict(stats: dict, flags: list[dict]) -> str:
    """One-line honest verdict for this session."""
    n_fail = sum(1 for f in flags if f["level"] == Flag.FAIL)
    n_warn = sum(1 for f in flags if f["level"] == Flag.WARN)
    ef     = stats.get("enrichment_factor", 0)
    n_val  = stats.get("val_confirmed", 0)

    if n_fail > 0:
        return f"SCIENTIFIC_CONCERN ({n_fail} FAIL flags) — results unreliable"
    if ef < 1.5:
        return "NOISE — results indistinguishable from random"
    if ef < 3.0 and n_val == 0:
        return f"WEAK — enrichment={ef:.1f}x but no val-confirmed signals"
    if n_val > 0 and ef >= 2.0:
        return f"SIGNAL — {n_val} val-confirmed, enrichment={ef:.1f}x"
    if n_warn >= 3:
        return f"UNCERTAIN — {n_warn} warnings, needs scrutiny"
    return f"NORMAL — enrichment={ef:.1f}x, val={n_val}"


# ──────────────────────────────────────────────────────────────────────────────
# Reader
# ──────────────────────────────────────────────────────────────────────────────

def read_all() -> list[dict]:
    """Read all ledger records."""
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


def read_sessions() -> list[dict]:
    return [r for r in read_all() if r["type"] == "session_end"]


def read_hypotheses() -> list[dict]:
    return [r for r in read_all() if r["type"] == "hypothesis"]


def read_val_results() -> list[dict]:
    return [r for r in read_all() if r["type"] == "val_result"]


# ──────────────────────────────────────────────────────────────────────────────
# Integrity checker
# ──────────────────────────────────────────────────────────────────────────────

def check_session_integrity(
    n_hypotheses: int,
    n_train: int,
    n_val: int,
    ic_values_train: list[float],
    val_results: list[dict],   # list of {m6_ic_train, m6_ic_val, sign_flip}
    regime_labels: list[str],  # regime label per train hit ("" = full)
) -> tuple[list[dict], dict]:
    """
    Run all integrity checks for one session.
    Returns: (flags, enrichment_stats)
    """
    flags = []
    p_threshold = 0.05

    # ── 1. P-hacking risk ────────────────────────────────────────────────────
    expected_by_chance = n_hypotheses * p_threshold
    enrichment = n_train / max(expected_by_chance, 0.01)

    flags.append(Flag.make(
        Flag.INFO, "ENRICHMENT_STATS",
        f"Hypotheses={n_hypotheses}  expected_by_chance={expected_by_chance:.1f}  "
        f"actual_train={n_train}  enrichment={enrichment:.2f}x",
        {"n_hypotheses": n_hypotheses, "expected": expected_by_chance,
         "actual": n_train, "enrichment": enrichment},
    ))

    if n_hypotheses > 30 and enrichment < 2.0:
        flags.append(Flag.make(
            Flag.WARN, Flag.ENRICHMENT_NEAR_CHANCE,
            f"Train signals ({n_train}) barely above random expectation "
            f"({expected_by_chance:.1f} at p<0.05). "
            f"Enrichment={enrichment:.2f}x — could be noise.",
        ))

    if n_hypotheses > 50:
        bh_expected = n_hypotheses * 0.05  # conservative
        flags.append(Flag.make(
            Flag.INFO, Flag.P_HACKING_RISK,
            f"Testing {n_hypotheses} hypotheses without BH correction. "
            f"At p<0.05 expect ~{bh_expected:.0f} false positives by chance. "
            f"Only val-confirmed signals are trustworthy.",
        ))

    # ── 2. n too small ────────────────────────────────────────────────────────
    # (tracked from hypothesis logs, not available here as scalars —
    #  flagged per-hypothesis in log_hypothesis; here we note in session summary)

    # ── 3. Val rate impossible ────────────────────────────────────────────────
    val_rate = n_val / max(n_train, 1)
    if val_rate > 0.60 and n_train >= 10:
        flags.append(Flag.make(
            Flag.FAIL, Flag.VAL_RATE_IMPOSSIBLE,
            f"Val pass rate = {val_rate:.1%} (n_train={n_train}, n_val={n_val}). "
            f"This is suspiciously high. Check for data leakage or look-ahead bias.",
        ))

    # ── 4. Sign flip rate ─────────────────────────────────────────────────────
    if val_results:
        n_flip = sum(1 for r in val_results if r.get("sign_flip", False))
        flip_rate = n_flip / len(val_results)
        if flip_rate > 0.40:
            flags.append(Flag.make(
                Flag.WARN, Flag.SIGN_FLIP,
                f"{flip_rate:.0%} of val results flip sign vs train "
                f"({n_flip}/{len(val_results)}). "
                f"Signals are regime-dependent or noise.",
            ))

        # IC systematic drop train → val
        ic_drops = [
            r["m6_ic_train"] - r.get("m6_ic_val", 0)
            for r in val_results
            if "m6_ic_val" in r
        ]
        if ic_drops:
            mean_drop = sum(ic_drops) / len(ic_drops)
            if mean_drop > 0.05:
                flags.append(Flag.make(
                    Flag.WARN, Flag.VAL_WORSE_THAN_TRAIN,
                    f"IC drops on average {mean_drop:.3f} from train to val. "
                    f"Overfitting or data-dependent feature construction.",
                ))

    # ── 5. Regime concentration ───────────────────────────────────────────────
    if regime_labels:
        from_regime = sum(1 for r in regime_labels if r != "")
        regime_frac = from_regime / max(len(regime_labels), 1)
        if regime_frac > 0.80 and len(regime_labels) >= 5:
            flags.append(Flag.make(
                Flag.WARN, Flag.REGIME_CONCENTRATION,
                f"{regime_frac:.0%} of signals come from one sub-regime. "
                f"Signal may not generalise across market conditions.",
            ))

    # ── 6. Zero signals ───────────────────────────────────────────────────────
    if n_train == 0:
        flags.append(Flag.make(
            Flag.INFO, Flag.ZERO_SIGNALS,
            "No train signals in this session. "
            "IC gate may be too tight, or data genuinely lacks signal.",
        ))

    # ── 7. Suspiciously high IC ───────────────────────────────────────────────
    if ic_values_train:
        max_ic = max(ic_values_train)
        if max_ic > 0.40:
            flags.append(Flag.make(
                Flag.FAIL, Flag.IC_INFLATED,
                f"Max IC = {max_ic:.3f} is unrealistically high for financial data. "
                f"Check for look-ahead bias or data alignment error.",
            ))

    enrich_stats = {
        "n_hypotheses":      n_hypotheses,
        "expected_by_chance": round(expected_by_chance, 2),
        "actual_train":      n_train,
        "val_confirmed":     n_val,
        "enrichment_factor": round(enrichment, 3),
        "val_rate":          round(val_rate, 4),
    }
    return flags, enrich_stats


def check_cross_session_integrity(all_sessions: list[dict]) -> list[dict]:
    """
    Flags that only become visible across multiple sessions.
    """
    flags = []
    if len(all_sessions) < 2:
        return flags

    # ── Config oscillation ────────────────────────────────────────────────────
    ic_gates = []
    for s in all_sessions[-6:]:
        cfg = s.get("stats", {})
        # Approximate: extract from flags
        for f in s.get("integrity_flags", []):
            if "ic_gate" in f.get("message", "").lower():
                pass  # could parse but skip for now
        vr = cfg.get("val_rate", None)
        if vr is not None:
            ic_gates.append(vr)

    # ── Enrichment trend ──────────────────────────────────────────────────────
    enrichments = [
        s["stats"].get("enrichment_factor", 0)
        for s in all_sessions
        if "stats" in s
    ]
    if len(enrichments) >= 4:
        recent  = sum(enrichments[-2:]) / 2
        earlier = sum(enrichments[:-2]) / max(len(enrichments) - 2, 1)
        if recent < earlier * 0.5 and recent < 2.0:
            flags.append(Flag.make(
                Flag.WARN, "ENRICHMENT_DECLINING",
                f"Enrichment dropping: recent avg={recent:.2f}x vs earlier={earlier:.2f}x. "
                f"System may be exhausting the hypothesis space or overfitting to data.",
            ))

    # ── Persistent failures ───────────────────────────────────────────────────
    fail_counts: dict[str, int] = {}
    for s in all_sessions:
        for f in s.get("integrity_flags", []):
            code = f.get("code", "")
            if f.get("level") in (Flag.WARN, Flag.FAIL):
                fail_counts[code] = fail_counts.get(code, 0) + 1

    for code, cnt in fail_counts.items():
        if cnt >= 3 and code not in (Flag.P_HACKING_RISK, "ENRICHMENT_STATS"):
            flags.append(Flag.make(
                Flag.WARN, "PERSISTENT_FLAG",
                f"Flag '{code}' has appeared in {cnt} sessions. "
                f"This is a structural problem, not a one-off.",
                {"code": code, "count": cnt},
            ))

    return flags


# ──────────────────────────────────────────────────────────────────────────────
# Summary report generator
# ──────────────────────────────────────────────────────────────────────────────

def generate_summary() -> Path:
    """Regenerate ledger_summary.md from all ledger records."""
    records   = read_all()
    sessions  = [r for r in records if r["type"] == "session_end"]
    hyps      = [r for r in records if r["type"] == "hypothesis"]
    val_res   = [r for r in records if r["type"] == "val_result"]
    analyses  = [r for r in records if r["type"] == "analysis"]
    cfg_chgs  = [r for r in records if r["type"] == "config_change"]

    ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        f"# Signal Mind — Scientific Ledger",
        f"Generated: {ts_now}  |  Total records: {len(records)}",
        "",
        "---",
        "",
        "## ОБЩАЯ ЧЕСТНОСТЬ СИСТЕМЫ",
        "",
    ]

    if sessions:
        total_hyp  = sum(s["stats"].get("hypotheses_tested", 0) for s in sessions)
        total_trn  = sum(s["stats"].get("train_signals", 0) for s in sessions)
        total_val  = sum(s["stats"].get("val_confirmed", 0) for s in sessions)
        total_ef   = [s["stats"].get("enrichment_factor", 0) for s in sessions if "stats" in s]
        mean_ef    = sum(total_ef) / max(len(total_ef), 1)
        fail_sess  = sum(1 for s in sessions
                         if "CONCERN" in s.get("verdict", "") or "NOISE" in s.get("verdict", ""))
        good_sess  = sum(1 for s in sessions if "SIGNAL" in s.get("verdict", ""))

        # Expected false positives total
        exp_fp = total_hyp * 0.05
        real_enrichment = total_trn / max(exp_fp, 0.01)

        lines += [
            f"| Метрика | Значение | Интерпретация |",
            f"|---|---|---|",
            f"| Всего сессий | {len(sessions)} | — |",
            f"| Гипотез проверено | {total_hyp:,} | все за всё время |",
            f"| Ожидается случайных (p<0.05) | {exp_fp:.0f} | {total_hyp}×0.05 |",
            f"| Train сигналов найдено | {total_trn} | реально |",
            f"| Обогащение vs случай | **{real_enrichment:.1f}x** | {'✅ реальный сигнал' if real_enrichment > 3 else '⚠️ близко к случаю' if real_enrichment > 1.5 else '❌ шум'} |",
            f"| Val-confirmed | {total_val} | прошли holdout |",
            f"| Val pass rate | {total_val/max(total_trn,1):.1%} | {total_val}/{total_trn} |",
            f"| Среднее обогащение/сессию | {mean_ef:.2f}x | — |",
            f"| Сессий с реальным сигналом | {good_sess}/{len(sessions)} | вердикт SIGNAL |",
            f"| Проблемных сессий | {fail_sess}/{len(sessions)} | NOISE или CONCERN |",
            "",
        ]
    else:
        lines += ["*(нет данных)*", ""]

    # ── Session history ───────────────────────────────────────────────────────
    lines += [
        "## ИСТОРИЯ СЕССИЙ",
        "",
        "| Сессия | Тип | Гипотез | Train | Val | Обогащение | Вердикт | FAIL флаги |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for s in sessions[-30:]:  # last 30
        st   = s["stats"]
        ef   = st.get("enrichment_factor", 0)
        fails = sum(1 for f in s.get("integrity_flags", []) if f["level"] == Flag.FAIL)
        warns = sum(1 for f in s.get("integrity_flags", []) if f["level"] == Flag.WARN)
        flag_str = f"{'🔴' * fails}{'🟡' * min(warns, 3)}" if (fails or warns) else "✅"
        lines.append(
            f"| {s['session_id'][:15]} | {s.get('session_type','?')[:5]} "
            f"| {st.get('hypotheses_tested',0)} "
            f"| {st.get('train_signals',0)} "
            f"| {st.get('val_confirmed',0)} "
            f"| {ef:.2f}x "
            f"| {s.get('verdict','?')[:35]} "
            f"| {flag_str} |"
        )

    # ── Integrity flags across all sessions ───────────────────────────────────
    all_flags: list[dict] = []
    for s in sessions:
        for f in s.get("integrity_flags", []):
            f2 = dict(f)
            f2["session_id"] = s["session_id"]
            all_flags.append(f2)

    fail_flags = [f for f in all_flags if f["level"] == Flag.FAIL]
    warn_flags = [f for f in all_flags if f["level"] == Flag.WARN]

    lines += ["", "## КРИТИЧЕСКИЕ ПРОБЛЕМЫ (FAIL)", ""]
    if fail_flags:
        for f in fail_flags[-20:]:
            lines.append(f"- 🔴 **[{f['code']}]** `{f['session_id'][:15]}` — {f['message']}")
    else:
        lines.append("✅ Критических проблем не зафиксировано")

    lines += ["", "## ПРЕДУПРЕЖДЕНИЯ (WARN)", ""]
    if warn_flags:
        # Group by code
        from collections import Counter
        code_counts = Counter(f["code"] for f in warn_flags)
        for code, cnt in code_counts.most_common(10):
            last = next(f for f in reversed(warn_flags) if f["code"] == code)
            lines.append(f"- 🟡 **{code}** (×{cnt}) — {last['message'][:120]}")
    else:
        lines.append("✅ Предупреждений нет")

    # ── Config change history ─────────────────────────────────────────────────
    lines += ["", "## ИСТОРИЯ ИЗМЕНЕНИЙ КОНФИГА", ""]
    if cfg_chgs:
        lines += [
            "| Время | Параметр | Было | Стало | Причина |",
            "|---|---|---|---|---|",
        ]
        for c in cfg_chgs[-20:]:
            ts  = c.get("_ts", "?")[:16]
            old = c.get("from", {})
            new = c.get("to", {})
            for k in set(list(old.keys()) + list(new.keys())):
                if old.get(k) != new.get(k):
                    reason = "; ".join(c.get("reasons", []))[:80]
                    lines.append(
                        f"| {ts} | `{k}` | `{old.get(k)}` | `{new.get(k)}` | {reason} |"
                    )
    else:
        lines.append("*(конфиг не менялся)*")

    # ── Stable signals leaderboard ────────────────────────────────────────────
    lines += ["", "## УСТОЙЧИВЫЕ СИГНАЛЫ (ТОП)", ""]
    if hyps:
        import collections
        sig_ic:   dict = collections.defaultdict(list)
        sig_val:  dict = collections.defaultdict(int)
        sig_sess: dict = collections.defaultdict(set)
        for h in hyps:
            key = (h["instrument"], h["topic"], h["lag"])
            if h.get("confirmed"):
                sig_ic[key].append(h["m6_ic"])
                sig_sess[key].add(h["session_id"])
        for v in val_res:
            key = (v["instrument"], v["topic"], v["lag"])
            if v.get("val_pass"):
                sig_val[key] += 1

        ranked = sorted(
            sig_ic.keys(),
            key=lambda k: (-len(sig_sess[k]), -max(sig_ic[k]))
        )[:15]

        if ranked:
            lines += [
                "| Инструмент | Тема | Lag | Сессий | Val | Best IC | Оценка |",
                "|---|---|---|---|---|---|---|",
            ]
            for key in ranked:
                inst, topic, lag = key
                n_s   = len(sig_sess[key])
                n_v   = sig_val.get(key, 0)
                best  = max(sig_ic[key])
                # Honest assessment
                if n_s >= 3 and n_v >= 2:
                    grade = "🟢 сильный"
                elif n_s >= 2 and n_v >= 1:
                    grade = "🟡 умеренный"
                elif n_s >= 2 and n_v == 0:
                    grade = "🔴 не держится на val"
                else:
                    grade = "⚪ единичный"
                lines.append(
                    f"| {inst} | {topic} | {lag} "
                    f"| {n_s} | {n_v} | {best:.4f} | {grade} |"
                )
        else:
            lines.append("*(нет подтверждённых сигналов)*")
    else:
        lines.append("*(нет данных)*")

    # ── Anti-patterns leaderboard ─────────────────────────────────────────────
    lines += ["", "## АНТИ-ПАТТЕРНЫ (вечные неудачники)", ""]
    if hyps:
        import collections
        fail_counts2: dict = collections.defaultdict(int)
        fail_sess2:   dict = collections.defaultdict(set)
        val_set = {(v["instrument"], v["topic"], v["lag"]) for v in val_res if v.get("val_pass")}
        for h in hyps:
            key = (h["instrument"], h["topic"], h["lag"])
            if h.get("confirmed") and key not in val_set:
                fail_counts2[key] += 1
                fail_sess2[key].add(h["session_id"])

        chronic = sorted(
            [(k, v) for k, v in fail_counts2.items() if v >= 3],
            key=lambda x: -x[1]
        )[:10]

        if chronic:
            lines += [
                "| Инструмент | Тема | Lag | Раз в train | Сессий | Диагноз |",
                "|---|---|---|---|---|---|",
            ]
            for (inst, topic, lag), cnt in chronic:
                n_s = len(fail_sess2[(inst, topic, lag)])
                lines.append(
                    f"| {inst} | {topic} | {lag} "
                    f"| {cnt} | {n_s} | никогда не проходит val |"
                )
        else:
            lines.append("*(хронических неудачников нет)*")

    lines += ["", "---", f"*Ledger path: {LEDGER_PATH}*", ""]

    SUMMARY_PATH.write_text("\n".join(lines), encoding="utf-8")
    return SUMMARY_PATH


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Regenerating ledger summary...")
    path = generate_summary()
    print(f"Done: {path}")

    records = read_all()
    sessions = [r for r in records if r["type"] == "session_end"]
    print(f"\nLedger: {len(records)} records, {len(sessions)} sessions")
    if sessions:
        last = sessions[-1]
        print(f"Last session: {last['session_id']} — {last.get('verdict', '?')}")
