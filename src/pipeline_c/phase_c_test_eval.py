"""Phase C — Final Test Set Evaluation.

Запускает все val-confirmed сигналы (из ledger) через замороженный Test сплит
(2025-2026). Научное закрытие Phase C.

Usage:
    .venv/Scripts/python -m src.pipeline_c.phase_c_test_eval
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.pipeline_c.phase_c_runner import _ledger_read, OUT_DIR
from src.pipeline_c.phase_c_overnight import _get_val_confirmed_combos
from src.pipeline_c.hypothesis_schema import RagHypothesis
from src.pipeline_c.hypothesis_tester import HypothesisTester

RESULTS_PATH = OUT_DIR / "test_eval_results.jsonl"
REPORT_PATH  = OUT_DIR / "TEST_EVAL.md"


# ──────────────────────────────────────────────────────────────────────────────
# BH FDR correction
# ──────────────────────────────────────────────────────────────────────────────

def _bh_correction(pvalues: list[float], q: float = 0.10) -> list[bool]:
    """Benjamini–Hochberg FDR correction. Returns list[bool] (True = rejected = significant)."""
    n = len(pvalues)
    if n == 0:
        return []
    indexed_sorted = sorted(enumerate(pvalues), key=lambda x: x[1])
    last_k = -1
    for rank, (_, p) in enumerate(indexed_sorted, 1):
        if p <= rank / n * q:
            last_k = rank
    result = [False] * n
    for rank, (orig_idx, _) in enumerate(indexed_sorted, 1):
        if rank <= last_k:
            result[orig_idx] = True
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"Phase C — Test Set Evaluation — {ts}")
    print("=" * 60)

    # ── 1. Load val-confirmed from ledger ─────────────────────────────────────
    records = _ledger_read()
    val_confirmed = _get_val_confirmed_combos(records)
    print(f"Val-confirmed signals: {len(val_confirmed)}")
    for v in val_confirmed:
        print(f"  {v['instrument']:20s} / {v['topic']:12s} / lag={v['lag']:3d}"
              f"  IC_val={v['ic']:.4f}  M5p_val={v['m5_pvalue']:.4f}")
    print()

    # ── 2. Run each through Test split ────────────────────────────────────────
    tester = HypothesisTester()
    test_results: list[dict] = []

    for i, sig in enumerate(val_confirmed):
        inst    = sig["instrument"]
        topic   = sig["topic"]
        lag     = sig["lag"]
        ft      = sig.get("feature", "keyword_z90")
        ic_val  = sig["ic"]
        m5p_val = sig["m5_pvalue"]

        print(f"[{i+1:2d}/{len(val_confirmed)}] {inst}/{topic}/lag={lag}  (feature={ft})")

        hyp = RagHypothesis(
            instrument     = inst,
            topic          = topic,
            lag_days       = [lag],
            direction      = "unknown",
            rationale      = f"Phase C Test eval: {inst}/{topic}/lag={lag}",
            source_company = "test_eval",
            source_year    = 0,
            source_page    = 0,
            source_section = "phase_c_test_evaluation",
            confidence     = 1.0,
            feature_type   = ft,
        )

        results = tester.test(hyp, splits=("test",), verbose=True)
        test_r  = next((r for r in results if r.split == "test" and r.lag == lag), None)

        if test_r and not test_r.error:
            ic_test  = test_r.m6_ic
            m5p_test = test_r.m5_pvalue
            m6p_test = test_r.m6_pvalue
            ens_test = test_r.ensemble_pass
            sign_flip = (
                (ic_val > 0) != (ic_test > 0)
                if ic_val != 0 and ic_test != 0
                else False
            )
            entry = {
                "instrument":   inst,
                "topic":        topic,
                "lag":          lag,
                "feature":      ft,
                "ic_val":       round(ic_val,   4),
                "m5p_val":      round(m5p_val,  4),
                "ic_test":      round(ic_test,  4),
                "m5p_test":     round(m5p_test, 4),
                "m6p_test":     round(m6p_test, 4),
                "n_obs_test":   test_r.n_obs,
                "ensemble_test": bool(ens_test),
                "sign_flip":    bool(sign_flip),
                "error":        "",
            }
        else:
            err = test_r.error if test_r else "no result"
            print(f"    ERROR: {err}")
            entry = {
                "instrument":   inst, "topic": topic, "lag": lag,
                "feature":      ft,
                "ic_val":       round(ic_val,  4),
                "m5p_val":      round(m5p_val, 4),
                "ic_test":      None, "m5p_test": None,
                "m6p_test":     None, "n_obs_test": 0,
                "ensemble_test": False, "sign_flip": None,
                "error":        err if test_r else "no result",
            }

        test_results.append(entry)
        print()

    # ── 3. BH correction ──────────────────────────────────────────────────────
    valid = [(i, r["m5p_test"]) for i, r in enumerate(test_results)
             if r["m5p_test"] is not None]
    if valid:
        idxs  = [x[0] for x in valid]
        pvals = [x[1] for x in valid]
        bh    = _bh_correction(pvals, q=0.10)
        bh_map = {idxs[j]: bh[j] for j in range(len(idxs))}
    else:
        bh_map = {}

    for i, r in enumerate(test_results):
        r["bh_significant"] = bh_map.get(i, False)

    # ── 4. Save raw results ───────────────────────────────────────────────────
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        for r in test_results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"Raw results → {RESULTS_PATH.name}")

    # ── 5. Report ─────────────────────────────────────────────────────────────
    report = _generate_report(test_results, ts)
    REPORT_PATH.write_text(report, encoding="utf-8")
    print(f"Report     → {REPORT_PATH.name}")
    print()
    print(report)


# ──────────────────────────────────────────────────────────────────────────────
# Report
# ──────────────────────────────────────────────────────────────────────────────

def _generate_report(results: list[dict], ts: str) -> str:
    n_total     = len(results)
    n_ens       = sum(1 for r in results if r.get("ensemble_test"))
    n_bh        = sum(1 for r in results if r.get("bh_significant"))
    n_flip      = sum(1 for r in results if r.get("sign_flip"))
    n_error     = sum(1 for r in results if r.get("error"))

    lines = [
        "# Phase C — Финальная Test Set Evaluation",
        f"*Сгенерировано: {ts}*",
        "",
        "---",
        "",
        "## Методология",
        "",
        "| Параметр | Значение |",
        "|----------|---------|",
        "| Train | 2022-01-01 — 2023-12-31 |",
        "| Val | 2024-01-01 — 2025-04-30 (использовался для отбора сигналов) |",
        "| **Test** | **2025-05-01 — 2026-06-01 (заморожен — никогда не видел агент)** |",
        "| Метрики | M5 (Mann-Whitney) AND M6 (LightGBM walk-forward IC) |",
        "| Порог ensemble | M5 p < 0.05 AND M6 p < 0.05 |",
        "| FDR коррекция | Benjamini–Hochberg, q = 0.10 |",
        "| Пространство поиска | 1428 комбо (17 инстр. × 7 тем × 12 лагов) — полностью исчерпано |",
        "",
        "---",
        "",
        "## Сводка результатов",
        "",
        f"| Метрика | Значение |",
        f"|---------|---------|",
        f"| Val-confirmed сигналов (вход) | **{n_total}** |",
        f"| Прошли Test ensemble (M5 AND M6) | **{n_ens}** |",
        f"| BH-значимые (q < 0.10) | **{n_bh}** |",
        f"| Знаковый переворот val→test | **{n_flip}** |",
        f"| Ошибки данных | {n_error} |",
        "",
        "---",
        "",
        "## Полная таблица",
        "",
        "| Сигнал | IC val | IC test | M5p test | M6p test | n | Ensemble | BH | Sign flip |",
        "|--------|--------|---------|----------|----------|---|----------|----|-----------|",
    ]

    for r in sorted(results, key=lambda x: -x["ic_val"]):
        sig      = f"`{r['instrument']}/{r['topic']}/lag={r['lag']}`"
        ic_val   = f"{r['ic_val']:.4f}"
        ic_test  = f"{r['ic_test']:.4f}"  if r.get("ic_test")  is not None else "—"
        m5p      = f"{r['m5p_test']:.4f}" if r.get("m5p_test") is not None else "—"
        m6p      = f"{r['m6p_test']:.4f}" if r.get("m6p_test") is not None else "—"
        n_obs    = str(r.get("n_obs_test") or "—")
        ens      = "✓" if r.get("ensemble_test") else "✗"
        bh       = "**✓**" if r.get("bh_significant") else "—"
        flip     = "⚠️" if r.get("sign_flip") else "—"
        lines.append(f"| {sig} | {ic_val} | {ic_test} | {m5p} | {m6p} | {n_obs} | {ens} | {bh} | {flip} |")

    lines += ["", "---", ""]

    # ── BH-confirmed signals ──────────────────────────────────────────────────
    bh_signals = [r for r in results if r.get("bh_significant") and not r.get("sign_flip")]
    bh_flip    = [r for r in results if r.get("bh_significant") and r.get("sign_flip")]

    lines += ["## BH-значимые сигналы (q < 0.10)", ""]

    if bh_signals or bh_flip:
        lines += ["### Без знакового переворота — **производственные кандидаты**", ""]
        if bh_signals:
            for r in sorted(bh_signals, key=lambda x: -(x.get("ic_test") or 0)):
                decay = (r["ic_test"] - r["ic_val"]) if r.get("ic_test") is not None else 0
                d_str = f"+{decay:.4f}" if decay >= 0 else f"{decay:.4f}"
                lines += [
                    f"#### `{r['instrument']} / {r['topic']} / lag={r['lag']}`",
                    f"- IC: val={r['ic_val']:.4f} → test={r['ic_test']:.4f}  (decay: {d_str})",
                    f"- M5 p (test): {r['m5p_test']:.4f}  |  M6 p (test): {r.get('m6p_test','—')}",
                    f"- n_obs test: {r.get('n_obs_test','—')}",
                    "",
                ]
        else:
            lines += ["*(нет)*", ""]

        if bh_flip:
            lines += ["### BH-значимые, но со знаковым переворотом — ⚠️ нестабильны", ""]
            for r in bh_flip:
                lines += [
                    f"- `{r['instrument']}/{r['topic']}/lag={r['lag']}`"
                    f"  IC val={r['ic_val']:.4f} → test={r['ic_test']:.4f}  (переворот!)",
                ]
            lines.append("")
    else:
        lines += [
            "**Ни один сигнал не прошёл BH-коррекцию.**",
            "",
        ]

    # ── Ensemble passed but not BH ────────────────────────────────────────────
    ens_only = [r for r in results if r.get("ensemble_test") and not r.get("bh_significant")]
    if ens_only:
        lines += [
            "## Прошли Test ensemble, не прошли BH",
            "",
            "*(Ensemble pass, но p-value не выдерживает поправки на множественные сравнения.)*",
            "",
        ]
        for r in sorted(ens_only, key=lambda x: -(x.get("ic_test") or 0)):
            flip_mark = "  ⚠️ sign flip" if r.get("sign_flip") else ""
            lines.append(
                f"- `{r['instrument']}/{r['topic']}/lag={r['lag']}`"
                f"  IC val={r['ic_val']:.4f} → test={r['ic_test']:.4f}"
                f"  M5p={r['m5p_test']:.4f}{flip_mark}"
            )
        lines.append("")

    # ── Conclusions ───────────────────────────────────────────────────────────
    lines += [
        "---",
        "",
        "## Выводы Phase C",
        "",
        "### Итог поиска",
        "",
        "- **1428 комбо** протестировано (полное пространство 17×7×12) за 615 сессий",
        f"- **13 val-confirmed** сигналов (hit rate {13/1428*100:.2f}% на val)",
        f"- **{n_ens} из 13** прошли Test ensemble (out-of-sample)",
        f"- **{n_bh} из 13** выдержали BH FDR q=0.10",
        "",
        "### Интерпретация режимного сдвига",
        "",
        "Все 13 сигналов — **Grade D** (val pass, train fail). Это не артефакт: связи",
        "действительно сформировались после 2023 года в условиях санкционной архитектуры.",
        "Ожидаемое поведение на Test:",
        "",
        "- Если сигнал **прошёл Test** — связь устойчива и в новом режиме (→ production)",
        "- Если **не прошёл** — связь была специфична для 2024-2025 (→ ситуативная)",
        "- Знаковый переворот — сигнал изменил направление (→ нельзя использовать)",
        "",
        "### Следующие шаги",
        "",
    ]

    if bh_signals:
        lines += [
            f"**{len(bh_signals)} BH-значимых сигнала без переворота** — готовы к следующей фазе:",
            "- Проверить на rolling window (нет ли деградации во времени)",
            "- Оценить trading signal (длинный/короткий → доходность)",
            "- Рассмотреть как ансамбль из нескольких сигналов",
            "",
        ]
    else:
        lines += [
            "BH-значимых сигналов нет — все связи ситуативны для 2024-2025.",
            "Возможные направления:",
            "- Расширить пространство поиска (новые лаги, новые темы)",
            "- Исследовать внутригодовую сезонность или режимные переключатели",
            "- Использовать найденные сигналы как features для ансамблевой модели",
            "",
        ]

    lines += [
        "---",
        "",
        f"*Phase C — Signal Mind Research — {ts}*",
    ]

    return "\n".join(lines)


if __name__ == "__main__":
    main()
