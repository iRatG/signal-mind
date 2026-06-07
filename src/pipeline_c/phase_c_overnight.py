"""Phase C — Ночной уроборос.

Логика:
  Итерация 0 (только первый раз): полная экстракция гипотез из всех компаний/лет.
  Итерация N:
    1. Читаем ledger — узнаём какие (hypothesis_id, lag) уже тестировались.
    2. Из session_config узнаём какие пары (inst, topic) провалились 3+ раза → skip.
    3. Берём только НОВЫЕ нетестированные гипотезы.
    4. Если новых нет → Ouroboros-генерация: DeepSeek читает результаты и предлагает
       новые гипотезы (другие компании, другие углы, другие лаги).
    5. Тестируем на Train + Val. Никогда не трогаем Test.
    6. Обновляем session_config_c.json.
    7. Спим 15 минут.
    8. Повторяем до 05:30.

Usage:
    .venv/Scripts/python -m src.pipeline_c.phase_c_overnight
    .venv/Scripts/python -m src.pipeline_c.phase_c_overnight --stop-at 04:00
    .venv/Scripts/python -m src.pipeline_c.phase_c_overnight --iterations 3    # для теста
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                  line_buffering=True)

from src.pipeline_c.hypothesis_schema import (
    RagHypothesis, VALID_INSTRUMENTS, VALID_TOPICS,
)
from src.pipeline_c.rag_extractor import (
    extract_hypotheses, load_all_hypotheses,
    COMPANY_INSTRUMENT_MAP, YEARS,
)
from src.pipeline_c.hypothesis_tester import test_batch
from src.pipeline_c.phase_c_runner import (
    _ledger_append, _ledger_read, _load_config, _save_config,
    _update_config, _update_findings, _integrity_check, _generate_report,
    OUT_DIR, LEDGER_PATH,
)
from src.agent.llm import chat

REPORT_DIR = OUT_DIR / "reports"
REPORT_DIR.mkdir(exist_ok=True)
HYPS_DIR   = OUT_DIR / "hypotheses"
HYPS_DIR.mkdir(exist_ok=True)
LOCK_FILE  = OUT_DIR / ".phase_c_lock"

SLEEP_BETWEEN_ITERATIONS = 15 * 60   # 15 минут
DEFAULT_STOP_HOUR = 5
DEFAULT_STOP_MIN  = 30
MAX_FAILURES_BEFORE_SKIP = 5


# ──────────────────────────────────────────────────────────────────────────────
# Эксклюзивный лок — только один экземпляр одновременно
# ──────────────────────────────────────────────────────────────────────────────

def _pid_alive(pid: int) -> bool:
    """Проверяет жив ли процесс с данным PID (кросс-платформенно)."""
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, OSError):
        return False


def _acquire_lock() -> bool:
    """Пытается занять лок. Возвращает True если успешно."""
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip())
            if _pid_alive(pid):
                return False   # другой экземпляр живёт
        except Exception:
            pass               # протухший лок — перезаписываем
    LOCK_FILE.write_text(str(os.getpid()))
    return True


def _release_lock() -> None:
    try:
        if LOCK_FILE.exists():
            pid = int(LOCK_FILE.read_text().strip())
            if pid == os.getpid():   # освобождаем только свой лок
                LOCK_FILE.unlink()
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# State helpers
# ──────────────────────────────────────────────────────────────────────────────

def _get_tested_ids(records: list[dict]) -> set:
    """IDs (hypothesis_id + lag) уже протестированных гипотез."""
    tested = set()
    for r in records:
        if r.get("type") == "result" and r.get("split") == "train":
            key = (r.get("hypothesis_id", ""), r.get("lag", 0))
            tested.add(key)
    return tested


def _get_tested_combos(records: list[dict]) -> set:
    """Уникальные (instrument, topic, lag) которые уже тестировались на train.
    Не зависит от hypothesis_id — позволяет пропустить дублирующие тесты."""
    tested = set()
    for r in records:
        if r.get("type") == "result" and r.get("split") == "train":
            key = (r.get("instrument", ""), r.get("topic", ""), r.get("lag", 0))
            tested.add(key)
    return tested


def _get_failed_triples(records: list[dict], threshold: int = MAX_FAILURES_BEFORE_SKIP) -> set:
    """(instrument, topic, lag) пары которые провалились на train >= threshold раз."""
    fail_count: dict = {}
    for r in records:
        if r.get("type") == "result" and r.get("split") == "train":
            if not r.get("ensemble_pass", False) and not r.get("error"):
                key = (r.get("instrument", ""), r.get("topic", ""), r.get("lag", 0))
                fail_count[key] = fail_count.get(key, 0) + 1

    return {k for k, cnt in fail_count.items() if cnt >= threshold}


def _get_confirmed_pairs(records: list[dict]) -> list[dict]:
    """Train-confirmed пары из всех итераций."""
    confirmed = []
    for r in records:
        if r.get("type") == "result" and r.get("split") == "train" and r.get("ensemble_pass"):
            confirmed.append({
                "instrument": r.get("instrument", ""),
                "topic":      r.get("topic", ""),
                "lag":        r.get("lag", 0),
                "m6_ic":      r.get("m6_ic", 0),
                "source":     r.get("source_label", ""),
            })
    return confirmed


def _get_val_confirmed_combos(records: list[dict]) -> list[dict]:
    """Val-confirmed (instrument, topic, lag, ic) — лучшие по IC."""
    val_pass: dict = {}
    for r in records:
        if r.get("type") == "result" and r.get("split") == "val" and r.get("ensemble_pass"):
            key = (r.get("instrument",""), r.get("topic",""), r.get("lag",0))
            ic  = r.get("m6_ic", 0)
            if key not in val_pass or ic > val_pass[key]["ic"]:
                val_pass[key] = {
                    "instrument": r["instrument"],
                    "topic":      r["topic"],
                    "lag":        r["lag"],
                    "ic":         ic,
                    "m5_pvalue":  r.get("m5_pvalue", 1.0),
                    "feature":    r.get("feature_type", "keyword_z90"),
                }
    return sorted(val_pass.values(), key=lambda x: -x["ic"])


def _get_warming_combos(records: list[dict], ic_min: float = 0.04, ic_max: float = 0.09) -> list[dict]:
    """Комбо с IC в диапазоне [ic_min, ic_max] на train — 'тёплые' кандидаты.
    Не прошли порог, но близко — стоит попробовать соседние лаги или embedding."""
    best: dict = {}
    for r in records:
        if r.get("type") == "result" and r.get("split") == "train":
            key = (r.get("instrument",""), r.get("topic",""), r.get("lag",0))
            ic  = r.get("m6_ic", 0)
            if ic_min <= ic < ic_max:
                if key not in best or ic > best[key]["ic"]:
                    best[key] = {
                        "instrument": r["instrument"],
                        "topic":      r["topic"],
                        "lag":        r["lag"],
                        "ic":         ic,
                    }
    return sorted(best.values(), key=lambda x: -x["ic"])[:20]


# ──────────────────────────────────────────────────────────────────────────────
# Систематическая генерация (детерминированная, не LLM)
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Signal quality scoring
# ──────────────────────────────────────────────────────────────────────────────

def _score_signal(train_r: dict | None, val_r: dict | None) -> dict:
    """Оцениваем качество сигнала. Возвращает score и grade.

    Grade:
      A — train AND val confirmed, IC > 0.10 на обоих
      B — train AND val confirmed (любой IC)
      C — только val confirmed, IC > 0.12 и M5 p < 0.02
      D — только val confirmed (стандарт — наш паттерн режимного сдвига)
      E — только train confirmed
      F — ничего не прошло
    """
    train_ok  = bool(train_r and train_r.get("ensemble_pass"))
    val_ok    = bool(val_r   and val_r.get("ensemble_pass"))
    train_ic  = train_r.get("m6_ic", 0) if train_r else 0
    val_ic    = val_r.get("m6_ic",   0) if val_r   else 0
    val_m5p   = val_r.get("m5_pvalue", 1.0) if val_r else 1.0
    sign_flip = bool(val_r and val_r.get("sign_flip"))

    if train_ok and val_ok and train_ic > 0.10 and val_ic > 0.10:
        grade, score = "A", 100
    elif train_ok and val_ok:
        grade, score = "B", 80
    elif val_ok and val_ic > 0.12 and val_m5p < 0.02:
        grade, score = "C", 60
    elif val_ok:
        grade, score = "D", 40
    elif train_ok:
        grade, score = "E", 20
    else:
        grade, score = "F", 0

    if sign_flip:
        score = max(0, score - 20)
        grade = grade + "~"  # отмечаем знаковый переворот

    return {
        "grade":      grade,
        "score":      score,
        "train_ok":   train_ok,
        "val_ok":     val_ok,
        "train_ic":   round(train_ic, 4),
        "val_ic":     round(val_ic, 4),
        "val_m5p":    round(val_m5p, 4),
        "sign_flip":  sign_flip,
    }


def _print_session_summary(results: list, hypotheses: list) -> None:
    """Краткая таблица найденных сигналов по сессии."""
    from src.pipeline_c.hypothesis_schema import HypothesisResult

    train_by_key: dict = {}
    val_by_key:   dict = {}
    for r in results:
        key = (r.instrument, r.topic, r.lag)
        if r.split == "train" and (key not in train_by_key or r.m6_ic > train_by_key[key].m6_ic):
            train_by_key[key] = r
        if r.split == "val" and (key not in val_by_key or r.m6_ic > val_by_key[key].m6_ic):
            val_by_key[key] = r

    # Все пары с хоть каким-то сигналом (IC > 0.08)
    interesting = {k for k, v in val_by_key.items() if v.m6_ic > 0.08}
    interesting |= {k for k, v in train_by_key.items() if v.m6_ic > 0.08}
    if not interesting:
        return

    _log("  --- Интересные комбо этой итерации ---")
    for key in sorted(interesting, key=lambda k: -max(
        val_by_key.get(k, type('', (), {'m6_ic':0})()).m6_ic,
        train_by_key.get(k, type('', (), {'m6_ic':0})()).m6_ic,
    )):
        inst, topic, lag = key
        t = train_by_key.get(key)
        v = val_by_key.get(key)
        qual = _score_signal(
            t.__dict__ if t else None,
            v.__dict__ if v else None,
        )
        t_str = f"tr_IC={t.m6_ic:.3f}" if t else "tr=—"
        v_str = f"vl_IC={v.m6_ic:.3f} M5p={v.m5_pvalue:.3f}" if v else "vl=—"
        _log(f"    [{qual['grade']}] {inst}/{topic}/lag={lag}  {t_str}  {v_str}")


ALL_TOPICS = ["oil", "rate", "ruble", "sanctions", "inflation", "banking", "gold"]
ALL_LAGS   = [1, 2, 3, 5, 7, 10, 14, 21, 30, 45, 60, 90]

def _neighbor_lags(lag: int, window: int = 3) -> list[int]:
    """Соседние лаги в окне ±window позиций по списку ALL_LAGS."""
    if lag not in ALL_LAGS:
        return [lag]
    idx = ALL_LAGS.index(lag)
    start = max(0, idx - window)
    end   = min(len(ALL_LAGS), idx + window + 1)
    return [l for l in ALL_LAGS[start:end] if l != lag]


def _systematic_hypotheses(
    val_confirmed: list[dict],
    warming:       list[dict],
    tested_combos: set,
    failed_triples: set,
    iteration: int,
) -> list[RagHypothesis]:
    """Детерминированная генерация гипотез на основе найденных сигналов.

    Три стратегии:
    A. Соседние лаги вокруг val-confirmed сигналов
    B. Все темы для val-confirmed инструментов (family search)
    C. Embedding-вариант для val-confirmed keyword сигналов
    D. Соседние лаги для 'тёплых' кандидатов
    """
    import copy
    hypotheses: list[RagHypothesis] = []
    seen_combos: set = set()

    def _add(inst: str, topic: str, lags: list, direction: str,
             rationale: str, ft: str = "keyword_z90") -> None:
        valid_lags = [
            l for l in lags
            if (inst, topic, l) not in tested_combos
            and (inst, topic, l) not in failed_triples
            and (inst, topic, l) not in seen_combos
        ]
        if not valid_lags:
            return
        from src.pipeline_c.hypothesis_schema import RagHypothesis as RH, VALID_INSTRUMENTS, VALID_TOPICS
        if inst not in VALID_INSTRUMENTS or topic not in VALID_TOPICS:
            return
        h = RH(
            instrument     = inst,
            topic          = topic,
            lag_days       = valid_lags,
            direction      = direction,
            rationale      = rationale,
            source_company = f"systematic_v{iteration}",
            source_year    = 0,
            source_page    = 0,
            source_section = "systematic_search",
            confidence     = 0.7,
            feature_type   = ft,
        )
        for l in valid_lags:
            seen_combos.add((inst, topic, l))
        hypotheses.append(h)

    # ── A: Соседние лаги для val-confirmed ──────────────────────────────────
    for sig in val_confirmed:
        inst, topic, lag = sig["instrument"], sig["topic"], sig["lag"]
        neighbors = _neighbor_lags(lag, window=2)
        _add(inst, topic, neighbors, "unknown",
             f"Соседние лаги вокруг val-confirmed {inst}/{topic}/lag={lag} (IC={sig['ic']:.3f})")

    # ── B: Все темы для val-confirmed инструментов ───────────────────────────
    val_instruments = {s["instrument"] for s in val_confirmed}
    for inst in val_instruments:
        for topic in ALL_TOPICS:
            # Берём средние лаги (не пробовали все комбо)
            candidate_lags = [l for l in [7, 14, 21, 30]
                              if (inst, topic, l) not in tested_combos
                              and (inst, topic, l) not in failed_triples]
            if candidate_lags:
                _add(inst, topic, candidate_lags, "unknown",
                     f"Family search: {inst} проверяем тему {topic}")

    # ── C: Embedding-вариант для keyword val-confirmed ───────────────────────
    for sig in val_confirmed:
        inst, topic, lag = sig["instrument"], sig["topic"], sig["lag"]
        ft_alt = "embedding_z90" if sig.get("feature", "keyword_z90") == "keyword_z90" else "keyword_z90"
        _add(inst, topic, [lag], "unknown",
             f"Feature swap: {inst}/{topic}/lag={lag} → {ft_alt}", ft=ft_alt)

    # ── D: Соседние лаги для 'тёплых' кандидатов ────────────────────────────
    for warm in warming[:10]:
        inst, topic, lag = warm["instrument"], warm["topic"], warm["lag"]
        neighbors = _neighbor_lags(lag, window=1)
        _add(inst, topic, neighbors, "unknown",
             f"Warming candidate {inst}/{topic}/lag={lag} (IC={warm['ic']:.3f}) — пробуем соседей")

    return hypotheses


def _filter_untested(
    hypotheses: list[RagHypothesis],
    tested_ids: set,
    failed_triples: set,
    tested_combos: set | None = None,
    verbose: bool = True,
) -> list[RagHypothesis]:
    """Оставляем только гипотезы с нетестированными лагами.

    tested_ids:     (hypothesis_id, lag) — уже запущены для данного source
    tested_combos:  (instrument, topic, lag) — глобальный дедуп по данным
    failed_triples: (instrument, topic, lag) — хронические провалы
    """
    import copy
    result = []
    for hyp in hypotheses:
        new_lags = []
        for lag in hyp.lag_days:
            # Пропуск если hypothesis_id уже тестировался с этим лагом
            if (hyp.hypothesis_id, lag) in tested_ids:
                continue
            # Пропуск если (inst, topic, lag) уже тестировался (дедуп по данным)
            if tested_combos and (hyp.instrument, hyp.topic, lag) in tested_combos:
                continue
            # Пропуск хронических провалов
            if (hyp.instrument, hyp.topic, lag) in failed_triples:
                continue
            new_lags.append(lag)

        if not new_lags:
            continue
        h = copy.deepcopy(hyp)
        h.lag_days = new_lags
        result.append(h)

    if verbose:
        skipped = len(hypotheses) - len(result)
        print(f"  Filter: {len(hypotheses)} → {len(result)} hypotheses "
              f"({skipped} skipped: already tested or chronic failures)")
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Ouroboros — генерация новых гипотез на основе результатов
# ──────────────────────────────────────────────────────────────────────────────

OUROBOROS_SYSTEM = """Ты — финансовый аналитик, работающий в режиме Ouroboros: анализируешь результаты предыдущих итераций и генерируешь НОВЫЕ гипотезы.

Твоя задача: на основе результатов предыдущих тестов предложить новые гипотезы, которых ещё НЕ БЫЛО.

Правила:
1. Не повторять уже протестированные комбинации (instrument, topic, lag)
2. Искать новые углы: другие инструменты для той же темы, другие темы для работающих инструментов
3. Если пара работает на одном лаге — попробовать соседние лаги
4. Если тема работает для одной компании — проверить другую
5. Опираться на экономическую логику, не на случайный перебор

Верни JSON-массив. Каждый элемент:
{
  "instrument": "MOEXFN",
  "topic": "rate",
  "lag_days": [7, 14, 30],
  "direction": "negative",
  "rationale": "Ставка +1% → давление на NIM → снижение MOEXFN",
  "source_section": "Ouroboros v2",
  "confidence": 0.7
}

Допустимые инструменты: USD_RUB, EUR_RUB, BRENT, GOLD, SILVER, SP500, MSCI_WORLD, DXY, MSCI_INDIA, CHINA_H_SHARES, FTSE_CHINA_50, DJ_SOUTH_AFRICA, IMOEX, MOEXFN, MOEXOG, MOEX10, RUGOLD
Допустимые темы: oil, rate, ruble, sanctions, inflation, banking, gold
Допустимые лаги (включая нестандартные): 1, 2, 3, 5, 7, 10, 14, 21, 30, 45, 60, 90
"""


def _ouroboros_generate(
    cfg: dict,
    confirmed: list[dict],
    failed_triples: set,
    tested_combos: set,
    iteration: int,
) -> list[RagHypothesis]:
    """DeepSeek генерирует новые гипотезы опираясь на результаты итераций."""
    print(f"  Ouroboros: генерируем новые гипотезы (итерация {iteration}) ...")

    # Val-confirmed сигналы из best_pairs
    best_pairs = cfg.get("best_pairs", [])
    val_text = (
        "Val-confirmed сигналы (ориентироваться на эти инструменты/темы):\n" +
        "\n".join(
            f"  {p['instrument']}/{p['topic']}/lag={p['lag']}  IC={p['ic']:.4f}"
            for p in best_pairs
        )
    ) if best_pairs else "Val-confirmed сигналов пока нет."

    # Train-confirmed (но не val) — интересны как слабые сигналы
    train_text = (
        "Train-confirmed (val не прошёл — интересны, но осторожно):\n" +
        "\n".join(
            f"  {p['instrument']} / {p['topic']} / lag={p['lag']} "
            f"IC={p['m6_ic']:.4f}"
            for p in confirmed[:10]
        )
    ) if confirmed else ""

    # Заблокированные — явно передаём чтобы НЕ генерировать
    blocked_sorted = sorted(failed_triples)[:40]
    blocked_text = (
        "ЗАБЛОКИРОВАНО — не генерировать эти комбинации:\n" +
        "\n".join(f"  {inst}/{topic}/lag={lag}" for inst, topic, lag in blocked_sorted)
    ) if blocked_sorted else ""

    # Уже протестированные (inst, topic, lag) — тоже не нужны
    tested_sample = sorted(tested_combos)[:30]
    tested_text = (
        "Уже протестированы (пропустить):\n" +
        "\n".join(f"  {inst}/{topic}/lag={lag}" for inst, topic, lag in tested_sample)
        + (f"\n  ... и ещё {len(tested_combos)-30} комбинаций" if len(tested_combos) > 30 else "")
    ) if tested_sample else ""

    prompt = f"""Это итерация {iteration} цикла Signal Mind Phase C.

{val_text}

{train_text}

{blocked_text}

{tested_text}

Предложи 10-14 НОВЫХ гипотез. Правила:
1. НЕ использовать заблокированные комбинации выше
2. НЕ повторять уже протестированные комбинации
3. Пробовать нестандартные лаги: 2, 3, 5, 10, 21, 45 дней
4. Исследовать новые связи: какие инструменты ещё не тестировали с рабочими темами?
5. Рассмотреть: DJ_SOUTH_AFRICA (gold работает), RUGOLD, MOEX10, DXY, MSCI_INDIA
6. Обоснование обязательно — экономическая логика, не перебор

Фокус: ищем новые (instrument, topic, lag) тройки которых ещё не было.
"""

    try:
        resp = chat(
            [
                {"role": "system", "content": OUROBOROS_SYSTEM},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.4,
        )
    except Exception as e:
        print(f"  Ouroboros LLM error: {e}")
        return []

    # Парсим
    resp = resp.strip()
    if resp.startswith("```"):
        resp = "\n".join(l for l in resp.split("\n") if not l.startswith("```"))
    start, end = resp.find("["), resp.rfind("]")
    if start == -1 or end == -1:
        print("  Ouroboros: не удалось распарсить JSON")
        return []
    try:
        items = json.loads(resp[start:end + 1])
    except json.JSONDecodeError:
        print("  Ouroboros: JSON decode error")
        return []

    hypotheses: list[RagHypothesis] = []
    for item in items:
        try:
            hyp = RagHypothesis(
                instrument     = item.get("instrument", ""),
                topic          = item.get("topic", ""),
                lag_days       = item.get("lag_days", [7, 14, 30]),
                direction      = item.get("direction", "unknown"),
                rationale      = item.get("rationale", ""),
                source_company = f"ouroboros_v{iteration}",
                source_year    = 0,
                source_page    = 0,
                source_section = item.get("source_section", f"ouroboros_iter_{iteration}"),
                confidence     = float(item.get("confidence", 0.6)),
                feature_type   = "keyword_z90",
            )
            ok, reason = hyp.is_valid()
            if ok:
                hypotheses.append(hyp)
                # Also add embedding variant
                import copy
                emb = copy.deepcopy(hyp)
                emb.feature_type = "embedding_z90"
                hypotheses.append(emb)
            else:
                print(f"    skip invalid: {reason}")
        except Exception as e:
            print(f"    skip error: {e}")

    print(f"  Ouroboros: {len(hypotheses)} новых гипотез")
    return hypotheses


# ──────────────────────────────────────────────────────────────────────────────
# Session logger
# ──────────────────────────────────────────────────────────────────────────────

def _log(msg: str) -> None:
    t = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)


# ──────────────────────────────────────────────────────────────────────────────
# Single iteration
# ──────────────────────────────────────────────────────────────────────────────

def _run_iteration(
    iteration: int,
    all_hypotheses: list[RagHypothesis],
    tested_ids: set,
    failed_triples: set,
    tested_combos: set,
    cfg: dict,
) -> tuple[list[RagHypothesis], list, int, int]:
    """Run one iteration. Returns (updated_all_hypotheses, results, n_train, n_val)."""
    session_id = datetime.now(timezone.utc).strftime(f"C_NIGHT_{iteration:02d}_%Y%m%d_%H%M%S")
    _log(f"--- Iteration {iteration}  session={session_id} ---")

    # Filter to untested (with combo dedup)
    untested = _filter_untested(
        all_hypotheses, tested_ids, failed_triples, tested_combos, verbose=True
    )

    # If exhausted → Systematic + Ouroboros
    if not untested:
        _log("  Все гипотезы протестированы. Генерируем новые ...")
        all_records   = _ledger_read()
        val_confirmed = _get_val_confirmed_combos(all_records)
        warming       = _get_warming_combos(all_records)
        confirmed     = _get_confirmed_pairs(all_records)

        # 1. Систематический поиск (детерминированный)
        sys_hyps = _systematic_hypotheses(
            val_confirmed, warming, tested_combos, failed_triples, iteration
        )
        _log(f"  Систематический поиск: {len(sys_hyps)} новых гипотез")

        # 2. LLM Ouroboros (творческий)
        llm_hyps = _ouroboros_generate(
            cfg, confirmed, failed_triples, tested_combos, iteration
        )
        _log(f"  LLM Ouroboros: {len(llm_hyps)} новых гипотез")

        new_hyps = sys_hyps + llm_hyps
        if not new_hyps:
            _log("  Нет новых гипотез — пропускаем итерацию")
            return all_hypotheses, [], 0, 0
        all_hypotheses.extend(new_hyps)
        untested = _filter_untested(
            new_hyps, tested_ids, failed_triples, tested_combos, verbose=False
        )
        _log(f"  Итого к тесту после фильтра: {len(untested)}")

    _log(f"  Тестируем {len(untested)} гипотез ...")

    _ledger_append({
        "type":           "session_start",
        "session_id":     session_id,
        "iteration":      iteration,
        "n_hypotheses":   len(untested),
        "n_total_pool":   len(all_hypotheses),
    })

    t0 = time.time()
    results = test_batch(untested, splits=("train", "val"), verbose=True)
    elapsed = time.time() - t0

    train_pass = [r for r in results if r.split == "train" and r.ensemble_pass]
    val_pass   = [r for r in results if r.split == "val"   and r.ensemble_pass]
    _log(f"  Train: {len(train_pass)}  Val: {len(val_pass)}  elapsed: {elapsed:.0f}s")
    _print_session_summary(results, untested)

    # Write results to ledger
    for r in results:
        _ledger_append({"type": "result", "session_id": session_id, **r.to_dict()})

    # Integrity check
    n_lags_tested = len(untested) * 3  # approx
    flags = _integrity_check(
        n_hypotheses = n_lags_tested,
        n_train_pass = len(train_pass),
        n_val_pass   = len(val_pass),
        results      = results,
    )

    _ledger_append({
        "type":          "session_end",
        "session_id":    session_id,
        "iteration":     iteration,
        "n_hypotheses":  len(untested),
        "n_train_pass":  len(train_pass),
        "n_val_pass":    len(val_pass),
        "elapsed_s":     round(elapsed, 1),
        "integrity_flags": flags,
    })

    # Update Ouroboros config
    cfg_updated = _update_config(cfg, results)
    _save_config(cfg_updated)

    # Update findings
    _update_findings(val_pass, untested, session_id)

    # Generate report
    report_path = REPORT_DIR / f"report_{session_id}.md"
    report_text = _generate_report(session_id, untested, results, flags, elapsed, cfg_updated)
    report_path.write_text(report_text, encoding="utf-8")

    _log(f"  Report: {report_path.name}")
    _log(f"  Train confirmed: {len(train_pass)}  Val confirmed: {len(val_pass)}")
    for r in sorted(val_pass, key=lambda x: -x.m6_ic):
        _log(f"    VAL SIGNAL: {r.instrument}/{r.topic}/lag={r.lag}  IC={r.m6_ic:.4f}  src={r.source_label}")

    # Update tested_ids
    for r in results:
        if r.split == "train":
            tested_ids.add((r.hypothesis_id, r.lag))

    return all_hypotheses, results, len(train_pass), len(val_pass)


# ──────────────────────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Phase C Overnight Ouroboros Loop")
    parser.add_argument("--stop-at",    default="05:30",
                        help="Stop time HH:MM UTC (default 05:30)")
    parser.add_argument("--iterations", type=int, default=0,
                        help="Max iterations (0 = unlimited, stop by time)")
    parser.add_argument("--sleep",      type=int, default=SLEEP_BETWEEN_ITERATIONS,
                        help="Seconds between iterations (default 900)")
    parser.add_argument("--no-extract", action="store_true",
                        help="Skip initial extraction (use saved hypotheses only)")
    args = parser.parse_args()

    # Parse stop time
    stop_h, stop_m = map(int, args.stop_at.split(":"))

    def _stop_time_today() -> datetime:
        now = datetime.now(timezone.utc)
        stop = now.replace(hour=stop_h, minute=stop_m, second=0, microsecond=0)
        if stop <= now:  # already past — next day
            stop += timedelta(days=1)
        return stop

    stop_at = _stop_time_today()

    # ── Эксклюзивный лок — один экземпляр одновременно ────────────────────
    if not _acquire_lock():
        pid = LOCK_FILE.read_text().strip()
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
              f"Phase C уже запущен (PID {pid}). Выход.", flush=True)
        return

    try:
        _run_main(args, stop_at)
    finally:
        _release_lock()


def _run_main(args, stop_at: datetime) -> None:
    """Тело основного цикла — вызывается внутри лока."""
    _log("=" * 60)
    _log("Phase C — Overnight Ouroboros Loop")
    _log(f"Stop at: {stop_at.strftime('%Y-%m-%d %H:%M UTC')}")
    _log(f"Max iterations: {args.iterations or 'unlimited'}")
    _log(f"Sleep between: {args.sleep}s ({args.sleep//60}min)")
    _log(f"Lock: {LOCK_FILE}")
    _log("=" * 60)

    # ── Step 0: Extract all hypotheses (once) ──────────────────────────────
    if not args.no_extract:
        _log("Step 0: Extracting hypotheses from all companies/years ...")
        all_hypotheses: list[RagHypothesis] = []
        for company in COMPANY_INSTRUMENT_MAP.keys():
            for year in YEARS:
                _log(f"  {company}/{year} ...")
                hyps = extract_hypotheses(company, year, dry_run=False, verbose=False)
                all_hypotheses.extend(hyps)
                _log(f"    {len(hyps)} hypotheses extracted")
        _log(f"Total extracted: {len(all_hypotheses)}")
    else:
        _log("Loading saved hypotheses ...")
        all_hypotheses = load_all_hypotheses()
        _log(f"Loaded {len(all_hypotheses)} saved hypotheses")

    if not all_hypotheses:
        _log("ERROR: No hypotheses available. Aborting.")
        return

    # ── Load state ─────────────────────────────────────────────────────────
    records        = _ledger_read()
    tested_ids     = _get_tested_ids(records)
    tested_combos  = _get_tested_combos(records)
    failed_triples = _get_failed_triples(records)
    cfg            = _load_config()

    _log(f"State: {len(tested_ids)} tested IDs, "
         f"{len(tested_combos)} tested combos, "
         f"{len(failed_triples)} failed triples in ledger")
    _log(f"Unique hypotheses in pool: {len(all_hypotheses)}")
    _log("")

    # ── Main loop ──────────────────────────────────────────────────────────
    iteration         = 1
    total_train_pass  = 0
    total_val_pass    = 0
    iterations_done   = 0

    while True:
        now = datetime.now(timezone.utc)

        # Check stop conditions
        if now >= stop_at:
            _log(f"Stop time reached ({stop_at.strftime('%H:%M UTC')}). Exiting.")
            break
        if args.iterations > 0 and iterations_done >= args.iterations:
            _log(f"Max iterations ({args.iterations}) reached. Exiting.")
            break

        # Check minimum time left (need at least 10 min for an iteration)
        time_left = (stop_at - now).total_seconds()
        if time_left < 600:
            _log(f"Less than 10 min to stop time. Exiting.")
            break

        try:
            all_hypotheses, results, n_train, n_val = _run_iteration(
                iteration      = iteration,
                all_hypotheses = all_hypotheses,
                tested_ids     = tested_ids,
                failed_triples = failed_triples,
                tested_combos  = tested_combos,
                cfg            = cfg,
            )
            # Reload state for next iteration
            records        = _ledger_read()
            tested_ids     = _get_tested_ids(records)
            tested_combos  = _get_tested_combos(records)
            failed_triples = _get_failed_triples(records)
            cfg            = _load_config()

            total_train_pass += n_train
            total_val_pass   += n_val
            iterations_done  += 1

        except Exception as e:
            _log(f"ERROR in iteration {iteration}: {e}")
            import traceback
            traceback.print_exc()

        iteration += 1

        # Sleep between iterations
        now = datetime.now(timezone.utc)
        if now < stop_at:
            sleep_secs = min(args.sleep, (stop_at - now).total_seconds() - 60)
            if sleep_secs > 0:
                wake_at = datetime.now(timezone.utc) + timedelta(seconds=sleep_secs)
                _log(f"Sleeping {sleep_secs/60:.0f}min ... (next at "
                     f"{wake_at.strftime('%H:%M UTC')})")
                time.sleep(sleep_secs)

    # ── Final summary ──────────────────────────────────────────────────────
    _log("")
    _log("=" * 60)
    _log("OVERNIGHT RUN COMPLETE")
    _log(f"Iterations done: {iterations_done}")
    _log(f"Total train-confirmed: {total_train_pass}")
    _log(f"Total val-confirmed:   {total_val_pass}")

    # Print all val-confirmed from overnight
    final_records = _ledger_read()
    val_pass_all = [
        r for r in final_records
        if r.get("type") == "result"
        and r.get("split") == "val"
        and r.get("ensemble_pass")
    ]
    if val_pass_all:
        _log("")
        _log("VAL-CONFIRMED SIGNALS (all overnight):")
        seen_keys: set = set()
        for r in sorted(val_pass_all, key=lambda x: -x.get("m6_ic", 0)):
            key = (r.get("instrument"), r.get("topic"), r.get("lag"))
            if key not in seen_keys:
                seen_keys.add(key)
                _log(f"  {r['instrument']}/{r['topic']}/lag={r['lag']}  "
                     f"IC={r.get('m6_ic',0):.4f}  "
                     f"src={r.get('source_label','')}")
    _log("=" * 60)


if __name__ == "__main__":
    main()
