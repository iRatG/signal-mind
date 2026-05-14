# Experiment v1 — Walk-forward signal discovery & validation

**Status:** design frozen
**Date:** 2026-05-14
**Author:** iRatG + Claude

---

## 1. Research question

Существуют ли тройки `(source_X, target_Y, lag_N)`, где `source_X` — новостной или
макроэкономический ряд, `target_Y` — рыночный инструмент, `lag_N` — временной сдвиг
в днях, такие что обнаруженная на одной выборке корреляция воспроизводится на
независимой выборке *вперёд во времени*?

### Hypotheses

- **H₀ (null):** найденные на Train корреляции *не* воспроизводятся out-of-sample —
  все они артефакты конкретного периода (режима ставки, валютного шока,
  специфики выборки HF-датасета).
- **H₁ (alt):** существует ≥ 1 тройка `(X, Y, N)`, удовлетворяющая всем критериям §5
  на всех трёх выборках.

### Pass criteria

Сигнал «прошёл», если выполнены одновременно:

1. `sign(r_train) == sign(r_val) == sign(r_test)`
2. `|r_test| ≥ 0.4 · |r_train|` (понижено с 0.5 из-за 2.5× падения IMOEX vol Train→Test)
3. `p_test < 0.05` **после Benjamini-Hochberg FDR коррекции** (q < 0.1)
4. `n_test ≥ 50`
5. для режимного сигнала (типа «работает при rate<15%»): режим выполняется
   в ≥ 30% дней Test-окна

---

## 2. Walk-forward design (chronological, no leakage)

```
Train (Discovery)        2022-01-01 → 2023-09-30      455 trading days
  buffer (no use)        2023-10-01 → 2023-12-31      lag-leak guard (max lag = 90d)
Validation (Replication) 2024-01-01 → 2025-04-30      348 trading days
  buffer (no use)        2025-05-01 → 2025-08-31      lag-leak guard
Test (Live, one-shot)    2025-09-01 → 2026-04-29      173 trading days
```

Случайный k-fold split запрещён — даёт утечку через автокорреляцию рядов.
Walk-forward имитирует реальную торговлю «вперёд во времени».

Test-окно открывается **ровно один раз**. Любая правка дизайна после этого =
эксперимент v2.

---

## 3. Pre-sample diagnostics (см. `data_audit_v1.md`)

Ключевые наблюдения, влияющие на интерпретацию:

| Факт | Влияние на эксперимент |
|---|---|
| EN-новостей: 1.09M / 40k / 43k по окнам | Validation имеет в 27× меньше статей, чем Train. Мощность тестов на Validation ниже. |
| Test: 63% RU, 37% EN, Train: 100% EN | Кросс-язык. Сигналы, ловимые EN-keyword'ами, могут пропасть в Test, и наоборот. |
| Ставка по окнам: 7.5-20 / 16-21 / 15-17 | Train содержит шок 2022 (низкая→высокая ставка). Validation — стабильно высокая. Test — снижающаяся. Это разные режимы. |
| IMOEX daily vol: 2.59% / 1.44% / 1.14% | Train сильно более волатилен. Это сделает |r| на Train завышенным относительно Val/Test. |
| key_rate в Test: 12% missing | Апрель 2026 не подтянут в `key_rate`. Игнорируем апрель в гипотезах с участием ставки или добираем точку 04.2026 = 03.2026 (15%) как proxy. |

### Disqualified topics (coverage < 30%)

- **Train**: `ruble` (29.3%) — на границе, исключаем как анти-conservative
- **Validation**: `ruble` (1.2%), `sanctions` (19.1%)
- **Test**: все ≥ 75% — без исключений

**Правило:** сигнал с участием disqualified-топика в любом из трёх окон —
автоматически отвергается на стадии Validation (нельзя сравнить базы).

Допустимые топики для эксперимента: `oil`, `rate`, `inflation`, `banking`, `gold`.

---

## 4. Frozen configuration

Эти решения **не меняются** между Train и Test:

| Параметр | Значение | Где зафиксирован |
|---|---|---|
| Confirm threshold (Train) | `\|r\| ≥ 0.4 ∧ n ≥ 150 ∧ p < 0.01` | hypothesis.py evaluate() |
| Multiple-testing correction | Benjamini-Hochberg FDR, q < 0.1 | test_oneshot.py |
| knowledge.md visibility | hidden during Train (backup .bak) | этап 1.3 |
| Regime applicability rule | режим выполняется в ≥ 30% дней Test | revizor.py |
| Train marathon length | 3 hours (~750 iterations) | watchdog 10800 |
| TOPIC_POOL | unchanged (22 topics) — но 2 топика disqualified per audit | agent.py |
| LAG_SWEEP | `[0, 7, 14, 0, 30, 0, 60, 0, 90, 0]` unchanged | agent.py |
| RANDOM_JUMP_PROB | 0.25 unchanged | agent.py |

---

## 5. Per-hypothesis metrics

На каждом окне для каждой гипотезы фиксируем:

| Метрика | Формула / источник |
|---|---|
| `r` | Pearson correlation |
| `n` | sample size |
| `p_value` | t-test для r |
| `ci95_low`, `ci95_high` | Fisher z-transform |
| `sign_match` | `sign(r) == sign(r_train)` |
| `r_decay` | `\|r_curr\| / \|r_train\|` |
| `regime_active_days_pct` | % дней окна, где условие гипотезы выполнено |

---

## 6. Aggregate funnel (expected)

| Stage | Метрика | Целевая цифра |
|---|---|---|
| Train | N hypotheses generated | ~700-800 |
| Train | N confirmed by Revizor (clean) | ~50-150 |
| Validation | N passed (sign_match ∧ r_decay ≥ 0.5) | ~5-30 |
| Test | N final (sign_match ∧ q<0.1 ∧ n≥50) | ≥ 0 |
| — | survival rate val/train | 10-25% |
| — | final rate test/val | 30-50% |

Нулевой результат на Test — это **валидный результат**, не провал эксперимента.

---

## 7. Anti-snooping rules

1. Test открывается **один раз**, после Validation. Если ноль сигналов прошло —
   фиксируем, объявляем H₀ не отвергнутой.
2. Конфиг агента, schema, news_retriever, revizor рулы — заморожены git-коммитом
   до старта Train.
3. Любые правки `forbidden_patterns.md` / `convergence_blacklist.json` после Train,
   но до Test — запрещены. Если Revizor хочет их обновить — пишет в shadow-файлы
   `*.v2.md` для следующего эксперимента.
4. `db/knowledge.md` спрятан в `knowledge.md.bak` на время Train; агент не видит
   накопленные «знания» прошлых марафонов (могут содержать aliasing-артефакты).
5. План коммитится в git до старта. Все артефакты эксперимента отделены меткой v1.

---

## 8. Pipeline phases

| Phase | Описание | Скрипт | Output |
|---|---|---|---|
| 0.1 | Подтянуть key_rate 04.2026 | вручную (proxy = 15%) | — |
| 0.2 | Pre-sample diagnostics | `analytics/data_audit.py` | `data_audit_v1.md` |
| 0.3 | Зафиксировать design | этот документ | git commit |
| 1.1 | DuckDB views `v_train_ctx` / `v_val_ctx` / `v_test_ctx` | `src/db/views_v1.py` | views + `split_manifest_v1.json` |
| 1.2 | DISCOVERY mode в schema.py + news_retriever.py | edit | code change |
| 1.3 | Спрятать `knowledge.md` | mv → `.bak` | empty stub |
| 2 | Train marathon | `watchdog 10800` | experiments.db rows |
| 3 | Validation re-run | `analytics/validation_pass.py` | `validation_survivors_v1.csv` |
| 4 | Test one-shot + BH FDR | `analytics/test_oneshot.py` | `final_signals_v1.md` |
| 5 | HTML report | `analytics/generate_report_v1.py` | `experiment_v1_report.html` |

---

## 9. Success / failure criteria (overall)

- **Success:** ≥ 1 сигнал прошёл все три окна. Записываем в knowledge.md (после
  восстановления) как verified signal.
- **Partial:** ≥ 5 сигналов прошли Validation, но 0 прошло Test. Это полезно —
  отделяет «работало в одном режиме» от «работает out-of-sample».
- **Failure:** 0 confirmed на Train, или 0 прошло Validation. Это означает что
  система не нашла даже кандидатов — нужно пересмотреть TOPIC_POOL и порог.

В любом из трёх случаев — фиксируем результат, делаем отчёт, **не правим
дизайн внутри текущего эксперимента**.

---

## 10. Open issues (на момент freeze)

- Apple 04.2026 в `key_rate`: 21 день missing. Решение: использовать 15% как proxy
  (равно значению 03.2026). Альтернатива — подтянуть с cbr.ru, если будет
  принципиально для апрельских гипотез.
- EN ↔ RU кросс-язык: пока не корректируем явно. Если сигнал на Train ловится через
  EN-keywords, а в Test работает на смешанной выборке (где 63% RU) — это
  *расширение* эффекта, не утечка. Если же сигнал был EN-only из-за специфики
  выборки 2022-2023 — это будет видно по drop n_test.
