# Testbed — синтетический полигон для DS-методологий

**Статус:** Phase A.0 (генерация датасетов), 2026-05-14
**Контекст:** [v2 pivot](../../db/../../memory/project_v2_pivot.md), [mistakes catalog](../../db/../../memory/project_mistakes_catalog.md)

## Зачем

Прежде чем применять любую методологию поиска сигналов «новости → рынок» на
реальных данных, мы должны убедиться, что **сам инструмент** работает:
находит сигнал там, где он есть, и НЕ находит там, где его нет.

Testbed — это шесть синтетических датасетов с **известной ground truth**
плюс универсальная upraжка для прогона всех кандидатных методов и сравнения
их FPR / TPR / F1.

## Шесть датасетов

| Каталог | Что внутри | Истина | Что проверяет |
|---|---|---|---|
| `datasets/S1/` | Два независимых random walk / Poisson | Сигнала нет | FPR baseline (ожидаемо ≈5%) |
| `datasets/S2/` | Два ряда с общим линейным трендом | Сигнала нет | **Spurious regression trap** (M001) |
| `datasets/S3/` | market[t+14] = -0.1·news[t] + ε | Сигнал есть, lag=14, r≈-0.3 | TPR, lag detection |
| `datasets/S4/` | Сигнал только при `high_rate` режиме | Условный сигнал | Regime awareness |
| `datasets/S5/` | 3 топика: 2 с разной силой, 1 шум | 2 real, 1 noise | Multi-hypothesis ranking |
| `datasets/S6/` | Скрытая 3-я переменная двигает оба ряда | Сигнала нет, есть confounder | Causal vs correlation |

Калибровка ансамбля: S1+S3+S4+S5. Hold-out для финальной оценки: S2+S6.

## Структура

```
analytics/testbed/
├── builders/         # генераторы данных (build_S1_*.py … build_S6_*.py)
├── datasets/         # сгенерированные parquet + ground_truth.json (НЕ в git, gitignored)
├── methods/          # реализации 5 кандидатных методов под единый интерфейс
├── harness/          # сравнительный прогон, расчёт метрик
└── results/          # отчёты (md + csv) — В GIT для истории
```

## Контракт «ничего не удалять»

Testbed **полностью изолирован**:
- Не пишет в `db/signal_mind.duckdb`, `db/experiments.db`, `db/signals.jsonl`
- Не читает их (кроме как для Phase A.5 — shuffle test на реальных Train data
  в **read-only** режиме)
- Не модифицирует `src/agent/`
- Не вызывает LLM (Phase A.0-A.4 — чистый Python+numpy+statsmodels)

## Стоимость прогона

| Phase | Compute | LLM | Время |
|---|---|---|---|
| A.0 (генерация датасетов) | <1 минута | 0 | разово |
| A.1 (диагноз текущего метода) | ~10 минут | 0 | один раз |
| A.3a (5 методов × 6 датасетов × 100 seeds) | ~30 минут | 0 | один раз |
| A.3b (перебор архитектур ансамбля) | ~30 минут | 0 | один раз |
| A.5 (shuffle на реальных Train) | ~30 минут | $0 (без LLM) или $3 (с LLM) | один раз |

Итого Phase A: **~2 часа compute, $0-3**, без watchdog, без 12-часовых марафонов.

## Roadmap

См. todo list в текущей сессии или `analytics/testbed/results/` после
завершения каждой фазы.
