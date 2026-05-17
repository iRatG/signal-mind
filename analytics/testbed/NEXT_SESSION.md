# Hand-off: Phase B — Real Train Run

**Last session:** 2026-05-15
**Last commit:** `c0b7825`  (testbed: Phase A.5 shuffle test PASS)
**Status:** ALL testbed phases A.0 through A.5 complete. Frozen ensemble ready.

---

## TL;DR — прочитай это первым

Синтетический полигон (Phases A.0-A.5) полностью завершён.
Мы эмпирически выбрали ансамбль методов с нулевым FPR на всех
no-signal датасетах и 0% на реальных shuffled данных.

**Следующий шаг — Phase B: запустить ансамбль на реальных Train данных
и получить первые подтверждённые сигналы.**

---

## Что сделано в последней сессии

```
c0b7825 testbed: Phase A.5 - shuffle test on real Train data PASS
4e921b5 testbed: Phase A.3b+A.4 - ensemble search + freeze M5+M6/AND
e600a81 testbed: Phase A.3a-4+5 - m5 VAR/IRF and m6 LightGBM
c4fa8f2 testbed: Phase A.3a-3 - m4 event study (single-lag CAR with HAC)
e644372 testbed: Phase A.3a-2 - m3 Granger single-lag F-test with HAC
```

---

## Архитектура — что построено

```
analytics/testbed/
├── methods/
│   ├── base.py, evaluator.py
│   ├── m1_corr_levels.py     ← v1 baseline (намеренно плохой)
│   ├── m2_corr_returns.py    ← returns + HAC (0% FPR, 0% TPR на S3/S5)
│   ├── m3_granger.py         ← single-lag Granger AR(5) + HAC
│   ├── m4_event_study.py     ← event CAR: z>1.5 spike -> return at lag L
│   ├── m5_var.py             ← VAR fixed-lag + key_rate Cholesky control
│   └── m6_lgbm.py            ← LightGBM walk-forward IC
├── ensemble/
│   ├── search.py             ← 73 configs evaluated, Loss function
│   ├── ensemble_config.yaml  ← FROZEN: M5+M6/AND
│   └── phase_a5_shuffle_test.py  ← 0/42 = 0% FPR on real shuffled data
├── results/
│   ├── comparison.md         ← all 6 methods side-by-side
│   ├── ensemble_search.md    ← 73 candidates ranked
│   └── phase_a5_shuffle_test.md  ← Phase A.5 verdict: PASS
└── tests/                    ← 35/35 passing
```

---

## Финальные результаты testbed

### Per-method сравнение (key datasets):

| Method | S3 TPR | S4 TPR | S5 TPR | S6 FPR | pre-pen | post-pen |
|---|---|---|---|---|---|---|
| m1 corr/levels | 0% | 0% | 0% | 0% | 4.1 | 5.1 |
| m2 corr/returns | 0% | 100% | 0% | 0% | 0.9 | 0.9 |
| m3 Granger | 100% | 100% | 100% | 13.5% | 0.9 | 1.9 |
| m4 event study | 100% | 100% | 91.5% | 6.5% | 0.9 | 0.9 |
| m5 VAR/IRF | 100% | 100% | 100% | **0.17%** | 0.9 | 0.9 |
| m6 LightGBM | 100% | 100% | 99% | 11.8% | 0.0 | 1.0 |

### Frozen ensemble M5+M6/AND:

| Метрика | Значение |
|---|---|
| avg_FPR (S1/S2/S6) | **0.0000** |
| avg_TPR (S3/S4/S5) | **0.9967** |
| max_FPR no-signal | **0.0000** |
| pre-penalty | **0.0** |
| post-penalty | **0.0** |
| Loss | **0.1033** |
| Phase A.5 shuffle FPR | **0.0%** (0/42 on real data) |

---

## Phase B: Real Train Run

### Что нужно сделать

1. **Написать `src/pipeline_v2/train_scanner.py`** — применить
   M5+M6/AND ансамбль ко всем реальным гипотезам из Train split.

   Гипотезы для Train:
   - Topics: oil, rate, ruble, sanctions, inflation, banking, gold
   - Instruments: выбрать 2-3 из v_train_market_data
   - Lags: 1, 7, 14, 30, 60, 90
   - Total: ~7×3×6 = 126 гипотез

   Для каждой гипотезы: построить DataFrame из реальных данных
   (как в phase_a5_shuffle_test.py), прогнать m5 + m6 через
   метод.evaluate(), записать verdict в results DB.

2. **Интерпретировать сигналы** — из подтверждённых гипотез
   отобрать те, что прошли как через m5, так и через m6.
   Записать направление (sign(b) из m5, sign(IC) из m6 должны совпадать).

3. **Рассмотреть Val split** — проверить, держится ли сигнал
   на валидационном периоде (2024..Q2 2025) — holdout check.

### Структура данных для Phase B

Реальные данные (signal_mind.duckdb, READ-ONLY):
```sql
-- news topics (Train: 2022-01-01..2023-09-30)
SELECT * FROM v_train_news;          -- date + 7 topics

-- market returns (Train)
SELECT trade_date, instrument, close FROM v_train_market_data;

-- key rate (daily)
SELECT period_date, rate_pct FROM v_key_rate_daily;
```

DataFrame для методов должен иметь колонки:
  - `market_return` (log-return)
  - `key_rate_pct` (для m5 auto-control)
  - `<topic_name>` (новостной топик)

### Шаблон (из phase_a5_shuffle_test.py):

```python
# загрузка уже реализована в:
# analytics/testbed/ensemble/phase_a5_shuffle_test.py  -> load_real_data()
# Переиспользовать этот код.

# Прогон одного метода:
from analytics.testbed.methods.m5_var import build as build_m5
from analytics.testbed.methods.m6_lgbm import build as build_m6
from analytics.testbed.methods.base import Hypothesis

hyp = Hypothesis(news_field="oil", target_field="market_return", lag_days=14)
m5 = build_m5()
m6 = build_m6()
v5 = m5.evaluate(df_real, hyp)
v6 = m6.evaluate(df_real, hyp)
confirmed = v5.confirmed and v6.confirmed
```

---

## Pre-flight checklist для Phase B

```powershell
# 1. Тесты
.venv\Scripts\python -m pytest analytics/testbed/tests/ -v
# Ожидается: 35 passed

# 2. Воспроизвести Phase A.5
.venv\Scripts\python -m analytics.testbed.ensemble.phase_a5_shuffle_test
# Ожидается: 0/42 confirmed, PASS

# 3. Убедиться что DB доступна
.venv\Scripts\python -c "import duckdb; con = duckdb.connect('db/signal_mind.duckdb', read_only=True); print('OK:', con.execute('SELECT COUNT(*) FROM v_train_news').fetchone())"
```

---

## Контракт «ничего не удалять»

- `src/agent/agent.py` и всё старое — остаётся нетронутым
- `analytics/testbed/` — нельзя удалять тесты и builders
- `db/signal_mind.duckdb` — только READ
- `analytics/testbed/ensemble/ensemble_config.yaml` — FROZEN, не менять

---

## Git и commit стиль

Prefix `testbed:` для testbed-работы, `pipeline_v2:` для новых Phase B модулей.
HEREDOC для message. Co-Authored-By: Claude Opus 4.7. Никаких force-push.

---

**Final reminder:** ансамбль M5+M6/AND прошёл все empirical tests.
0% FPR на 5400 синтетических и 42 реальных shuffled гипотезах.
Готово к Phase B — первому реальному запуску на Train данных.
