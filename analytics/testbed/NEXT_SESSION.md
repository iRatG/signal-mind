# Hand-off: продолжение работы над v2 testbed

**Last session:** 2026-05-14
**Last commit:** `3f82243`  (testbed: Phase A.3a-1)
**Status:** Phase A complete до A.3a-1 (m1, m2). Осталось m3-m6 + A.3b/A.4/A.5.

---

## TL;DR — прочитай это первым

Мы НЕ строим v1. Старый `src/agent/agent.py` + `watchdog 3-12h`
заморожен, но **не удалён** (контракт «ничего не удалять»).

Новая работа происходит в `analytics/testbed/`. Цель — **эмпирически
выбрать ансамбль методов на синтетическом полигоне с known ground truth,
прежде чем когда-либо запускать что-либо на реальных Train data.**

Это решение принято после DS-аудита 2026-05-14, который обнаружил,
что v1-методология (Pearson r на price levels, классический p-value,
LLM решает confirmed/score) даёт **90.3% false-positive rate** на
trend-trap синтетике и **0% true-positive rate** на инжектированных
сигналах. Текущая m2 (Pearson r на returns с Newey-West HAC) показала
**0%** FPR и **100%** TPR на сильных сигналах. Направление доказано.

---

## Где мы стоим (5 коммитов, master)

```
3f82243 testbed: Phase A.3a-1 - m2 corr-on-returns + HAC, first comparison
fccdd93 testbed: Phase A.2 - deterministic evaluator + 35 unit tests
1fb17e1 testbed: Phase A.1 - diagnose v1-style method (CORR on levels)
33d52b9 testbed: Phase A.0b - mistakes catalogue with 9 seed entries
5ee6b00 testbed: Phase A.0a - six synthetic datasets with known ground truth
```

## Архитектура — что построено

```
analytics/testbed/
├── README.md                        — описание полигона
├── NEXT_SESSION.md                  — этот документ
├── builders/                        — 6 генераторов синтетических датасетов
│   ├── build_S1_pure_noise.py
│   ├── build_S2_spurious_trend.py   ← trend trap, ловит M001
│   ├── build_S3_known_signal.py     ← lag=14, r=-0.3
│   ├── build_S4_regime_dependent.py ← high_rate-only signal
│   ├── build_S5_multi_signal.py     ← 2 real + 1 noise topic
│   ├── build_S6_confounder.py       ← hidden AR(1) key_rate
│   └── verify_all.py                — sanity-check всех 6 на одном seed
├── datasets/                        — 6 × 100 seeds × 1000 days (parquet, gitignored)
│   └── S*/ground_truth.json         ← frozen experimental design (в git)
├── mistakes/                        — каталог методологических ошибок
│   ├── catalog.py                   ← single source of truth (Mistake + matchers)
│   ├── catalog.md                   ← auto-generated human view
│   └── catalog.duckdb               ← runtime DuckDB (gitignored)
├── methods/                         — кандидаты под единым интерфейсом
│   ├── base.py                      ← Hypothesis, MethodVerdict, shift_forward
│   ├── evaluator.py                 ← детерминистический gate (M002/M003-proof)
│   ├── m1_corr_levels.py            ← v1-style baseline (намеренно худший)
│   └── m2_corr_returns.py           ← returns + HAC, первая «нормальная»
├── tests/                           — 35/35 passing
│   ├── test_base.py
│   ├── test_evaluator.py
│   └── test_mistakes_catalog.py
├── harness/
│   ├── run_method.py                — прогон одного метода на 6 датасетах
│   └── compare_methods.py           — агрегация всех summary.csv в comparison.md
└── results/                         — отчёты + сырые verdicts (в git)
    ├── M1_corr_levels.{md,csv}
    ├── M2_corr_returns_hac.{md,csv}
    └── comparison.{md,csv}
```

## Эмпирический результат m1 vs m2

| Метрика | M1 (v1-style: levels) | M2 (v2-style: returns + HAC) |
|---|---|---|
| Pre-measurement penalty | **4.1** (M001/M002/M006/M007/M008) | **0.9** (M008 only) |
| Max FPR на no-signal датасетах | **90.3%** (S2 trend trap) | **0.0%** |
| TPR на S4 (r=-0.5) | 0% | **100%** |
| TPR на S1 noise | (no-signal) | (no-signal) |
| Post-measurement penalty | **5.1** (+ M009) | **0.9** |

S3 (r=-0.3) и S5 (r=-0.3, r=+0.2) — у обоих методов TPR=0%, потому что
gate `|r|≥0.40` deliberately строгий. Это **не баг**, это conservative
design. m3-m6 должны быть либо чувствительнее (Granger F, IC), либо
иметь собственный, более низкий порог.

---

## Что НЕ делать

- ❌ Не удалять / не модифицировать защищённые БД и файлы
  (см. `CLAUDE.md` фронтматтер: `db/hf_news.db`, `db/signal_mind.duckdb`,
  `db/experiments.db`, `db/signals.jsonl`, `db/journals/*`, `db/knowledge*`,
  `db/forbidden_patterns.md`, `db/sql_patterns.md`,
  `db/convergence_blacklist.json`, `db/current_regime.json`,
  `db/chroma/`)
- ❌ Не запускать `src/agent/agent.py` или `watchdog 10800`. v1 заморожена.
- ❌ Не модифицировать `analytics/experiment_v1.md` — frozen design doc.
- ❌ Не отключать / не удалять `analytics/testbed/tests/` — это quality gate.
- ❌ Не уменьшать `r_min=0.40` глобально в `evaluator.py` без обсуждения.
  Если метод требует своего threshold — пусть передаёт его через
  `GateThresholds(r_min=...)`.

## Контракт «ничего не удалять»

Это касается **всей** существующей кодовой базы: `src/agent/*`, RAG,
Loop 4 Obsidian, news loaders, VPN — всё остаётся. Новая работа
**в стороне**, в `analytics/testbed/` и (позже) `src/pipeline_v2/`.

---

## Pre-flight checklist (запусти перед началом)

```powershell
# 1. Тесты
.venv\Scripts\python -m pytest analytics/testbed/tests/ -v
# Ожидается: 35 passed in <1s

# 2. Датасеты на месте (если parquet потерялись — перегенерить):
.venv\Scripts\python analytics/testbed/builders/build_S1_pure_noise.py
.venv\Scripts\python analytics/testbed/builders/build_S2_spurious_trend.py
.venv\Scripts\python analytics/testbed/builders/build_S3_known_signal.py
.venv\Scripts\python analytics/testbed/builders/build_S4_regime_dependent.py
.venv\Scripts\python analytics/testbed/builders/build_S5_multi_signal.py
.venv\Scripts\python analytics/testbed/builders/build_S6_confounder.py
.venv\Scripts\python analytics/testbed/builders/verify_all.py
# Ожидается: "All 6 datasets passed sanity check."

# 3. Каталог ошибок
.venv\Scripts\python -m analytics.testbed.mistakes.catalog
# Ожидается: 9 mistakes registered, self-test OK

# 4. Существующие методы воспроизводятся
.venv\Scripts\python -m analytics.testbed.harness.run_method --method analytics.testbed.methods.m1_corr_levels
.venv\Scripts\python -m analytics.testbed.harness.run_method --method analytics.testbed.methods.m2_corr_returns
.venv\Scripts\python -m analytics.testbed.harness.compare_methods
# Ожидается: m1 FPR 0.9033, m2 FPR 0.0000
```

Если что-то падает — **сначала чини, потом строй**.

---

## Что делать дальше (по порядку)

### Step 1: Phase A.3a-2 — `m3_granger.py`

**Цель:** Granger F-test news → market на returns. Должен быть
чувствительнее m2 к среднем сигналам (S3, S5).

**Конфиг (ожидаемый):**
```python
M3_CONFIG = {
    "method": "granger",
    "target_transform": "log_returns",
    "p_value_method": "HAC",                # statsmodels grangercausalitytests
    "n_correction": "effective_n",
    "confirmed_decision": "deterministic_threshold",
    "subsetting_in_sql": False,
    "regime_split_check": "same_sign_required",
    "coverage_check": True,
    "out_of_sample_validation": False,      # M008 fires (систематически)
}
# Pre-penalty: 0.9 (M008)
```

**Реализация:**
- Использовать `statsmodels.tsa.stattools.grangercausalitytests`
- Pass `maxlag = hyp.lag_days`, выбрать lag через F-statistic
- Confirmed если F-test p-value < `p_max` И размер выборки `n >= n_min`
- score = F-statistic (не r)
- Поскольку Granger не даёт r напрямую — для evaluator передавать
  знак F-stat'а как направление, или пропускать r-check через
  `r=None`. Возможно понадобится небольшой helper в evaluator для
  методов без скалярного r (или собственный gate в m3).

**Ожидаемые testbed результаты:**
- S1, S2, S6: FPR ≈ 5% (Granger calibrated)
- S3 lag=14: TPR > 0% (Granger чувствительнее к weak signals than corr)
- S4: TPR высокий
- S5: A и B confirmed, C rejected
- На S6 — критический тест: Granger должен **не** confirm news→market
  (так как сигнала нет, только common cause)

**Команды:**
```powershell
.venv\Scripts\python -m analytics.testbed.harness.run_method --method analytics.testbed.methods.m3_granger
.venv\Scripts\python -m analytics.testbed.harness.compare_methods
git add analytics/testbed/methods/m3_granger.py analytics/testbed/results/
git commit -m "testbed: Phase A.3a-2 - m3 Granger F-test..."
```

### Step 2: Phase A.3a-3 — `m4_event_study.py`

**Цель:** Event study CAR — выявить аномальные returns в окне ±5 дней
вокруг «новостных событий» (всплеск упоминаний).

**Дизайн:**
- Определить event: день, когда `news_mentions[t] > μ + 2σ` (z-score >2)
- Событийное окно: `[-5, +5]` дней относительно события
- CAR = sum of returns в окне after event
- t-test: средний CAR по всем событиям того же типа ≠ 0?
- Confirmed если |mean CAR| значимо > 0 при HAC p < 0.01 и n_events >= 30

**Конфиг (ожидаемый):**
```python
M4_CONFIG = {
    "method": "event_study",
    "target_transform": "log_returns",
    "p_value_method": "HAC",
    "n_correction": "effective_n",
    "confirmed_decision": "deterministic_threshold",
    ...
}
```

**Замечание:** event study даёт интерпретируемый результат —
«после крупных новостей про X через 3 дня CAR=-1.8% (p=0.001, n=47
событий)». Это ценнее голой корреляции.

### Step 3: Phase A.3a-4 — `m5_var.py`

**Цель:** Vector Autoregression + Impulse Response Function.

**Дизайн:**
- VAR(p) на (news_mentions, market_return) или (news, market, key_rate
  если есть)
- Lag order через AIC/BIC
- Impulse response market к шоку в news
- Confirmed если IRF выходит за 95% CI на лагах [1..max_lag]
- **Критический тест:** на S6 — VAR с key_rate как третья переменная
  должен показать near-zero impulse response (направление сигнала
  через confounder)

**Конфиг (ожидаемый):**
```python
M5_CONFIG = {
    "method": "var",
    ...
}
# Specifically, на S6 нужно использовать включение key_rate колонки —
# можно через harness-передачу дополнительной control_field
```

### Step 4: Phase A.3a-5 — `m6_lgbm.py`

**Цель:** LightGBM regression / classification с walk-forward CV
inside one seed (или, точнее, in-sample R² для testbed simplification).

**Дизайн:**
- Features: news counts at lags 1, 7, 14, 30, 60, 90
- Target: market_return next day (или next k days)
- TimeSeriesSplit с 5 folds
- Метрика: out-of-fold information coefficient (rank corr predicted vs
  actual returns)
- Confirmed если IC >= 0.05 with bootstrap CI > 0
- SHAP для interpretability — какие лаги важны

**Замечание:** требует `lightgbm` пакет — `pip install lightgbm`. Также
полезно `shap` для interpretation, но не required для confirmed/rejected.

### Step 5: Phase A.3b — ensemble configurations

После m1-m6 в `results/comparison.md` будет таблица 6×6 (методы ×
датасеты). Теперь подбираем архитектуру ансамбля.

**Что делать:**
1. Создать `analytics/testbed/ensemble/` папку
2. Написать `ensemble/configurations.py` — генератор кандидатных
   конфигураций (voting K из N, cascade orders, weighted schemes)
3. Написать `ensemble/loss.py` — функция потерь:
   ```
   Loss(config) = α·avg_FPR_calibration + β·(1 - avg_TPR_calibration)
                + γ·Σ w_i · 1[config matches M_i]
                + δ·complexity(config)
   ```
4. Перебрать ~50-200 кандидатов
5. Top-10 по min Loss → financial по hold-out (S2, S6)
6. Финальный single best

**Гиперпараметры loss:** α=1.0, β=1.0, γ=2.0 (mistakes доминируют),
δ=0.05.

### Step 6: Phase A.4 — заморозка ensemble

- Сохранить выбранную конфигурацию в `analytics/testbed/ensemble/ensemble_config.yaml`
- Git commit с message `testbed: Phase A.4 - freeze ensemble config`
- Это финальная заморозка дизайна v2 ансамбля

### Step 7: Phase A.5 — shuffle test на реальных Train data

**Цель:** убедиться, что выбранный ансамбль даёт ≈5% FPR на
shuffled real news (а не 30%, как v1).

**Дизайн:**
- Создать новый mode `EXPERIMENT_MODE=v1_train_shuffle` или сделать
  отдельный скрипт
- Перемешать `news_date` в `v_train_news` (с фиксированным seed)
- Прогнать ансамбль на 100 гипотез против shuffled news
- Ожидаемая confirmed rate ≈5%

**Если результат > 10%** — M009 fires в каталоге, добавляем new
penalty, возвращаемся в A.3b с обновлённым каталогом.

**Если результат ≈ 5%** — ансамбль ready для Phase B (real Train run).

---

## Reference (короткие напоминания)

- **Memory:** `project_v2_pivot.md`, `project_mistakes_catalog.md`,
  `feedback_testbed_first.md`, `project_v2_continuation.md`
- **Контракт защищённых файлов:** `CLAUDE.md` (фронтматтер)
- **Frozen v1 design:** `analytics/experiment_v1.md`
- **Каталог ошибок:** `analytics/testbed/mistakes/catalog.md`
- **Текущее сравнение методов:** `analytics/testbed/results/comparison.md`

## Git и commit стиль

Все commits в этой работе — префикс `testbed:` + краткая суть фазы.
HEREDOC для message. Co-Authored-By: Claude Opus 4.7. Никаких force-push,
никаких rebase. Push только когда пользователь явно попросит.

## Контакт состояния

После каждой фазы (m3, m4, m5, m6, A.3b, A.4, A.5):
1. Прогнать `pytest analytics/testbed/tests/` — должен быть green
2. Прогнать `compare_methods.py` — обновить comparison.md
3. Git commit
4. Обновить todo list (`TodoWrite`)
5. Краткий отчёт пользователю с ключевыми цифрами

---

**Final reminder:** мы боремся за качество, не за скорость. Test-first,
commit-per-phase, никаких shortcuts. Пользователь устал от пятого
переоткрытия одной и той же ошибки — текущая методология должна
предотвратить это математически (через mistakes_catalog штраф) и
эмпирически (через testbed FPR/TPR measurement).
