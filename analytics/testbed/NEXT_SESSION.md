# Hand-off: Phase C — Regime Detection + Feature Normalization

**Last session:** 2026-05-20  
**Last commit:** `f9ee939`  
**Status:** Phase 1 (Option A) + Phase 2 (Option B1 Embeddings) complete. Root cause diagnosed.

---

## TL;DR — прочитай это первым

### Что сделано

| Дата | Что | Результат |
|---|---|---|
| 2026-05-19 | Option A: loosened gates (P_MAX 0.01→0.05, IC_MIN 0.03→0.01) | 1 Train signal (BRENT/sanctions/7d), failed Val |
| 2026-05-19 | Phase A.5 shuffle: 0/42 FPR (PASS) | Gates не слишком ослаблены |
| 2026-05-19 | embedding_feature_builder.py построен | 1210 дней, 7 *_emb колонок в news_daily |
| 2026-05-20 | Phase A.5 embedding shuffle: 0/42 FPR (PASS) | Embeddings не overfit |
| 2026-05-20 | big_scanner --feature-type embedding: 0/672 Train | Embeddings не помогли найти сигналы |

### Root Cause — честный диагноз

**Feature quality — НЕ ботлнек.** Embeddings дали +2.4% IC — маргинально лучше, но недостаточно.

**Истинная проблема — два слоя:**

**Слой 1: Features не нормализованы.**
Raw counts и raw similarity scores не стационарны. Новостной поток меняется со временем (2022: волна санкционных новостей, 2023: спад). VAR и LightGBM видят изменение медиа-активности, а не сигнал.
→ Нужна rolling z-score нормализация.

**Слой 2: AND rule слишком строгая для реальных данных.**
M5 (VAR/IRF) ищет Granger-causality. M6 (LightGBM) ищет predictive IC. Это разные вопросы — они не обязаны давать одинаковый ответ на одних данных. MOEXOG cluster показывает M6 IC=0.12-0.13 но M5 p=0.35-0.54. Оба верны про своё.

---

## Текущее состояние данных

```sql
-- news_daily теперь имеет 15 колонок:
-- keyword: oil, rate, ruble, sanctions, inflation, banking, gold (INT)
-- embedding: oil_emb, rate_emb, ruble_emb, sanctions_emb,
--            inflation_emb, banking_emb, gold_emb (DOUBLE [0,1])

-- Покрытие embeddings:
SELECT MIN(news_date), MAX(news_date), COUNT(*)
FROM news_daily WHERE oil_emb IS NOT NULL;
-- 2022-01-01 → 2025-04-30, 1210 rows
```

## Архитектура после Phase 2

```
analytics/testbed/
├── methods/                     ← FROZEN, не трогать
│   ├── m5_var.py                ← P_MAX=0.05, N_MIN=150 (research gates)
│   └── m6_lgbm.py               ← IC_MIN=0.01 (research gates)
├── ensemble/
│   ├── ensemble_config.yaml     ← FROZEN
│   ├── phase_a5_shuffle_test.py ← FROZEN (keyword)
│   └── phase_a5_embedding_test.py ← новый (embedding)

src/pipeline_v2/
├── big_scanner.py               ← поддерживает --feature-type keyword|embedding
└── embedding_feature_builder.py ← 1210 дней, кеш в db/emb_cache/

analytics/phase_b/
├── OPTION_A_RESULTS.md          ← Phase 1 анализ
├── OPTION_B1_COMPARISON.md      ← Phase 2 анализ
└── big_scan_*.csv               ← все результаты
```

---

## Следующие шаги (приоритизировано)

### Шаг 1: Rolling Z-Score Нормализация Features (ВЫСОКИЙ ПРИОРИТЕТ)

**Почему:** Raw features нестационарны. Это фундаментальная проблема для VAR.

**Что делать:**

```python
# В big_scanner.py и train_scanner.py добавить нормализацию:
def normalize_features(df: pd.DataFrame, window: int = 90) -> pd.DataFrame:
    """Rolling z-score для каждого news feature колонки."""
    feature_cols = [c for c in df.columns 
                    if c in TOPICS or c.endswith('_emb')]
    for col in feature_cols:
        rolling_mean = df[col].rolling(window, min_periods=30).mean()
        rolling_std  = df[col].rolling(window, min_periods=30).std()
        df[f"{col}_z"] = (df[col] - rolling_mean) / rolling_std.clip(lower=1e-6)
    return df
```

Потом использовать `{topic}_z` как `hyp.news_field` вместо `{topic}`.

**Как проверить:**
- Phase A.5 shuffle test с `_z` features → должно быть 0% FPR
- big_scanner `--feature-type keyword_norm` → сравнить IC distribution

---

### Шаг 2: Volatility Target (ВЫСОКИЙ ПРИОРИТЕТ)

**Почему:** Academic literature однозначно: новости → volatility сильнее чем новости → returns. Direction слишком шумный target.

**Что делать:**

```python
# В big_scanner._merge_news_keyrate добавить:
mkt_df["realized_vol_5d"] = (
    mkt_df["market_return"].rolling(5).std() * np.sqrt(252)
)
# Потом тестировать с target_field="realized_vol_5d"
# В Hypothesis: target_field="realized_vol_5d"
```

M5 (VAR) и M6 (LightGBM) оба поддерживают любой числовой target — ничего менять в методах не нужно.

---

### Шаг 3: Режимная Детекция (СРЕДНИЙ ПРИОРИТЕТ)

**Почему:** Rolling scanner показал сигналы в 17% временных окон — это режимно-зависимый паттерн. BRENT/sanctions подтвердилось в Train (2022) но не в Val (2024) — режим сменился.

**Что делать:**

Разбить Train на два sub-period:
- Режим 1: 2022-01-01 → 2022-12-31 (острый кризис: вторжение, санкции, шок)
- Режим 2: 2023-01-01 → 2023-09-30 (нормализация: адаптация экономики)

```python
# Добавить в big_scanner --regime-split flag
# При активации: тестировать каждый инструмент на каждом sub-period
# Hypothesis:
#   train_regime1_df = df[(df.date >= '2022-01-01') & (df.date < '2023-01-01')]
#   train_regime2_df = df[(df.date >= '2023-01-01') & (df.date <= '2023-09-30')]
```

Искать сигналы, которые подтверждаются ВНУТРИ одного режима. Потом проверить на Val (тоже разбить на режимы).

---

### Шаг 4 (при необходимости): M6-Only Ансамбль

**Почему:** Для некоторых гипотез M6 IC=0.12-0.13 — это реальный предсказательный сигнал. AND rule его убивает, потому что M5 не находит structural causality.

**Что делать:**

Запустить big_scanner только через M6, с более жёстким bootstrap gate:
- IC ≥ 0.05 (не 0.01)
- Bootstrap 5th-pct > 0.01 (не > 0)
- N_BOOTSTRAP = 500 (не 200)

Посмотреть: MOEXOG cluster пройдёт ли с этими gates?

---

## Что НЕ нужно делать

- Не менять M5 и M6 методы сами по себе — они работают верно
- Не опускать IC_MIN ниже 0.01 — это уже граница шума
- Не добавлять более сложные модели (LSTM, Transformer) — на 450 строках это overfit
- Не менять splits и буферные зоны — frozen design

---

## Git коммит стиль

Prefix `pipeline_v2:` для всего нового. HEREDOC. Co-Authored-By: Claude Haiku 4.5.

---

## Контракт "ничего не удалять"

- `db/hf_news.db` — нельзя
- `db/signal_mind.duckdb` — только append/update, не truncate
- `analytics/testbed/` — нельзя удалять тесты и методы
- `analytics/testbed/ensemble/ensemble_config.yaml` — FROZEN
- `analytics/phase_b/` — сохранять все CSV результаты
- `db/emb_cache/` — 14 NPZ файлов с embeddings, дорого пересчитывать
