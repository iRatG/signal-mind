# Hand-off — Signal Mind v2

**Последнее обновление:** 2026-05-25  
**Последний коммит:** fde6231  
**Статус:** Полный цикл поиска сигналов завершён. Два подтверждённых сигнала.

---

## Прочитай это первым

Мы прошли полный научный цикл: синтетический testbed → feature engineering → Ouroboros → двухэтапная валидация. Результат честный и методически чистый.

**Два сигнала прошли Train → Val → Test:**

| Сигнал | Test M5 p | Test IC | Смысл |
|---|---|---|---|
| SP500 / inflation_z / lag=1d | 0.039 | 0.185 | Нормализованные news об инфляции → S&P500 +1 день |
| FTSE_CHINA_50 / rate_z / lag=1d | 0.041 | 0.069 | Нормализованные news о ставке ЦБ → Китай H-shares +1 день |

**Главный вывод о данных:** z-score нормализация (window=90) обязательна. Без неё сигналы невидимы. Режим 2022-2023 ≠ режим 2025-2026 (57% sign flip).

---

## Что запускать следующим

### Задача 1: Backtest двух сигналов
```python
# Нужно написать backtest.py
# Для SP500/inflation_z/1d и FTSE_CHINA_50/rate_z/1d:
# - Sharpe ratio на каждом сплите
# - Max drawdown
# - Hit rate (% дней где знак верный)
# - Сравнение с buy-and-hold SP500/FTSE
```

### Задача 2: Rolling retraining (еженедельно)
```powershell
.venv\Scripts\python -m src.pipeline_v2.night_search --rolling 8
```
Запускать раз в неделю. Результаты — в `analytics/phase_b/night_search/rolling_stable_*.csv`.

### Задача 3: Свежие данные
Загрузить market data за 2026-01 → сейчас (через parsers).
Добавить в rolling Ouroboros как актуальное окно.

### Задача 4: Contrarian тест
Проверить BRENT/sanctions/7d с отрицательным знаком на Test (2025-2026).
Если держится — живой contrarian сигнал.

---

## Архитектура (что готово)

```
src/pipeline_v2/
  feature_transformer.py     — rolling z-score (window=90), volatility targets
  embedding_feature_builder.py — embeddings, NPZ cache
  night_search.py             — Ouroboros (--loop-hours, --rolling)
  test_validation.py          — глубокая проверка списка на Test
  full_validation.py          — двухэтапная Val→Test на списке

analytics/
  LEARNED.md                 — всё что узнали, компактно
  phase_b/full_validation/   — результаты финальной валидации
  phase_b/night_search/      — все Ouroboros прогоны
  embedding_design.md        — архитектура embeddings

db/
  news_daily: 15 колонок (7 keyword + 7 embedding + date), 1210 дней
  emb_cache/: 14 NPZ файлов (не удалять — пересчёт 8+ часов)
```

---

## Данные (READ-ONLY)

```sql
-- Фичи в news_daily:
-- keyword raw: oil, rate, ruble, sanctions, inflation, banking, gold (INT)
-- embedding: oil_emb, ..., gold_emb (DOUBLE [0,1])
-- z-score добавляется в runtime через FeatureTransformer.rolling_zscore()

-- Splits:
-- Train: 2022-01-01 → 2023-09-30 (buffer: Oct-Dec 2023)
-- Val:   2024-01-01 → 2025-04-30 (buffer: May-Aug 2025)
-- Test:  2025-09-01 → 2026-04-29  ← уже использован, больше не holdout
```

---

## Ключевые параметры

```python
# Рабочий ensemble
P_MAX   = 0.05   # M5 gate (research mode)
IC_MIN  = 0.01   # M6 gate (AND) или 0.08 (M6-only, Ouroboros)
N_MIN_M5 = 80    # для Test split
N_MIN_M6 = 100   # для Test split

# Feature
FEATURE_TYPE = "keyword_z90"   # лучший по всем прогонам
WINDOW_Z     = 90               # rolling z-score window

# Лаги
LAGS = [1, 7, 14, 30, 60, 90]  # стандартный набор
```

---

## Правила (не нарушать)

- `db/hf_news.db` — только читать, никогда не удалять
- `db/signal_mind.duckdb` — только читать или append, не truncate
- `db/emb_cache/` — не удалять (8 часов пересчёта)
- `analytics/testbed/` — frozen, не трогать методы и тесты
- `analytics/testbed/ensemble/ensemble_config.yaml` — frozen
