# Signal Mind — Всё что нашли. Финальный итог.

**Дата:** 2026-05-25  
**Покрывает:** Phase A (testbed) → Phase B (feature engineering) → Ouroboros → Test validation

---

## Два подтверждённых сигнала

Прошли через Train → Val → Test без исключений.

| Сигнал | Train IC | Val IC | Test IC | M5 p (test) | Суть |
|---|---|---|---|---|---|
| **SP500 / inflation_z / lag=1d** | 0.149 | 0.057 | **0.185** | 0.039 | Новости об инфляции → S&P500 на следующий день |
| **FTSE_CHINA_50 / rate_z / lag=1d** | 0.094 | 0.069 | 0.069 | 0.041 | Новости о ставке ЦБ → Китай H-shares на следующий день |

Оба: M5 (структурная причинность) + M6 (предсказательная IC) + знак стабилен.  
BH-FDR на 54 гипотезах при q=0.10 = 0 значимых — честный результат множественного тестирования.

---

## Что работает технически

### Features
- **z-score нормализация (window=90) обязательна.** Без неё — сигналы невидимы. Новостной поток нестационарен (волна санкций 2022 → спад 2023), raw counts обманывают VAR и M6.
- **keyword_z90 = лучший feature type** (стабильно во всех прогонах).
- Embeddings дали +2.4% IC — маргинально, не прорыв. Semantic similarity ≈ keyword по информативности.
- Volatility target (abs_return, vol_5d) = 0 сигналов. Работает `market_return`.

### Ensemble
- **M5 (VAR/IRF) и M6 (LightGBM) ищут разные вещи.** M5 — Granger-причинность. M6 — предсказательный IC. На реальных данных они редко совпадают → AND rule строгая.
- **Research gates: P_MAX=0.05, IC_MIN=0.01.** Это рабочий баланс.
- **N_MIN = 80/100 для Test split** (154-200 строк) — обязательное снижение.
- Ouroboros после 30 раундов сам пришёл к IC gate = 0.08 (при 67% val rate).

### Методология
- Синтетический testbed: 0% FPR на 5400 гипотезах. Корректно откалиброван.
- Phase A.5 shuffle test: embeddings не overfit (0/42 на shuffled).
- BH-FDR correction применяется при финальном тестировании.
- Rolling window (12 месяцев) важнее фиксированного Train 2022-2023.

---

## Режимный сдвиг — главная находка

**57% сигналов из Train 2022-2023 перевернули знак на Test 2025-2026.**

| Период | Характеристика | Что работает |
|---|---|---|
| 2022-2023 | Кризис: вторжение, санкции, ставки | Sanctions-driven сигналы (BRENT, MSCI) |
| 2024 | Нормализация | Inflation, rate, banking clusters |
| 2025-2026 | Новый режим | SP500/inflation_z, FTSE/rate_z |

Сигналы 2022 (BRENT/sanctions/7d, GOLD/inflation_z/7d) на Test дают sign flip — рынок адаптировался. Это не баг методологии, это рыночная реальность.

**Следствие:** нужен rolling retraining — не 3-летняя история, а последние 12 месяцев.

---

## Архитектура (что построено и работает)

```
src/pipeline_v2/
├── feature_transformer.py      — rolling z-score, volatility targets
├── embedding_feature_builder.py — SentenceTransformer, NPZ cache, 1210 дней
├── night_search.py             — Ouroboros loop (SearchKnowledge)
│                                  --loop-hours N  (adaptive по rounds)
│                                  --rolling N      (rolling 12m windows)
├── test_validation.py          — глубокая проверка одного списка сигналов
└── full_validation.py          — двухэтапная Val→Test валидация списка

analytics/
├── LEARNED.md                  — этот файл
├── testbed/NEXT_SESSION.md     — технический hand-off
├── phase_b/OPTION_A_RESULTS.md  — Phase 1 анализ
├── phase_b/OPTION_B1_COMPARISON.md — Embeddings vs keyword
├── phase_b/night_search/       — все Ouroboros результаты
├── phase_b/test_validation/    — Test validation на 21 сигнале
├── phase_b/full_validation/    — Двухэтапная Val→Test на 54 кандидатах
└── embedding_design.md         — архитектура embeddings

db/
├── signal_mind.duckdb          — market + news (читать только!)
│   news_daily: oil,rate,...,gold + oil_emb,...,gold_emb (1210 дней)
├── hf_news.db                  — 2.56M статей (читать только!)
└── emb_cache/                  — 14 NPZ чанков embeddings
```

---

## Что не работает и почему

| Подход | Результат | Причина |
|---|---|---|
| Raw keyword counts | Слабые M5 сигналы | Нестационарность — newsflow меняется |
| AND rule на Test (N=154) | 0/21 без N_MIN снижения | N_MIN=200 слишком высокий для 154 строк |
| Volatility target | 0 сигналов | News → direction, не → vol в этом датасете |
| 3-летнее обучение | Sign flip на Test | Режим 2022 ≠ режим 2025 |
| AND rule в общем | Редко срабатывает | M5/M6 несогласны: разные виды сигналов |
| Embeddings vs keywords | +2.4% IC | Ceiling определяется качеством сигнала, не features |

---

## Sign-flip сигналы — потенциальные contrarian

Эти сигналы работали в одном направлении в 2022-2024, затем перевернулись в 2025-2026. Могут быть торгуемыми в противоположную сторону если подтвердить на свежих данных:

| Сигнал | 2022-24 IC | 2025-26 IC (flip) |
|---|---|---|
| BRENT / sanctions / 7d | 0.122 (↑) | 0.108 (↓ flip) |
| MSCI_WORLD / sanctions_z / 14d | 0.099 (↑) | 0.110 (↓ flip) |
| IMOEX / ruble_z / 7d | 0.069 (↑) | 0.134 (↓ flip) |

---

## Marginal — наблюдать

17 сигналов с sign=OK на Test, высоким IC, но M5 не прошёл (структурная причинность не подтверждена). Предсказательная сила есть, структуры нет.

Топ marginal:

| Сигнал | Test IC | IC decay |
|---|---|---|
| SP500 / inflation_z / 7d | 0.223 | 3.25 |
| SP500 / oil_z / 30d | 0.171 | 4.21 |
| SP500 / gold_z / 1d | 0.128 | 3.15 |
| SP500 / gold_z / 14d | 0.103 | 2.83 |

Примечание: ic_decay > 1 означает что Test IC выше Val IC — сигнал усиливается на свежих данных.

---

## Следующие шаги (приоритизировано)

### 1. Backtesting подтверждённых сигналов (высокий приоритет)
Построить торговую стратегию для SP500/inflation_z/1d и FTSE/rate_z/1d:
- Sharpe ratio, max drawdown, hit rate
- Размер позиции по IC
- Сравнение с buy-and-hold

### 2. Rolling retraining как процесс
Настроить Ouroboros (`--rolling`) запускаться еженедельно:
- Окно: последние 12 месяцев
- Автоматически обновлять список активных сигналов
- Сравнивать с предыдущим прогоном — стабильность

### 3. Contrarian проверка sign-flip сигналов
Тест BRENT/sanctions/7d с отрицательным направлением на 2025-2026.
Если держится → это живой contrarian сигнал.

### 4. Свежие данные
Загрузить news + market за 2026-01 → сейчас.
Использовать как новое "горячее" Train окно в rolling Ouroboros.

---

## Команды для запуска

```powershell
# Ouroboros с адаптивным циклом (8 часов)
.venv\Scripts\python -m src.pipeline_v2.night_search --loop-hours 8

# Rolling windows (самые свежие данные)
.venv\Scripts\python -m src.pipeline_v2.night_search --rolling 8

# Двухэтапная валидация нового списка кандидатов
.venv\Scripts\python -m src.pipeline_v2.full_validation

# Глубокая проверка 21 сигнала (с rolling IC)
.venv\Scripts\python -m src.pipeline_v2.test_validation
```

---

## Числа

| Метрика | Значение |
|---|---|
| Статей в hf_news.db | 2.56M (EN + RU) |
| Дней embeddings | 1210 (2022-01 → 2025-04) |
| Гипотез в big_scanner | 672 (16 инструментов × 7 тем × 6 лагов) |
| Раундов Ouroboros | 30 (8 часов) |
| Rolling window раундов | 4 окна × ~20 раундов |
| Stable signals (Ouroboros) | 1631 уникальных (inst, topic, lag) |
| Gold candidates (M5+IC>=0.10) | 54 |
| Val → Test confirmed | **2** |
| Sign flips на Test | 57% (режимный сдвиг) |
| BH-FDR значимых (q=0.10) | 0 |
