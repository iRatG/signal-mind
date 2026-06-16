# Phase D — Полный план реализации

**Зафиксировано:** 2026-06-16  
**Статус:** ПЛАНИРОВАНИЕ — не запускать до завершения подготовки  
**Предыдущая фаза:** Phase C закрыта 2026-06-14, 1428/1428 комбо, 0 test-confirmed

---

## Ключевой методологический сдвиг

### Почему Phase A/B/C не работало

```
Старый подход (все три фазы):
  Train:  2022–2023   (режим: кризис, IMOEX -50%, инфляция 15%+)
                ↕ ДРУГОЙ РЕЖИМ — связи не переносятся
  Val:    2024–2025   (режим: высокая ставка, адаптация)
                ↕ ДРУГОЙ РЕЖИМ — связи не переносятся
  Test:   2025–2026   (режим: новый)

Результат Phase C: 13 val-confirmed → 0 test-confirmed (57% знаковых переворотов)
```

### Правильный подход Phase D

```
Берём 18-месячное окно ОДНОГО режима (Jan 2025 → Apr 2026):
  
  Train:  2025-01-01 → 2025-07-31   ~150 торг. дней
          [буфер 2 нед — защита от lag утечки]
  Val:    2025-08-15 → 2025-11-30   ~75 торг. дней
          [буфер 2 нед]
  Test:   2025-12-15 → 2026-04-29   ~95 торг. дней  ← заморожен до конца

Все три периода: ставка ЦБ 15-21%, рубль 85-92, Brent 70-80$
Сигнал найденный в Train воспроизводится в Val/Test потому что УСЛОВИЯ ТЕ ЖЕ.
```

**Принцип:** не ищем вечный сигнал через разные режимы. Ищем сигнал который работает СЕЙЧАС, в текущем режиме, и воспроизводится внутри него.

---

## Инвентарь данных — точное состояние

### Рыночные данные (signal_mind.duckdb)

| Таблица | Покрытие | Инструменты | Статус |
|---------|----------|-------------|--------|
| `market_data` | 2021-01-04 → **2026-04-29** | BRENT, SP500, DXY, GOLD, SILVER, USD_RUB, EUR_RUB, MSCI_INDIA, MSCI_WORLD, FTSE_CHINA_50, CHINA_H_SHARES, DJ_SOUTH_AFRICA, ALUMINUM | ✅ актуально |
| `moex_indices` | 2016-03-01 → **2026-04-29** | IMOEX, MOEXFN, MOEXOG, MOEX10, RUGOLD, MOEXBC, MOEXBMI, + др. | ✅ актуально |
| `forex_cbr` | → **2026-04-22** | USD/RUB, EUR/RUB (ЦБ курс) | ✅ актуально |
| `key_rate` | 2014-01 → **2026-03** (15%, инфляция 5.86%) | ставка ЦБ, инфляция, days_to_meeting | ✅ актуально |
| `rosstat_macro` | 1991–2026 | avg_wage_rub и др. | ✅ не изменится |

### Новостные данные

| Источник | Покрытие | Строк | Статус |
|----------|----------|-------|--------|
| `hf_news.db / articles` (HF dataset) | 2021–2025 | **2,520,591** | ✅ читать только |
| `hf_news.db / articles` (ru_archive) | 2025-09 → 2026-05-11 | **28,533** | ✅ читать только |
| `hf_news.db / articles` (en_archive) | разное | **~7,700** | ✅ читать только |
| `news_daily` (агрегат) | 2021-01-01 → 2026-05-11 | 1957 строк | ✅ обновляется |
| `news_daily` keyword cols | 2021 → 2026-05-11 | ненулевые везде | ✅ |
| `news_daily` embedding cols | **2022-01-01 → 2025-04-30** | 1130 ненулевых | ⚠️ **GAP: 2025-05-01 → 2026-05-11** |

**Критический gap по годам в hf_news.db:**
```
2021:  629,029  ████████████████████  хорошо
2022:  654,170  █████████████████████  хорошо
2023: 1,015,520 █████████████████████████████  хорошо
2024:    14,663  ▌  ← ДЫРА (не загружены)
2025:   228,615  ████████  (HF + ru_archive с сентября)
2026:    14,602  ▌ (до мая, ru_archive)
```

**Вывод:** Окно 2025-01-01 → 2026-04-29 имеет удовлетворительное покрытие новостей. 2024 пропускаем.

### Векторная база (ChromaDB)

| Коллекция | Чанков | Содержание | Статус |
|-----------|--------|------------|--------|
| corporate_reports | ~15,574 | Годовые отчёты Сбер, Лукойл, Газпром, Яндекс (2020-2025) | ✅ |
| cbr_documents | ~1,918 | Документы ЦБ РФ (2020-2025) | ✅ |
| Obsidian vault | ~221 | Методология, известные атаки, deprecated подходы | ✅ |

**Итого ChromaDB: 17,713 чанков** — доступны через `src/agent/rag.py`

---

## Инвентарь механизмов — что уже построено

### Статистические методы (frozen, не менять)

| Метод | Описание | FPR на тестбеде | Файл |
|-------|----------|-----------------|------|
| **M5** | VAR (Cholesky, key_rate control) — структурная причинность | 0.0% | `src/pipeline_v2/` |
| **M6** | LightGBM walk-forward IC | 0.0% | `src/pipeline_v2/` |
| **Тестбед** | S1-S6 синтетика, avg_FPR=0.000, avg_TPR=0.997 | — | `analytics/testbed/` |
| **BH-FDR** | Benjamini-Hochberg q=0.10 | — | встроен |
| **Rolling z-score** | window=90, clip±5 | — | встроен |

Конфигурация заморожена: `analytics/testbed/ensemble/ensemble_config.yaml`

### Инфраструктура поиска

| Механизм | Описание | Статус |
|----------|----------|--------|
| **Ouroboros loop** | LLM-итеративный поиск гипотез | ✅ Phase C: 615 сессий без краша |
| **Strategy E** | Детерминированный перебор нетестированных | ✅ нашёл banking тему |
| **Lock file** | Один экземпляр (db/phase_d.lock) | ✅ проверено |
| **Task Scheduler** | 3 цикла/день (22:00, 07:00, 15:00) | ✅ настроен |
| **Ledger** | Append-only лог (ledger_d.jsonl) | ✅ паттерн отработан |
| **Session config** | Адаптивные параметры между сессиями | ✅ |
| **Inter-session analyzer** | Пересчёт топ-инструментов, IC gate | ✅ |

### Источники гипотез

| Источник | Что даёт | Статус |
|----------|----------|--------|
| **RAG (ChromaDB)** | Цитаты из корп. отчётов + ЦБ → конкретные гипотезы | ✅ |
| **DeepSeek** | Генерация и reasoning | ✅ |
| **Phase C ledger** | 13 Grade D сигналов → стартовые кандидаты для нового окна | ✅ |
| **Strategy E** | Систематический перебор (не зависит от LLM интуиции) | ✅ |
| **Obsidian vault** | Методологические антипаттерны | ✅ |

---

## Пространство поиска Phase D

### Инструменты (20 итого)

**Из market_data (13):**
BRENT, SP500, DXY, GOLD, SILVER, USD_RUB, EUR_RUB, MSCI_INDIA, MSCI_WORLD, FTSE_CHINA_50, CHINA_H_SHARES, DJ_SOUTH_AFRICA, ALUMINUM

**Из moex_indices (7):**
IMOEX, MOEXFN, MOEXOG, MOEX10, RUGOLD, MOEXBC, MOEXBMI

### Темы (13 итого)

**Из Phase C (7, проверены):**
`oil`, `rate`, `ruble`, `sanctions`, `inflation`, `banking`, `gold`

**Новые (6, для текущего режима):**
`geopolitics`, `credit`, `trade`, `energy_transition`, `china_economy`, `fed_policy`

### Лаги (12): 1, 2, 3, 5, 7, 10, 14, 21, 30, 45, 60, 90 дней

### Итоговое пространство: 20 × 13 × 12 = **3,120 комбинаций**

---

## Фичи (feature types)

| Тип | Описание | Статус |
|-----|----------|--------|
| `keyword_z90` | Rolling z-score(90) от keyword счётчиков | ✅ уже в news_daily |
| `embedding_z90` | Rolling z-score(90) от semantic embeddings | ⚠️ нужно продлить на 2025-2026 |
| `keyword_raw` | Сырые счётчики (для дебаггинга) | ✅ |

---

## Архитектура Phase D — полная схема

```
┌───────────────────────────────────────────────────────────────┐
│  ПОДГОТОВКА (разово, до первого запуска)                       │
│                                                               │
│  D0. data_check_d.py     → убедиться что все источники ok     │
│  D1. regime_detector.py  → HMM на macro, current_regime       │
│  D2. topic_config_d.py   → 13 тем + keyword sets              │
│  D3. news_precompute_d.py→ новые колонки + embeddings 2025-26 │
│  D4. split_d.py          → Train/Val/Test views (одно окно)   │
│  D5. testbed_check_d.py  → weighted score FPR/TPR на S1-S6    │
└───────────────────────────────────────────────────────────────┘
                         ↓ (после подтверждения)
┌───────────────────────────────────────────────────────────────┐
│  ОСНОВНОЙ ЦИКЛ (автономно, Task Scheduler)                    │
│                                                               │
│  phase_d_overnight.py                                         │
│  │                                                            │
│  ├─ acquire_lock()                                            │
│  │                                                            │
│  ├─ regime_detector.classify_current()                        │
│  │   └─ если режим сменился → reset confirmed + новое окно   │
│  │                                                            │
│  ├─ ГЕНЕРАЦИЯ ГИПОТЕЗ                                         │
│  │   ├─ RAG запрос (3 стратегии по режиму)                   │
│  │   ├─ DeepSeek reasoning (chain-of-thought)                 │
│  │   ├─ LLM гипотезы из RAG контекста                        │
│  │   └─ Strategy E (детерминированный sweep нетестированных)  │
│  │                                                            │
│  ├─ ПРОВЕРКА (на Train окне 2025-01 → 2025-07)               │
│  │   ├─ rolling_HAC_corr → pre-filter (top 50%)              │
│  │   ├─ M5 VAR (Cholesky + key_rate control)                  │
│  │   ├─ M6 LightGBM walk-forward IC                          │
│  │   └─ weighted_score = 0.45·M5 + 0.45·M6 + 0.10·rag_bonus │
│  │                                                            │
│  ├─ GRADE                                                     │
│  │   ├─ Grade A: score ≥ 0.70  → немедленная Val проверка    │
│  │   ├─ Grade B: score ≥ 0.50  → Val при следующей сессии    │
│  │   ├─ Grade C: score ≥ 0.30  → архив, пересмотр при смене  │
│  │   └─ Grade D: score < 0.30  → отклонено                   │
│  │                                                            │
│  ├─ VAL проверка (2025-08-15 → 2025-11-30)                   │
│  │   └─ val_confirmed → ledger_d.jsonl                        │
│  │                                                            │
│  ├─ update session_config_d.json                              │
│  └─ release_lock()                                            │
│                                                               │
│  [TEST: 2025-12-15 → 2026-04-29 — заморожен до конца]        │
└───────────────────────────────────────────────────────────────┘
```

---

## Новое в Phase D: режим-детектор

### Зачем

Все 13 сигналов Phase C — Grade D: специфичны для 2024-2025. Как только режим меняется — сигналы умирают. Детектор режима = система раннего предупреждения.

### Архитектура

```python
# src/pipeline_d/regime_detector.py

REGIME_FEATURES = {
    "imoex_vol_60d":   rolling_vol(IMOEX, 60),      # рыночный стресс
    "usd_rub_vol_30d": rolling_vol(USD_RUB, 30),     # рублёвый стресс
    "cbr_rate_level":  key_rate_current,             # монетарный режим
    "brent_trend_90d": trend_slope(BRENT, 90),       # сырьевой режим
}

# HMM с 3-4 состояниями
model = GaussianHMM(n_components=4, covariance_type='diag', n_iter=100)
model.fit(historical_features_2022_2026)

def classify_current(lookback_days=30) -> dict:
    return {
        "regime": int,           # 0-3
        "confidence": float,     # вероятность текущего состояния
        "days_in_regime": int,   # сколько дней в этом состоянии
        "transition_risk": float # вероятность перехода в следующие 30д
    }
```

### Валидация детектора

Ожидаемые результаты на исторических данных:
- 2022 Q1-Q2: Режим 0 (кризис) — IMOEX vol высокий, рубль нестабилен
- 2022 Q3-2023: Режим 1 (стабилизация под санкциями)
- 2024-2025: Режим 2 (высокая ставка ЦБ, инфляция снижается)
- 2025-2026: Режим 2 или 3 (определим эмпирически)

---

## Новое в Phase D: расширение тем

### Keyword sets для новых 6 тем

```python
# src/pipeline_d/topic_config_d.py

TOPIC_KEYWORDS_D = {
    # --- существующие 7 тем (не менять) ---
    "oil":        ["oil", "crude", "brent", "нефть", "нефти"],
    "rate":       ["rate", "interest", "ставка", "цб", "ключевая"],
    "ruble":      ["ruble", "rub", "рубль", "рублей", "курс"],
    "sanctions":  ["sanctions", "sanction", "санкции", "санкций"],
    "inflation":  ["inflation", "инфляция", "инфляции", "цены"],
    "banking":    ["bank", "banking", "банк", "банки", "кредит"],
    "gold":       ["gold", "золото", "золота", "precious"],

    # --- новые 6 тем ---
    "geopolitics":        ["war", "conflict", "ceasefire", "nato", "переговор",
                           "война", "геополитик", "мир", "санкционн"],
    "credit":             ["default", "debt", "credit spread", "bond yield",
                           "долг", "дефолт", "кредитн", "облигаци"],
    "trade":              ["export", "import", "trade", "embargo", "logistics",
                           "экспорт", "импорт", "торговл", "логистик"],
    "energy_transition":  ["lng", "solar", "wind", "renewables", "carbon",
                           "спг", "зелен", "возобновляем", "углерод"],
    "china_economy":      ["china gdp", "pmi china", "yuan", "pboc", "юань",
                           "китай экономик", "экономика китая", "народный банк"],
    "fed_policy":         ["fed", "federal reserve", "fomc", "powell",
                           "фрс", "федрезерв", "ставка сша"],
}
```

### Проверка coverage в hf_news.db

До добавления темы — проверить: сколько статей в 2025-2026 содержат эти слова. Если < 1000 за год — тема слабо покрыта и сигнал будет ненадёжным.

---

## Новое в Phase D: thinking hypothesis generation

### Три шага вместо одного

```
БЫЛО (Phase C):
  LLM prompt → гипотезы JSON

СТАЛО (Phase D):
  Шаг 1 — АНАЛИЗ РЕЖИМА (chain-of-thought, без структуры):
    "Текущий режим: {regime_description}
     Что аномально vs исторической нормы?"
    → свободный reasoning 3-5 предложений

  Шаг 2 — RAG КОНТЕКСТ:
    Запрос 1: "влияние текущей ставки ЦБ на {top_instruments}"
    Запрос 2: "geopolitics energy trade impact 2025"
    Запрос 3: "российский рынок текущий режим сигналы"
    → 7-10 релевантных чанков из 17,713

  Шаг 3 — ГИПОТЕЗЫ (структурированный JSON):
    "На основе анализа режима и документов — предложи 8 гипотез.
     Для каждой: instrument, topic, lag, direction, source_citation"
```

### RAG стратегии по типу запроса

| Стратегия | Запрос | Коллекция |
|-----------|--------|-----------|
| Regime context | f"текущий режим {current_regime} рыночные связи" | corporate + cbr |
| Instrument-specific | f"{instrument} факторы влияния 2025" | corporate |
| Cross-market | f"{topic} влияние на {instrument_class}" | cbr |
| Anti-pattern check | "известные ложные сигналы методологические ошибки" | obsidian |

---

## Новое в Phase D: weighted score

### Почему AND-gate убивал реальные слабые сигналы

Big Scanner результаты (672 гипотезы):
- M6 нашёл 19 сигналов с IC > 0.03
- M5 нашёл 9 с p < 0.05
- Пересечение M5 AND M6 = **0**
- MOEXOG/banking IC=0.158 — убит M5 p=0.35

M5 и M6 ищут разные вещи: структурную причинность vs предсказательный паттерн. На реальных данных со слабым SNR они редко соглашаются одновременно.

### Новая формула

```python
def weighted_score(m5_result, m6_result, rag_bonus=0.0):
    # Нормализуем: p-value → z-score (1-p → 0..1, потом z)
    m5_norm = norm.ppf(1 - m5_result["pvalue"]).clip(0, 4) / 4.0
    m6_norm = min(m6_result["ic"] / 0.20, 1.0)  # IC=0.20 = 1.0
    
    score = 0.45 * m5_norm + 0.45 * m6_norm + 0.10 * rag_bonus
    return score

# Grade thresholds:
# A: score >= 0.70 — сильный, проверить Val немедленно
# B: score >= 0.50 — умеренный, мониторинг
# C: score >= 0.30 — слабый, архив
# D: score < 0.30  — отклонено
```

**ВАЖНО:** перед изменением gate — прогнать через тестбед (S1-S6). Ослабление порога = рост FPR. Нужно измерить эмпирически.

### RAG bonus

+0.10 к score если гипотеза подкреплена конкретной цитатой из ChromaDB с source_file и страницей. Мотивирует LLM давать traceable гипотезы.

---

## Точные сплиты Phase D

```
РАБОЧЕЕ ОКНО: 2025-01-01 → 2026-04-29 (16.5 месяцев, ~345 торг. дней)

Сплит:
  Train:  2025-01-01 → 2025-07-31    ~150 торг. дней
                                      (буфер 14 дней — lag guard для lag≤7)
  Val:    2025-08-15 → 2025-11-28    ~75 торг. дней
                                      (буфер 14 дней)
  Test:   2025-12-15 → 2026-04-29    ~95 торг. дней   ← ЗАМОРОЖЕН

Буфер 14 дней достаточен: max рабочий lag = 21 дней, но мы берём
максимальный используемый lag в гипотезе + 7 дней запаса.
Для lag=90 (редкие) — буфер не помогает, но это лаги исследовательского
класса, не production.
```

**Режим в окне:**
- Ставка ЦБ: 15-21% (весь период 2025)
- USD/RUB: 85-92 (стабильный диапазон)
- IMOEX: 2700-3300 (умеренная волатильность)
- Инфляция: 7-10% (снижается от пика)

---

## Использование Phase C наработок

Phase C нашла 13 Grade D сигналов — специфичных для 2024-2025. В новом окне (2025-2026) часть из них может быть актуальна:

```python
# Приоритетные кандидаты для Phase D (из Phase C ledger):
PHASE_C_CANDIDATES = [
    ("MOEXFN",  "rate",      7,  0.0999),   # M5p=0.006 — самый структурный
    ("DXY",     "oil",       7,  0.1685),   # IC val самый высокий
    ("DXY",     "sanctions", 14, 0.1437),   # sanctions → DXY
    ("MSCI_INDIA", "banking", 2, 0.1062),   # M5p=0.0006 — статистически сильнейший
    ("SP500",   "banking",   7,  0.0967),   # banking → SP500
]
# Запустить эти первыми на новом окне 2025-01 → 2026-04
```

---

## Этапы реализации — детальный план

### Этап 0: Проверка данных (0.5 дня)

**Задача:** убедиться что источники готовы для нового окна.

```
D0. src/pipeline_d/data_check_d.py

Проверяет:
  [ ] market_data: есть ли данные за 2025-01 → 2026-04-29
  [ ] moex_indices: IMOEX, MOEXFN, MOEXOG, MOEX10 — то же
  [ ] forex_cbr: USD/RUB за весь период
  [ ] key_rate: покрытие 2025 года (строки есть?)
  [ ] news_daily: coverage по новым темам (сколько статей?)
  [ ] hf_news.db: статьи за 2025-01 → 2026-05 по источникам

Выход: data_check_d_report.md
```

### Этап 1: Расширение news_daily (1-2 дня)

**Задача 1A:** Добавить 6 новых тем в `news_daily`

```
src/pipeline_d/news_precompute_d.py

Действия:
  - Читает hf_news.db/articles (WHERE date >= '2021-01-01')
  - Считает keyword hits для 6 новых тем
  - ALTER TABLE news_daily ADD COLUMN geopolitics INTEGER DEFAULT 0
  - (и остальные 5)
  - UPDATE news_daily SET geopolitics = computed_count WHERE news_date = date

Риск: ⚠️ модифицирует news_daily → нужен бэкап или проверка DESCRIBE
```

**Задача 1B:** Продлить embedding columns на 2025-05-01 → 2026-05-11

```
Используем существующий: src/pipeline_v2/embedding_feature_builder.py
Запускаем с date_from='2025-05-01' date_to='2026-05-11'
Время: ~30-60 минут (кэш есть до 2025-04-30)
Риск: низкий (append-only к news_daily emb cols)
```

### Этап 2: Режим-детектор (1 день)

```
src/pipeline_d/regime_detector.py

Зависимости: hmmlearn (уже в .venv? проверить)
Входные данные:
  - moex_indices (IMOEX) → rolling_vol
  - market_data (USD_RUB) → rolling_vol
  - key_rate → rate_pct
  - market_data (BRENT) → trend

Выход:
  - analytics/phase_d/regime_history.csv (2022-2026, лейблы по дням)
  - analytics/phase_d/regime_current.json (текущее состояние)
  - Визуализация: режимы наложенные на IMOEX price chart

Валидация (ручная):
  2022-Q1 → должен быть "кризис" (режим 0)
  2024-01  → должен быть "высокая ставка" (режим 2)
  Если не так — тюнинг n_components или features
```

### Этап 3: Сплиты Phase D (0.5 дня)

```
src/pipeline_d/split_d.py

Создаёт DuckDB views для нового окна:
  v_d_train_market  — market_data WHERE trade_date BETWEEN '2025-01-01' AND '2025-07-31'
  v_d_train_news    — news_daily WHERE news_date BETWEEN '2025-01-01' AND '2025-07-31'
  v_d_val_market    — 2025-08-15 → 2025-11-28
  v_d_val_news      — 2025-08-15 → 2025-11-28
  v_d_test_market   — 2025-12-15 → 2026-04-29  ← ЗАМОРОЖЕН в коде
  v_d_test_news     — 2025-12-15 → 2026-04-29  ← ЗАМОРОЖЕН в коде

+ manifest: analytics/phase_d/split_manifest_d.json
  {
    "window": "same_regime",
    "regime_label": "high_rate_2025",
    "train": {"start": "2025-01-01", "end": "2025-07-31", "trading_days": 150},
    "val":   {"start": "2025-08-15", "end": "2025-11-28", "trading_days": 75},
    "test":  {"start": "2025-12-15", "end": "2026-04-29", "trading_days": 95},
    "frozen_at": "2026-06-16"
  }
```

### Этап 4: Тестбед — проверка weighted score (0.5 дня)

```
analytics/testbed/phase_d_gates_check.py

Берёт S1-S6 синтетику (уже готова)
Прогоняет с новой формулой weighted_score (0.45/0.45/0.10)
Измеряет FPR и TPR для каждого grade threshold

Ожидаем:
  Grade A (≥0.70): FPR < 5%, TPR > 80%
  Grade B (≥0.50): FPR < 15%, TPR > 90%

Если FPR Grade B > 15% → поднять порог до 0.55
Если TPR Grade A < 70% → опустить порог до 0.65
→ зафиксировать в ensemble_config_d.yaml ПЕРЕД запуском
```

### Этап 5: Hypothesis generator Phase D (1 день)

```
src/pipeline_d/hypothesis_gen_d.py

Функции:
  get_regime_rag_context(regime) → List[str]  # 3 запроса в ChromaDB
  reason_about_regime(regime, rag_ctx) → str  # DeepSeek reasoning step
  generate_hypotheses(reasoning, rag_ctx) → List[Hypothesis]  # JSON
  get_phase_c_candidates() → List[Hypothesis]  # из ledger_c.jsonl
  get_systematic_sweep(tested_combos) → List[Hypothesis]  # Strategy E

Hypothesis schema:
  {
    "instrument": str,   # из INSTRUMENTS_D
    "topic": str,        # из TOPICS_D (13 тем)
    "lag": int,          # из [1,2,3,5,7,10,14,21,30,45,60,90]
    "direction": str,    # "positive" / "negative" / "unknown"
    "rationale": str,    # обоснование
    "source_citation": str | null,  # цитата из ChromaDB (для rag_bonus)
  }
```

### Этап 6: Rolling scanner Phase D (1-2 дня)

```
src/pipeline_d/rolling_scanner_d.py

На каждой гипотезе:
  1. Загрузка данных из v_d_train_* views
  2. rolling_HAC_corr() → pre-filter (быстро, отсекаем явный шум)
     Если |corr| < 0.03 → skip (не тратим время на M5+M6)
  3. run_m5(data, hypothesis) → pvalue, z_stat
  4. run_m6(data, hypothesis) → ic, ic_pvalue
  5. weighted_score() → float
  6. grade() → A/B/C/D
  7. Если Grade A/B → run_val(hypothesis) → val_confirmed bool
  8. append_ledger_d(result)
  9. update_session_config_d()
```

### Этап 7: Phase D overnight (главный цикл, 0.5 дня)

```
src/pipeline_d/phase_d_overnight.py

acquire_lock("db/phase_d.lock")
regime = regime_detector.classify_current()
if regime_changed(regime):
    reset_confirmed_signals()
    update_window(new_regime_start)

hypotheses = hypothesis_gen_d.generate(regime, tested_combos)
results = rolling_scanner_d.run_batch(hypotheses)
update_session_config_d(results)
release_lock()
```

### Этап 8: Task Scheduler (0.5 дня)

Три задачи Windows Task Scheduler:
```
phase_d_night:   22:00 → 05:30  phase_d_overnight.py --hours 7.5
phase_d_morning: 07:00 → 14:00  phase_d_overnight.py --hours 7
phase_d_day:     15:00 → 21:00  phase_d_overnight.py --hours 6
```

---

## Файловая структура Phase D

```
src/pipeline_d/
  data_check_d.py          ← Этап 0
  regime_detector.py       ← Этап 2
  split_d.py               ← Этап 3
  topic_config_d.py        ← настройки тем (часть Этапа 1)
  news_precompute_d.py     ← Этап 1
  hypothesis_gen_d.py      ← Этап 5
  rolling_scanner_d.py     ← Этап 6
  phase_d_overnight.py     ← Этап 7

analytics/phase_d/
  PLAN.md                  ← этот файл
  regime_history.csv       ← создаётся Этапом 2
  regime_current.json      ← создаётся Этапом 2
  split_manifest_d.json    ← создаётся Этапом 3
  ensemble_config_d.yaml   ← создаётся Этапом 4
  ledger_d.jsonl           ← append-only лог (создаётся при первом запуске)
  session_config_d.json    ← адаптивный конфиг
  FINDINGS.md              ← лог найденных сигналов (создаётся при первом запуске)
```

---

## Ожидания от Phase D

На основе трёх фаз:

| Метрика | Phase B/C | Ожидание Phase D |
|---------|-----------|------------------|
| Grade A сигналы | 2 (через разные режимы) | 0-5 (в рамках одного режима) |
| Grade B сигналы | 0 (AND убивал) | 5-20 (weighted score мягче) |
| Val pass rate | ~31% → ~5% на Test | ~20-35% (один режим) |
| Test pass rate | 0/13 | TBD (надеемся на >30%) |
| Пространство | 1428 комбо | 3120 комбо |

**Главный сдвиг результата:** не ноль сигналов, а знание — «сигнал X работает в режиме 2 (высокая ставка), исчезает в режиме 3 (нормализация)». Это уже торгуемая информация.

---

## Критерии готовности к запуску

Перед первым overnight — все чекбоксы:

```
[ ] D0: data_check_d_report.md показывает ok для 2025-01 → 2026-04-29
[ ] D1: news_daily имеет 13 тем (7 старых + 6 новых) с покрытием > 500 статей/год
[ ] D1: embeddings ненулевые для 2025-05 → 2026-05
[ ] D2: regime_history.csv создан, 2022 помечен как режим ≠ 2025
[ ] D3: split_manifest_d.json зафиксирован и закоммичен
[ ] D4: ensemble_config_d.yaml содержит FPR < 15% для Grade B на тестбеде
[ ] D5: Phase C candidates (5 штук) протестированы на новом окне вручную
[ ] Task Scheduler: три задачи настроены и протестированы
```

---

## Что не делаем в Phase D

- **Не меняем** M5 и M6 core логику (frozen, FPR=0% на тестбеде)
- **Не трогаем** Phase C ledger (данные неприкосновенны)
- **Не запускаем** старый agent.py / watchdog (v1 заморожена)
- **Не используем** Test split до полного окончания поиска
- **Не добавляем** новые инструменты без проверки coverage в market_data

---

*Signal Mind Research — Phase D Plan — зафиксирован 2026-06-16*
