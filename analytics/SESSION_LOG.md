# Signal Mind — Полный лог сессии 2026-04-30

Документ фиксирует всё что было сделано, найдено, исправлено и понято.
Написан для человека и для модели в будущих сессиях.

---

## 1. ЧТО СТРОИЛИ (Phase 5.5 → Phase 6)

### Новые модули созданы

| Файл | Что делает |
|------|-----------|
| `src/agent/news_precompute.py` | Одноразовый: ATTACH hf_news.db → агрегирует 2.52M статей в `news_daily` таблицу DuckDB (1710 дней × 7 тем). Запущен за 60.9s. |
| `src/agent/news_retriever.py` | Живой поиск в hf_news.db по ключевым словам (RU→EN). Read-only. |
| `src/agent/telemetry.py` | `IterationTelemetry` dataclass. Пишет в `db/telemetry.jsonl` на каждой итерации: токены gen/eval, время каждого шага (RAG/news/SQL/LLM), размеры контекста. |
| `src/agent/watchdog.py` | Запускает агента как subprocess. Проверяет каждые 60s. При падении — ждёт 10s и перезапускает с оставшимся временем. Часовой лог статуса. |
| `analytics/generate_report.py` | Читает telemetry.jsonl + signals.jsonl + metrics.jsonl + experiments.db. Генерирует самодостаточный HTML с 8 Chart.js графиками (Chart.js встроен). |
| `analytics/signal_scan.py` | Систематический чистый скан: 618 комбинаций (source × target × lag). Каждый источник строго из правильной таблицы. |
| `analytics/verify_signals.py` | Точечная верификация конкретных кандидатов. |

### Изменения в существующих файлах

| Файл | Что изменили |
|------|-------------|
| `src/agent/agent.py` | LAG_SWEEP=[0,7,14,0,30,0,60,0,90,0], TOPIC_POOL (22 темы), RANDOM_JUMP_PROB=0.25, max_seconds, watchdog-совместимость |
| `src/agent/hypothesis.py` | Возвращает (result, attempts, IterationTelemetry). chat_with_usage() для токен-трекинга. lag_days параметр. |
| `src/agent/llm.py` | Добавлен chat_with_usage() → (text, usage_dict) |
| `src/agent/schema.py` | news_daily, LAG HYPOTHESIS TEMPLATE, предупреждения об aliasing с верифицированными числами |
| `src/agent/experiments.py` | Колонка lag_days |
| `src/parsers/investing_market.py` | +7 инструментов: MSCI_WORLD, MSCI_INDIA, FTSE_CHINA_50, CHINA_H_SHARES, DJ_SOUTH_AFRICA, SILVER, ALUMINUM |
| `db/forbidden_patterns.md` | Паттерн #8: instrument aliasing (с примерами из реального марафона) |
| `analysis_principles.md` | Принцип "Data source integrity" + tautology detector |

---

## 2. МАРАФОН (9 часов, 2026-04-30 01:01–10:01)

```
Запуск: .venv/Scripts/python -m src.agent.watchdog 32400
Session: 20260430_010153_e24e91
```

| Метрика | Значение |
|---------|---------|
| Итераций | 1 943 |
| Confirmed (по версии агента) | 1 315 (67.7%) |
| Реальный confirmed rate | ~15–25% |
| Partial | 26 |
| Rejected | 602 |
| Токенов | 409.8M |
| Стоимость DeepSeek | $5.76 |
| Avg время итерации | 14.4s |
| SQL repair events | 22 (0 failed) |
| Перезапусков watchdog | 0 |
| Avg шаги | RAG 65ms, News 25ms, SQL 40ms, LLM-gen 8s, LLM-eval 5s |

---

## 3. ГЛАВНАЯ НАХОДКА: СИСТЕМНАЯ ОШИБКА АГЕНТА

### Что нашли

Агент при генерации SQL для гипотез о **иностранных индексах** (FTSE_CHINA_50, MSCI_INDIA,
DJ_SOUTH_AFRICA, DXY) использовал **неправильные колонки**:

```sql
-- ЧТО АГЕНТ ПИСАЛ (ОШИБКА):
SELECT m1.imoex_close AS ftse_china_50, s.moexog_oil_gas ...
--     ↑ это IMOEX, не FTSE!

SELECT m1.usd_rub AS dxy, m2.usd_rub ...
--     ↑ это USD/RUB, не DXY!
```

```sql
-- КАК ДОЛЖНО БЫТЬ (ПРАВИЛЬНО):
SELECT d.close AS ftse_china_50, s.moexog_oil_gas ...
FROM market_data d
WHERE d.instrument = 'FTSE_CHINA_50'
```

### Почему агент "подтверждал" эти сигналы

IMOEX (российский индекс) vs MOEXOG (российский сектор): **r = 0.877** — оба российские,
конечно коррелируют. Агент видел сильную корреляцию и ставил confirmed=True, score=85.

### Масштаб ущерба

~60% сигналов с score=85 в кластерах DXY/FTSE/MSCI/DJ — артефакты этой ошибки.

### Исправление

1. `db/forbidden_patterns.md` — паттерн #8 с примерами и source map
2. `analysis_principles.md` — принцип verifikации источника + детектор тавтологии
3. `src/agent/schema.py` — CRITICAL предупреждения с реальными верифицированными числами
4. `analytics/signal_brief.md` — задокументировано для истории

---

## 4. ВЕРИФИЦИРОВАННЫЕ РЕАЛЬНЫЕ СИГНАЛЫ

Проверены независимо через DuckDB с правильными источниками. Минимум n=150.

### Сильные (|r| ≥ 0.60)

| Сигнал | r | lag | n | Механизм |
|--------|---|-----|---|----------|
| USD/RUB → MOEXFN | **+0.758** | 14d | 990 | Девальвация → переоценка рублёвых активов вверх |
| USD/RUB → IMOEX  | **+0.757** | 60d | 595 | Курс рубля определяет рынок с 2-месячным лагом |
| USD/RUB → MOEX10 | **+0.743** | 14d | 990 | То же, голубые фишки |
| Brent → MOEXFN   | **-0.702** | 90d | 799 | Рост нефти → укрепление рубля → сжатие банковской маржи |
| KEY_RATE → MOEXFN | **+0.651** | 0d | 992 | Высокая ставка → банки зарабатывают на марже |
| MSCI_INDIA → MOEXFN | **+0.631** | 0d | 986 | Глобальный EM-аппетит двигает оба рынка |

### Умеренные (0.35 ≤ |r| < 0.60)

| Сигнал | r | lag | n |
|--------|---|-----|---|
| MSCI_WORLD → MOEXFN | +0.599 | 0d  | 992 |
| SP500 → MOEXFN      | +0.593 | 7d  | 987 |
| Brent → USD/RUB     | -0.564 | 90d | 827 |
| Banking news → MOEXFN | +0.549 | 14d | 383 | ⚠️ ТОЛЬКО при ставке < 15% |
| GOLD → MOEXFN       | +0.434 | 60d | 378 |
| SILVER → MOEXFN     | +0.366 | 14d | 760 |

### Условные (режимозависимые)

**MSCI_INDIA → MOEXFN lag 14d** — классический пример режимного сигнала:

| Рубль | Ставка | r | n |
|-------|--------|---|---|
| Сильный (<80) | Высокая (≥15%) | +0.776 | 110 |
| Слабый (>80) | Низкая (<15%)  | +0.884 | 108 |
| Слабый (>80) | Высокая (≥15%) | **-0.060** | **495** ← текущий режим |
| Сильный (<80) | Низкая (<15%)  | +0.463 | 270 |

Вывод: в текущем режиме (слабый рубль + высокая ставка) сигнал **отключён**.

### Нулевые (задокументировано честно)

| Сигнал | r | Почему нет сигнала |
|--------|---|-------------------|
| DXY → USD/RUB lag 7d | -0.09 | USD/RUB отвязан от DXY санкциями и капитальными ограничениями |
| Oil news → MOEXOG | 0.11 | Слишком слабо на всех лагах |
| Sanctions news → MOEX10 | -0.08 | Нет предиктивной силы |
| FTSE_CHINA_50 → MOEXOG | -0.20 | Обратная связь (не та что агент "нашёл") |

---

## 5. ИНСТРУМЕНТЫ АНАЛИЗА (созданы в этой сессии)

```bash
# Генерация HTML отчёта (8 графиков, Chart.js встроен)
.venv/Scripts/python -m analytics.generate_report

# Систематический чистый скан всех комбинаций
.venv/Scripts/python -m analytics.signal_scan
# → analytics/signal_scan_results.csv (618 строк)

# Точечная верификация конкретных гипотез
.venv/Scripts/python -m analytics.verify_signals

# Запуск следующего марафона (с watchdog)
.venv/Scripts/python -m src.agent.watchdog 32400
```

---

## 6. ДАННЫЕ (текущий статус всего)

| Источник | Статус | Объём |
|----------|--------|-------|
| DuckDB signal_mind.duckdb | Полный | MOEX 2016-2026, Forex, Brent, Gold, 13 инструментов |
| ChromaDB | Полный | 17 492 чанка (15 574 corp + 1 918 regulatory) |
| SQLite hf_news.db | Полный, read-only | 2.52M статей, 9.93 GB, 2021-2025 |
| news_daily (DuckDB) | Полный | 1710 дней × 7 тем |
| experiments.db | 1 953 строки | 8 сессий, датасет A/B/C |
| signals.jsonl | 1 981 запись | Полный лог |
| telemetry.jsonl | 1 943 записи | Токены, время, контекст |
| sql_patterns.md | Актуален | Рабочие SQL шаблоны |
| forbidden_patterns.md | 8 паттернов | Включая aliasing bug |
| knowledge.md | Актуален | Confirmed findings |

---

## 7. СЛЕДУЮЩИЕ ШАГИ (дорожная карта)

| Приоритет | Задача |
|-----------|--------|
| 1 | **Phase 7: Второй марафон** с исправленным агентом. Без aliasing. Ожидаем реальный confirmed rate ~20-30% и честные данные для fine-tuning. |
| 2 | Углубить режимный анализ — Brent→MOEXFN lag 90d: проверить на подпериодах (до/после 2024). |
| 3 | MSCI_INDIA→MOEXFN: мониторить — как только ставка снизится ниже 15%, сигнал должен включиться. |
| 4 | **Phase 8: Fine-tuning** на 1 953 строках experiments.db. Dataset B (sql_repair) особенно ценен — содержит реальные ошибки с исправлениями. |

---

## 8. МЕТА-УРОК

Мы прошли полный научный цикл:

```
1. Сформулировали гипотезы (TOPIC_POOL)
2. Агент проверял их автономно 9 часов
3. Независимо верифицировали результаты
4. Нашли системную ошибку
5. Исправили правило в памяти агента
6. Провели чистый скан → нашли настоящие сигналы
7. Задокументировали всё
```

Ошибка агента (aliasing) — это не провал. Это:
- Ценный обучающий пример для Dataset B (sql_repair)
- Задокументированный anti-pattern который не повторится
- Доказательство что верификация необходима

**Честный вывод:** 6 сильных реальных сигналов из систематического скана —
это реальный научный результат. Brent→MOEXFN (-0.70, lag 90d) и USD/RUB→MOEXFN (+0.76, lag 14d)
— устойчивые, большой n, понятный механизм. Это основа для следующей фазы.
