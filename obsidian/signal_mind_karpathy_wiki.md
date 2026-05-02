# Signal Mind + Karpathy LLM Wiki: Архитектура методологической памяти

> Документ описывает полную концепцию интеграции LLM Wiki (методология Карпаты) в систему Signal Mind.
> Цель: превратить плоские лог-файлы в живую, семантически связную базу знаний, встроенную в RAG.

---

## 1. Где мы сейчас

### Signal Mind — что это

Signal Mind — самообучающийся AI-агент для поиска слабых рыночных сигналов на российском рынке.
Архитектура: три вложенные петли обучения (Ouroboros), работающие поверх DuckDB (числовые данные),
ChromaDB (регуляторные и корпоративные документы) и SQLite (2.52M новостных статей).

**Формула:** слабый сигнал → доказательства → проверочное действие

**Достижения к маю 2026:**
- 3416+ итераций агента (марафоны 1–5)
- 17 492 чанков в RAG (ЦБ + корп. отчёты)
- 6 верифицированных реальных сигналов (r ≥ 0.60)
- Revizor: автономный аудитор с 6 проверками
- Реальный confirmed rate: ~15–25% (не 68% как было до aliasing fix)

### Текущие файлы "памяти" агента

| Файл | Тип | Состояние |
|------|-----|-----------|
| `db/knowledge.md` | Плоский лог подтверждённых паттернов | Append-only, неструктурированный |
| `db/forbidden_patterns.md` | Список анти-паттернов | 8 паттернов, плоский текст |
| `db/sql_patterns.md` | Рабочие SQL шаблоны | Плоский, без контекста применения |
| `db/signals.jsonl` | Лог всех сигналов | Сырые данные, не синтезированы |
| `db/journals/` | Дневные журналы итераций | Временные, не агрегированные |
| `db/experiments.db` | SQLite датасет fine-tuning | Структурированный, 1953+ строк |

---

## 2. Проблема: почему плоские файлы не масштабируются

### Симптомы

**Агент переоткрывает одно и то же.** На 1000-й итерации агент не помнит, что лаг 14 дней для USD/RUB → MOEXFN
уже проверен и подтверждён 47 раз с r=0.76. Он пробует снова — тратит токены, время, деньги.

**Forbidden patterns не находятся семантически.** Файл `forbidden_patterns.md` содержит 8 паттернов в виде текста.
Агент получает его целиком в контекст. Если файл вырастет до 80 паттернов — передавать его целиком
невозможно. Семантический поиск нужного паттерна по запросу невозможен (нет эмбеддингов).

**Знание плоское, не связное.** `knowledge.md` хранит факты разрозненно: "USD/RUB → MOEXFN r=0.76 lag=14d"
не связано с записью про режим ставки, не связано с тем что Revizor нашёл в том же марафоне,
не связано с условиями применимости. Это просто список строк.

**Марафонные результаты умирают.** После каждого марафона генерируется `analytics/report.html` и `audit_*.md`.
Но агент не читает эти файлы. Знание о том, что "в 4-м марафоне лаг 7 дней показал 80.2% confirmed rate"
не попадает в контекст следующей итерации.

**Итог:** система обучается медленно, повторяет ошибки, не накапливает структурное знание.
Три петли обучения (SQL repair, Ouroboros, experiments.db) работают хорошо, но
**четвёртой петли — методологической памяти — не хватает.**

---

## 3. Идея: LLM Wiki как четвёртая петля обучения

### Принцип Карпаты

Андрей Карпаты описал паттерн: Obsidian — IDE, LLM — программист, wiki — кодовая база.
Смысл: знание не извлекается заново из внешней БД при каждом вопросе — оно накапливается
в самих markdown-файлах, которые становятся умнее по мере роста.

### Применение к Signal Mind

Мы адаптируем идею так:

- **Obsidian vault** (`obsidian/`) = методологическая память агента
- **Wiki Writer** = автоматический модуль, который пишет в vault после каждой итерации
- **ChromaDB коллекция `methodology`** = семантический индекс этой памяти
- **Агент** = читает из methodology RAG как из четвёртого источника знания

**Ключевое отличие от текущего подхода:** знание не просто логируется — оно структурируется,
связывается, синтезируется и становится семантически доступным через поиск.

---

## 4. Новая архитектура системы

```
┌─────────────────────────────────────────────────────────────────┐
│                     SIGNAL MIND — ПОЛНАЯ АРХИТЕКТУРА            │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                     AGENT LOOP (Ouroboros)                │  │
│  │                                                          │  │
│  │  ① Query domain RAG     → ChromaDB: regulatory_docs     │  │
│  │  ② Query domain RAG     → ChromaDB: corp_reports        │  │
│  │  ③ Query methodology    → ChromaDB: methodology  [NEW]  │  │
│  │  ④ Query news           → hf_news.db (SQLite)           │  │
│  │  ⑤ Query market data    → signal_mind.duckdb            │  │
│  │                                                          │  │
│  │  → Generate hypothesis                                   │  │
│  │  → Execute SQL (with repair loop)                        │  │
│  │  → Evaluate signal                                       │  │
│  │  → Write to experiments.db                               │  │
│  │  → Wiki Writer  [NEW]                                    │  │
│  └──────────────────────────┬───────────────────────────────┘  │
│                             │                                   │
│              ┌──────────────▼──────────────┐                   │
│              │         Wiki Writer          │                   │
│              │  (Librarian + Synthesizer)   │                   │
│              │  1 маленький LLM-вызов       │                   │
│              │  ~$0.0002 на итерацию        │                   │
│              └──────────────┬───────────────┘                   │
│                             │                                   │
│              ┌──────────────▼──────────────┐                   │
│              │       obsidian/ vault        │                   │
│              │                             │                   │
│              │  00_System/   ← правила      │                   │
│              │  02_Sources/  ← эксперименты │                   │
│              │  03_Wiki/     ← знание       │                   │
│              │  04_Maps/     ← MOC         │                   │
│              │  05_Workbench/← синтез       │                   │
│              └──────────────┬───────────────┘                   │
│                             │ embedding при изменении           │
│              ┌──────────────▼──────────────┐                   │
│              │  ChromaDB: methodology       │                   │
│              │  paraphrase-multilingual-    │                   │
│              │  MiniLM-L12-v2              │                   │
│              └──────────────────────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
```

### Четыре слоя данных (было три, стало четыре)

| # | Слой | Источник | Что хранит |
|---|------|----------|-----------|
| 1 | Рыночные числа | DuckDB `signal_mind.duckdb` | MOEX, ставка, форекс, макро |
| 2 | Регуляторные документы | ChromaDB `regulatory_docs` | ЦБ: KGO, MFI, статистика |
| 3 | Корпоративные отчёты | ChromaDB `corp_reports` | Газпром, Лукойл, Сбер и др. |
| 4 | **Методологическая память** | **ChromaDB `methodology`** | **Что работает, что нет, как подходить** |
| + | Новости | SQLite `hf_news.db` | 2.52M статей, временной контекст |

---

## 5. Структура vault

```
obsidian/
│
├── 00_System/
│   ├── CLAUDE.md          ← главный системный промпт агента
│   ├── AGENTS.md          ← роли: Librarian, Researcher, Synthesizer, Editor, Planner
│   ├── STYLE_GUIDE.md     ← правила именования, frontmatter, wikilinks
│   └── WORKFLOWS.md       ← описание workflow A/B/C
│
├── 02_Sources/
│   ├── marathons/         ← по одной source note на марафон
│   │   ├── marathon_1.md
│   │   ├── marathon_2.md
│   │   └── ...
│   └── experiments/       ← ключевые итерации / группы итераций
│
├── 03_Wiki/
│   ├── concepts/          ← устойчивые концепты системы
│   │   ├── lag_hypothesis.md
│   │   ├── regime_detection.md
│   │   ├── aliasing_error.md
│   │   ├── ouroboros_loop.md
│   │   └── signal_confirmation.md
│   │
│   ├── signals/           ← verified сигналы — по одному файлу на сигнал
│   │   ├── usd_rub_moexfn_14d.md
│   │   ├── brent_moexfn_90d.md
│   │   ├── key_rate_moexfn_0d.md
│   │   ├── msci_india_moexfn_0d.md
│   │   ├── sp500_moexfn_7d.md
│   │   └── banking_news_moexfn_14d.md
│   │
│   ├── methods/           ← SQL паттерны, подходы анализа
│   │   ├── lag_sweep_sql.md
│   │   ├── correlation_query.md
│   │   ├── regime_conditional.md
│   │   └── news_join.md
│   │
│   └── anti_patterns/     ← forbidden patterns — по одному файлу
│       ├── instrument_aliasing.md
│       ├── tautological_correlation.md
│       ├── regime_mismatch.md
│       ├── weak_r_confirmation.md
│       └── convergence_trap.md
│
├── 04_Maps/
│   ├── MOC_Signals.md     ← карта всех сигналов
│   ├── MOC_Methods.md     ← карта методов
│   └── MOC_AntiPatterns.md← карта anti-patterns
│
└── 05_Workbench/
    ├── briefs/            ← research briefs по темам
    │   ├── brief_lag_analysis.md
    │   └── brief_regime_dependency.md
    └── synthesis/         ← агрегированные выводы
        └── marathon_synthesis.md
```

---

## 6. Что куда переезжает

| Было | Станет | Формат |
|------|--------|--------|
| `db/knowledge.md` (плоский лог) | `03_Wiki/signals/*.md` | По одному файлу на сигнал, с frontmatter, wikilinks |
| `db/forbidden_patterns.md` (список) | `03_Wiki/anti_patterns/*.md` | По одному файлу, с примером SQL, условием, историей |
| `db/sql_patterns.md` | `03_Wiki/methods/*.md` | По паттерну, с когда применять, ограничениями |
| `analytics/audit_*.md` | `02_Sources/marathons/*.md` | Source notes + синтез в wiki |
| `db/journals/*.md` | `02_Sources/experiments/` | При накоплении → синтез в concepts |

**Важно:** старые файлы не удаляются сразу. Они остаются как источники (source of truth),
wiki — производный слой, который агент читает через RAG.

---

## 7. Роли агентов в контексте Signal Mind

### Role 1: Librarian (Ingestion)

Запускается после каждой итерации агента (автоматически через Wiki Writer).

**Задача:** принять результат итерации и создать/обновить source note.

**Входные данные:**
- исход (confirmed/rejected/partial)
- сигнал (инструмент, лаг, r, n)
- SQL запрос
- режим рынка (bull/bear/rate-hike/neutral)
- топик итерации

**Выходные данные:**
- обновлённый файл сигнала в `03_Wiki/signals/` (если confirmed)
- обновлённый anti-pattern в `03_Wiki/anti_patterns/` (если новая ошибка)
- запись в `02_Sources/experiments/` (если значимая итерация)

### Role 2: Synthesizer (Knowledge Building)

Запускается после каждого марафона (через Revizor или вручную).

**Задача:** из накопленных source notes за марафон обновить canonical wiki pages.

**Например:** после Marathon 5 Synthesizer читает 400+ source notes и:
- обновляет `03_Wiki/concepts/lag_hypothesis.md` новыми данными о лагах
- обновляет `03_Wiki/signals/usd_rub_moexfn_14d.md` свежими r/n цифрами
- создаёт `05_Workbench/synthesis/marathon_5_synthesis.md`

### Role 3: Researcher (Deep Dive)

Запускается при необходимости найти ответ по базе знаний.

**Например:** агент хочет понять "что мы знаем про режим высокой ставки + медвежий рынок?"
→ Researcher ищет по vault → формирует brief → агент использует его в гипотезе.

### Role 4: Editor (Quality Maintenance)

Запускается раз в неделю или вручную.

**Задача:** чистка vault — удаление дублей, починка ссылок, нормализация frontmatter.

### Role 5: Planner (Strategy)

Запускается вручную для планирования следующего марафона.

**Задача:** взять vault как контекст → спланировать: какие лаги тестировать, какие режимы,
где пробелы в знании.

---

## 8. Wiki Writer — автоматический модуль

### Место в архитектуре

```
src/agent/wiki_writer.py   ← новый файл
```

Вызывается в конце каждой итерации в `agent.py`, после записи в `experiments.db`.

### Что делает

1. Принимает `IterationTelemetry` + результат оценки сигнала
2. Формирует маленький запрос к DeepSeek (Librarian роль)
3. LLM решает: какой файл создать/обновить
4. Wiki Writer пишет/обновляет .md файл
5. Если файл изменился → re-embed в ChromaDB `methodology`

### Стоимость

- 1 маленький LLM-вызов ≈ 300 входных + 200 выходных токенов
- ≈ $0.0001–0.0002 на итерацию при DeepSeek ценах
- За 1000 итераций марафона ≈ $0.10–0.20 дополнительно
- Re-embedding: только при изменении файла (~10–20% итераций) → незначительно

### Логика принятия решений

```
if signal.confirmed AND signal.r >= 0.50:
    → обновить или создать 03_Wiki/signals/{signal_id}.md

if signal.rejected AND new_pattern_detected:
    → обновить или создать 03_Wiki/anti_patterns/{pattern_id}.md

if new_sql_pattern AND repair_succeeded:
    → обновить 03_Wiki/methods/{method_id}.md

always:
    → append краткую запись в 02_Sources/experiments/{date}.md
```

### Структура сигнального файла (пример)

```markdown
---
type: signal
status: verified
instrument_a: USD_RUB
instrument_b: MOEXFN
lag_days: 14
r: 0.758
n: 990
regime: all
confidence: high
updated: 2026-05-02
tags: [signal, forex, moexfn, confirmed]
---

# USD/RUB → MOEXFN (lag 14d)

## Summary
Курс доллара к рублю предсказывает финансовый индекс MOEX через 14 дней.
Корреляция r=0.758 на n=990 наблюдений. Один из самых стабильных сигналов системы.

## Статистика
- r: +0.758
- lag: 14 дней
- n: 990
- Первый подтверждённый: Marathon 1 (2026-04-30)
- Последнее подтверждение: Marathon 5 (2026-05-02)
- Количество подтверждений: 47

## Условия применимости
- Работает во всех режимах рынка
- Ослабевает при резких административных интервенциях (e.g., сентябрь 2022)
- Усиливается при ключевой ставке > 15%

## Экономическая логика
Ослабление рубля → рост стоимости активов в рублёвом выражении для финансового сектора,
который держит значимую долю валютных активов.

## Связанные концепты
- [[regime_detection]]
- [[lag_hypothesis]]

## Связанные сигналы
- [[brent_moexfn_90d]] — через нефть влияет на тот же сектор
- [[key_rate_moexfn_0d]] — высокая ставка усиливает этот сигнал

## История подтверждений
| Марафон | r | n | Условия |
|---------|---|---|---------|
| Marathon 1 | 0.742 | 850 | mixed regime |
| Marathon 3 | 0.758 | 990 | rate-hike regime |
| Marathon 5 | 0.761 | 995 | rate-hike regime |

## Open questions
- Насколько сигнал сохранится при снижении ставки ниже 10%?
- Есть ли нелинейный эффект при USD/RUB > 100?
```

### Структура anti-pattern файла (пример)

```markdown
---
type: anti_pattern
status: active
pattern_id: instrument_aliasing
severity: critical
first_seen: 2026-04-30
updated: 2026-05-02
tags: [anti_pattern, sql, aliasing]
---

# Instrument Aliasing

## Summary
Агент переименовывает колонку одного инструмента именем другого в SQL.
Результат: CORR() считает корреляцию инструмента с самим собой → r ≈ 0.88 по построению.

## Пример ошибки
```sql
-- НЕПРАВИЛЬНО: обе колонки — это imoex_close
SELECT CORR(imoex_close, imoex_close) AS ftse_correlation
FROM v_market_context
-- Агент видел r=0.88 и считал это "confirmed"
```

## Правильно
```sql
-- market_data.ftse_china_50 — отдельная таблица с реальными данными FTSE
SELECT CORR(m.imoex_close, f.close_price) AS ftse_correlation
FROM v_market_context m
JOIN market_data f ON m.trade_date = f.trade_date
WHERE f.instrument = 'FTSE_CHINA_50'
```

## Масштаб ущерба
Marathon 1-2: ~60% "confirmed" сигналов были артефактами этой ошибки.
Реальный confirmed rate был 68%, должен был быть ~15%.

## Как Revizor детектирует
Проверка: одна и та же колонка используется в обоих аргументах CORR().
Файл: `src/agent/revizor.py`, метод `check_aliasing()`.

## Связанные концепты
- [[signal_confirmation]]
- [[tautological_correlation]]

## История
- 2026-04-30: обнаружен в Marathon 1 при Phase 6 верификации
- 2026-04-30: добавлен в forbidden_patterns.md #8
- 2026-05-01: Revizor.check_aliasing() — автоматическое обнаружение
```

---

## 9. ChromaDB: коллекция `methodology`

### Параметры

| Параметр | Значение |
|----------|---------|
| Коллекция | `methodology` |
| Модель | `paraphrase-multilingual-MiniLM-L12-v2` (та же что regulatory/corp) |
| Chunk size | 600 (меньше чем regulatory — wiki страницы короче) |
| Overlap | 100 |
| Хранилище | `db/chroma/` (рядом с regulatory/corp) |

### Что индексируется

- Все файлы `03_Wiki/signals/*.md`
- Все файлы `03_Wiki/anti_patterns/*.md`
- Все файлы `03_Wiki/methods/*.md`
- Все файлы `03_Wiki/concepts/*.md`
- Все файлы `04_Maps/*.md`
- Файлы `05_Workbench/synthesis/*.md`

НЕ индексируется:
- `02_Sources/` — сырые данные, слишком объёмные, для поиска не нужны
- `00_System/` — системные промпты, не методологическое знание

### Изменения в `rag.py`

Новая функция:

```python
def get_methodology_context(query: str, top_k: int = 5) -> str:
    """
    Запрашивает методологическую память: что работало, что нет, как подходить.
    Используется агентом перед генерацией гипотезы.
    """
```

Обновление `get_context()`:

```python
def get_context(query: str, year: int = None, top_k: int = 8) -> str:
    """
    Полный контекст из всех трёх ChromaDB коллекций:
    - regulatory_docs (домен: ЦБ)
    - corp_reports (корп. отчёты)
    - methodology (методологическая память)  ← NEW
    """
```

### Как агент использует methodology RAG

В `hypothesis.py`, в `generate_hypothesis()`:

```python
# ① Получить регуляторный/корп контекст (как сейчас)
domain_context = rag.get_context(query=topic, year=current_year)

# ② Получить методологический контекст (НОВОЕ)
method_context = rag.get_methodology_context(
    query=f"{topic} lag={lag_days} signal approach"
)

# ③ Объединить в промпт для DeepSeek
prompt = build_hypothesis_prompt(
    topic=topic,
    domain_context=domain_context,
    method_context=method_context,  ← НОВОЕ
    schema=schema,
    regime=current_regime
)
```

Агент узнаёт из methodology RAG:
- "Этот сигнал уже проверялся 12 раз — вот что нашли"
- "В этом режиме этот лаг не работает — вот почему"
- "SQL для этого запроса должен выглядеть вот так (из methods/)"
- "Вот anti-pattern которого надо избежать при таком типе запроса"

---

## 10. Обновление петель обучения

### Было: три петли

```
Петля 3 (cross-session): experiments.db → fine-tuning датасет
  Петля 2 (per-session): Ouroboros hypothesis cycle
    Петля 1 (per-iteration): SQL self-repair loop
```

### Стало: четыре петли

```
Петля 4 (cumulative): LLM Wiki → methodology RAG → агент читает из неё
  Петля 3 (cross-session): experiments.db → fine-tuning датасет
    Петля 2 (per-session): Ouroboros hypothesis cycle
      Петля 1 (per-iteration): SQL self-repair loop
```

**Петля 4** — самая медленная и самая стратегическая. Она накапливается across сессий и марафонов.
С каждым марафоном wiki становится богаче → methodology RAG становится точнее → агент
делает меньше ошибок и быстрее находит рабочие подходы.

### Связь с существующими модулями

| Модуль | Что меняется |
|--------|-------------|
| `agent.py` | В конце итерации: вызов `wiki_writer.update(telemetry, result)` |
| `hypothesis.py` | В `generate_hypothesis()`: добавить `method_context` из RAG |
| `rag.py` | Добавить `get_methodology_context()` и обновить `get_context()` |
| `revizor.py` | После аудита: запуск Synthesizer роли для синтеза марафона |
| `watchdog.py` | После завершения марафона: запуск Synthesizer (полный синтез) |

**Новые файлы:**
- `src/agent/wiki_writer.py` — Librarian + Synthesizer роли
- `src/agent/wiki_embedder.py` — re-embedding при изменении файлов

---

## 11. Workflow системы

### Workflow A: Per-iteration (автоматически)

```
Итерация завершается
   ↓
experiments.db ← telemetry записана
   ↓
wiki_writer.update()
   ↓
Librarian: определить что обновить
   ↓
if confirmed signal → обновить 03_Wiki/signals/{id}.md
if new anti-pattern → обновить 03_Wiki/anti_patterns/{id}.md
if good SQL found  → обновить 03_Wiki/methods/{id}.md
always             → append 02_Sources/experiments/{date}.md
   ↓
wiki_embedder.check_and_reembed()
   ↓
if file changed → re-embed в ChromaDB:methodology
```

### Workflow B: Per-marathon (автоматически через Revizor)

```
Марафон завершился
   ↓
Revizor: 6 проверок, apply_fixes()
   ↓
Wiki Synthesizer (через revizor.py или watchdog.py)
   ↓
Читает все source notes за марафон
   ↓
Обновляет canonical concept pages (lag_hypothesis, regime_detection, etc.)
   ↓
Создаёт 05_Workbench/synthesis/marathon_N_synthesis.md
   ↓
Обновляет 04_Maps/MOC_Signals.md
   ↓
Re-embed всех изменённых файлов
```

### Workflow C: Weekly maintenance (вручную или по расписанию)

```
Editor pass по vault
   ↓
Найти дубли, broken links, страницы без summary
   ↓
Planner: где в базе знаний пробелы? что тестировать в следующем марафоне?
   ↓
Обновить дорожную карту
```

---

## 12. Первые три шага реализации

### Шаг 1: Структурировать существующее знание (без нового кода)

Перевести плоские файлы в wiki формат:

1. `db/forbidden_patterns.md` → 8 файлов в `03_Wiki/anti_patterns/`
2. Верифицированные сигналы → 6 файлов в `03_Wiki/signals/`
3. `db/sql_patterns.md` → файлы в `03_Wiki/methods/`
4. Создать `00_System/CLAUDE.md`, `00_System/AGENTS.md`
5. Создать `04_Maps/` MOC файлы

Это чистая работа с markdown, никаких изменений в коде.

### Шаг 2: Добавить ChromaDB коллекцию `methodology`

1. Написать `src/agent/wiki_embedder.py`
   - сканирует `obsidian/03_Wiki/` и `obsidian/04_Maps/`
   - чанкует (600/100)
   - загружает в ChromaDB `methodology`
2. Обновить `src/agent/rag.py`
   - `get_methodology_context(query, top_k=5)`
   - обновить `get_context()` — добавить третий источник
3. Запустить первый embedding всего vault

### Шаг 3: Написать `wiki_writer.py` и встроить в loop

1. Написать `src/agent/wiki_writer.py`
   - принимает `IterationTelemetry` + `evaluation_result`
   - делает маленький DeepSeek вызов (Librarian режим)
   - пишет/обновляет .md файлы
   - вызывает `wiki_embedder.check_and_reembed()` для изменённых файлов
2. В `agent.py` в конце итерации:
   - `wiki_writer.update(telemetry, result)`
3. В `revizor.py` после `apply_fixes()`:
   - запуск Synthesizer для марафонного синтеза

---

## 13. Ожидаемый эффект

### Через 3 марафона

- Wiki: ~50–100 файлов, 30–50 сигналов, 15–20 anti-patterns, 20+ методов
- Агент перестаёт предлагать aliasing-подобные паттерны (RAG находит anti_pattern и предупреждает)
- Confirmed signals начинают цитировать историю: "этот сигнал подтверждён 23 раза"

### Через 10 марафонов

- Wiki: ~300–500 файлов
- Methodology RAG содержит полную историю экспериментов
- Агент генерирует гипотезы нового класса: "в этом режиме, с этим лагом, на основе прошлых данных,
  ожидаю r=0.65–0.75 — вот почему"
- Fine-tuning датасет (experiments.db) обогащён wiki-контекстом → датасет для обучения
  модели "с памятью"

### Ключевой эффект

**Агент на 1000-й итерации знает всё что знал на 100-й — структурно, семантически, через поиск.
Каждый следующий марафон умнее предыдущего не потому что модель изменилась,
а потому что база знаний выросла.**

---

## 14. Связь с Karpathy — точное соответствие

| Karpathy | Signal Mind |
|----------|-------------|
| Obsidian = IDE | `obsidian/` vault |
| LLM = программист | DeepSeek Librarian/Synthesizer роли |
| Wiki = кодовая база | `03_Wiki/` — structured knowledge |
| Пользователь управляет направлением | Марафоны, Revizor, ручной синтез |
| Знание накапливается в .md, не в векторной БД | Wiki → ChromaDB (производный слой) |
| "База умнее по мере роста" | Каждый марафон → богаче RAG → лучше гипотезы |

---

## 15. Что НЕ меняется

- Три петли обучения (SQL repair, Ouroboros, experiments.db) — остаются как есть
- DuckDB данные — нетронуты
- ChromaDB `regulatory_docs` и `corp_reports` — нетронуты
- `hf_news.db` — ТОЛЬКО ЧИТАТЬ (правило неизменно)
- Revizor, watchdog — расширяются, не переписываются
- DeepSeek API — тот же клиент

---

*Документ: `obsidian/signal_mind_karpathy_wiki.md`*
*Создан: 2026-05-02*
*Следующий шаг: Шаг 1 — структурировать существующее знание в wiki формат*
