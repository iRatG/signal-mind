# ТЗ: Obsidian + Methodology RAG — Петля обучения 4

```
Статус    : черновик
Версия    : 0.1
Дата      : 2026-05-02
Автор     : Claude Sonnet 4.6 + iRatG
Приоритет : P1 (после Phase 10 data split)
```

---

## 1. Контекст: что мы построили

Signal Mind — AI-агент который ищет слабые рыночные сигналы на данных MOEX/ЦБ/Росстат.
За 4 марафона (3 416 итераций, ~$10, ~714M токенов) система прошла несколько эволюций:

### Три существующих петли обучения

```
Петля 1 (per-iteration): SQL self-repair
  ошибка DuckDB → классификация (8 типов) → автоисправление

Петля 2 (per-session): Ouroboros hypothesis cycle
  гипотеза → SQL → данные → оценка → новая гипотеза

Петля 3 (cross-session): накопление датасета
  experiments.db (3 416 строк) → fine-tuning DeepSeek
```

### Текущая система памяти агента (плоские файлы, загружаются при старте)

| Файл | Содержание | Проблема |
|------|-----------|---------|
| `db/principles.md` | Аналитическая конституция | Статичная, не растёт |
| `db/knowledge.md` | Подтверждённые находки | Нет структуры, нет эволюции |
| `db/forbidden_patterns.md` | SQL антипаттерны | Плоский текст, нет семантики |
| `db/sql_patterns.md` | Рабочие SQL шаблоны | Нет связи с причиной/следствием |
| `db/convergence_blacklist.json` | Темы-ловушки | Только fingerprints, нет объяснений |
| `db/current_regime.json` | Режим рынка | Нет истории режимов |

**Фундаментальная проблема:** агент читает методологию как плоский текст при старте.
Он не может спросить: *"что мы уже знаем именно про этот тип сигнала?"*
Он не видит: *"почему этот подход был отброшен в Marathon 2?"*
Он не знает: *"какие атаки открыты против текущей гипотезы?"*

---

## 2. Проблема которую решаем

### 2.1 Методологический дрейф

Агент повторяет одни и те же системные ошибки потому что:
- `forbidden_patterns.md` растёт как список правил, но без причинно-следственного контекста
- Нет трассировки: "это правило появилось потому что в Marathon 1 случилось X"
- Нет связи между правилом и его scope: когда оно применяется, когда — нет

### 2.2 Потеря эволюции идей

Сейчас нет ответа на вопрос: *"почему мы используем Ouroboros а не [другой подход]?"*
Решения принимались в чате, фиксировались в памяти Claude Code, но не в структурированном
виде доступном агенту и пользователю одновременно.

### 2.3 Атаки на сигналы не систематизированы

Три «сильных» сигнала (USD/RUB→MOEXFN, Brent→MOEXFN, MSCI_INDIA→MOEXFN) имеют открытые
методологические уязвимости — но агент не знает о них при генерации гипотез.
Он может снова «подтвердить» нестационарный сигнал не зная что он уже атакован.

### 2.4 Нет живой связи между наблюдением и действием

Паттерн сейчас:
```
наблюдение в сессии → memory/ Claude Code (не видит агент)
                    → db/forbidden_patterns.md (плоский текст)
```

Нужный паттерн:
```
наблюдение → Obsidian vault (структурированно) → ChromaDB → агент запрашивает
```

---

## 3. Новый подход: Obsidian как Epistemological Journal

### 3.1 Концепция

Obsidian vault — не просто документация. Это **живая методологическая память**
которую одновременно читают: пользователь (через Obsidian UI) и агент (через RAG).

Принцип из Karpathy LLM Wiki:
> Vault — это IDE. LLM — программист. Markdown-файлы — кодовая база знаний.
> Знание накапливается в самих файлах, а не извлекается заново при каждом вопросе.

Адаптация для Signal Mind:
> Каждый сигнал — живой документ с историей верификации и открытыми атаками.
> Каждый подход — страница с полем `status: active | deprecated` и `replaced_by:`.
> Агент запрашивает vault семантически перед генерацией каждой гипотезы.

### 3.2 Петля 4 — Methodology RAG

```
┌─────────────────────────────────────────────────────────────────┐
│  Петля 4: methodology-driven hypothesis generation              │
│                                                                 │
│  Obsidian vault                                                 │
│    01_Signals/    ← живые документы сигналов                   │
│    02_Approaches/ ← эволюция методов                           │
│    03_Attacks/    ← атаки на гипотезы                          │
│    04_Decisions/  ← архитектурные решения                      │
│    05_Concepts/   ← стационарность, режимность, self-eval      │
│         ↓  obsidian_indexer.py (при обновлении файлов)         │
│  ChromaDB collection: "methodology"                             │
│         ↓  search_methodology(query) в rag.py                  │
│  Агент получает контекст ДО генерации гипотезы                 │
│  "USD/RUB → MOEXFN уже атакован: 2024 инверсия r=-0.47"       │
│         ↓                                                       │
│  Более осознанная гипотеза → более честная оценка              │
└─────────────────────────────────────────────────────────────────┘
         ↓ после сессии
┌─────────────────────────────────────────────────────────────────┐
│  Revizor читает новые находки → обновляет vault файлы           │
│  → re-index → следующий марафон знает больше                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. Структура Obsidian vault

```
obsidian/
├── 00_System/
│   ├── CLAUDE.md          ← системный промпт для Claude в роли vault-агента
│   ├── AGENTS.md          ← описание ролей (Auditor, Researcher, Synthesizer)
│   ├── STYLE_GUIDE.md     ← правила frontmatter, именования, wikilinks
│   └── INDEX.md           ← карта всего vault (обновляется автоматически)
│
├── 01_Signals/            ← каждый сигнал — отдельный живой документ
│   ├── signal_usd_rub_moexfn.md
│   ├── signal_brent_moexfn.md
│   └── signal_msci_india_moexfn.md
│
├── 02_Approaches/         ← методологические подходы с историей
│   ├── approach_ouroboros_loop.md
│   ├── approach_llm_self_eval.md        ← status: deprecated
│   ├── approach_aliasing_detection.md
│   ├── approach_revizor_autofixes.md
│   └── approach_anti_convergence.md
│
├── 03_Attacks/            ← атаки на гипотезы и методологию
│   ├── attack_in_sample_overfitting.md  ← P0: не закрыта
│   ├── attack_non_stationarity.md       ← P0: не закрыта
│   ├── attack_multiple_testing.md       ← P1: не закрыта
│   ├── attack_llm_eval_inflation.md     ← частично закрыта
│   └── attack_regime_conditionality.md  ← в работе
│
├── 04_Decisions/          ← почему выбрали именно это
│   ├── decision_duckdb_vs_postgres.md
│   ├── decision_deepseek_api.md
│   ├── decision_lag_sweep_design.md
│   └── decision_chromadb_rag.md
│
├── 05_Concepts/           ← устойчивые концепты проекта
│   ├── concept_regime_conditionality.md
│   ├── concept_signal_nonstationarity.md
│   ├── concept_weak_signal_detection.md
│   └── concept_self_repair_loop.md
│
└── 06_Workbench/          ← активные research briefs и планы
    ├── brief_phase10_holdout_protocol.md
    ├── brief_devil_advocate_agent.md
    └── plan_marathon5_prep.md
```

---

## 5. Шаблоны ключевых типов файлов

### 5.1 Сигнал (01_Signals/)

```markdown
---
type: signal
status: under_attack          # confirmed | under_attack | invalidated | conditional
instrument_a: USD/RUB
instrument_b: MOEXFN
lag_days: 14
r_full_sample: 0.758
n_full_sample: 990
date_found: 2026-04-30
date_verified: 2026-05-02
marathon: 1
confidence: medium
tags: [signal, forex, banking]
updated: 2026-05-02
---

# Signal: USD/RUB → MOEXFN (lag 14d)

## Суть
Рост USD/RUB (ослабление рубля) коррелирует с ростом MOEXFN через 14 дней.
r = +0.758, n = 990, период 2022–2025.

## SQL верификация (live DuckDB)
\`\`\`sql
WITH lagged AS (
    SELECT m1.trade_date,
           m1.usd_rub,
           m2.moexfn_finance
    FROM v_market_context m1
    JOIN v_moex_sectors m2 ON m2.trade_date = m1.trade_date + INTERVAL 14 DAYS
    WHERE m1.usd_rub IS NOT NULL AND m2.moexfn_finance IS NOT NULL
      AND m1.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
)
SELECT ROUND(CORR(usd_rub, moexfn_finance), 4) AS r, COUNT(*) AS n FROM lagged;
-- r = 0.7580, n = 990
\`\`\`

## Стабильность по годам
| Год | r | n | Статус |
|-----|---|---|--------|
| 2022 | +0.81 | ~250 | ✅ сильный |
| 2023 | +0.96 | ~250 | ✅ очень сильный |
| 2024 | **-0.47** | ~250 | ❌ инверсия |
| 2025 | +0.34 | ~240 | ⚠️ слабый |

## Механизм
Ослабление рубля → импортная инфляция → ЦБ удерживает ставку →
банки зарабатывают на процентных доходах → MOEXFN растёт.
Но при ставке 21%+ механизм перегружается → инверсия в 2024.

## Открытые атаки
- [[attack_non_stationarity]] — 2024 инверсия НЕ объяснена ← P0
- [[attack_in_sample_overfitting]] — нет holdout теста ← P0
- [[attack_regime_conditionality]] — работает ли при ставке > 15%?

## Закрытые атаки
- [[attack_aliasing]] — проверено: уsd_rub берётся из v_market_context корректно ✅

## Связанные концепты
- [[concept_regime_conditionality]]
- [[concept_signal_nonstationarity]]
```

---

### 5.2 Подход (02_Approaches/)

```markdown
---
type: approach
status: deprecated            # active | deprecated | replaced | experimental
replaced_by: approach_deterministic_holdout
deprecated_date: 2026-04-30
marathon_introduced: 1
marathon_deprecated: 2
confidence_at_intro: high
confidence_at_deprecation: very_low
tags: [methodology, evaluation]
updated: 2026-05-02
---

# Approach: LLM Self-Evaluation

## Суть подхода
DeepSeek сам оценивает свои гипотезы после получения SQL-результата.
Ставит `confirmed: true/false` и `signal_score: 0-100`.

## Почему взяли
- Быстро и дёшево — не нужна дополнительная логика
- Казалось достаточным для первой версии
- confirmed rate 67.7% выглядел правдоподобно

## Что обнаружили
Независимая верификация (signal_scan.py, 618 комбинаций) показала:
- LLM confirmed rate: **66.4%**
- Real confirmed rate: **~17–20%**
- Разрыв: агент хвалит сам себя (~3x inflation)

Основные причины инфляции:
1. Instrument aliasing: `imoex_close AS ftse_china_50` → CORR(IMOEX, IMOEX) = 0.88
2. Level-price tautology: EUR/RUB vs USD/RUB → r = 0.97 at any lag
3. Confirmation bias: агент "хочет" найти сигнал

## Почему отказались
Метрика не отражает реальность. 60% "подтверждённых" сигналов были тавтологией.
Датасет experiments.db с 66% confirmed rate непригоден для fine-tuning без чистки.

## Что заменяет
[[approach_deterministic_holdout]] (Phase 10) — детерминированный критерий на holdout данных.
Split: Discovery 2022–2023 / Validation 2024 / Live 2025.

## Уроки
- Никогда не доверять self-reported метрикам без независимой верификации
- Размер выборки не защищает от системного смещения
- aliasing-паттерн должен быть проверкой ДО выполнения SQL
```

---

### 5.3 Атака (03_Attacks/)

```markdown
---
type: attack
status: open                  # open | partially_closed | closed | wontfix
target_signals: [signal_usd_rub_moexfn, signal_brent_moexfn, signal_msci_india_moexfn]
target_approaches: [approach_llm_self_eval]
severity: critical            # critical | major | minor
closes_with: phase_10_holdout
tags: [methodology, overfitting]
updated: 2026-05-02
---

# Attack: In-Sample Overfitting

## Суть атаки
Все три «сильных» сигнала найдены и верифицированы на одних и тех же данных 2022–2025.
Это значит: агент мог «найти» корреляции которые случайны или специфичны для этого периода.

## Формальное описание
Пусть D = данные 2022–2025, S = найденный сигнал.
Мы проверили: S работает на D? → Да, r > 0.6.
Но мы НЕ проверили: S работает на D' (2024–2025, holdout)? → Неизвестно.

Это классический data snooping bias: из 3 416 гипотез несколько случайно дадут r > 0.6
просто по законам больших чисел. Без коррекции на множественное тестирование
мы не знаем — реальный ли это сигнал или артефакт выборки.

## Количественная оценка угрозы
При 3 416 тестах и α = 0.05 ожидаемое число ложных позитивов = 3416 × 0.05 = **171**.
Реальных confirmed ~20% = ~683. Из них ложных может быть до 25%.

## Что нужно для закрытия
Phase 10: Data split protocol
- Discovery set: 2022–2023 (генерация гипотез)
- Validation set: 2024 (первичная верификация)
- Live set: 2025 (финальная проверка, используется один раз)

Критерий закрытия: сигнал работает на Validation set с r > 0.5, n > 200.

## Текущий статус
**OPEN** — Phase 10 ещё не реализована. P0 приоритет.

## Связанные атаки
- [[attack_multiple_testing]] — количественная сторона той же проблемы
- [[attack_non_stationarity]] — отдельная атака: даже на in-sample данных сигнал нестабилен
```

---

### 5.4 Решение (04_Decisions/)

```markdown
---
type: decision
status: active
decision_date: 2026-04-15
decision_maker: iRatG + Claude
alternatives_considered: [PostgreSQL, ClickHouse, pandas+parquet]
tags: [architecture, database]
updated: 2026-05-02
---

# Decision: DuckDB как основная аналитическая БД

## Что решили
Все числовые данные (MOEX, ЦБ, Росстат, глобальные рынки) хранятся в DuckDB.
Агент выполняет SQL-запросы напрямую к файлу `db/signal_mind.duckdb`.

## Почему именно DuckDB

| Критерий | DuckDB | PostgreSQL | pandas |
|----------|--------|-----------|--------|
| Latency на аналитич. запрос | **40 мс** | 200+ мс | 2–10 с |
| Деплой | один файл | сервер | в памяти |
| CORR(), window functions | ✅ native | ✅ | ограничено |
| Объём данных (наш размер) | идеально | избыточно | норм |

DuckDB даёт 40 мс на запрос — это критично для агента который делает 10–20 SQL/итерацию.

## Что рассматривали и отклонили
- **PostgreSQL**: нужен сервер, деплой сложнее, для нашего объёма избыточен
- **ClickHouse**: мощный но переусложнён для одного файла данных
- **pandas**: медленный на JOIN-запросах, нет нативного CORR()

## Ограничения которые приняли
- Не подходит для конкурентной записи (агент только читает — ок)
- Максимальный объём ~100 GB (у нас ~2 GB числовых данных — ок)
- Новостная БД отдельно в SQLite (9.93 GB, DuckDB не оптимален для full-text)

## Связанные решения
- [[decision_sqlite_for_news]] — почему новости в SQLite
- [[decision_chromadb_rag]] — почему векторный поиск через ChromaDB
```

---

## 6. Технические компоненты

### 6.1 obsidian_indexer.py

**Назначение:** читает vault, чанкует markdown по секциям, индексирует в ChromaDB.

**Расположение:** `src/parsers/obsidian_indexer.py`

**Алгоритм:**
```python
def index_vault(vault_path: str, chroma_collection: str = "methodology"):
    """
    1. Найти все .md файлы в vault (кроме 00_System/)
    2. Для каждого файла:
       a. Прочитать frontmatter (type, status, tags, updated)
       b. Разбить на чанки по заголовкам H2/H3
       c. Добавить метаданные: file_path, type, status, signal, approach, attack
    3. Upsert в ChromaDB collection "methodology"
       (upsert = не дублировать при повторном запуске)
    4. Логировать: сколько файлов обработано, сколько чанков добавлено/обновлено
    """
```

**Важные детали:**
- Чанки по H2 — оптимальный размер (≈200–400 токенов)
- ID чанка = `{file_stem}__{section_title_normalized}` — стабильный для upsert
- Фильтр: файлы с `status: archived` не индексируются
- Метаданные в ChromaDB: `type`, `status`, `severity` (для attacks), `instrument_a/b` (для signals)

**Запуск:**
```bash
# После обновления vault
python -m src.parsers.obsidian_indexer

# Или из Revizor после марафона:
from src.parsers.obsidian_indexer import index_vault
index_vault("obsidian/")
```

---

### 6.2 Изменения в src/agent/rag.py

Добавить функцию `search_methodology()` рядом с существующими `search_regulatory()` и `search_corp()`:

```python
def search_methodology(query: str, n_results: int = 3, filter_type: str = None) -> list[dict]:
    """
    Semantic search over Obsidian methodology vault.
    
    Args:
        query: natural language query, e.g. "USD/RUB MOEXFN signal attacks"
        n_results: top-N chunks to return
        filter_type: optionally filter by type ('signal', 'attack', 'approach', 'concept')
    
    Returns:
        list of dicts: {text, metadata: {type, status, file, section}, distance}
    
    Examples:
        search_methodology("USD/RUB MOEXFN корреляция")
        → chunk from signal_usd_rub_moexfn.md: "2024 инверсия r=-0.47"
        
        search_methodology("self-evaluation inflation", filter_type="attack")
        → chunk from attack_llm_eval_inflation.md
        
        search_methodology("режимная зависимость ставка")
        → chunk from concept_regime_conditionality.md
    """
```

---

### 6.3 Изменения в src/agent/hypothesis.py

Два момента интеграции:

**Момент 1: перед генерацией гипотезы**

```python
# В функции generate_hypothesis() или run_hypothesis_cycle()
# ДОБАВИТЬ: методологический контекст из vault

methodology_context = ""
if rag_methodology_available():
    hits = search_methodology(
        query=f"{hypothesis_hint} {lag_days}d correlation signal",
        n_results=3
    )
    if hits:
        methodology_context = "\n\n## Методологический контекст из vault:\n"
        for h in hits:
            methodology_context += f"- [{h['metadata']['type']}] {h['text'][:300]}\n"

# Добавить methodology_context в промпт генерации гипотезы
```

**Момент 2: перед оценкой результата**

```python
# В функции evaluate_result()
# ДОБАВИТЬ: проверка открытых атак

attacks = search_methodology(
    query=f"{instrument_a} {instrument_b} attack weakness problem",
    n_results=2,
    filter_type="attack"
)
attack_warnings = ""
if attacks:
    open_attacks = [a for a in attacks if a['metadata'].get('status') == 'open']
    if open_attacks:
        attack_warnings = "\n\nВНИМАНИЕ — открытые методологические атаки:\n"
        for a in open_attacks:
            attack_warnings += f"- {a['text'][:200]}\n"

# Добавить attack_warnings в промпт оценки
# Это снижает confirmation bias: агент видит слабые стороны перед тем как оценить
```

---

### 6.4 Изменения в src/agent/revizor.py

После каждого марафона Revizor дополнительно:
1. Ищет новые паттерны → обновляет соответствующие vault файлы
2. Если найден новый WEAK_R паттерн → добавляет чанк в `03_Attacks/attack_weak_patterns.md`
3. Запускает `obsidian_indexer.py` для переиндексации

```python
# В apply_fixes() добавить:
def _update_vault_after_marathon(self, marathon_stats: dict):
    """Update Obsidian vault with new findings from marathon."""
    # 1. Обновить статусы сигналов если найдены новые данные
    # 2. Добавить новые WEAK_R паттерны в attacks
    # 3. Обновить approach статусы если что-то deprecated
    # 4. Re-index vault
    from src.parsers.obsidian_indexer import index_vault
    index_vault("obsidian/")
    print("[revizor] Vault re-indexed after marathon")
```

---

## 7. Схема полного потока данных

```
Пользователь наблюдает проблему
         ↓
Записывает в Obsidian (03_Attacks/ или 02_Approaches/)
         ↓
obsidian_indexer.py → ChromaDB "methodology"
         ↓
Агент стартует следующую сессию
         ↓
hypothesis.py вызывает search_methodology("тема гипотезы")
         ↓ получает
"signal_usd_rub_moexfn: атака non_stationarity OPEN"
"approach_llm_self_eval: DEPRECATED, replaced by holdout"
         ↓
Агент генерирует более осторожную гипотезу
Агент запрашивает holdout-версию данных (если Phase 10 готова)
         ↓
Результат → Revizor анализирует
         ↓
Revizor обновляет vault файлы
         ↓
Re-index → следующий марафон знает больше
```

---

## 8. Что НЕ нужно делать

- **Не дублировать `forbidden_patterns.md`** — он остаётся как плоский файл для быстрой загрузки.
  Vault — дополнительный семантический слой, а не замена.

- **Не индексировать `00_System/`** — системные промпты для Claude не нужны агенту в RAG.

- **Не создавать страницы на каждую итерацию** — vault растёт через устойчивые концепты,
  не через 3 416 записей из experiments.db.

- **Не хранить SQL в vault** — для SQL есть `db/sql_patterns.md` и `experiments.db`.
  Vault = методология и почему, не что именно.

- **Не автоматизировать полностью** — пользователь должен участвовать в обновлении vault.
  Ценность в том что человек осмысляет находки, а не только агент.

---

## 9. Ожидаемый эффект

### Прямые эффекты

| Метрика | Сейчас | После Loop 4 |
|---------|--------|-------------|
| Повторные WEAK_R паттерны | 12x одинаковые | Блокируются семантически |
| Aliasing ошибки | 0 (починено) | 0 (защита на двух уровнях) |
| Знание о нестационарности при генерации | Нет | Есть (из vault) |
| Инфляция confirmed rate | ~3x (66% vs 20%) | Снижается (attack_warnings) |
| Видимость эволюции подходов | Только в git | В Obsidian graph view |

### Стратегические эффекты

1. **Онбординг**: новый участник видит в Obsidian всю историю решений без изучения git log
2. **Научная воспроизводимость**: каждый сигнал содержит SQL + год-за-годом + открытые атаки
3. **Антирегрессия**: Revizor обновляет vault → агент не возвращается к уже отброшенным подходам

---

## 10. Открытые вопросы

### Технические
- [ ] Как часто перезапускать indexer? (При каждом изменении файла? Раз в день? После каждого марафона?)
- [ ] Оптимальный размер чанка? (H2-секция ~300 токенов vs H3 ~150 токенов)
- [ ] Нужен ли отдельный embedding model для русского текста? (сейчас MiniLM-L12-v2)
- [ ] Как обрабатывать SQL-блоки внутри vault файлов? (индексировать или пропускать?)

### Методологические
- [ ] Кто решает когда атака "закрыта"? Агент? Пользователь? Revizor?
- [ ] Как предотвратить confirmation bias в vault? (Пользователь тоже склонен фиксировать успехи)
- [ ] Нужна ли "confidence score" для каждого vault-чанка при возврате агенту?

### Архитектурные
- [ ] Разделять ли collection "methodology" на подколлекции (signals, attacks, approaches)?
  Или фильтровать по metadata `type`?
- [ ] Как интегрировать с будущим Phase 10 holdout? Vault должен знать split-границы.

---

## 11. План реализации

### Фаза A — Vault content (2–3 часа, без кода)
1. Создать структуру папок в `obsidian/`
2. Заполнить `01_Signals/` — три сигнала из верифицированных данных
3. Заполнить `02_Approaches/` — 5 подходов с историей (LLM self-eval = deprecated)
4. Заполнить `03_Attacks/` — 5 открытых атак с severity и close criteria
5. Заполнить `04_Decisions/` — 4 архитектурных решения
6. Заполнить `05_Concepts/` — 4 концепта

### Фаза B — Indexer (1–2 часа, код)
1. Написать `src/parsers/obsidian_indexer.py`
2. Протестировать на текущем vault содержимом
3. Добавить в `src/agent/rag.py` функцию `search_methodology()`
4. Ручной тест: запрос → релевантный чанк?

### Фаза C — Agent integration (2–3 часа, код)
1. Добавить вызов `search_methodology()` в `hypothesis.py`
2. Добавить `attack_warnings` в evaluator промпт
3. Добавить vault re-index в конец `revizor.apply_fixes()`
4. Тест: запустить 10-итерационную сессию, смотреть влияние

### Фаза D — Observability (1 час)
1. Логировать какие vault-чанки были использованы в каждой итерации
2. Сохранять в telemetry: `vault_hits` count per iteration
3. Через 1 марафон: анализ — какие файлы vault используются чаще всего?

**Общая оценка:** 6–9 часов. Фаза A — первый приоритет (ценность без кода).

---

## 12. Связанные файлы проекта

| Файл | Отношение |
|------|----------|
| `src/agent/rag.py` | Добавить `search_methodology()` |
| `src/agent/hypothesis.py` | Вызов vault перед генерацией |
| `src/agent/revizor.py` | Re-index vault после марафона |
| `src/parsers/obsidian_indexer.py` | НОВЫЙ: индексация vault |
| `db/chroma/` | Добавить collection "methodology" |
| `db/forbidden_patterns.md` | Остаётся, vault — дополнение |
| `analytics/PROJECT_REPORT.md` | Источник для Фазы A контента |
| `memory/` | Claude Code память — отдельный слой от vault |
