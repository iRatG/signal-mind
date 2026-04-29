# План действий — Signal Mind

## Последовательность фаз

```
Фаза 1 → Фаза 2 → Фаза 3 → Фаза 4 → Фаза 5
Данные    Связка   Агент    Цикл     Сигналы
```

Нельзя перепрыгивать фазы. Агент без данных — фантазия.

---

## Фаза 0 — Подготовка окружения (один раз)

**Что сделать:**
1. Установить Python 3.11 или 3.12 с сайта python.org
   - При установке поставить галочку **"Add Python to PATH"**
2. Создать папки проекта:
   ```
   mkdir C:\project\signal_mind\db
   mkdir C:\project\signal_mind\notebooks
   mkdir C:\project\signal_mind\src
   ```
3. Установить зависимости:
   ```bash
   pip install duckdb pandas openpyxl xlrd pdfplumber chromadb sentence-transformers
   pip install huggingface-hub datasets requests jupyter notebook matplotlib seaborn plotly scipy tqdm
   pip install anthropic python-dotenv
   ```
4. Получить HuggingFace токен (Settings → Access Tokens на huggingface.co)
5. Войти: `huggingface-cli login`

---

## Фаза 1 — Данные в базу

**Цель:** все числовые данные лежат в DuckDB, все тексты в ChromaDB.

### 1.1 Создать DuckDB и таблицы

Создать файл `C:\project\signal_mind\db\signal_mind.duckdb` и выполнить CREATE TABLE из `03_tech_stack.md`.

### 1.2 MOEX индексы → DuckDB

- Читать все 15 ZIP файлов из `statistic\moex\*.csv.zip`
- Каждый ZIP содержит `security.csv`
- Параметры: `encoding='cp1251'`, `sep=';'`, `skiprows=1`, `decimal=','`
- Нужные колонки: `TRADEDATE, SECID, CLOSE, OPEN, HIGH, LOW, VALUE, CAPITALIZATION`
- Конвертировать TRADEDATE из строки `"30.03.2026"` в DATE
- Загрузить в таблицу `moex_indices`

**Проверка:**
```sql
SELECT secid, COUNT(*), MIN(trade_date), MAX(trade_date)
FROM moex_indices GROUP BY secid ORDER BY secid;
```

### 1.3 Курсы валют ЦБ → DuckDB

- Читать `RC_F01_04_2018_T29_04_2026.xlsx` через openpyxl
- Колонки: `nominal | data (Excel serial) | curs | cdx`
- Конвертировать дату: `datetime(1899,12,30) + timedelta(days=int(data))`
- Извлечь код валюты из `cdx` ("Доллар США" → "USD", "Евро" → "EUR" и т.д.)
- Загрузить в `cbr_fx_rates`

### 1.4 Ключевая ставка → DuckDB

- Парсить PDF через pdfplumber
- Файл: `statistic\moex\cbr_hd_base_KeyRate__...pdf`
- Искать таблицу: дата | ставка %
- Паттерн дат: `\d{2}\.\d{2}\.\d{4}`
- Загрузить в `cbr_key_rate`

Альтернатива если PDF плохо парсится: скачать с cbr.ru как Excel.

### 1.5 Росстат → DuckDB (итеративно)

Начать с самых простых файлов, добавлять постепенно:

**Порядок:**
1. `tab4-zpl_2025.xlsx` — зарплаты, два листа (2000-2017 и с 2018)
2. `Effect_VRP_2024.xlsx` — ВРП по регионам
3. `Chisl_RF_*.xls` — численность населения
4. Остальные файлы

**Общий алгоритм для каждого файла:**
```python
# 1. Открыть все листы
wb = pd.read_excel(file, sheet_name=None, engine='openpyxl')

# 2. Пропустить листы "Содержание"
data_sheets = {k:v for k,v in wb.items() if 'содержание' not in k.lower()}

# 3. Для каждого листа: найти строку заголовка, обрезать сноски снизу

# 4. Конвертировать в long format: регион | год | значение

# 5. Нормализовать названия регионов (см. 05_parsing_notes.md)

# 6. Загрузить в rosstat с указанием dataset и metric
```

### 1.6 КГО PDF → ChromaDB

- Читать каждый PDF через pdfplumber
- Разбивать на чанки по страницам
- Определять раздел по ключевым словам (banking, mfi, insurance, pension)
- Добавлять метаданные: year, doc_type, section, page_num, source_file
- Эмбеддить через `paraphrase-multilingual-MiniLM-L12-v2`
- Загружать в коллекцию `cb_reports`

### 1.7 Новости → ChromaDB

- Войти через `huggingface-cli login`
- Запустить streaming с фильтрацией (см. `02_data_map.md`)
- Фильтровать: дата >= 2016-01-01 + ключевые слова
- Батчами по 1000 записей загружать в ChromaDB коллекцию `financial_news`
- Это долгая операция — запустить и оставить работать

---

## Фаза 2 — Связка данных

**Цель:** уметь задавать вопросы сразу по нескольким источникам.

### 2.1 Проверить покрытие

```sql
-- За какие периоды есть данные из всех источников?
SELECT 
    m.trade_date,
    m.close as imoex,
    k.rate_pct as key_rate,
    f.rate_rub as usd_rub
FROM moex_indices m
LEFT JOIN cbr_key_rate k ON k.effective_date <= m.trade_date
LEFT JOIN cbr_fx_rates f ON f.rate_date = m.trade_date AND f.currency = 'USD'
WHERE m.secid = 'IMOEX'
ORDER BY m.trade_date;
```

### 2.2 Создать аналитические вью

```sql
-- Месячный агрегат (для связки с Росстатом)
CREATE VIEW monthly_summary AS
SELECT
    DATE_TRUNC('month', trade_date) as month,
    secid,
    AVG(close) as avg_close,
    MAX(close) as max_close,
    MIN(close) as min_close,
    (MAX(close) - MIN(close)) / AVG(close) * 100 as volatility_pct
FROM moex_indices
GROUP BY 1, 2;

-- Годовой агрегат (для JOIN с Росстатом)
CREATE VIEW annual_market AS
SELECT
    YEAR(trade_date) as year,
    secid,
    AVG(close) as avg_close,
    FIRST(close ORDER BY trade_date) as open_year,
    LAST(close ORDER BY trade_date) as close_year,
    (LAST(close ORDER BY trade_date) - FIRST(close ORDER BY trade_date)) 
        / FIRST(close ORDER BY trade_date) * 100 as annual_return_pct
FROM moex_indices
GROUP BY 1, 2;
```

### 2.3 Первые аналитические запросы (руками)

1. Как MOEXFN (финансы) реагирует на изменения ключевой ставки?
2. Есть ли связь между ВРП нефтедобывающих регионов и MOEXOG?
3. Как курс рубля коррелирует с ценой на нефть Brent?

---

## Фаза 3 — Первый агент

**Цель:** LLM умеет задавать SQL-вопросы к данным и записывать выводы.

### Архитектура агента

```python
class SignalAgent:
    def __init__(self, duckdb_conn, chroma_client, llm_client):
        self.db = duckdb_conn
        self.chroma = chroma_client
        self.llm = llm_client
    
    def run_cycle(self, topic: str):
        # 1. Сгенерировать гипотезу
        hypothesis = self.generate_hypothesis(topic)
        
        # 2. Составить SQL-запрос
        sql = self.generate_sql(hypothesis)
        
        # 3. Выполнить запрос
        data = self.db.execute(sql).df()
        
        # 4. Поискать в новостях/PDF
        news_context = self.search_news(hypothesis)
        
        # 5. Оценить результат
        result = self.evaluate(hypothesis, data, news_context)
        
        # 6. Сохранить в БД
        self.save_hypothesis(hypothesis, result)
        
        return result
```

### Промпты агента

**Генерация гипотезы:**
> "Тема: {topic}. Доступные данные: {available_tables}. Сформулируй одну конкретную проверяемую гипотезу о связи между показателями. Гипотеза должна быть проверяема через SQL-запрос к временным рядам."

**Генерация SQL:**
> "Гипотеза: {hypothesis}. Схема таблиц: {schema}. Напиши SQL-запрос для DuckDB который проверит эту гипотезу. Используй LAG/LEAD для временных сдвигов. Верни только SQL без объяснений."

**Оценка:**
> "Гипотеза: {hypothesis}. Данные: {data_summary}. Новостной контекст: {news_context}. Оцени: подтверждена / опровергнута / недостаточно данных. Объясни почему. Какую гипотезу проверить следующей?"

---

## Фаза 4 — Цикл (Уроборос)

**Цель:** агент работает в петле, каждый цикл умнее предыдущего.

```python
def run_ouroboros(agent, initial_topics, max_cycles=50):
    queue = initial_topics.copy()
    
    for cycle in range(max_cycles):
        topic = queue.pop(0)
        result = agent.run_cycle(topic)
        
        # Если нашли что-то интересное — углубиться
        if result['signal_score'] > 50:
            queue.insert(0, result['suggested_next_topic'])
        
        # Добавить новые темы из выводов
        queue.extend(result['related_topics'])
        
        print(f"Цикл {cycle}: {topic} → {result['status']} (score: {result['signal_score']})")
    
    return agent.get_top_signals()
```

**Начальные темы для запуска:**
- "ключевая ставка и поведение банковского сектора MOEXFN"
- "нефтяные новости и MOEXOG с временным лагом"
- "курс рубля и инфляционные ожидания"
- "зарплаты в регионах и инновационная активность"
- "санкционные новости и волатильность IMOEX"

---

## Фаза 5 — Сигналы

**Цель:** лучшие находки агента оформляются в Signal Brief.

### Signal Score

```
Signal Score (0-100) =
  Magnitude (насколько сильное отклонение)      0-25
+ Trend Quality (устойчивость тренда, R²)       0-25
+ Repeatability (сколько раз паттерн встретился) 0-20
+ News Correlation (новости подтверждают)        0-15
+ Buyer Clarity (есть понятный покупатель)       0-15
- Boilerplate Risk (это не общеизвестно?)        0-10
```

### Signal Brief (шаблон)

```markdown
## Signal Brief: [название]

**Score:** X/100 | **Статус:** strong_signal / watch / noise
**Дата:** YYYY-MM-DD

### Наблюдение
[что обнаружил агент]

### Доказательства
- **Источник 1 (DuckDB):** [SQL запрос + результат]
- **Источник 2 (ChromaDB/новости):** [релевантные цитаты]
- **Источник 3 (КГО ЦБ):** [цитата из PDF]

### Паттерн
[описание повторяющейся связи с временным лагом]

### Покупатель
[кто может заплатить за понимание этого сигнала]

### Следующий шаг
[ ] Провести 5 интервью с ...
[ ] Проверить за период X-Y
[ ] Kill criteria: ...
```
