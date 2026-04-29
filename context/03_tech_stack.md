# Технический стек — Signal Mind

## Принцип выбора

Всё локально. API только для LLM (DeepSeek или Claude). Никакого облака для хранения данных.

---

## База данных для числовых данных — DuckDB

**Выбор:** DuckDB
**Альтернативы которые рассматривались:** SQLite, PostgreSQL, pandas в памяти

**Почему DuckDB:**
- Аналитическая БД — создана именно для агрегатов, JOIN-ов, временных рядов
- Работает как один файл (`.duckdb`) — не нужен сервер
- В 10–100 раз быстрее SQLite на аналитических запросах (GROUP BY, window functions)
- Отлично интегрируется с pandas: `duckdb.sql("SELECT ...").df()`
- Читает Parquet напрямую — можно запросить HuggingFace данные без загрузки в память
- Поддерживает SQL полностью включая window functions (LAG, LEAD, OVER PARTITION)

**Установка:** `pip install duckdb`

**Файл БД:** `C:\project\signal_mind\db\signal_mind.duckdb`

### Схема таблиц

```sql
-- MOEX индексы (все 15 тикеров в одной таблице)
CREATE TABLE moex_indices (
    trade_date  DATE,
    secid       VARCHAR,
    close       DOUBLE,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    value       DOUBLE,        -- объём в рублях
    capitalization DOUBLE,
    PRIMARY KEY (trade_date, secid)
);

-- Официальные курсы валют ЦБ
CREATE TABLE cbr_fx_rates (
    rate_date   DATE,
    currency    VARCHAR,       -- 'USD', 'EUR', 'CNY', 'GBP'...
    rate_rub    DOUBLE,        -- курс за nominal единиц
    nominal     INTEGER,
    PRIMARY KEY (rate_date, currency)
);

-- Ключевая ставка ЦБ
CREATE TABLE cbr_key_rate (
    effective_date DATE PRIMARY KEY,
    rate_pct       DOUBLE      -- 21.0, 16.0, 7.5...
);

-- Международные цены (Brent, Gold, S&P500, USD Index)
CREATE TABLE market_prices (
    trade_date   DATE,
    instrument   VARCHAR,      -- 'BRENT', 'GOLD', 'SP500', 'USDX', 'EUR_RUB'
    close        DOUBLE,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    change_pct   DOUBLE,
    PRIMARY KEY (trade_date, instrument)
);

-- Росстат — длинный формат (все показатели в одной таблице)
CREATE TABLE rosstat (
    region_canonical VARCHAR,
    year             INTEGER,
    dataset          VARCHAR,  -- 'wages', 'housing', 'vrp', 'demography'...
    metric           VARCHAR,  -- 'avg_monthly_wage_rub', 'housing_sqm_per_capita'...
    value            DOUBLE,
    source_file      VARCHAR,
    PRIMARY KEY (region_canonical, year, dataset, metric)
);

-- База гипотез и найденных паттернов (для циклического агента)
CREATE TABLE hypotheses (
    id           INTEGER PRIMARY KEY,
    created_at   TIMESTAMP DEFAULT now(),
    hypothesis   TEXT,
    status       VARCHAR,   -- 'pending', 'confirmed', 'rejected', 'needs_more_data'
    evidence     TEXT,      -- JSON с доказательствами
    signal_score DOUBLE,
    next_action  TEXT
);
```

---

## База данных для текстов — ChromaDB

**Выбор:** ChromaDB
**Альтернативы:** Qdrant, Weaviate, pgvector, FAISS

**Почему ChromaDB:**
- Простейший старт — работает локально без сервера
- `pip install chromadb` — и готово
- Python API очень простой
- Поддерживает метаданные для фильтрации (фильтровать по году, источнику, разделу)
- Достаточно для нашего объёма (миллионы, не миллиарды документов)

**Если масштаб вырастет:** мигрировать на Qdrant (self-hosted, лучше масштабируется)

**Путь:** `C:\project\signal_mind\db\chroma\`

### Коллекции

```python
# Коллекция 1: PDF-отчёты ЦБ (КГО, МФО, ПФ)
collection_cb_reports = client.create_collection(
    name="cb_reports",
    metadata={"description": "Отчёты ЦБ РФ: КГО 2022-2025, МФО, ПФ"}
)
# Метаданные чанка: year, doc_type, section, page_num, source_file

# Коллекция 2: Финансовые новости
collection_news = client.create_collection(
    name="financial_news",
    metadata={"description": "Bloomberg, Reuters, CNBC, Yahoo Finance 2016-2025"}
)
# Метаданные чанка: date, publisher, tickers, text_type
```

### Модель эмбеддингов

**Выбор:** `paraphrase-multilingual-MiniLM-L12-v2` (sentence-transformers)

**Почему:**
- Поддерживает русский и английский одновременно
- Лёгкая модель (118MB), работает на CPU
- Достаточное качество для финансовых текстов
- Локальная — не нужен API

**Установка:** `pip install sentence-transformers`

---

## LLM для агента

**Основной:** DeepSeek API (дешевле, хорошо работает с аналитическими задачами)
**Резервный:** Claude API (anthropic) — для более сложных рассуждений

**Почему не локальная модель:**
- Qwen 30B / DeepSeek локально требует GPU (RTX 3090/4090)
- На CPU слишком медленно для циклического агента
- API стоит дёшево при нашем объёме запросов

**Использование:**
- Циклический агент: DeepSeek (дёшево, быстро)
- Финальный Signal Brief: Claude Sonnet (качество)

---

## Парсинг данных

### Excel (MOEX ZIP CSV)
```python
import pandas as pd
import zipfile

with zipfile.ZipFile("IMOEX.csv.zip") as z:
    with z.open("security.csv") as f:
        df = pd.read_csv(f, encoding='cp1251', sep=';', 
                         skiprows=1, decimal=',')
```

### Excel (Росстат .xlsx)
```python
df = pd.read_excel("file.xlsx", engine='openpyxl', sheet_name=None)
# sheet_name=None возвращает dict {лист: DataFrame}
# Пропускать листы с именем 'Содержание'
```

### Excel (Росстат .xls — старый формат)
```python
df = pd.read_excel("file.xls", engine='xlrd', sheet_name=None)
# xlrd >= 2.0 не читает .xlsx! Только для .xls
```

### XLSX (ЦБ курсы — RC_ файлы)
```python
import openpyxl
wb = openpyxl.load_workbook("RC_F01_04_2018_T29_04_2026.xlsx", 
                             read_only=True, data_only=True)
ws = wb.active
# Дата — Excel serial number, конвертация:
from datetime import datetime, timedelta
excel_date = 46141
date = datetime(1899, 12, 30) + timedelta(days=excel_date)
```

### PDF (ЦБ КГО + ключевая ставка)
```python
import pdfplumber

with pdfplumber.open("kgo_2024.pdf") as pdf:
    for page in pdf.pages:
        text = page.extract_text(x_tolerance=3, y_tolerance=3)
        tables = page.extract_tables()
```

### Ключевая ставка (PDF)
Этот конкретный файл содержит таблицу: дата решения | ставка %.
После парсинга через pdfplumber — искать строки с паттерном `\d{2}\.\d{2}\.\d{4}` и числом рядом.

### Investing.com CSV
```python
import re

df = pd.read_csv("Прошлые данные - USD_RUB.csv", 
                 encoding='utf-8', quotechar='"')
# Числа в формате "75,0600" → float
df['Цена'] = df['Цена'].str.replace(',', '.').astype(float)
# Объём может быть "406,18K" → парсить K/M суффиксы
def parse_volume(v):
    if isinstance(v, str):
        v = v.replace(',', '.')
        if 'K' in v: return float(v.replace('K','')) * 1000
        if 'M' in v: return float(v.replace('M','')) * 1e6
    return None
```

### HuggingFace новости
```python
from datasets import load_dataset

ds = load_dataset(
    "Brianferrell787/financial-news-multisource",
    data_files="data/*/*.parquet",
    split="train",
    streaming=True
)

keywords = ["russia", "ruble", "oil", "brent", "moex", "sanctions",
            "interest rate", "central bank", "inflation", "gold",
            "gazprom", "sberbank", "lukoil", "rouble"]

filtered = ds.filter(lambda x: 
    x["date"] >= "2016-01-01" and
    any(kw in x["text"].lower() for kw in keywords)
)
```

---

## Структура проекта

```
C:\project\signal_mind\
├── context\                  # ← ты здесь (документация для модели)
├── statistic\                # исходные данные (не трогать)
│   ├── moex\                 # MOEX ZIP файлы + курсы ЦБ
│   ├── pdf\                  # КГО ЦБ PDF
│   └── *.xlsx / *.xls        # Росстат
├── db\
│   ├── signal_mind.duckdb    # числовая БД (создать)
│   └── chroma\               # векторная БД (создать)
├── notebooks\                # Jupyter ноутбуки (создать)
│   ├── 01_ingest_moex.ipynb
│   ├── 02_ingest_cbr.ipynb
│   ├── 03_ingest_rosstat.ipynb
│   ├── 04_ingest_pdf.ipynb
│   ├── 05_ingest_news.ipynb
│   ├── 06_eda.ipynb
│   └── 07_signal_search.ipynb
├── src\                      # Python модули (создать)
│   ├── db.py                 # соединение с DuckDB
│   ├── ingestion\
│   │   ├── moex.py
│   │   ├── cbr.py
│   │   ├── rosstat.py
│   │   ├── pdf_rag.py
│   │   └── news.py
│   ├── normalization\
│   │   └── regions.py        # нормализация названий регионов РФ
│   └── agent\
│       ├── loop.py           # циклический агент
│       └── scorer.py         # Signal Score
└── requirements.txt
```

---

## requirements.txt

```
duckdb>=1.1.0
pandas>=2.2.0
openpyxl>=3.1.0
xlrd>=2.0.1
pdfplumber>=0.11.0
chromadb>=0.5.0
sentence-transformers>=3.0.0
huggingface-hub>=0.24.0
datasets>=2.20.0
requests>=2.32.0
jupyter>=1.1.0
notebook>=7.3.0
matplotlib>=3.9.0
seaborn>=0.13.0
plotly>=5.24.0
scipy>=1.13.0
numpy>=1.26.0
tqdm>=4.67.0
anthropic>=0.30.0
openai>=1.40.0
python-dotenv>=1.0.0
```
