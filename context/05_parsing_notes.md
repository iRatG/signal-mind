# Заметки по парсингу — Signal Mind

Этот файл содержит конкретные нюансы для каждого типа данных.
Читать перед написанием парсера.

---

## MOEX ZIP CSV файлы

**Главный нюанс:** первая строка в каждом CSV — это слово `"history"`, не заголовок.

```python
import zipfile, pandas as pd

def read_moex_zip(zip_path):
    with zipfile.ZipFile(zip_path) as z:
        # Внутри всегда один файл security.csv
        fname = z.namelist()[0]
        with z.open(fname) as f:
            df = pd.read_csv(
                f,
                encoding='cp1251',
                sep=';',
                skiprows=1,      # ← пропустить строку "history"
                decimal=',',     # ← числа: "2800,69" → 2800.69
            )
    # Конвертировать дату
    df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'], format='%d.%m.%Y')
    # Оставить только нужные колонки
    cols = ['TRADEDATE', 'SECID', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'VALUE', 'CAPITALIZATION']
    return df[[c for c in cols if c in df.columns]]
```

**Проблема с IMOEX:** файл IMOEX.csv.zip содержит только ~2 строки (конец марта 2026).
Вероятно, пользователь скачал его недавно и выбрал короткий период. Можно докачать с MOEX ISS API:
`https://iss.moex.com/iss/history/engines/stock/markets/index/securities/IMOEX.csv?from=2016-01-01`

---

## Курсы ЦБ (RC_*.xlsx)

**Нюанс 1:** нет shared strings — все значения inline в XML.

**Нюанс 2:** дата хранится как Excel serial number (целое число).

```python
import openpyxl
from datetime import datetime, timedelta

def excel_serial_to_date(serial):
    return datetime(1899, 12, 30) + timedelta(days=int(serial))

def read_cbr_fx(xlsx_path):
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active
    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        nominal, data_serial, curs, cdx = row[0], row[1], row[2], row[3]
        if data_serial and curs:
            rows.append({
                'rate_date': excel_serial_to_date(data_serial),
                'currency_name': cdx,
                'rate_rub': float(curs),
                'nominal': int(nominal) if nominal else 1
            })
    wb.close()
    return pd.DataFrame(rows)

# Маппинг названий валют → ISO коды
CURRENCY_MAP = {
    'Доллар США': 'USD',
    'Евро': 'EUR',
    'Китайский юань': 'CNY',
    'Фунт стерлингов': 'GBP',
    'Японская иена': 'JPY',
    'Белорусский рубль': 'BYR',
    'Казахстанский тенге': 'KZT',
    'Армянский драм': 'AMD',
    'Азербайджанский манат': 'AZN',
}
```

---

## Ключевая ставка PDF

PDF сжат (FlateDecode) и содержит таблицу примерно такого вида:

| Дата установления | Ключевая ставка |
|---|---|
| 28.04.2025 | 21,00 |
| 14.02.2025 | 21,00 |
| ... | ... |

```python
import pdfplumber, re, pandas as pd

def parse_key_rate_pdf(pdf_path):
    rows = []
    date_pattern = re.compile(r'(\d{2}\.\d{2}\.\d{4})')
    rate_pattern = re.compile(r'(\d{1,2}[,\.]\d{2})')
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Сначала пробуем извлечь таблицы
            for table in page.extract_tables():
                for row in table:
                    row_str = ' '.join([str(c) for c in row if c])
                    dates = date_pattern.findall(row_str)
                    rates = rate_pattern.findall(row_str)
                    if dates and rates:
                        rows.append({
                            'effective_date': pd.to_datetime(dates[0], format='%d.%m.%Y'),
                            'rate_pct': float(rates[0].replace(',', '.'))
                        })
    
    df = pd.DataFrame(rows).drop_duplicates('effective_date')
    return df.sort_values('effective_date')
```

**Если PDF плохо парсится:** скачать данные напрямую с API ЦБ:
```python
import requests, pandas as pd
from io import StringIO

# ЦБ отдаёт данные в виде XML
url = "https://www.cbr.ru/hd_base/KeyRate/?UniDbQuery.Posted=True&UniDbQuery.From=17.09.2013&UniDbQuery.To=29.04.2026&UniDbQuery.format=export"
# Или через requests получить HTML и распарсить таблицу через pd.read_html()
response = requests.get("https://www.cbr.ru/hd_base/KeyRate/")
tables = pd.read_html(response.text, decimal=',', thousands=' ')
key_rate_df = tables[0]  # первая таблица на странице
```

---

## Росстат Excel — общие правила

### Проблема 1: Объединённые ячейки

Росстат объединяет ячейки для группировки регионов по ФО (федеральным округам).
При чтении pandas ставит NaN в не-первые ячейки объединённого диапазона.

**Решение:**
```python
# После чтения — заполнить NaN вниз в колонке регионов
df['region'] = df['region'].ffill()
```

**Но:** нужно отличать объединение для ФО (строки-агрегаты) от объединения для регионов.

### Проблема 2: Многострочные заголовки

Заголовки часто занимают 3-5 строк. Первые строки — название таблицы, следующие — единицы измерения, потом сами заголовки.

```python
# Читаем без заголовка чтобы посмотреть структуру
df_raw = pd.read_excel(file, header=None, engine='openpyxl')

# Ищем строку с годами (числа 2016-2026)
for i, row in df_raw.iterrows():
    if any(str(v) in [str(y) for y in range(2016, 2027)] for v in row):
        header_row = i
        break

# Читаем снова с правильным skiprows
df = pd.read_excel(file, skiprows=header_row, engine='openpyxl')
```

### Проблема 3: Сноски снизу

В конце листа часто идут строки-примечания начинающиеся с "1)", "2)", "Примечание:", "Источник:".

```python
import re

def find_data_end(df, region_col=0):
    """Находит последнюю строку с реальными данными."""
    for i in range(len(df)-1, -1, -1):
        val = str(df.iloc[i, region_col]).strip()
        if re.match(r'^\d+\)', val):  # "1) ...", "2) ..."
            continue
        if any(val.lower().startswith(kw) for kw in ['примечание', 'источник', 'формирование']):
            continue
        if val in ['nan', '', 'None']:
            continue
        return i + 1
    return len(df)
```

### Проблема 4: Лист "Содержание"

Первый лист часто "Содержание" — это навигационный лист без данных.

```python
SKIP_SHEETS = {'содержание', 'содержание:', 'content'}

def get_data_sheets(wb):
    return [s for s in wb.sheetnames if s.lower().strip() not in SKIP_SHEETS]
```

### Проблема 5: Два периода в одном файле (tab4-zpl)

`tab4-zpl_2025.xlsx` имеет два листа: "2000-2017" и "с 2018".
Это намеренный разрыв Росстата — методология расчёта зарплат изменилась.
**Не смешивать** два периода без нормализации. Добавлять колонку `methodology_period`.

### Проблема 6: t12.xlsx — не региональные данные

`t12.xlsx` содержит зарплаты **по видам деятельности (ОКВЭД2)**, а не по регионам.
Это федеральный срез. Структура другая — не делать JOIN с regional данными напрямую.

---

## Нормализация регионов РФ

Критически важно для JOIN между разными файлами Росстата.

### Типичные расхождения

| Файл | Написание |
|---|---|
| Effect_VRP | `г. Москва` |
| tab4-zpl | `г. Москва` |
| Chisl_RF | `Москва` |
| innov_1 | `г.Москва` (без пробела) |
| Jil_fond | `Город Москва` |

### Строки которые НЕ являются регионами (пропускать при JOIN)

```python
NON_REGION = {
    'российская федерация',
    'центральный федеральный округ',
    'северо-западный федеральный округ', 
    'южный федеральный округ',
    'северо-кавказский федеральный округ',
    'приволжский федеральный округ',
    'уральский федеральный округ',
    'сибирский федеральный округ',
    'дальневосточный федеральный округ',
    # Строки-заголовки
    'субъекты российской федерации',
    'регион',
    'наименование',
}
```

### Специфика Ненецкого АО

Ненецкий АО в Росстате часто идёт как "в том числе Ненецкий автономный округ" с отступом внутри строки Архангельской области. Распознавать отдельно.

---

## Investing.com CSV

**Проблема:** числа в формате `"75,0600"` — запятая как десятичный разделитель, значения в кавычках.

```python
def parse_investing_csv(path):
    df = pd.read_csv(path, encoding='utf-8', quotechar='"')
    
    # Конвертировать числа
    for col in ['Цена', 'Откр.', 'Макс.', 'Мин.']:
        if col in df.columns:
            df[col] = df[col].str.replace(',', '.').astype(float)
    
    # Конвертировать изменение в %
    if 'Изм. %' in df.columns:
        df['change_pct'] = df['Изм. %'].str.replace('%','').str.replace(',','.').astype(float)
    
    # Конвертировать объём (может быть "406,18K" или "1,23M")
    def parse_volume(v):
        if pd.isna(v) or v == '': return None
        v = str(v).replace(',', '.')
        if 'K' in v: return float(v.replace('K','')) * 1_000
        if 'M' in v: return float(v.replace('M','')) * 1_000_000
        if 'B' in v: return float(v.replace('B','')) * 1_000_000_000
        try: return float(v)
        except: return None
    
    if 'Объём' in df.columns:
        df['volume'] = df['Объём'].apply(parse_volume)
    
    # Конвертировать дату
    df['trade_date'] = pd.to_datetime(df['Дата'], format='%d.%m.%Y')
    
    return df

```

---

## КГО PDF (ChromaDB)

```python
import pdfplumber
import chromadb
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
client = chromadb.PersistentClient(path="C:/project/signal_mind/db/chroma")
collection = client.get_or_create_collection("cb_reports")

SECTION_KEYWORDS = {
    'banking': ['банк', 'кредитн', 'финансов'],
    'mfi': ['мфо', 'микрофинанс'],
    'pension': ['пенсион', 'нпф'],
    'insurance': ['страхов'],
    'payments': ['платёж', 'платеж'],
}

def detect_section(text):
    text_lower = text.lower()
    for section, keywords in SECTION_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return section
    return 'general'

def ingest_kgo_pdf(pdf_path, year):
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text(x_tolerance=3, y_tolerance=3) or ""
            if len(text.strip()) < 50:
                continue  # пустая страница
            
            section = detect_section(text[:300])
            embedding = model.encode(text).tolist()
            
            collection.add(
                ids=[f"kgo_{year}_p{i+1}"],
                documents=[text],
                embeddings=[embedding],
                metadatas=[{
                    'year': year,
                    'doc_type': 'kgo',
                    'section': section,
                    'page_num': i + 1,
                    'source_file': pdf_path
                }]
            )
```

---

## HuggingFace новости

```python
from datasets import load_dataset
import chromadb
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
client = chromadb.PersistentClient(path="C:/project/signal_mind/db/chroma")
collection = client.get_or_create_collection("financial_news")

KEYWORDS = [
    "russia", "ruble", "rouble", "oil", "brent", "moex",
    "sanctions", "interest rate", "central bank", "inflation",
    "gold", "gazprom", "sberbank", "lukoil", "moscow exchange",
    "cbr", "bank of russia"
]

def is_relevant(text, date_str):
    if date_str < "2016-01-01":
        return False
    text_lower = text.lower()
    return any(kw in text_lower for kw in KEYWORDS)

ds = load_dataset(
    "Brianferrell787/financial-news-multisource",
    data_files="data/*/*.parquet",
    split="train",
    streaming=True
)

batch = []
batch_size = 500

for item in ds:
    if not is_relevant(item['text'], item['date'][:10]):
        continue
    batch.append(item)
    
    if len(batch) >= batch_size:
        texts = [b['text'][:2000] for b in batch]  # обрезаем длинные статьи
        embeddings = model.encode(texts, batch_size=32).tolist()
        
        collection.add(
            ids=[f"news_{b['date']}_{hash(b['text'])}" for b in batch],
            documents=texts,
            embeddings=embeddings,
            metadatas=[{
                'date': b['date'][:10],
                'publisher': b.get('extra_fields', {}).get('publisher', 'unknown'),
                'text_type': 'news'
            } for b in batch]
        )
        batch = []
        print(f"Загружено: {collection.count()} документов")
```
