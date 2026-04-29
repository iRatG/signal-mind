# Парсинг архива новостей (.ru домены) через Common Crawl CC-NEWS

## Обзор

Common Crawl CC-NEWS — это **бесплатный публичный архив новостных статей**, который хранится на AWS S3 и доступен без регистрации, без паролей и без API-ключей. Архив пополняется с 2016 года и содержит материалы тысяч новостных сайтов со всего мира, включая российские СМИ. Данные хранятся в формате WARC (Web ARChive).

**Ключевое преимущество:** не нужно качать терабайты — CDX-индекс позволяет получить точные байтовые координаты нужных записей и скачать только их через HTTP Range Request.

---

## Доступ — пароли не нужны

Все данные открыты и бесплатны:

| Ресурс | URL | Авторизация |
|--------|-----|-------------|
| CDX-индекс CC-NEWS | `https://index.commoncrawl.org/CC-NEWS-index` | Не нужна |
| WARC-файлы (S3/HTTP) | `https://data.commoncrawl.org/` | Не нужна |
| AWS S3 (публичный бакет) | `s3://commoncrawl/` | Не нужна |

---

## Архитектура решения

```
┌─────────────────────────────────────────────────────────┐
│  1. CDX Index API                                        │
│     → запрос по домену + дате                           │
│     → получаем: filename, offset, length                │
└────────────────────────┬────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│  2. HTTP Range Request                                   │
│     → скачиваем только нужный кусок WARC-файла          │
│     → НЕ качаем весь файл (200–500 МБ)                  │
└────────────────────────┬────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│  3. warcio                                              │
│     → читаем WARC-запись из скачанного куска            │
└────────────────────────┬────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│  4. newspaper3k                                          │
│     → извлекаем: title, text, publish_date              │
└────────────────────────┬────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│  5. pandas / SQLite                                      │
│     → фильтрация по ключевым словам                     │
│     → сохранение в CSV / JSON / SQLite                  │
└─────────────────────────────────────────────────────────┘
```

---

## Установка зависимостей

```bash
pip install warcio newspaper3k requests pandas lxml tqdm
```

---

## Список .ru источников для мониторинга

```python
RU_DOMAINS = [
    "rbc.ru",
    "kommersant.ru",
    "ria.ru",
    "tass.ru",
    "vedomosti.ru",
    "interfax.ru",
    "lenta.ru",
    "gazeta.ru",
    "fontanka.ru",
    "iz.ru",
    "novayagazeta.ru",
    "meduza.io",
    "forbes.ru",
    "banki.ru",
]
```

---

## Словарь ключевых слов по категориям

Словарь охватывает **~120 слов** по 8 категориям. Включает как классическую лексику российских СМИ, так и новый словарный пласт, появившийся после февраля 2022 года.

> **Логика фильтра:** статья сохраняется если в ней встречается **хотя бы одно** слово из любой активированной категории. Можно комбинировать категории под конкретную задачу.

```python
KEYWORDS = {

    # ── Военная тематика (новый пласт с 2022 года) ───────────────────────────
    # Слова «спецоперация», «СВО», «мобилизация» появились именно в 2022-м.
    # «Позывной», «беспилотник», «РЭБ» официально закреплены в словаре РАН (2025).
    "военная": [
        "СВО", "спецоперация", "мобилизация", "демобилизация",
        "беспилотник", "БПЛА", "дрон", "РЭБ",
        "позывной", "оперштаб", "фронт", "линия соприкосновения",
        "контрнаступление", "наступление", "отступление",
        "эвакуация", "обстрел", "удар", "ракетный удар",
        "ПВО", "Байрактар",
    ],

    # ── Геополитика и санкции ─────────────────────────────────────────────────
    "геополитика": [
        "санкции", "контрсанкции", "ограничения",
        "параллельный импорт", "импортозамещение",
        "НАТО", "G7", "G20", "ШОС", "БРИКС",
        "заморозка активов", "конфискация",
        "недружественные страны", "дружественные страны",
        "нейтралитет", "переговоры", "перемирие",
        "разворот на восток",
    ],

    # ── Экономика и финансы ───────────────────────────────────────────────────
    "экономика": [
        "инфляция", "ключевая ставка", "ЦБ", "Центробанк",
        "рубль", "девальвация", "укрепление рубля",
        "ВВП", "рецессия", "стагфляция",
        "бюджет", "дефицит бюджета", "профицит",
        "госдолг", "резервы", "ФНБ",
        "нефтегазовые доходы", "ненефтегазовые доходы",
        "экспорт", "импорт", "торговый баланс",
    ],

    # ── Рынки и инвестиции ────────────────────────────────────────────────────
    "рынки": [
        "акции", "облигации", "дивиденды", "IPO",
        "MOEX", "Московская биржа", "СПБ Биржа",
        "индекс МосБиржи", "РТС",
        "нефть", "Brent", "Urals",
        "природный газ", "СПГ", "нефтепровод",
        "дисконт", "потолок цен", "эмбарго",
        "buyback", "делистинг", "редомициляция",
    ],

    # ── Крупные российские компании ───────────────────────────────────────────
    "компании": [
        "Газпром", "Роснефть", "Лукойл", "Новатэк",
        "Сбербанк", "Сбер", "ВТБ", "Альфа-Банк",
        "Яндекс", "VK", "МТС", "Ростелеком",
        "Норникель", "Русал", "Северсталь", "НЛМК",
        "Аэрофлот", "РЖД", "Россети",
        "Росатом", "Ростех",
    ],

    # ── Государство и политика ────────────────────────────────────────────────
    "политика": [
        "Путин", "Мишустин", "Набиуллина",
        "Правительство", "Госдума", "Совет Федерации",
        "национализация", "приватизация",
        "госкорпорация", "госзакупки",
        "льготная ипотека", "маткапитал",
        "нацпроект", "федеральный бюджет",
        "регуляторика", "деофшоризация",
    ],

    # ── Социальная повестка ───────────────────────────────────────────────────
    "социальная": [
        "безработица", "занятость", "дефицит кадров",
        "зарплата", "МРОТ", "прожиточный минимум",
        "эмиграция", "релокация", "релоканты",
        "демография", "рождаемость",
        "пенсионная реформа", "пенсия",
        "медицина", "здравоохранение",
    ],

    # ── Технологии и цифровая экономика ──────────────────────────────────────
    "технологии": [
        "искусственный интеллект", "ИИ", "нейросеть",
        "импортозамещение ПО", "отечественный софт",
        "цифровой рубль", "CBDC", "криптовалюта",
        "кибератака", "утечка данных",
        "суверенный интернет", "Рунет",
        "маркетплейс", "Wildberries", "Ozon",
        "финтех", "СБП", "QR-оплата",
    ],
}

# ─── Удобная плоская версия для фильтра ───────────────────────────────────────
# Можно использовать все категории сразу:
ALL_KEYWORDS = [kw for cat in KEYWORDS.values() for kw in cat]

# Или только нужные категории:
SELECTED_KEYWORDS = KEYWORDS["военная"] + KEYWORDS["геополитика"] + KEYWORDS["экономика"]
```

---

## Полный скрипт

```python
"""
cc_news_ru_parser.py

Парсинг новостных архивов Common Crawl CC-NEWS для .ru доменов.
Без паролей, без API-ключей — данные полностью открыты.
"""

import requests
import json
import gzip
import sqlite3
import time
import logging
from io import BytesIO
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm
from warcio.archiveiterator import ArchiveIterator
from newspaper import Article

# ─── Настройки ────────────────────────────────────────────────────────────────

# Выбрать нужные категории или использовать ALL_KEYWORDS
ACTIVE_KEYWORDS = ALL_KEYWORDS   # ← заменить на SELECTED_KEYWORDS для точечного поиска

RU_DOMAINS = [
    "rbc.ru",
    "kommersant.ru",
    "ria.ru",
    "tass.ru",
    "vedomosti.ru",
    "interfax.ru",
    "lenta.ru",
    "gazeta.ru",
]

DATE_FROM = "20220101"   # начало периода (YYYYMMDD)
DATE_TO   = "20261231"   # конец периода  (YYYYMMDD)

CDX_API   = "https://index.commoncrawl.org/CC-NEWS-index"
CDX_LIMIT = 500
OUTPUT_DB = "news_archive.db"

# ─── Логирование ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("parser.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── База данных ──────────────────────────────────────────────────────────────

def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            url          TEXT UNIQUE,
            domain       TEXT,
            title        TEXT,
            text         TEXT,
            date         TEXT,
            matched_kw   TEXT,
            parsed_at    TEXT
        )
    """)
    conn.commit()
    return conn


def save_article(conn: sqlite3.Connection, article: dict):
    try:
        conn.execute("""
            INSERT OR IGNORE INTO articles
            (url, domain, title, text, date, matched_kw, parsed_at)
            VALUES (:url, :domain, :title, :text, :date, :matched_kw, :parsed_at)
        """, article)
        conn.commit()
    except Exception as e:
        log.warning(f"DB insert error: {e}")

# ─── CDX-индекс ───────────────────────────────────────────────────────────────

def query_cdx(domain: str, date_from: str, date_to: str) -> list[dict]:
    records = []
    offset = 0
    while True:
        params = {
            "url":    f"*.{domain}/*",
            "output": "json",
            "from":   date_from,
            "to":     date_to,
            "fl":     "timestamp,url,filename,offset,length,status",
            "filter": "status:200",
            "limit":  CDX_LIMIT,
            "offset": offset,
        }
        try:
            r = requests.get(CDX_API, params=params, timeout=30)
            if r.status_code != 200 or not r.text.strip():
                break
            batch = [json.loads(line) for line in r.text.strip().split("\n") if line]
            if not batch:
                break
            records.extend(batch)
            log.info(f"  {domain}: получено {len(records)} записей")
            if len(batch) < CDX_LIMIT:
                break
            offset += CDX_LIMIT
            time.sleep(0.5)
        except Exception as e:
            log.error(f"CDX error for {domain}: {e}")
            break
    return records

# ─── Скачивание WARC-записи ────────────────────────────────────────────────────

def fetch_warc_record(filename: str, offset: str, length: str) -> bytes | None:
    url = f"https://data.commoncrawl.org/{filename}"
    headers = {"Range": f"bytes={int(offset)}-{int(offset)+int(length)-1}"}
    try:
        r = requests.get(url, headers=headers, timeout=60)
        if r.status_code in (200, 206):
            return r.content
    except Exception as e:
        log.warning(f"WARC fetch error: {e}")
    return None

# ─── Извлечение статьи ────────────────────────────────────────────────────────

def extract_article(warc_bytes: bytes, source_url: str) -> dict | None:
    try:
        stream = BytesIO(gzip.decompress(warc_bytes))
    except Exception:
        stream = BytesIO(warc_bytes)
    try:
        for record in ArchiveIterator(stream):
            if record.rec_type == "response":
                html = record.content_stream().read().decode("utf-8", errors="ignore")
                art  = Article(source_url, language="ru")
                art.set_html(html)
                art.parse()
                if art.title and art.text:
                    return {
                        "title": art.title,
                        "text":  art.text,
                        "date":  str(art.publish_date) if art.publish_date else None,
                    }
    except Exception as e:
        log.debug(f"Extract error for {source_url}: {e}")
    return None

# ─── Фильтр: найти сработавшие ключевые слова ─────────────────────────────────

def find_matched_keywords(article: dict, keywords: list[str]) -> list[str]:
    haystack = ((article.get("title") or "") + " " + (article.get("text") or "")).lower()
    return [kw for kw in keywords if kw.lower() in haystack]

# ─── Главный цикл ─────────────────────────────────────────────────────────────

def main():
    conn = init_db(OUTPUT_DB)
    total_saved = 0

    for domain in RU_DOMAINS:
        log.info(f"=== Домен: {domain} ===")
        records = query_cdx(domain, DATE_FROM, DATE_TO)
        log.info(f"  Записей в индексе: {len(records)}")

        for rec in tqdm(records, desc=domain):
            warc_bytes = fetch_warc_record(rec["filename"], rec["offset"], rec["length"])
            if not warc_bytes:
                continue

            article = extract_article(warc_bytes, rec["url"])
            if not article:
                continue

            matched = find_matched_keywords(article, ACTIVE_KEYWORDS)
            if not matched:
                continue

            row = {
                "url":        rec["url"],
                "domain":     domain,
                "title":      article["title"],
                "text":       article["text"],
                "date":       article.get("date") or rec.get("timestamp", "")[:8],
                "matched_kw": ", ".join(matched),   # ← какие слова сработали
                "parsed_at":  datetime.now().isoformat(),
            }
            save_article(conn, row)
            total_saved += 1
            log.info(f"  ✓ [{', '.join(matched[:3])}] {article['title'][:70]}")
            time.sleep(0.3)

    log.info(f"\n=== Готово. Сохранено статей: {total_saved} ===")

    df = pd.read_sql("SELECT * FROM articles", conn)
    df.to_csv("news_result.csv", index=False, encoding="utf-8-sig")
    log.info(f"Экспорт в news_result.csv ({len(df)} строк)")
    conn.close()


if __name__ == "__main__":
    main()
```

---

## Структура выходных данных

### SQLite (`news_archive.db`)

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | INTEGER | Автоинкремент |
| `url` | TEXT | Полный URL статьи |
| `domain` | TEXT | Источник (rbc.ru и т.д.) |
| `title` | TEXT | Заголовок |
| `text` | TEXT | Полный текст статьи |
| `date` | TEXT | Дата публикации |
| `matched_kw` | TEXT | Ключевые слова, по которым найдена статья |
| `parsed_at` | TEXT | Время парсинга |

### CSV (`news_result.csv`)

Тот же набор полей, удобен для Excel или pandas.

---

## Работа с результатами в pandas

```python
import pandas as pd

df = pd.read_csv("news_result.csv")

# Статьи за конкретный день
df[df['date'].str.startswith('2023-04-29')]

# Топ источников
df['domain'].value_counts()

# Какие ключевые слова чаще всего срабатывали
from collections import Counter
all_kw = ", ".join(df['matched_kw'].dropna()).split(", ")
Counter(all_kw).most_common(20)

# Только статьи по военной тематике
mask = df['matched_kw'].str.contains('СВО|спецоперация|мобилизация', na=False)
df[mask]

# Хронология упоминаний по месяцам
df['month'] = pd.to_datetime(df['date'], errors='coerce').dt.to_period('M')
df.groupby('month').size().plot(kind='bar', title='Статьи по месяцам')
```

---

## Оценки объёма и времени

| Параметр | Оценка |
|----------|--------|
| Доменов в списке | 8–15 |
| Период | 2022–2026 (~4 года) |
| Записей в CDX-индексе | 50 000–200 000 |
| Статей после фильтра (ALL_KEYWORDS) | 10 000–50 000 |
| Статей после точечного фильтра (компания) | 500–10 000 |
| Трафик (только нужные куски) | 2–15 ГБ |
| Время работы скрипта | 3–8 часов |

---

## Советы по запуску

- Запускать лучше ночью или оставить на фоне
- Скрипт сохраняет каждую статью сразу в SQLite — при обрыве можно продолжить (дубли игнорируются через `INSERT OR IGNORE`)
- Поле `matched_kw` покажет **почему** статья попала в выборку — удобно для отладки словаря
- Для ускорения можно добавить `concurrent.futures.ThreadPoolExecutor` (параллельные запросы)
- Вежливые паузы (`time.sleep`) оставить — CDX API может блокировать при агрессивных запросах
- Для теста запусти сначала с `CDX_LIMIT = 10` и одним доменом

---

## Расширения на будущее

- **Анализ тональности** — библиотека `dostoevsky` (предобученная модель для русского языка)
- **NER (распознавание организаций)** — библиотека `natasha` автоматически извлекает компании и персоны из текстов
- **Дашборд** — загрузить CSV в любой BI-инструмент или построить через `plotly`
- **Telegram-бот** — отправлять новые упоминания в канал по расписанию через `APScheduler`
- **Расширение словаря** — добавлять новые слова по мере появления (словарь живёт отдельно от кода)
