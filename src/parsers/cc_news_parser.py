"""
CC-NEWS parser for Russian financial news (.ru domains).
Source: Common Crawl CC-NEWS archive via CDX index + HTTP Range Requests.
No API keys needed — data is fully open.

Storage: SQLite for raw articles + ChromaDB for embeddings.
Run overnight: 3-8 hours, 2-15 GB traffic.
"""

import gzip
import json
import logging
import sqlite3
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path

import requests
from tqdm import tqdm

DB_DIR = Path(__file__).parents[2] / "db"
NEWS_DB = DB_DIR / "news_archive.db"
LOG_FILE = DB_DIR / "cc_news_parser.log"

CDX_API = "https://index.commoncrawl.org/CC-NEWS-index"
CDX_LIMIT = 500

DATE_FROM = "20220101"
DATE_TO = "20261231"

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

KEYWORDS = {
    "военная": [
        "СВО", "спецоперация", "мобилизация", "демобилизация",
        "беспилотник", "БПЛА", "дрон", "РЭБ",
        "позывной", "оперштаб", "фронт", "линия соприкосновения",
        "контрнаступление", "наступление", "отступление",
        "эвакуация", "обстрел", "удар", "ракетный удар",
        "ПВО", "Байрактар",
    ],
    "геополитика": [
        "санкции", "контрсанкции", "ограничения",
        "параллельный импорт", "импортозамещение",
        "НАТО", "G7", "G20", "ШОС", "БРИКС",
        "заморозка активов", "конфискация",
        "недружественные страны", "дружественные страны",
        "нейтралитет", "переговоры", "перемирие",
        "разворот на восток",
    ],
    "экономика": [
        "инфляция", "ключевая ставка", "ЦБ", "Центробанк",
        "рубль", "девальвация", "укрепление рубля",
        "ВВП", "рецессия", "стагфляция",
        "бюджет", "дефицит бюджета", "профицит",
        "госдолг", "резервы", "ФНБ",
        "нефтегазовые доходы", "ненефтегазовые доходы",
        "экспорт", "импорт", "торговый баланс",
    ],
    "рынки": [
        "акции", "облигации", "дивиденды", "IPO",
        "MOEX", "Московская биржа", "СПБ Биржа",
        "индекс МосБиржи", "РТС",
        "нефть", "Brent", "Urals",
        "природный газ", "СПГ", "нефтепровод",
        "дисконт", "потолок цен", "эмбарго",
        "buyback", "делистинг", "редомициляция",
    ],
    "компании": [
        "Газпром", "Роснефть", "Лукойл", "Новатэк",
        "Сбербанк", "Сбер", "ВТБ", "Альфа-Банк",
        "Яндекс", "VK", "МТС", "Ростелеком",
        "Норникель", "Русал", "Северсталь", "НЛМК",
        "Аэрофлот", "РЖД", "Россети",
        "Росатом", "Ростех",
    ],
    "политика": [
        "Путин", "Мишустин", "Набиуллина",
        "Правительство", "Госдума", "Совет Федерации",
        "национализация", "приватизация",
        "госкорпорация", "госзакупки",
        "льготная ипотека", "маткапитал",
        "нацпроект", "федеральный бюджет",
        "регуляторика", "деофшоризация",
    ],
    "социальная": [
        "безработица", "занятость", "дефицит кадров",
        "зарплата", "МРОТ", "прожиточный минимум",
        "эмиграция", "релокация", "релоканты",
        "демография", "рождаемость",
        "пенсионная реформа", "пенсия",
        "медицина", "здравоохранение",
    ],
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

ALL_KEYWORDS = [kw for cat in KEYWORDS.values() for kw in cat]


# ── logging ────────────────────────────────────────────────────────────────────

def setup_logging():
    DB_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger(__name__)


# ── SQLite ─────────────────────────────────────────────────────────────────────

def init_db() -> sqlite3.Connection:
    DB_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(NEWS_DB))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            url         TEXT UNIQUE,
            domain      TEXT,
            title       TEXT,
            text        TEXT,
            date        TEXT,
            matched_kw  TEXT,
            parsed_at   TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_domain ON articles(domain)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date   ON articles(date)")
    conn.commit()
    return conn


def save_article(conn: sqlite3.Connection, row: dict):
    try:
        conn.execute("""
            INSERT OR IGNORE INTO articles
            (url, domain, title, text, date, matched_kw, parsed_at)
            VALUES (:url, :domain, :title, :text, :date, :matched_kw, :parsed_at)
        """, row)
        conn.commit()
    except Exception as e:
        logging.warning(f"DB insert error: {e}")


# ── CDX index ──────────────────────────────────────────────────────────────────

def query_cdx(domain: str, log, limit: int = CDX_LIMIT) -> list[dict]:
    records = []
    offset = 0
    while True:
        params = {
            "url": f"*.{domain}/*",
            "output": "json",
            "from": DATE_FROM,
            "to": DATE_TO,
            "fl": "timestamp,url,filename,offset,length,status",
            "filter": "status:200",
            "limit": limit,
            "offset": offset,
        }
        for attempt in range(3):
            try:
                r = requests.get(CDX_API, params=params, timeout=60)
                if r.status_code == 200 and r.text.strip():
                    batch = [json.loads(line) for line in r.text.strip().split("\n") if line]
                    break
                elif r.status_code in (429, 503, 504):
                    wait = 30 * (attempt + 1)
                    log.warning(f"  CDX {r.status_code}, retry in {wait}s...")
                    time.sleep(wait)
                    batch = []
                else:
                    batch = []
                    break
            except Exception as e:
                wait = 20 * (attempt + 1)
                log.warning(f"  CDX error ({e}), retry in {wait}s...")
                time.sleep(wait)
                batch = []
        else:
            log.error(f"  CDX failed after 3 attempts for {domain}, skipping")
            break

        if not batch:
            break
        records.extend(batch)
        log.info(f"  {domain}: {len(records)} CDX records so far")
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(1)
    return records


# ── WARC fetch ─────────────────────────────────────────────────────────────────

def fetch_warc_record(filename: str, offset: str, length: str) -> bytes | None:
    url = f"https://data.commoncrawl.org/{filename}"
    headers = {"Range": f"bytes={int(offset)}-{int(offset) + int(length) - 1}"}
    try:
        r = requests.get(url, headers=headers, timeout=60)
        if r.status_code in (200, 206):
            return r.content
    except Exception as e:
        logging.warning(f"WARC fetch error: {e}")
    return None


# ── article extraction ─────────────────────────────────────────────────────────

def extract_article(warc_bytes: bytes, source_url: str) -> dict | None:
    try:
        from warcio.archiveiterator import ArchiveIterator
        from newspaper import Article
    except ImportError:
        logging.error("warcio or newspaper4k not installed. Run: pip install warcio newspaper4k")
        return None

    try:
        stream = BytesIO(gzip.decompress(warc_bytes))
    except Exception:
        stream = BytesIO(warc_bytes)

    try:
        for record in ArchiveIterator(stream):
            if record.rec_type == "response":
                html = record.content_stream().read().decode("utf-8", errors="ignore")
                art = Article(source_url, language="ru")
                art.set_html(html)
                art.parse()
                if art.title and len(art.text) > 100:
                    return {
                        "title": art.title,
                        "text": art.text,
                        "date": str(art.publish_date) if art.publish_date else None,
                    }
    except Exception as e:
        logging.debug(f"Extract error {source_url}: {e}")
    return None


# ── keyword filter ─────────────────────────────────────────────────────────────

def find_keywords(article: dict) -> list[str]:
    hay = ((article.get("title") or "") + " " + (article.get("text") or "")).lower()
    return [kw for kw in ALL_KEYWORDS if kw.lower() in hay]


# ── main ───────────────────────────────────────────────────────────────────────

def run(domains: list[str] | None = None, test_mode: bool = False):
    log = setup_logging()
    conn = init_db()
    target_domains = domains or RU_DOMAINS
    cdx_limit_override = 10 if test_mode else CDX_LIMIT
    total_saved = 0

    log.info(f"Starting CC-NEWS parser | domains={len(target_domains)} | test={test_mode}")

    for domain in target_domains:
        log.info(f"=== {domain} ===")

        records = query_cdx(domain, log, limit=cdx_limit_override)

        log.info(f"  CDX records: {len(records)}")
        if not records:
            continue

        for rec in tqdm(records, desc=domain, unit="rec"):
            warc = fetch_warc_record(rec["filename"], rec["offset"], rec["length"])
            if not warc:
                continue

            article = extract_article(warc, rec["url"])
            if not article:
                continue

            matched = find_keywords(article)
            if not matched:
                continue

            date = article.get("date") or rec.get("timestamp", "")[:8]
            row = {
                "url": rec["url"],
                "domain": domain,
                "title": article["title"],
                "text": article["text"],
                "date": date,
                "matched_kw": ", ".join(matched),
                "parsed_at": datetime.now().isoformat(),
            }
            save_article(conn, row)
            total_saved += 1
            log.info(f"  + [{matched[0]}] {article['title'][:70]}")
            time.sleep(0.3)

    count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    log.info(f"\nDone. Session saved: {total_saved} | DB total: {count}")
    conn.close()


if __name__ == "__main__":
    import sys
    test = "--test" in sys.argv
    run(test_mode=test)
