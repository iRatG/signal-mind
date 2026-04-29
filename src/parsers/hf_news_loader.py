"""
hf_news_loader.py

Downloads Brianferrell787/financial-news-multisource from HuggingFace (streaming),
filters articles from 2021-01-01 onwards, saves to SQLite at db/hf_news.db.

Resume-safe: completed subsets are tracked in the `progress` table and skipped on restart.
"""

import os
import sqlite3
import logging
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from datasets import load_dataset, get_dataset_config_names

load_dotenv()
HF_TOKEN    = os.getenv("huggingface_token")
DATASET_ID  = "Brianferrell787/financial-news-multisource"
DATE_CUTOFF = datetime(2021, 1, 1)
BATCH_SIZE  = 2000

ROOT     = Path(__file__).resolve().parents[2]
DB_PATH  = ROOT / "db" / "hf_news.db"
LOG_PATH = ROOT / "db" / "hf_loader.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-65536")  # 64 MB page cache
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            source       TEXT,
            date         TEXT,
            text         TEXT,
            extra_fields TEXT,
            loaded_at    TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS progress (
            subset      TEXT PRIMARY KEY,
            status      TEXT,
            rows_loaded INTEGER DEFAULT 0,
            started_at  TEXT,
            finished_at TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date   ON articles(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_source ON articles(source)")
    conn.commit()
    return conn


def completed_subsets(conn: sqlite3.Connection) -> set:
    rows = conn.execute(
        "SELECT subset FROM progress WHERE status='done'"
    ).fetchall()
    return {r[0] for r in rows}


def parse_date(date_str: str) -> datetime | None:
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str[:10])
    except (ValueError, TypeError):
        return None


def load_subset(conn: sqlite3.Connection, subset: str) -> int:
    conn.execute("""
        INSERT OR REPLACE INTO progress (subset, status, started_at)
        VALUES (?, 'in_progress', ?)
    """, (subset, datetime.now().isoformat()))
    conn.commit()

    try:
        ds = load_dataset(
            DATASET_ID,
            subset,
            split="train",
            streaming=True,
            token=HF_TOKEN,
        )
    except Exception as e:
        log.error(f"  Cannot load subset {subset}: {e}")
        conn.execute("UPDATE progress SET status='error' WHERE subset=?", (subset,))
        conn.commit()
        return 0

    batch = []
    rows  = 0
    now   = datetime.now().isoformat()

    for record in ds:
        d = parse_date(record.get("date", ""))
        if d is None or d < DATE_CUTOFF:
            continue

        batch.append((
            subset,
            record.get("date", "")[:10],
            record.get("text", ""),
            record.get("extra_fields", ""),
            now,
        ))

        if len(batch) >= BATCH_SIZE:
            conn.executemany(
                "INSERT INTO articles (source,date,text,extra_fields,loaded_at) VALUES (?,?,?,?,?)",
                batch,
            )
            conn.commit()
            rows += len(batch)
            batch = []

    if batch:
        conn.executemany(
            "INSERT INTO articles (source,date,text,extra_fields,loaded_at) VALUES (?,?,?,?,?)",
            batch,
        )
        conn.commit()
        rows += len(batch)

    conn.execute("""
        UPDATE progress SET status='done', rows_loaded=?, finished_at=?
        WHERE subset=?
    """, (rows, datetime.now().isoformat(), subset))
    conn.commit()
    return rows


def main():
    log.info("=" * 60)
    log.info(f"DB path  : {DB_PATH}")
    log.info(f"Log path : {LOG_PATH}")
    log.info(f"Filter   : date >= {DATE_CUTOFF.date()}")
    log.info("=" * 60)

    conn = init_db()
    done = completed_subsets(conn)

    log.info("Fetching subset list from HuggingFace...")
    configs = get_dataset_config_names(DATASET_ID, token=HF_TOKEN)
    log.info(f"Subsets found: {len(configs)} | already done: {len(done)}")

    for i, cfg in enumerate(configs, 1):
        if cfg in done:
            log.info(f"[{i:02d}/{len(configs)}] {cfg}: skip (already done)")
            continue

        log.info(f"[{i:02d}/{len(configs)}] {cfg}: loading...")
        try:
            n = load_subset(conn, cfg)
            log.info(f"[{i:02d}/{len(configs)}] {cfg}: done — {n:,} rows saved")
        except Exception as e:
            log.error(f"[{i:02d}/{len(configs)}] {cfg}: FAILED — {e}")
            conn.execute("UPDATE progress SET status='error' WHERE subset=?", (cfg,))
            conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    log.info(f"{'=' * 60}")
    log.info(f"ALL DONE. Total rows in DB: {total:,}")
    log.info(f"DB size: {DB_PATH.stat().st_size / 1024**3:.2f} GB")
    conn.close()


if __name__ == "__main__":
    main()
