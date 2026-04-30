"""One-time precomputation: hf_news.db → news_daily table in signal_mind.duckdb.

Creates table news_daily(news_date, oil, rate, ruble, sanctions, inflation, banking, gold)
with daily mention counts per topic. One full pass over 2.52M rows (~60s).
Run once; after that all lag SQL queries are instant.

Usage:
    .venv/Scripts/python -m src.agent.news_precompute
"""
from pathlib import Path
import time
import duckdb

DB_PATH   = Path(__file__).parents[2] / "db" / "signal_mind.duckdb"
NEWS_PATH = Path(__file__).parents[2] / "db" / "hf_news.db"

# Topic → LIKE keywords (OR-combined). Keep short for scan speed.
TOPICS = {
    "oil":        ["oil", "Brent", "crude", "energy"],
    "rate":       ["interest rate", "central bank", "key rate", "CBR"],
    "ruble":      ["ruble", "RUB", "Russian currency"],
    "sanctions":  ["sanction", "embargo", "Russia ban"],
    "inflation":  ["inflation", "CPI", "consumer price"],
    "banking":    ["banking", "bank sector", "financial sector"],
    "gold":       ["gold", "precious metal"],
}

CREATE_TABLE = """
CREATE OR REPLACE TABLE news_daily (
    news_date   DATE PRIMARY KEY,
    oil         INTEGER DEFAULT 0,
    rate        INTEGER DEFAULT 0,
    ruble       INTEGER DEFAULT 0,
    sanctions   INTEGER DEFAULT 0,
    inflation   INTEGER DEFAULT 0,
    banking     INTEGER DEFAULT 0,
    gold        INTEGER DEFAULT 0
)
"""

CREATE_INDEX = "CREATE INDEX IF NOT EXISTS idx_news_daily_date ON news_daily(news_date)"


def _build_case(keywords: list[str]) -> str:
    conditions = " OR ".join(f"text LIKE '%{kw}%'" for kw in keywords)
    return f"COUNT(CASE WHEN {conditions} THEN 1 END)"


def run():
    print("=== news_precompute: building news_daily table ===")
    print(f"  Source : {NEWS_PATH}")
    print(f"  Target : {DB_PATH}")
    print(f"  Topics : {list(TOPICS.keys())}")

    con = duckdb.connect(str(DB_PATH))
    con.execute(f"ATTACH '{NEWS_PATH}' AS news (TYPE sqlite)")

    con.execute(CREATE_TABLE)

    # Build SELECT with one pass over all articles
    topic_cols = ",\n        ".join(
        f"{_build_case(kws)} AS {name}"
        for name, kws in TOPICS.items()
    )

    sql = f"""
        INSERT INTO news_daily
        SELECT
            CAST(date AS DATE) AS news_date,
            {topic_cols}
        FROM news.articles
        WHERE date IS NOT NULL
          AND date BETWEEN '2021-01-01' AND '2025-12-31'
        GROUP BY CAST(date AS DATE)
        ORDER BY news_date
    """

    print("\n  Scanning 2.52M articles (one pass)...")
    t0 = time.time()
    con.execute(sql)
    elapsed = time.time() - t0
    print(f"  Done in {elapsed:.1f}s")

    con.execute(CREATE_INDEX)

    # Stats
    stats = con.execute("""
        SELECT
            COUNT(*)         AS days,
            MIN(news_date)   AS from_date,
            MAX(news_date)   AS to_date,
            SUM(oil)         AS total_oil,
            SUM(rate)        AS total_rate,
            SUM(sanctions)   AS total_sanctions
        FROM news_daily
    """).fetchone()

    print(f"\n  news_daily rows  : {stats[0]:,} days ({stats[1]} — {stats[2]})")
    print(f"  oil mentions     : {stats[3]:,}")
    print(f"  rate mentions    : {stats[4]:,}")
    print(f"  sanctions        : {stats[5]:,}")

    # Quick lag-14 sanity check
    r = con.execute("""
        SELECT ROUND(CORR(n.oil, m.imoex_close), 4), COUNT(*)
        FROM news_daily n
        JOIN v_market_context m ON m.trade_date = n.news_date + INTERVAL 14 DAYS
    """).fetchone()
    print(f"\n  Sanity: oil -> IMOEX lag-14 corr={r[0]}, n={r[1]}")

    con.close()
    print("\n=== news_daily ready. Lag queries are now instant. ===")


if __name__ == "__main__":
    run()
