"""Appends news_daily in signal_mind.duckdb from two sources at once:

    hf_news.db     — full-text articles (legacy English HF dataset + ru_archive: RU)
    news_gdelt.db  — pre-aggregated English daily counts from GDELT API

Append-only: existing news_daily rows are NEVER touched. Only dates > MAX(news_date)
are computed and inserted. Counts from both sources for the same (date, topic) are
summed via UNION ALL + outer GROUP BY.

Usage:
    .venv/Scripts/python -m src.agent.news_precompute
"""
from datetime import timedelta
from pathlib import Path
import time
import duckdb

DB_PATH    = Path(__file__).parents[2] / "db" / "signal_mind.duckdb"
NEWS_PATH  = Path(__file__).parents[2] / "db" / "hf_news.db"
GDELT_PATH = Path(__file__).parents[2] / "db" / "news_gdelt.db"

# Topic → ILIKE keywords (OR-combined). Mix of EN and RU stems so the scan
# catches both the legacy HF English corpus and the ru_archive Russian articles.
# ILIKE is case-insensitive in DuckDB (unicode-aware), so lowercase stems suffice.
TOPICS = {
    "oil":        ["oil", "Brent", "crude", "energy", "нефть", "нефт", "газ"],
    "rate":       ["interest rate", "central bank", "key rate", "CBR",
                   "ставк", "центробанк", "ключевая ставка"],
    "ruble":      ["ruble", "RUB", "Russian currency",
                   "рубл", "курс рубля"],
    "sanctions":  ["sanction", "embargo", "Russia ban",
                   "санкци", "эмбарго"],
    "inflation":  ["inflation", "CPI", "consumer price",
                   "инфляц", "потребительские цены"],
    "banking":    ["banking", "bank sector", "financial sector",
                   "банк", "банковск", "кредит"],
    "gold":       ["gold", "precious metal",
                   "золото", "золот"],
}

DATE_FROM = "2021-01-01"
DATE_TO   = "2027-12-31"

CREATE_TABLE_IF = """
CREATE TABLE IF NOT EXISTS news_daily (
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


def run():
    print("=== news_precompute: appending news_daily ===")
    print(f"  Source HF    : {NEWS_PATH}")
    print(f"  Source GDELT : {GDELT_PATH}")
    print(f"  Target       : {DB_PATH}")
    print(f"  Topics       : {list(TOPICS.keys())}")

    con = duckdb.connect(str(DB_PATH))
    con.execute(f"ATTACH '{NEWS_PATH}' AS news (TYPE sqlite)")
    if GDELT_PATH.exists():
        con.execute(f"ATTACH '{GDELT_PATH}' AS gd (TYPE sqlite)")
        gd_rows = con.execute("SELECT COUNT(*) FROM gd.gdelt_daily").fetchone()[0]
        has_gdelt = gd_rows > 0
        print(f"  GDELT rows   : {gd_rows} {'(skipped — empty)' if not has_gdelt else ''}")
    else:
        has_gdelt = False
        print("  (no news_gdelt.db — GDELT side skipped)")

    # Ensure schema exists. Existing rows are NEVER touched.
    con.execute(CREATE_TABLE_IF)
    con.execute(CREATE_INDEX)

    existing_count = con.execute("SELECT COUNT(*) FROM news_daily").fetchone()[0]
    max_row = con.execute("SELECT MAX(news_date) FROM news_daily").fetchone()
    max_date = max_row[0] if max_row and max_row[0] else None
    if max_date is None:
        start = DATE_FROM
        print(f"  news_daily empty — will append from {start}")
    else:
        start = (max_date + timedelta(days=1)).isoformat()
        print(f"  news_daily has {existing_count} rows, max date = {max_date}")
        print(f"  will append dates >= {start} (up to {DATE_TO}), existing rows untouched")

    topic_names = list(TOPICS.keys())

    # hf_news side: text scan with mixed EN+RU keywords (ILIKE = unicode case-insensitive)
    def _hf_case(name: str) -> str:
        ors = " OR ".join(f"text ILIKE '%{kw}%'" for kw in TOPICS[name])
        return f"SUM(CASE WHEN ({ors}) THEN 1 ELSE 0 END) AS {name}"

    hf_topic_cols = ",\n            ".join(_hf_case(n) for n in topic_names)

    sql_parts = [f"""
        SELECT CAST(date AS DATE) AS news_date,
            {hf_topic_cols}
        FROM news.articles
        WHERE date IS NOT NULL
          AND CAST(date AS DATE) >= '{start}'
          AND CAST(date AS DATE) <= '{DATE_TO}'
        GROUP BY CAST(date AS DATE)
    """]

    if has_gdelt:
        gd_topic_cols = ",\n            ".join(
            f"SUM(CASE WHEN topic='{name}' THEN count ELSE 0 END) AS {name}"
            for name in topic_names
        )
        sql_parts.append(f"""
        SELECT CAST(news_date AS DATE) AS news_date,
            {gd_topic_cols}
        FROM gd.gdelt_daily
        WHERE CAST(news_date AS DATE) >= '{start}'
          AND CAST(news_date AS DATE) <= '{DATE_TO}'
        GROUP BY CAST(news_date AS DATE)
        """)

    inner = "\n        UNION ALL\n        ".join(sql_parts)
    insert_sql = f"""
        INSERT INTO news_daily (news_date, {', '.join(topic_names)})
        SELECT news_date,
            {', '.join(f'SUM({n}) AS {n}' for n in topic_names)}
        FROM ({inner}) combined
        GROUP BY news_date
        ORDER BY news_date
    """

    print("\n  Computing counts for new dates only...")
    t0 = time.time()
    con.execute(insert_sql)
    elapsed = time.time() - t0
    print(f"  Done in {elapsed:.1f}s")

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

    print(f"\n  news_daily total : {stats[0]:,} days ({stats[1]} -> {stats[2]})")
    print(f"  total oil        : {stats[3]:,}")
    print(f"  total rate       : {stats[4]:,}")
    print(f"  total sanctions  : {stats[5]:,}")

    added = stats[0] - existing_count
    if added > 0:
        new_stats = con.execute(
            f"SELECT COUNT(*), SUM(oil), SUM(rate), SUM(sanctions) FROM news_daily WHERE news_date >= '{start}'"
        ).fetchone()
        print(f"\n  newly added rows : {new_stats[0]} ({start} -> {stats[2]})")
        print(f"  new oil count    : {new_stats[1]:,}")
        print(f"  new rate count   : {new_stats[2]:,}")
        print(f"  new sanctions    : {new_stats[3]:,}")

    # Optional sanity check (skip if v_market_context missing)
    try:
        r = con.execute("""
            SELECT ROUND(CORR(n.oil, m.imoex_close), 4), COUNT(*)
            FROM news_daily n
            JOIN v_market_context m ON m.trade_date = n.news_date + INTERVAL 14 DAYS
        """).fetchone()
        print(f"\n  Sanity: oil -> IMOEX lag-14 corr={r[0]}, n={r[1]}")
    except Exception as e:
        print(f"\n  (sanity check skipped: {type(e).__name__})")

    con.close()
    print("\n=== news_daily appended. Existing rows preserved. ===")


if __name__ == "__main__":
    run()
