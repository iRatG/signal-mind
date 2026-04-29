import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parents[2] / "db" / "signal_mind.duckdb"


def get_connection() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def init_tables():
    con = get_connection()
    con.execute("""
        CREATE TABLE IF NOT EXISTS moex_indices (
            ticker      VARCHAR,
            trade_date  DATE,
            close       DOUBLE,
            open        DOUBLE,
            high        DOUBLE,
            low         DOUBLE,
            value       DOUBLE,
            volume      DOUBLE,
            currency    VARCHAR,
            PRIMARY KEY (ticker, trade_date)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS key_rate (
            period      VARCHAR,
            rate_pct    DOUBLE,
            inflation   DOUBLE,
            days_to_meeting INTEGER
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS forex_cbr (
            trade_date  DATE,
            currency    VARCHAR,
            rate        DOUBLE,
            nominal     INTEGER,
            PRIMARY KEY (trade_date, currency)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            trade_date  DATE,
            instrument  VARCHAR,
            close       DOUBLE,
            open        DOUBLE,
            high        DOUBLE,
            low         DOUBLE,
            change_pct  DOUBLE,
            PRIMARY KEY (trade_date, instrument)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS rosstat_macro (
            period      VARCHAR,
            indicator   VARCHAR,
            region      VARCHAR,
            value       DOUBLE,
            unit        VARCHAR,
            source_file VARCHAR
        )
    """)
    con.close()
    print("DB tables initialized.")


if __name__ == "__main__":
    init_tables()
