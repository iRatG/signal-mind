"""Build a compact DB schema description for the LLM context."""
from src.db.init_db import get_connection


SCHEMA_HINT = """
DuckDB database with the following tables and views:

TABLES:
- moex_indices(ticker, trade_date DATE, close, open, high, low, value, volume, currency)
    Tickers: IMOEX, MOEXFN, MOEXOG, MOEX10, MOEXBC, RUGOLD, RUSFAR3M, RUPCI, ...
- key_rate(period TEXT like '3.2026', rate_pct, inflation, days_to_meeting)
- forex_cbr(trade_date DATE, currency TEXT, rate DOUBLE, nominal INT)
    Currencies: USD
- market_data(trade_date DATE, instrument TEXT, close, open, high, low, change_pct)
    Instruments: USD_RUB, EUR_RUB, SP500, GOLD, BRENT, DXY
- rosstat_macro(period TEXT, indicator TEXT, region TEXT, value DOUBLE, unit TEXT, source_file TEXT)
    Key indicators: avg_wage_rub, real_wage_idx_*, housing_total_*, ...

VIEWS (pre-joined, use these for analysis):
- v_market_context(trade_date, imoex_close, usd_rub, eur_rub, brent_usd, gold_usd,
                   key_rate_pct, inflation_pct)
    Daily: 2022-01-02 to 2026-04-29. Key rate is monthly (April 2026 = NULL).
- v_key_rate_daily(period_date DATE, rate_pct, inflation)
    Monthly key rate with proper DATE, 2014-09 to 2026-03.
- v_wage_dynamics(year INT, avg_wage_rub, real_wage_idx)
    Annual wages, 2000-2021.
- v_moex_sectors(trade_date, imoex, moexfn_finance, moexog_oil_gas, moex10_bluechip,
                 rugold, rusfar3m_rate)
    Daily sector indices, 2022+.

QUERY TIPS:
- Date filter: WHERE trade_date BETWEEN '2022-01-01' AND '2026-04-30'
- Percent change: (close - LAG(close) OVER (ORDER BY trade_date)) / LAG(close) * 100
- Correlation: CORR(a, b) in DuckDB works as aggregate
- Monthly avg: DATE_TRUNC('month', trade_date)
"""


def get_schema() -> str:
    return SCHEMA_HINT.strip()


def get_table_stats() -> str:
    con = get_connection()
    lines = []
    for table in ["moex_indices", "key_rate", "forex_cbr", "market_data", "rosstat_macro"]:
        n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        lines.append(f"  {table}: {n:,} rows")
    con.close()
    return "\n".join(lines)
