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
    Instruments: USD_RUB, EUR_RUB, SP500, GOLD, BRENT, DXY,
                 MSCI_WORLD, MSCI_INDIA, FTSE_CHINA_50, CHINA_H_SHARES,
                 DJ_SOUTH_AFRICA, SILVER
    All instruments: 2022-01-01 to 2026-04-29
- rosstat_macro(period TEXT, indicator TEXT, region TEXT, value DOUBLE, unit TEXT, source_file TEXT)
    Key indicators: avg_wage_rub, real_wage_idx_*, housing_total_*, ...

VIEWS (pre-joined, use these for analysis):
- v_market_context(trade_date, imoex_close, usd_rub, eur_rub, brent_usd, gold_usd,
                   key_rate_pct, inflation_pct)
    Daily: 2022-01-02 to 2026-04-29. Key rate is monthly (April 2026 = NULL).
- v_key_rate_daily(period_date DATE, rate_pct, inflation)
    Monthly key rate with proper DATE, 2014-09 to 2026-03.
- v_wage_dynamics(year INT, avg_wage_rub, real_wage_idx)
    Annual wages, 2000-2021. WARNING: do NOT join with v_key_rate_daily (year vs month mismatch).
    real_wage_idx IS ALREADY inflation-adjusted — no need to join with inflation data.
- v_moex_sectors(trade_date, imoex, moexfn_finance, moexog_oil_gas, moex10_bluechip,
                 rugold, rusfar3m_rate)
    Daily sector indices, 2022+.

- news_daily(news_date DATE, oil INT, rate INT, ruble INT, sanctions INT, inflation INT, banking INT, gold INT)
    Daily news mention counts by topic, 2021-2025. Source: 2.52M English articles.
    Topics: oil=Brent/crude/energy, rate=interest rate/central bank/CBR,
            ruble=RUB/Russian currency, sanctions=sanction/embargo,
            inflation=CPI/consumer price, banking=bank sector, gold=precious metals

QUERY TIPS:
- Date filter: WHERE trade_date BETWEEN '2022-01-01' AND '2026-04-30'
- Percent change: ALWAYS wrap lag() in a subquery first, then compute: WITH t AS (SELECT col, lag(col) OVER (ORDER BY d) AS prev FROM ...) SELECT (col-prev)/prev*100 FROM t WHERE prev IS NOT NULL
- CRITICAL: NEVER write lag(x)*100 or /lag(x) directly — lag() MUST always appear inside OVER() clause or be aliased in a subquery first
- CRITICAL: window functions (lag, lead, row_number) CANNOT be used in WHERE clause — put them in a CTE first, then filter in the outer query
- Correlation: CORR(a, b) in DuckDB works as aggregate; returns NULL if <2 rows
- Monthly avg: DATE_TRUNC('month', trade_date)
- Window functions: use lowercase lag(), lead(), row_number() — DuckDB is case-sensitive here
- MOEXOG sector index: use v_moex_sectors.moexog_oil_gas (NOT in v_market_context)
- MOEXFN sector index: use v_moex_sectors.moexfn_finance (NOT in v_market_context)
- v_market_context has: imoex_close, usd_rub, eur_rub, brent_usd, gold_usd, key_rate_pct, inflation_pct
- CRITICAL — FOREIGN INDICES: DXY, FTSE_CHINA_50, MSCI_INDIA, MSCI_WORLD, DJ_SOUTH_AFRICA, SILVER, ALUMINUM are in market_data table ONLY.
  NEVER use v_market_context.imoex_close as a proxy for a foreign index — imoex_close IS the Russian IMOEX index, NOT any foreign instrument.
  NEVER alias usd_rub AS dxy — they are completely different instruments.
  CORRECT pattern: FROM market_data d WHERE d.instrument = 'FTSE_CHINA_50' (use d.close as the price)
  Verified correlations (2022-2025, n>500): FTSE_CHINA_50->MOEXOG lag14d r=-0.20 (weak ruble) | MSCI_INDIA->MOEXFN lag14d r=0.63 | DXY->USD/RUB lag7d r=-0.09 (no signal)
- Join sectors with rate: JOIN v_key_rate_daily k ON DATE_TRUNC('month', s.trade_date) = k.period_date
- v_wage_dynamics is ANNUAL (year column) — use it standalone, not joined with daily/monthly tables
- For wage analysis: SELECT year, avg_wage_rub, real_wage_idx FROM v_wage_dynamics WHERE year BETWEEN X AND Y
- CORR() returns NULL (not 0) when there are <2 matching rows — always check n before interpreting

LAG HYPOTHESIS TEMPLATE (news_daily → market, lag N days):
  -- Use v_market_context for imoex_close (2016+), usd_rub, brent_usd, gold_usd (2022+)
  -- Use v_moex_sectors for MOEXFN/MOEXOG (data from 2016+)
  -- Preferred market targets: imoex_close, moexfn_finance, moexog_oil_gas, usd_rub, brent_usd, gold_usd

  -- Example: oil news at T → MOEXOG price at T+N:
  SELECT
      {N} AS lag_days,
      ROUND(CORR(n.{topic}, s.{sector_col}), 4) AS correlation,
      COUNT(*) AS n,
      ROUND(AVG(n.{topic}), 1) AS avg_daily_mentions
  FROM news_daily n
  JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL {N} DAYS
  WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01'

  -- Or: news → forex/commodity:
  SELECT {N} AS lag_days, ROUND(CORR(n.{topic}, m.usd_rub), 4) AS correlation, COUNT(*) AS n
  FROM news_daily n
  JOIN v_market_context m ON m.trade_date = n.news_date + INTERVAL {N} DAYS
  WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01' AND m.usd_rub IS NOT NULL

  -- topic: oil, rate, ruble, sanctions, inflation, banking, gold
  -- sector_col: moexfn_finance (2016+), moexog_oil_gas (2019+), moex10_bluechip (2020+)
  -- CRITICAL: use literal N in INTERVAL (e.g. INTERVAL 14 DAYS), NOT a variable
  -- CRITICAL: CROSS JOIN + lag variable in ON-clause does NOT work in DuckDB — use a single fixed N
  -- Good signal: |corr| > 0.3 AND n > 100
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
