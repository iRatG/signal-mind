"""Create analytical views in DuckDB for cross-table analysis."""
from src.db.init_db import get_connection


def create_views():
    con = get_connection()

    # ── 1. key_rate_daily: ключевая ставка с нормальной датой (MM.YYYY -> DATE) ──
    con.execute("DROP VIEW IF EXISTS v_key_rate_daily")
    con.execute("""
        CREATE VIEW v_key_rate_daily AS
        SELECT
            -- period like '3.2026' -> 2026-03-01
            make_date(
                CAST(split_part(period, '.', 2) AS INTEGER),
                CAST(split_part(period, '.', 1) AS INTEGER),
                1
            ) AS period_date,
            rate_pct,
            inflation
        FROM key_rate
        WHERE period LIKE '%.%'
          AND TRY_CAST(split_part(period, '.', 1) AS INTEGER) BETWEEN 1 AND 12
          AND TRY_CAST(split_part(period, '.', 2) AS INTEGER) > 2000
    """)

    # ── 2. market_context: дата + IMOEX + USD/RUB + ставка + нефть ─────────────
    con.execute("DROP VIEW IF EXISTS v_market_context")
    con.execute("""
        CREATE VIEW v_market_context AS
        WITH
        imoex AS (
            SELECT trade_date, close AS imoex_close
            FROM moex_indices WHERE ticker = 'IMOEX'
        ),
        usd AS (
            SELECT trade_date, close AS usd_rub
            FROM market_data WHERE instrument = 'USD_RUB'
        ),
        eur AS (
            SELECT trade_date, close AS eur_rub
            FROM market_data WHERE instrument = 'EUR_RUB'
        ),
        brent AS (
            SELECT trade_date, close AS brent_usd
            FROM market_data WHERE instrument = 'BRENT'
        ),
        gold AS (
            SELECT trade_date, close AS gold_usd
            FROM market_data WHERE instrument = 'GOLD'
        ),
        rate AS (
            SELECT period_date, rate_pct, inflation
            FROM v_key_rate_daily
        )
        SELECT
            usd.trade_date,
            imoex.imoex_close,
            usd.usd_rub,
            eur.eur_rub,
            brent.brent_usd,
            gold.gold_usd,
            -- last known rate (monthly, fill forward via asof)
            rate.rate_pct    AS key_rate_pct,
            rate.inflation   AS inflation_pct
        FROM usd
        LEFT JOIN imoex  ON imoex.trade_date  = usd.trade_date
        LEFT JOIN eur    ON eur.trade_date    = usd.trade_date
        LEFT JOIN brent  ON brent.trade_date  = usd.trade_date
        LEFT JOIN gold   ON gold.trade_date   = usd.trade_date
        LEFT JOIN rate   ON rate.period_date  = CAST(date_trunc('month', usd.trade_date) AS DATE)
        ORDER BY usd.trade_date
    """)

    # ── 3. wage_dynamics: зарплата по годам с реальным индексом ─────────────────
    # Get exact indicator name for "Все организации" (encoding-safe, by value)
    row = con.execute("""
        SELECT indicator FROM rosstat_macro
        WHERE indicator LIKE 'real_wage_idx_%' AND period='2000' AND ABS(value - 120.9) < 0.05
    """).fetchone()
    all_orgs_indicator = row[0] if row else "real_wage_idx_Все организации"

    con.execute("DROP VIEW IF EXISTS v_wage_dynamics")
    con.execute(f"""
        CREATE VIEW v_wage_dynamics AS
        SELECT
            CAST(period AS INTEGER) AS year,
            AVG(CASE WHEN indicator = 'avg_wage_rub' THEN value END)              AS avg_wage_rub,
            AVG(CASE WHEN indicator = '{all_orgs_indicator}' THEN value END)      AS real_wage_idx
        FROM rosstat_macro
        WHERE source_file IN ('tab1-zpl_01-2026.xlsx', 'tab5-zpl_2025.xlsx')
          AND TRY_CAST(period AS INTEGER) BETWEEN 2000 AND 2030
        GROUP BY period
        ORDER BY year
    """)

    # ── 4. moex_sector_compare: секторные индексы на одну дату ──────────────────
    con.execute("DROP VIEW IF EXISTS v_moex_sectors")
    con.execute("""
        CREATE VIEW v_moex_sectors AS
        SELECT
            trade_date,
            MAX(CASE WHEN ticker = 'IMOEX'  THEN close END) AS imoex,
            MAX(CASE WHEN ticker = 'MOEXFN' THEN close END) AS moexfn_finance,
            MAX(CASE WHEN ticker = 'MOEXOG' THEN close END) AS moexog_oil_gas,
            MAX(CASE WHEN ticker = 'MOEX10' THEN close END) AS moex10_bluechip,
            MAX(CASE WHEN ticker = 'RUGOLD' THEN close END) AS rugold,
            MAX(CASE WHEN ticker = 'RUSFAR3M' THEN close END) AS rusfar3m_rate
        FROM moex_indices
        GROUP BY trade_date
        ORDER BY trade_date
    """)

    con.close()
    print("Views created:")
    print("  v_key_rate_daily   — key rate with proper DATE column")
    print("  v_market_context   — IMOEX + USD/EUR + Brent + Gold + key rate by day")
    print("  v_wage_dynamics    — wages + real wage index by year")
    print("  v_moex_sectors     — all sector indices side by side")


def check_views():
    con = get_connection()
    print("\n=== v_market_context (last 5 rows) ===")
    rows = con.execute("""
        SELECT trade_date, imoex_close, usd_rub, brent_usd, key_rate_pct
        FROM v_market_context
        WHERE imoex_close IS NOT NULL
        ORDER BY trade_date DESC LIMIT 5
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}  IMOEX={r[1]:.0f}  USD={r[2]:.2f}  Brent={r[3]:.1f}  Rate={r[4]}%")

    print("\n=== v_wage_dynamics (last 5 years) ===")
    rows = con.execute("""
        SELECT year, avg_wage_rub, real_wage_idx
        FROM v_wage_dynamics WHERE avg_wage_rub IS NOT NULL
        ORDER BY year DESC LIMIT 5
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}  wage={r[1]:,.0f} rub  real_idx={r[2]}")

    print("\n=== v_moex_sectors (last 3 rows) ===")
    rows = con.execute("""
        SELECT trade_date, imoex, moexfn_finance, moexog_oil_gas
        FROM v_moex_sectors
        WHERE imoex IS NOT NULL
        ORDER BY trade_date DESC LIMIT 3
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}  IMOEX={r[1]:.0f}  Finance={r[2]:.0f}  OilGas={r[3]:.0f}")

    con.close()


if __name__ == "__main__":
    create_views()
    check_views()
