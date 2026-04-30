"""Independent verification of structurally correct signal candidates."""
import duckdb, sys
from pathlib import Path

con = duckdb.connect(str(Path(__file__).parents[1] / 'db' / 'signal_mind.duckdb'), read_only=True)

def run(label, sql):
    try:
        rows = con.execute(sql).fetchall()
        cols = [d[0] for d in con.description]
        sys.stdout.buffer.write(f'\n=== {label} ===\n'.encode('utf-8'))
        sys.stdout.buffer.write(('\t'.join(cols) + '\n').encode('utf-8'))
        for r in rows:
            sys.stdout.buffer.write(('\t'.join(str(x) for x in r) + '\n').encode('utf-8'))
    except Exception as e:
        sys.stdout.buffer.write(f'\n=== {label} ERROR: {e}\n'.encode('utf-8'))

# 1. SILVER -> RUGOLD, lag sweep
run('1. SILVER -> RUGOLD lag sweep 0/7/14/30d', """
SELECT lag_days, ROUND(CORR(silver, rugold), 4) AS corr, COUNT(*) AS n FROM (
    SELECT 0 AS lag_days, d.close AS silver, m.close AS rugold
    FROM market_data d JOIN market_data m ON m.trade_date = d.trade_date
    WHERE d.instrument='SILVER' AND m.instrument='RUGOLD'
      AND d.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
    UNION ALL
    SELECT 7, d.close, m.close
    FROM market_data d JOIN market_data m ON m.trade_date = d.trade_date + INTERVAL 7 DAYS
    WHERE d.instrument='SILVER' AND m.instrument='RUGOLD'
      AND d.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
    UNION ALL
    SELECT 14, d.close, m.close
    FROM market_data d JOIN market_data m ON m.trade_date = d.trade_date + INTERVAL 14 DAYS
    WHERE d.instrument='SILVER' AND m.instrument='RUGOLD'
      AND d.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
    UNION ALL
    SELECT 30, d.close, m.close
    FROM market_data d JOIN market_data m ON m.trade_date = d.trade_date + INTERVAL 30 DAYS
    WHERE d.instrument='SILVER' AND m.instrument='RUGOLD'
      AND d.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
) t GROUP BY lag_days ORDER BY lag_days
""")

# 2. SILVER -> RUGOLD lag 14d by ruble regime
run('2. SILVER -> RUGOLD lag 14d by ruble regime', """
WITH t AS (
    SELECT d.close AS silver, m2.close AS rugold, mc.usd_rub
    FROM market_data d
    JOIN market_data m2 ON m2.trade_date = d.trade_date + INTERVAL 14 DAYS
    JOIN v_market_context mc ON mc.trade_date = d.trade_date
    WHERE d.instrument='SILVER' AND m2.instrument='RUGOLD'
      AND d.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
      AND d.close IS NOT NULL AND m2.close IS NOT NULL AND mc.usd_rub IS NOT NULL
)
SELECT CASE WHEN usd_rub > 80 THEN 'weak_ruble' ELSE 'strong_ruble' END AS regime,
       ROUND(CORR(silver, rugold), 4) AS corr, COUNT(*) AS n
FROM t GROUP BY regime ORDER BY regime
""")

# 3. Gold price -> RUGOLD lag sweep
run('3. Gold price -> RUGOLD index lag sweep', """
SELECT lag_days, ROUND(CORR(gold, rugold), 4) AS corr, COUNT(*) AS n FROM (
    SELECT 0 AS lag_days, m.gold_usd AS gold, md.close AS rugold
    FROM v_market_context m JOIN market_data md ON md.trade_date = m.trade_date
    WHERE md.instrument='RUGOLD' AND m.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
      AND m.gold_usd IS NOT NULL AND md.close IS NOT NULL
    UNION ALL
    SELECT 7, m.gold_usd, md.close
    FROM v_market_context m JOIN market_data md ON md.trade_date = m.trade_date + INTERVAL 7 DAYS
    WHERE md.instrument='RUGOLD' AND m.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
      AND m.gold_usd IS NOT NULL AND md.close IS NOT NULL
    UNION ALL
    SELECT 14, m.gold_usd, md.close
    FROM v_market_context m JOIN market_data md ON md.trade_date = m.trade_date + INTERVAL 14 DAYS
    WHERE md.instrument='RUGOLD' AND m.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
      AND m.gold_usd IS NOT NULL AND md.close IS NOT NULL
    UNION ALL
    SELECT 30, m.gold_usd, md.close
    FROM v_market_context m JOIN market_data md ON md.trade_date = m.trade_date + INTERVAL 30 DAYS
    WHERE md.instrument='RUGOLD' AND m.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
      AND m.gold_usd IS NOT NULL AND md.close IS NOT NULL
) t GROUP BY lag_days ORDER BY lag_days
""")

# 4. Brent -> MOEXOG lag sweep
run('4. Brent -> MOEXOG lag sweep 0/7/14/30d', """
SELECT lag_days, ROUND(CORR(brent, moexog), 4) AS corr, COUNT(*) AS n FROM (
    SELECT 0 AS lag_days, m.brent_usd AS brent, s.moexog_oil_gas AS moexog
    FROM v_market_context m JOIN v_moex_sectors s ON s.trade_date = m.trade_date
    WHERE m.trade_date BETWEEN '2022-01-01' AND '2025-12-31' AND m.brent_usd IS NOT NULL
    UNION ALL
    SELECT 7, m.brent_usd, s.moexog_oil_gas
    FROM v_market_context m JOIN v_moex_sectors s ON s.trade_date = m.trade_date + INTERVAL 7 DAYS
    WHERE m.trade_date BETWEEN '2022-01-01' AND '2025-12-31' AND m.brent_usd IS NOT NULL
    UNION ALL
    SELECT 14, m.brent_usd, s.moexog_oil_gas
    FROM v_market_context m JOIN v_moex_sectors s ON s.trade_date = m.trade_date + INTERVAL 14 DAYS
    WHERE m.trade_date BETWEEN '2022-01-01' AND '2025-12-31' AND m.brent_usd IS NOT NULL
    UNION ALL
    SELECT 30, m.brent_usd, s.moexog_oil_gas
    FROM v_market_context m JOIN v_moex_sectors s ON s.trade_date = m.trade_date + INTERVAL 30 DAYS
    WHERE m.trade_date BETWEEN '2022-01-01' AND '2025-12-31' AND m.brent_usd IS NOT NULL
) t GROUP BY lag_days ORDER BY lag_days
""")

# 5. Oil news -> MOEXOG, full lag sweep + regime
run('5. Oil news -> MOEXOG lag sweep 7/14/30/60/90d', """
SELECT lag_days, ROUND(CORR(oil, moexog), 4) AS corr, COUNT(*) AS n FROM (
    SELECT 7  AS lag_days, n.oil, s.moexog_oil_gas AS moexog
    FROM news_daily n JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL 7 DAYS
    WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01'
    UNION ALL
    SELECT 14, n.oil, s.moexog_oil_gas
    FROM news_daily n JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL 14 DAYS
    WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01'
    UNION ALL
    SELECT 30, n.oil, s.moexog_oil_gas
    FROM news_daily n JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL 30 DAYS
    WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01'
    UNION ALL
    SELECT 60, n.oil, s.moexog_oil_gas
    FROM news_daily n JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL 60 DAYS
    WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01'
    UNION ALL
    SELECT 90, n.oil, s.moexog_oil_gas
    FROM news_daily n JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL 90 DAYS
    WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01'
) t GROUP BY lag_days ORDER BY lag_days
""")

# 6. Oil news -> MOEXOG lag 90d by ruble regime
run('6. Oil news -> MOEXOG lag 90d by regime', """
WITH t AS (
    SELECT n.oil, s.moexog_oil_gas, m.usd_rub
    FROM news_daily n
    JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL 90 DAYS
    JOIN v_market_context m ON m.trade_date = n.news_date
    WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01'
      AND n.oil IS NOT NULL AND s.moexog_oil_gas IS NOT NULL AND m.usd_rub IS NOT NULL
)
SELECT CASE WHEN usd_rub > 80 THEN 'weak_ruble' ELSE 'strong_ruble' END AS regime,
       ROUND(CORR(oil, moexog_oil_gas), 4) AS corr, COUNT(*) AS n
FROM t GROUP BY regime ORDER BY regime
""")

# 7. Sanctions -> MOEX10 lag 60/90d by rate regime
run('7. Sanctions -> MOEX10 lag 60/90d by rate regime', """
SELECT lag_days, regime, ROUND(CORR(sanctions, moex10), 4) AS corr, COUNT(*) AS n FROM (
    SELECT 60 AS lag_days, n.sanctions, s.moex10_bluechip AS moex10,
           CASE WHEN m.key_rate_pct >= 15 THEN 'high_rate' ELSE 'low_rate' END AS regime
    FROM news_daily n
    JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL 60 DAYS
    JOIN v_market_context m ON m.trade_date = n.news_date
    WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01'
    UNION ALL
    SELECT 90, n.sanctions, s.moex10_bluechip,
           CASE WHEN m.key_rate_pct >= 15 THEN 'high_rate' ELSE 'low_rate' END
    FROM news_daily n
    JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL 90 DAYS
    JOIN v_market_context m ON m.trade_date = n.news_date
    WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01'
) t GROUP BY lag_days, regime ORDER BY lag_days, regime
""")

# 8. Banking news -> MOEXFN, regime sweep (low/high rate)
run('8. Banking news -> MOEXFN, all lags x rate regime', """
SELECT lag_days, regime, ROUND(CORR(banking, moexfn), 4) AS corr, COUNT(*) AS n FROM (
    SELECT 7 AS lag_days, n.banking, s.moexfn_finance AS moexfn,
           CASE WHEN m.key_rate_pct < 15 THEN 'low_rate' ELSE 'high_rate' END AS regime
    FROM news_daily n
    JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL 7 DAYS
    JOIN v_market_context m ON m.trade_date = n.news_date
    WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01' AND m.key_rate_pct IS NOT NULL
    UNION ALL
    SELECT 14, n.banking, s.moexfn_finance,
           CASE WHEN m.key_rate_pct < 15 THEN 'low_rate' ELSE 'high_rate' END
    FROM news_daily n
    JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL 14 DAYS
    JOIN v_market_context m ON m.trade_date = n.news_date
    WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01' AND m.key_rate_pct IS NOT NULL
    UNION ALL
    SELECT 30, n.banking, s.moexfn_finance,
           CASE WHEN m.key_rate_pct < 15 THEN 'low_rate' ELSE 'high_rate' END
    FROM news_daily n
    JOIN v_moex_sectors s ON s.trade_date = n.news_date + INTERVAL 30 DAYS
    JOIN v_market_context m ON m.trade_date = n.news_date
    WHERE n.news_date BETWEEN '2022-01-01' AND '2025-01-01' AND m.key_rate_pct IS NOT NULL
) t GROUP BY lag_days, regime ORDER BY lag_days, regime
""")

# 9. MSCI_INDIA -> MOEXFN full regime matrix
run('9. MSCI_INDIA -> MOEXFN lag 14d full regime matrix', """
WITH t AS (
    SELECT d.close AS msci, s.moexfn_finance, mc.usd_rub, mc.key_rate_pct
    FROM market_data d
    JOIN v_moex_sectors s ON s.trade_date = d.trade_date + INTERVAL 14 DAYS
    JOIN v_market_context mc ON mc.trade_date = d.trade_date
    WHERE d.instrument = 'MSCI_INDIA'
      AND d.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
      AND d.close IS NOT NULL AND s.moexfn_finance IS NOT NULL
)
SELECT
    CASE WHEN usd_rub > 80 THEN 'weak_ruble' ELSE 'strong_ruble' END AS ruble_regime,
    CASE WHEN key_rate_pct >= 15 THEN 'high_rate' ELSE 'low_rate' END AS rate_regime,
    ROUND(CORR(msci, moexfn_finance), 4) AS corr,
    COUNT(*) AS n
FROM t GROUP BY ruble_regime, rate_regime ORDER BY ruble_regime, rate_regime
""")

con.close()
