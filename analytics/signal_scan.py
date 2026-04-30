"""
Systematic clean signal scan.
Every source comes from its correct table.
No aliasing, no self-correlation, no tautologies.

Tests all (source × target × lag) combinations, reports top results by |corr|.
"""
import duckdb
from pathlib import Path
from itertools import product

con = duckdb.connect(
    str(Path(__file__).parents[1] / 'db' / 'signal_mind.duckdb'),
    read_only=True
)

LAGS = [0, 7, 14, 30, 60, 90]
MIN_N = 150  # minimum data points to consider a result meaningful

# ── Source definitions ─────────────────────────────────────────────────────
# Each source: (label, sql_col, from_clause, where_extra, date_col)

SOURCES_MARKET = [
    # (label, instrument name in market_data)
    ("SP500",          "SP500"),
    ("GOLD_USD",       "GOLD"),
    ("BRENT_USD",      "BRENT"),
    ("DXY",            "DXY"),
    ("MSCI_WORLD",     "MSCI_WORLD"),
    ("MSCI_INDIA",     "MSCI_INDIA"),
    ("FTSE_CHINA_50",  "FTSE_CHINA_50"),
    ("CHINA_H_SHARES", "CHINA_H_SHARES"),
    ("DJ_SA",          "DJ_SOUTH_AFRICA"),
    ("SILVER",         "SILVER"),
]

SOURCES_NEWS = [
    # (label, column in news_daily)
    ("news_oil",       "oil"),
    ("news_rate",      "rate"),
    ("news_ruble",     "ruble"),
    ("news_sanctions", "sanctions"),
    ("news_inflation", "inflation"),
    ("news_banking",   "banking"),
    ("news_gold",      "gold"),
]

SOURCES_MACRO = [
    # (label, column in v_market_context)
    ("USD_RUB",    "usd_rub"),
    ("EUR_RUB",    "eur_rub"),
    ("BRENT_MC",   "brent_usd"),   # same Brent but via v_market_context
    ("GOLD_MC",    "gold_usd"),
    ("KEY_RATE",   "key_rate_pct"),
]

TARGETS = [
    # (label, sql expression, from_clause)
    ("IMOEX",   "mc.imoex_close",    "v_market_context mc",  "mc.trade_date", "mc.imoex_close IS NOT NULL"),
    ("MOEXFN",  "s.moexfn_finance",  "v_moex_sectors s",     "s.trade_date",  "s.moexfn_finance IS NOT NULL"),
    ("MOEXOG",  "s.moexog_oil_gas",  "v_moex_sectors s",     "s.trade_date",  "s.moexog_oil_gas IS NOT NULL"),
    ("MOEX10",  "s.moex10_bluechip", "v_moex_sectors s",     "s.trade_date",  "s.moex10_bluechip IS NOT NULL"),
    ("USD_RUB", "mc.usd_rub",        "v_market_context mc",  "mc.trade_date", "mc.usd_rub IS NOT NULL"),
]

results = []

def compute(src_label, src_expr, src_from, src_date, src_where,
            tgt_label, tgt_expr, tgt_from, tgt_date, tgt_where,
            lag):
    # Skip self (same underlying series)
    if src_label == tgt_label:
        return
    # Skip macro sources vs macro targets if they're the same field
    if src_label in ("BRENT_MC", "BRENT_USD") and tgt_label in ("BRENT_MC", "BRENT_USD"):
        return
    if src_label in ("GOLD_MC", "GOLD_USD") and tgt_label in ("GOLD_MC", "GOLD_USD"):
        return
    if src_label == "USD_RUB" and tgt_label == "USD_RUB":
        return
    if src_label == "EUR_RUB" and tgt_label == "USD_RUB" and lag <= 1:
        return  # known tautology

    if lag == 0:
        interval = ""
        join_cond = f"{src_date} = {tgt_date}"
    else:
        interval = f"INTERVAL {lag} DAYS"
        join_cond = f"{tgt_date} = {src_date} + {interval}"

    sql = f"""
        SELECT ROUND(CORR({src_expr}, {tgt_expr}), 4) AS corr, COUNT(*) AS n
        FROM {src_from}
        JOIN {tgt_from} ON {join_cond}
        WHERE {src_date} BETWEEN '2022-01-01' AND '2025-12-31'
          AND {src_where} AND {tgt_where}
    """
    try:
        row = con.execute(sql).fetchone()
        if row and row[0] is not None and row[1] >= MIN_N:
            results.append({
                "source": src_label, "target": tgt_label, "lag": lag,
                "corr": float(row[0]), "n": int(row[1]),
                "abs_corr": abs(float(row[0]))
            })
    except Exception:
        pass


print("Scanning market_data sources...")
for (slabel, sinstr), (tlabel, texpr, tfrom, tdate, twhere), lag in product(
        SOURCES_MARKET, TARGETS, LAGS):
    src_expr = "src.close"
    src_from = f"market_data src"
    src_date = "src.trade_date"
    src_where = f"src.instrument = '{sinstr}' AND src.close IS NOT NULL"
    compute(slabel, src_expr, src_from, src_date, src_where,
            tlabel, texpr, tfrom, tdate, twhere, lag)

print("Scanning news sources...")
for (slabel, scol), (tlabel, texpr, tfrom, tdate, twhere), lag in product(
        SOURCES_NEWS, TARGETS, LAGS):
    if lag == 0:
        continue  # news lag=0 not meaningful
    src_expr = f"nd.{scol}"
    src_from = "news_daily nd"
    src_date = "nd.news_date"
    src_where = f"nd.{scol} IS NOT NULL"
    compute(slabel, src_expr, src_from, src_date, src_where,
            tlabel, texpr, tfrom, tdate, twhere, lag)

print("Scanning macro sources...")
for (slabel, scol), (tlabel, texpr, tfrom, tdate, twhere), lag in product(
        SOURCES_MACRO, TARGETS, LAGS):
    if slabel == "EUR_RUB" and tlabel == "USD_RUB" and lag == 0:
        continue  # skip contemporaneous tautology
    src_expr = f"mc2.{scol}"
    src_from = "v_market_context mc2"
    src_date = "mc2.trade_date"
    src_where = f"mc2.{scol} IS NOT NULL"
    compute(slabel, src_expr, src_from, src_date, src_where,
            tlabel, texpr, tfrom, tdate, twhere, lag)

con.close()

# ── Report top results ─────────────────────────────────────────────────────
results.sort(key=lambda x: -x["abs_corr"])

import sys
out_buf = sys.stdout.buffer

def p(s): out_buf.write((s + "\n").encode("utf-8"))

p(f"\n{'='*72}")
p(f"CLEAN SIGNAL SCAN - top results (n >= {MIN_N}, 2022-2025)")
p(f"Total combinations tested: {len(results)}")
p(f"{'='*72}")

# De-duplicate: keep best lag per (source, target) pair
seen = {}
for r in results:
    key = (r["source"], r["target"])
    if key not in seen or r["abs_corr"] > seen[key]["abs_corr"]:
        seen[key] = r

deduped = sorted(seen.values(), key=lambda x: -x["abs_corr"])

p(f"\n{'--- STRONG: |corr| >= 0.60 ':->72}")
for r in deduped:
    if r["abs_corr"] >= 0.60:
        sign = "+" if r["corr"] > 0 else "-"
        p(f"  {r['source']:18s} -> {r['target']:8s}  lag={r['lag']:2d}d  "
          f"r={sign}{r['abs_corr']:.3f}  n={r['n']:5d}")

p(f"\n{'--- MODERATE: 0.35-0.60 ':->72}")
for r in deduped:
    if 0.35 <= r["abs_corr"] < 0.60:
        sign = "+" if r["corr"] > 0 else "-"
        p(f"  {r['source']:18s} -> {r['target']:8s}  lag={r['lag']:2d}d  "
          f"r={sign}{r['abs_corr']:.3f}  n={r['n']:5d}")

p(f"\n{'--- WEAK: 0.20-0.35 ':->72}")
for r in deduped:
    if 0.20 <= r["abs_corr"] < 0.35:
        sign = "+" if r["corr"] > 0 else "-"
        p(f"  {r['source']:18s} -> {r['target']:8s}  lag={r['lag']:2d}d  "
          f"r={sign}{r['abs_corr']:.3f}  n={r['n']:5d}")

# Save full results to CSV
import csv
out = Path(__file__).parent / "signal_scan_results.csv"
with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["source","target","lag","corr","n","abs_corr"])
    w.writeheader()
    w.writerows(sorted(results, key=lambda x: -x["abs_corr"]))
print(f"\nFull results saved to: {out}")
