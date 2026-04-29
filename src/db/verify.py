"""Quick data quality report across all DuckDB tables."""
from src.db.init_db import get_connection


def report():
    con = get_connection()

    checks = {
        "moex_indices": {
            "date_col": "trade_date",
            "value_cols": ["close", "open", "high", "low", "value"],
            "group_col": "ticker",
        },
        "key_rate": {
            "date_col": "period",
            "value_cols": ["rate_pct", "inflation"],
            "group_col": None,
        },
        "forex_cbr": {
            "date_col": "trade_date",
            "value_cols": ["rate"],
            "group_col": "currency",
        },
        "market_data": {
            "date_col": "trade_date",
            "value_cols": ["close", "open", "high", "low"],
            "group_col": "instrument",
        },
    }

    for table, cfg in checks.items():
        print(f"\n{'='*60}")
        print(f"TABLE: {table}")
        print(f"{'='*60}")

        total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"Total rows: {total}")

        dc = cfg["date_col"]
        if table != "key_rate":
            dates = con.execute(
                f"SELECT MIN({dc}), MAX({dc}) FROM {table}"
            ).fetchone()
            print(f"Date range: {dates[0]}  to  {dates[1]}")
        else:
            dates = con.execute(
                f"SELECT MIN({dc}), MAX({dc}) FROM {table}"
            ).fetchone()
            print(f"Period range: {dates[0]}  to  {dates[1]}")

        # Null counts per value column
        null_parts = ", ".join(
            f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_nulls"
            for c in cfg["value_cols"]
        )
        nulls = con.execute(f"SELECT {null_parts} FROM {table}").fetchone()
        null_info = ", ".join(
            f"{cfg['value_cols'][i]}={nulls[i]}" for i in range(len(cfg["value_cols"]))
        )
        print(f"Nulls: {null_info}")

        # Per-group breakdown
        if cfg["group_col"]:
            gc = cfg["group_col"]
            rows = con.execute(
                f"""
                SELECT {gc}, COUNT(*) as cnt,
                       MIN({dc}) as date_from, MAX({dc}) as date_to
                FROM {table}
                GROUP BY {gc}
                ORDER BY {gc}
                """
            ).fetchall()
            print(f"\n  {'Instrument':<22} {'Rows':>6}  {'From':<12}  {'To':<12}")
            print(f"  {'-'*56}")
            for r in rows:
                print(f"  {str(r[0]):<22} {r[1]:>6}  {str(r[2]):<12}  {str(r[3]):<12}")

    con.close()
    print(f"\n{'='*60}")
    print("Report complete.")


if __name__ == "__main__":
    report()
