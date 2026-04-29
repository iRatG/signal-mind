"""Parse Investing.com CSV files into DuckDB market_data table."""
from pathlib import Path

import pandas as pd

from src.db.init_db import get_connection

MOEX_DIR = Path(__file__).parents[2] / "statistic" / "moex"

INSTRUMENT_MAP = {
    "Прошлые данные - USD_RUB.csv": "USD_RUB",
    "Прошлые данные - EUR_RUB.csv": "EUR_RUB",
    "Прошлые данные - US 500 Cash.csv": "SP500",
    "Прошлые данные - Фьючерс на золото.csv": "GOLD",
    "Прошлые данные - Фьючерс на нефть Brent.csv": "BRENT",
    "Прошлые данные - Фьючерс на индекс USD.csv": "DXY",
}


def parse_volume(val) -> float | None:
    if pd.isna(val) or val == "-":
        return None
    val = str(val).strip().replace(",", ".")
    if val.endswith("K"):
        return float(val[:-1]) * 1_000
    if val.endswith("M"):
        return float(val[:-1]) * 1_000_000
    if val.endswith("B"):
        return float(val[:-1]) * 1_000_000_000
    try:
        return float(val)
    except ValueError:
        return None


def load_investing_data():
    con = get_connection()
    con.execute("DELETE FROM market_data")
    total = 0

    for filename, instrument in INSTRUMENT_MAP.items():
        path = MOEX_DIR / filename
        if not path.exists():
            print(f"  {filename}: not found, skip")
            continue
        try:
            df = pd.read_csv(path, thousands="\xa0")
            # Columns differ by encoding — use positional
            df.columns = ["trade_date", "close", "open", "high", "low", "volume", "change_pct"]
            df["trade_date"] = pd.to_datetime(df["trade_date"], dayfirst=True, errors="coerce")
            for col in ["close", "open", "high", "low"]:
                df[col] = (
                    df[col].astype(str)
                    .str.replace("\xa0", "")
                    .str.replace(".", "", regex=False)   # strip thousands dot
                    .str.replace(",", ".", regex=False)  # decimal comma → dot
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["change_pct"] = df["change_pct"].astype(str).str.replace("%", "").str.replace(",", ".")
            df["change_pct"] = pd.to_numeric(df["change_pct"], errors="coerce")
            df["instrument"] = instrument
            df = df[["trade_date", "instrument", "close", "open", "high", "low", "change_pct"]]
            df = df.dropna(subset=["trade_date", "close"])

            con.execute("INSERT OR IGNORE INTO market_data SELECT * FROM df")
            total += len(df)
            print(f"  {instrument}: {len(df)} rows")
        except Exception as e:
            print(f"  {instrument}: ERROR — {e}")

    con.close()
    print(f"Market data total: {total} rows")


if __name__ == "__main__":
    load_investing_data()
