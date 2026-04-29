"""Parse CBR forex rate Excel files (RC_F*) into DuckDB forex_cbr table."""
from pathlib import Path

import pandas as pd

from src.db.init_db import get_connection

MOEX_DIR = Path(__file__).parents[2] / "statistic" / "moex"

CURRENCY_MAP = {
    "Армянский драм": "AMD",
    "Белорусский рубль": "BYN",
    "Вона Республики Корея": "KRW",
    "Гонконгский доллар": "HKD",
    "Дирхам ОАЭ": "AED",
    "Доллар США": "USD",
    "Евро": "EUR",
    "Индийская рупия": "INR",
    "Казахстанский тенге": "KZT",
    "Канадский доллар": "CAD",
    "Китайский юань": "CNY",
    "Киргизский сом": "KGS",
    "Швейцарский франк": "CHF",
    "Японская иена": "JPY",
    "Фунт стерлингов": "GBP",
    "Таджикский сомони": "TJS",
    "Туркменский манат": "TMT",
    "Узбекский сум": "UZS",
    "Южноафриканский рэнд": "ZAR",
}


def load_cbr_forex():
    con = get_connection()
    con.execute("DELETE FROM forex_cbr")
    total = 0

    for path in sorted(MOEX_DIR.glob("RC_F*.xlsx")):
        try:
            df = pd.read_excel(path, header=0)
            df.columns = ["nominal", "trade_date", "rate", "currency_name"]
            df = df.dropna(subset=["rate", "trade_date"])
            df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
            df = df.dropna(subset=["trade_date"])
            df["currency"] = df["currency_name"].map(CURRENCY_MAP).fillna(df["currency_name"])
            df = df[["trade_date", "currency", "rate", "nominal"]]

            con.execute("INSERT OR IGNORE INTO forex_cbr SELECT * FROM df")
            total += len(df)
            print(f"  {path.name}: {len(df)} rows")
        except Exception as e:
            print(f"  {path.name}: ERROR — {e}")

    con.close()
    print(f"CBR forex total: {total} rows")


if __name__ == "__main__":
    load_cbr_forex()
