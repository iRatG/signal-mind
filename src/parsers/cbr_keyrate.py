"""Parse CBR key rate + inflation Excel into DuckDB key_rate table."""
from pathlib import Path

import pandas as pd

from src.db.init_db import get_connection

MOEX_DIR = Path(__file__).parents[2] / "statistic" / "moex"
# Use the longer history file
KEY_RATE_FILE = MOEX_DIR / "Инфляция и ключевая ставка Банка России_F17_09_2013_T29_04_2026.xlsx"


def load_key_rate():
    df = pd.read_excel(KEY_RATE_FILE, header=0)
    df.columns = ["period", "rate_pct", "inflation", "days_to_meeting"]
    df = df.dropna(subset=["rate_pct"])
    df["days_to_meeting"] = pd.to_numeric(df["days_to_meeting"], errors="coerce").astype("Int64")

    con = get_connection()
    con.execute("DELETE FROM key_rate")
    con.execute("INSERT INTO key_rate SELECT * FROM df")
    con.close()
    print(f"Key rate: {len(df)} rows loaded")


if __name__ == "__main__":
    load_key_rate()
