"""Parse MOEX index ZIP CSV files into DuckDB moex_indices table."""
import zipfile
import io
from pathlib import Path

import pandas as pd

from src.db.init_db import get_connection

MOEX_DIR = Path(__file__).parents[2] / "statistic" / "moex"


def parse_zip(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            raw = f.read().decode("cp1251")

    # First line is "history", second is empty, third is header
    lines = raw.split("\n")
    header_idx = next(i for i, l in enumerate(lines) if l.startswith("BOARDID"))
    csv_text = "\n".join(lines[header_idx:])

    df = pd.read_csv(
        io.StringIO(csv_text),
        sep=";",
        decimal=",",
        parse_dates=["TRADEDATE"],
        dayfirst=True,
        low_memory=False,
    )
    return df


def load_moex_indices():
    con = get_connection()
    total = 0

    for zip_path in sorted(MOEX_DIR.glob("*.csv.zip")):
        ticker = zip_path.stem.replace(".csv", "")
        try:
            df = parse_zip(zip_path)
            df = df.rename(columns={
                "TRADEDATE": "trade_date",
                "CLOSE": "close",
                "OPEN": "open",
                "HIGH": "high",
                "LOW": "low",
                "VALUE": "value",
                "VOLUME": "volume",
                "CURRENCYID": "currency",
            })
            df["ticker"] = ticker
            for col in ["open", "high", "low"]:
                if col not in df.columns:
                    df[col] = None
            df = df[["ticker", "trade_date", "close", "open", "high", "low", "value", "volume", "currency"]]
            df = df.dropna(subset=["close"])

            con.execute("DELETE FROM moex_indices WHERE ticker = ?", [ticker])
            con.execute("INSERT INTO moex_indices SELECT * FROM df")
            count = len(df)
            total += count
            print(f"  {ticker}: {count} rows")
        except Exception as e:
            print(f"  {ticker}: ERROR — {e}")

    con.close()
    print(f"Total loaded: {total} rows")


if __name__ == "__main__":
    load_moex_indices()
