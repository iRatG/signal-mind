"""Parse Rosstat Excel files into DuckDB rosstat_macro table."""
import warnings
from pathlib import Path

import pandas as pd

from src.db.init_db import get_connection

warnings.filterwarnings("ignore")

STAT_DIR = Path(__file__).parents[2] / "statistic"


# ── helpers ────────────────────────────────────────────────────────────────

def _to_float(val) -> float | None:
    try:
        s = str(val).strip().replace(",", ".").replace(" ", "").replace("\xa0", "")
        if s in ("", "nan", "None", "-", "�"):
            return None
        return float(s)
    except (ValueError, TypeError):
        return None


def _insert(con, rows: list[dict], source: str):
    if not rows:
        return
    df = pd.DataFrame(rows)
    df["source_file"] = source
    df = df[["period", "indicator", "region", "value", "unit", "source_file"]]
    df = df.dropna(subset=["value"])
    con.execute("INSERT INTO rosstat_macro SELECT * FROM df")
    print(f"  {source}: {len(df)} rows")


# ── tab1-zpl: средняя зарплата по месяцам ──────────────────────────────────

def parse_tab1_wages(con):
    """Monthly average wages (руб.) from tab1-zpl_01-2026.xlsx"""
    path = STAT_DIR / "tab1-zpl_01-2026.xlsx"
    df_raw = pd.read_excel(path, sheet_name=0, header=None)

    # row 4 = column labels, row 7+ = data
    col_labels = df_raw.iloc[4].tolist()
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    rows = []
    for _, row in df_raw.iloc[7:].iterrows():
        year_val = _to_float(row.iloc[0])
        if year_val is None or year_val < 1990:
            continue
        year = int(year_val)

        # columns 4..15 = months Jan-Dec (0-indexed in row: 2=annual, 3=prev, 4..7=quarters, 8..19=months)
        # Actual layout: col0=year, col1=annual, col2=prev_year, col3-6=Q1-Q4, col7-18=months
        data_vals = [_to_float(v) for v in row.iloc[1:]]

        # annual average
        annual = data_vals[0] if data_vals else None
        if annual is not None:
            rows.append({
                "period": str(year),
                "indicator": "avg_wage_rub",
                "region": "RF",
                "value": annual,
                "unit": "RUB",
            })

        # monthly values start at offset 6 (after annual, prev, Q1-Q4)
        for i, mname in enumerate(month_names):
            idx = 6 + i
            if idx < len(data_vals):
                v = data_vals[idx]
                if v is not None:
                    rows.append({
                        "period": f"{year}-{mname}",
                        "indicator": "avg_wage_rub",
                        "region": "RF",
                        "value": v,
                        "unit": "RUB",
                    })

    _insert(con, rows, "tab1-zpl_01-2026.xlsx")


# ── tab4-zpl: зарплата по отраслям (уровень) ───────────────────────────────

def parse_tab4_wages_by_sector(con):
    """Annual wages by sector (руб.)"""
    path = STAT_DIR / "tab4-zpl_2025.xlsx"
    rows = []

    for sheet in ["2000-2017", "с 2018"]:
        try:
            df_raw = pd.read_excel(path, sheet_name=sheet, header=None)
        except Exception:
            continue

        # row 1 = years, row 2+ = sector data
        year_row = df_raw.iloc[1].tolist()
        years = []
        for v in year_row:
            y = _to_float(str(v).split(")")[0].strip())
            years.append(int(y) if y and y > 1990 else None)

        for _, row in df_raw.iloc[2:].iterrows():
            sector = str(row.iloc[0]).strip()
            if not sector or sector in ("nan", "None") or sector.startswith("1)"):
                continue
            for col_idx, year in enumerate(years):
                if year is None:
                    continue
                v = _to_float(row.iloc[col_idx])
                if v is not None:
                    rows.append({
                        "period": str(year),
                        "indicator": f"avg_wage_{sector[:60]}",
                        "region": "RF",
                        "value": v,
                        "unit": "RUB",
                    })

    _insert(con, rows, "tab4-zpl_2025.xlsx")


# ── tab5-zpl: индексы реальной зарплаты ────────────────────────────────────

def parse_tab5_real_wage_index(con):
    """Real wage index (% to prev year) by sector"""
    path = STAT_DIR / "tab5-zpl_2025.xlsx"
    rows = []

    xl = pd.ExcelFile(path)
    # Use first two data sheets (skip notes sheet)
    for sheet in xl.sheet_names[:2]:
        try:
            df_raw = pd.read_excel(path, sheet_name=sheet, header=None)
        except Exception:
            continue

        # Find the row containing years (search first 6 rows)
        year_row_idx = 1
        for ri in range(6):
            candidate = df_raw.iloc[ri].tolist()
            year_count = sum(1 for v in candidate if _to_float(str(v).split(")")[0]) and _to_float(str(v).split(")")[0]) > 1990)
            if year_count >= 3:
                year_row_idx = ri
                break

        year_row = df_raw.iloc[year_row_idx].tolist()
        years = []
        for v in year_row:
            y = _to_float(str(v).split(")")[0].strip())
            years.append(int(y) if y and y > 1990 else None)

        for _, row in df_raw.iloc[year_row_idx + 1:].iterrows():
            sector = str(row.iloc[0]).strip()
            if not sector or sector in ("nan", "None") or sector.startswith("1)"):
                continue
            for col_idx, year in enumerate(years):
                if year is None:
                    continue
                v = _to_float(row.iloc[col_idx])
                if v is not None:
                    rows.append({
                        "period": str(year),
                        "indicator": f"real_wage_idx_{sector[:55]}",
                        "region": "RF",
                        "value": v,
                        "unit": "%_prev_year",
                    })

    _insert(con, rows, "tab5-zpl_2025.xlsx")


# ── demo32: урбанизация ─────────────────────────────────────────────────────

def parse_demo32_urbanization(con):
    """Urban population count and rate per 1000"""
    path = STAT_DIR / "demo32_2023.xlsx"
    df_raw = pd.read_excel(path, sheet_name=0, header=None)

    rows = []
    for _, row in df_raw.iloc[3:].iterrows():
        year = _to_float(row.iloc[0])
        count = _to_float(row.iloc[1])
        rate = _to_float(row.iloc[2])
        if year is None or year < 1900:
            continue
        if count is not None:
            rows.append({"period": str(int(year)), "indicator": "urban_pop_count",
                         "region": "RF", "value": count, "unit": "persons"})
        if rate is not None:
            rows.append({"period": str(int(year)), "indicator": "urban_pop_rate_per1000",
                         "region": "RF", "value": rate, "unit": "per_1000"})

    _insert(con, rows, "demo32_2023.xlsx")


# ── Jil_fond: жилищный фонд (региональные листы) ───────────────────────────

def parse_jil_fond(con):
    """Housing stock by sector/period from Jil_fond xls.
    Sheet name format: YYYY.N (e.g. 2025.1). Columns: total | urban | rural.
    Data starts at row 9 (0-indexed).
    """
    path = STAT_DIR / "Jil_fond_2019-2025.xls"
    xl = pd.ExcelFile(path)
    rows = []

    col_names = ["total", "urban", "rural"]

    for sheet in xl.sheet_names:
        period = str(sheet).strip()
        # Only process period sheets like "2025.1"
        if "." not in period:
            continue
        parts = period.split(".")
        if len(parts) != 2 or not parts[0].isdigit():
            continue

        try:
            df_raw = pd.read_excel(path, sheet_name=sheet, header=None)
            # Data rows start at row 9, cols 0(indicator), 1(total), 2(urban), 3(rural)
            for _, row in df_raw.iloc[9:].iterrows():
                indicator = str(row.iloc[0]).strip()
                if not indicator or indicator in ("nan", "None") or len(indicator) < 3:
                    continue
                for col_offset, col_name in enumerate(col_names):
                    v = _to_float(row.iloc[col_offset + 1])
                    if v is not None:
                        rows.append({
                            "period": period,
                            "indicator": f"housing_{col_name}_{indicator[:55]}",
                            "region": "RF",
                            "value": v,
                            "unit": "sqm_thousands",
                        })
        except Exception:
            continue

    _insert(con, rows, "Jil_fond_2019-2025.xls")


# ── main ───────────────────────────────────────────────────────────────────

def load_rosstat():
    con = get_connection()
    con.execute("DELETE FROM rosstat_macro")

    parse_tab1_wages(con)
    parse_tab4_wages_by_sector(con)
    parse_tab5_real_wage_index(con)
    parse_demo32_urbanization(con)
    parse_jil_fond(con)

    total = con.execute("SELECT COUNT(*) FROM rosstat_macro").fetchone()[0]
    con.close()
    print(f"\nRosstat total: {total} rows")


if __name__ == "__main__":
    load_rosstat()
