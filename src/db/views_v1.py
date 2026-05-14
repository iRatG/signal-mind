"""Experiment v1 split views — chronological walk-forward windows.

Creates 9 read-only views in signal_mind.duckdb that pre-filter v_market_context,
v_moex_sectors, and news_daily by the three experiment windows. The agent (in
DISCOVERY mode) and the validation/test scripts query these instead of touching
raw tables — this is the single point that enforces "you cannot see data outside
your window".

Run once after design freeze:
    .venv/Scripts/python -m src.db.views_v1

Also emits db/split_manifest_v1.json with the frozen window definitions for
downstream scripts to read.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).parents[2]
DUCKDB_PATH = ROOT / "db" / "signal_mind.duckdb"
MANIFEST_PATH = ROOT / "db" / "split_manifest_v1.json"

WINDOWS = {
    "train": ("2022-01-01", "2023-09-30"),
    "val":   ("2024-01-01", "2025-04-30"),
    "test":  ("2025-09-01", "2026-04-29"),
}
BUFFERS = {
    "train_val": ("2023-10-01", "2023-12-31"),
    "val_test":  ("2025-05-01", "2025-08-31"),
}
DISQUALIFIED_TOPICS = {
    "train": ["ruble"],
    "val":   ["ruble", "sanctions"],
    "test":  [],
}
FROZEN_CONFIG = {
    "confirm_threshold": {
        "abs_r_min": 0.4,
        "n_min": 150,
        "p_max": 0.01,
    },
    "test_pass": {
        "r_decay_min": 0.4,
        "fdr_q_max": 0.1,
        "n_min": 50,
        "regime_active_pct_min": 30,
    },
}


def create_split_views(con: duckdb.DuckDBPyConnection) -> None:
    for name, (start, end) in WINDOWS.items():
        con.execute(f"DROP VIEW IF EXISTS v_{name}_ctx")
        con.execute(f"""
            CREATE VIEW v_{name}_ctx AS
            SELECT * FROM v_market_context
            WHERE trade_date BETWEEN DATE '{start}' AND DATE '{end}'
        """)

        con.execute(f"DROP VIEW IF EXISTS v_{name}_sectors")
        con.execute(f"""
            CREATE VIEW v_{name}_sectors AS
            SELECT * FROM v_moex_sectors
            WHERE trade_date BETWEEN DATE '{start}' AND DATE '{end}'
        """)

        con.execute(f"DROP VIEW IF EXISTS v_{name}_news")
        con.execute(f"""
            CREATE VIEW v_{name}_news AS
            SELECT * FROM news_daily
            WHERE news_date BETWEEN DATE '{start}' AND DATE '{end}'
        """)

        con.execute(f"DROP VIEW IF EXISTS v_{name}_market_data")
        con.execute(f"""
            CREATE VIEW v_{name}_market_data AS
            SELECT * FROM market_data
            WHERE trade_date BETWEEN DATE '{start}' AND DATE '{end}'
        """)

        con.execute(f"DROP VIEW IF EXISTS v_{name}_moex_indices")
        con.execute(f"""
            CREATE VIEW v_{name}_moex_indices AS
            SELECT * FROM moex_indices
            WHERE trade_date BETWEEN DATE '{start}' AND DATE '{end}'
        """)

        con.execute(f"DROP VIEW IF EXISTS v_{name}_forex_cbr")
        con.execute(f"""
            CREATE VIEW v_{name}_forex_cbr AS
            SELECT * FROM forex_cbr
            WHERE trade_date BETWEEN DATE '{start}' AND DATE '{end}'
        """)


def verify_views(con: duckdb.DuckDBPyConnection) -> dict:
    out: dict = {}
    for name, (start, end) in WINDOWS.items():
        ctx_rows = con.execute(f"SELECT COUNT(*) FROM v_{name}_ctx").fetchone()[0]
        sect_rows = con.execute(f"SELECT COUNT(*) FROM v_{name}_sectors").fetchone()[0]
        news_rows = con.execute(f"SELECT COUNT(*) FROM v_{name}_news").fetchone()[0]
        md_rows = con.execute(f"SELECT COUNT(*) FROM v_{name}_market_data").fetchone()[0]
        mi_rows = con.execute(f"SELECT COUNT(*) FROM v_{name}_moex_indices").fetchone()[0]
        fx_rows = con.execute(f"SELECT COUNT(*) FROM v_{name}_forex_cbr").fetchone()[0]
        ctx_dates = con.execute(
            f"SELECT MIN(trade_date), MAX(trade_date) FROM v_{name}_ctx"
        ).fetchone()
        out[name] = {
            "window_start": start,
            "window_end": end,
            "v_ctx_rows": ctx_rows,
            "v_ctx_min": str(ctx_dates[0]) if ctx_dates[0] else None,
            "v_ctx_max": str(ctx_dates[1]) if ctx_dates[1] else None,
            "v_sectors_rows": sect_rows,
            "v_news_rows": news_rows,
            "v_market_data_rows": md_rows,
            "v_moex_indices_rows": mi_rows,
            "v_forex_cbr_rows": fx_rows,
        }
    return out


def write_manifest(verification: dict, commit_hash: str | None = None) -> None:
    manifest = {
        "experiment": "v1",
        "frozen_at_commit": commit_hash,
        "windows": {
            name: {
                "start": s,
                "end": e,
                **verification[name],
            }
            for name, (s, e) in WINDOWS.items()
        },
        "buffer_zones": {
            name: {"start": s, "end": e}
            for name, (s, e) in BUFFERS.items()
        },
        "disqualified_topics": DISQUALIFIED_TOPICS,
        "frozen_config": FROZEN_CONFIG,
        "views_created": [
            f"v_{name}_{kind}"
            for name in WINDOWS
            for kind in ("ctx", "sectors", "news", "market_data", "moex_indices", "forex_cbr")
        ],
    }
    MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def main() -> None:
    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        create_split_views(con)
        verification = verify_views(con)
    finally:
        con.close()

    # Read commit hash from git if available
    commit_hash = None
    try:
        import subprocess
        commit_hash = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        pass

    write_manifest(verification, commit_hash)

    print("Split views created:")
    for name, info in verification.items():
        print(
            f"  v_{name}_ctx     : {info['v_ctx_rows']:>4} rows  "
            f"({info['v_ctx_min']} -> {info['v_ctx_max']})"
        )
        print(f"  v_{name}_sectors : {info['v_sectors_rows']:>4} rows")
        print(f"  v_{name}_news    : {info['v_news_rows']:>4} rows")
    print(f"\nManifest written: {MANIFEST_PATH}")
    if commit_hash:
        print(f"Frozen at commit: {commit_hash}")


if __name__ == "__main__":
    main()
