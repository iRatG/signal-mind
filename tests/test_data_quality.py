"""Data quality tests for all DuckDB tables."""
import pytest
from src.db.init_db import get_connection


@pytest.fixture(scope="module")
def con():
    c = get_connection()
    yield c
    c.close()


# ── moex_indices ────────────────────────────────────────────────

class TestMoexIndices:
    EXPECTED_TICKERS = [
        "IMOEX", "IMOEXW", "MDIAMR", "MOEX10", "MOEXBC",
        "MOEXFN", "MOEXOG", "MREDC", "RUCBTR3YNS", "RUCBTR5YNS",
        "RUGOLD", "RUPCI", "RUSFAR3M", "RUSFAR3MRT", "SUGAROTCVOL",
    ]

    def test_not_empty(self, con):
        count = con.execute("SELECT COUNT(*) FROM moex_indices").fetchone()[0]
        assert count > 0, "moex_indices is empty"

    def test_all_tickers_present(self, con):
        tickers = {r[0] for r in con.execute("SELECT DISTINCT ticker FROM moex_indices").fetchall()}
        for t in self.EXPECTED_TICKERS:
            assert t in tickers, f"Ticker {t} missing from moex_indices"

    def test_close_no_nulls(self, con):
        nulls = con.execute("SELECT COUNT(*) FROM moex_indices WHERE close IS NULL").fetchone()[0]
        assert nulls == 0, f"moex_indices.close has {nulls} nulls"

    def test_close_positive(self, con):
        # SUGAROTCVOL is a volume index — zero close is valid on low-liquidity days
        bad = con.execute(
            "SELECT COUNT(*) FROM moex_indices WHERE close < 0"
        ).fetchone()[0]
        assert bad == 0, f"moex_indices has {bad} rows with negative close"

    def test_no_duplicate_primary_key(self, con):
        dupes = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT ticker, trade_date, COUNT(*) c
                FROM moex_indices
                GROUP BY ticker, trade_date
                HAVING c > 1
            )
        """).fetchone()[0]
        assert dupes == 0, f"moex_indices has {dupes} duplicate (ticker, trade_date) pairs"

    def test_date_range_reasonable(self, con):
        min_d, max_d = con.execute(
            "SELECT MIN(trade_date), MAX(trade_date) FROM moex_indices"
        ).fetchone()
        assert str(min_d) >= "2010-01-01", f"Unexpectedly old data: {min_d}"
        assert str(max_d) >= "2020-01-01", f"Max date too old: {max_d}"


# ── key_rate ────────────────────────────────────────────────────

class TestKeyRate:
    def test_not_empty(self, con):
        count = con.execute("SELECT COUNT(*) FROM key_rate").fetchone()[0]
        assert count > 100, f"key_rate has only {count} rows, expected 100+"

    def test_rate_no_nulls(self, con):
        nulls = con.execute("SELECT COUNT(*) FROM key_rate WHERE rate_pct IS NULL").fetchone()[0]
        assert nulls == 0, f"key_rate.rate_pct has {nulls} nulls"

    def test_rate_in_range(self, con):
        bad = con.execute(
            "SELECT COUNT(*) FROM key_rate WHERE rate_pct < 0 OR rate_pct > 100"
        ).fetchone()[0]
        assert bad == 0, f"key_rate has {bad} rows with rate outside 0-100%"

    def test_inflation_no_nulls(self, con):
        nulls = con.execute("SELECT COUNT(*) FROM key_rate WHERE inflation IS NULL").fetchone()[0]
        assert nulls == 0, f"key_rate.inflation has {nulls} nulls"


# ── forex_cbr ───────────────────────────────────────────────────

class TestForexCbr:
    # Only USD RC file downloaded from CBR; EUR/CNY covered via market_data (Investing.com)
    EXPECTED_CURRENCIES = ["USD"]

    def test_not_empty(self, con):
        count = con.execute("SELECT COUNT(*) FROM forex_cbr").fetchone()[0]
        assert count > 500, f"forex_cbr has only {count} rows"

    def test_key_currencies_present(self, con):
        currencies = {r[0] for r in con.execute("SELECT DISTINCT currency FROM forex_cbr").fetchall()}
        for c in self.EXPECTED_CURRENCIES:
            assert c in currencies, f"Currency {c} missing from forex_cbr"

    def test_rate_no_nulls(self, con):
        nulls = con.execute("SELECT COUNT(*) FROM forex_cbr WHERE rate IS NULL").fetchone()[0]
        assert nulls == 0, f"forex_cbr.rate has {nulls} nulls"

    def test_rate_positive(self, con):
        bad = con.execute("SELECT COUNT(*) FROM forex_cbr WHERE rate <= 0").fetchone()[0]
        assert bad == 0, f"forex_cbr has {bad} rows with rate <= 0"

    def test_no_duplicate_primary_key(self, con):
        dupes = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT trade_date, currency, COUNT(*) c
                FROM forex_cbr
                GROUP BY trade_date, currency
                HAVING c > 1
            )
        """).fetchone()[0]
        assert dupes == 0, f"forex_cbr has {dupes} duplicate (date, currency) pairs"


# ── market_data ─────────────────────────────────────────────────

class TestMarketData:
    EXPECTED_INSTRUMENTS = ["USD_RUB", "EUR_RUB", "SP500", "GOLD", "BRENT", "DXY"]

    def test_not_empty(self, con):
        count = con.execute("SELECT COUNT(*) FROM market_data").fetchone()[0]
        assert count > 1000, f"market_data has only {count} rows"

    def test_all_instruments_present(self, con):
        instruments = {r[0] for r in con.execute("SELECT DISTINCT instrument FROM market_data").fetchall()}
        for inst in self.EXPECTED_INSTRUMENTS:
            assert inst in instruments, f"Instrument {inst} missing from market_data"

    def test_close_no_nulls(self, con):
        nulls = con.execute("SELECT COUNT(*) FROM market_data WHERE close IS NULL").fetchone()[0]
        assert nulls == 0, f"market_data.close has {nulls} nulls"

    def test_close_positive(self, con):
        bad = con.execute("SELECT COUNT(*) FROM market_data WHERE close <= 0").fetchone()[0]
        assert bad == 0, f"market_data has {bad} rows with close <= 0"

    def test_no_duplicate_primary_key(self, con):
        dupes = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT trade_date, instrument, COUNT(*) c
                FROM market_data
                GROUP BY trade_date, instrument
                HAVING c > 1
            )
        """).fetchone()[0]
        assert dupes == 0, f"market_data has {dupes} duplicate (date, instrument) pairs"

    def test_each_instrument_min_rows(self, con):
        rows = con.execute("""
            SELECT instrument, COUNT(*) c FROM market_data GROUP BY instrument
        """).fetchall()
        for inst, cnt in rows:
            assert cnt >= 100, f"market_data[{inst}] has only {cnt} rows"
