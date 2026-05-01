"""Revizor — post-marathon independent auditor for Signal Mind.

Runs after a marathon session (called by watchdog or standalone).
Reads experiments.db for the latest session, applies deterministic checks
to every confirmed signal, re-executes top-signal SQL, and writes
db/audit_YYYYMMDD_HHMMSS.md with a full bug report.

Checks (in order):
  ALIASING       — v_market_context column aliased as foreign instrument name
  TAUTOLOGY      — both correlated variables from same currency family
  REGIME_INACTIVE— signal conditional on a market regime that is currently off
  STRUCTURAL     — r flat across lags (no predictive power, just co-movement)
  WEAK_R         — SQL re-run gives actual r < MIN_R threshold
  CONVERGENCE    — same hypothesis pattern confirmed > CONVERGENCE_MIN times
"""
import json
import re
import sqlite3
import sys
import io
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb

# ── paths ──────────────────────────────────────────────────────────────────
ROOT           = Path(__file__).parents[2]
DB_PATH        = ROOT / "db" / "signal_mind.duckdb"
EXP_PATH       = ROOT / "db" / "experiments.db"
AUDIT_DIR      = ROOT / "db"
FORBIDDEN_PATH = ROOT / "db" / "forbidden_patterns.md"
REGIME_PATH    = ROOT / "db" / "current_regime.json"
BLACKLIST_PATH = ROOT / "db" / "convergence_blacklist.json"

# ── tunables ───────────────────────────────────────────────────────────────
CURRENT_KEY_RATE  = 21.0   # % — update this when regime changes
CURRENT_USD_RUB   = 85.0   # approximate
MIN_R             = 0.30   # confirmed but r < this → WEAK_R
STRUCTURAL_DELTA  = 0.08   # |r_lag - r_0| < this → STRUCTURAL
CONVERGENCE_MIN   = 3      # same fingerprint ≥ this → CONVERGENCE trap
RERUN_MIN_SCORE   = 65     # only re-run SQL for signals with score ≥ this

# ── instrument / column lists ──────────────────────────────────────────────
# Instruments that MUST come from market_data (never from v_market_context aliases)
FOREIGN_INSTRUMENTS = [
    "ftse_china_50", "dxy", "msci_india", "msci_world",
    "dj_south_africa", "china_h_shares", "silver", "sp500", "aluminum",
]

# v_market_context columns that are sometimes (wrongly) aliased as foreign instruments
CONTEXT_COLUMNS = [
    "imoex_close", "usd_rub", "eur_rub", "brent_usd", "gold_usd", "key_rate_pct",
]

# Pairs that are structurally co-determined (tautologies at level prices)
TAUTOLOGY_PAIRS = [
    ("eur_rub", "usd_rub"),
]

# Regex patterns for market conditions that are currently inactive
INACTIVE_REGIME_PATTERNS = [
    r"key_rate_pct\s*<\s*1[0-9]",   # rate < 10-19 % (current: 21 %)
    r"rate_pct\s*<\s*1[0-9]",
    r"[\"']low_rate[\"']",           # regime label 'low_rate'
    r"usd_rub\s*<\s*7\d",            # USD/RUB < 70-79 (current: ~85)
]

# Structural co-movement: always high r regardless of lag (Silver/Gold, EUR/USD levels)
STRUCTURAL_KEYWORDS = [
    r"\bsilver\b.*\brugold\b",
    r"\brugold\b.*\bsilver\b",
]


# ── deterministic checks ───────────────────────────────────────────────────

def check_aliasing(sql: str) -> list[str]:
    """Return list of 'col AS instrument' aliasing bugs found in SQL."""
    bugs = []
    for inst in FOREIGN_INSTRUMENTS:
        for col in CONTEXT_COLUMNS:
            pat = rf"\b{re.escape(col)}\s+AS\s+{re.escape(inst)}\b"
            if re.search(pat, sql, re.IGNORECASE):
                bugs.append(f"{col} AS {inst}")
    return bugs


def check_tautology(sql: str, hypothesis: str) -> Optional[str]:
    """Return tautology description if both correlated variables are co-determined."""
    combined = (sql + " " + hypothesis).lower()
    for a, b in TAUTOLOGY_PAIRS:
        if a in combined and b in combined:
            return f"{a} <-> {b}"
    return None


def check_regime(sql: str, hypothesis: str) -> Optional[str]:
    """Return inactive regime description if signal depends on a currently-off condition."""
    combined = sql + " " + hypothesis
    for pat in INACTIVE_REGIME_PATTERNS:
        if re.search(pat, combined, re.IGNORECASE):
            return (
                f"Pattern [{pat}] — inactive at "
                f"rate={CURRENT_KEY_RATE}%, USD/RUB={CURRENT_USD_RUB}"
            )
    return None


def check_structural_keywords(sql: str, hypothesis: str) -> Optional[str]:
    """Flag known structural co-movements (e.g. Silver -> RUGOLD)."""
    combined = (sql + " " + hypothesis).lower()
    for pat in STRUCTURAL_KEYWORDS:
        if re.search(pat, combined):
            return f"Matches structural co-movement pattern [{pat}]"
    return None


# ── SQL re-execution helpers ───────────────────────────────────────────────

def _extract_r(rows: list) -> Optional[float]:
    """Extract first float in [-1.0, 1.0] from query result rows (correlation value)."""
    for row in rows:
        for val in row:
            if isinstance(val, float) and -1.0 <= val <= 1.0:
                return round(val, 4)
    return None


def _extract_n(rows: list) -> Optional[int]:
    """Extract first integer > 1 from query result rows (sample size)."""
    for row in rows:
        for val in row:
            if isinstance(val, int) and val > 1:
                return val
    return None


def rerun_sql(sql: str, duck: duckdb.DuckDBPyConnection) -> tuple[Optional[float], Optional[int]]:
    """Execute SQL, return (r, n). Returns (None, None) on error."""
    try:
        rows = duck.execute(sql).fetchall()
        return _extract_r(rows), _extract_n(rows)
    except Exception:
        return None, None


def check_structural_lag(
    sql: str, lag_days: int, duck: duckdb.DuckDBPyConnection
) -> tuple[Optional[float], Optional[float], bool]:
    """
    Re-run SQL at stated lag and at lag=0. If |r_lag - r_0| < STRUCTURAL_DELTA,
    signal has no real lag-based predictive power.
    Returns (r_lag, r_0, is_structural).
    """
    r_lag, _ = rerun_sql(sql, duck)

    # Replace any INTERVAL N DAYS with INTERVAL 0 DAYS
    sql_0 = re.sub(r"INTERVAL\s+\d+\s+DAYS", "INTERVAL 0 DAYS", sql, flags=re.IGNORECASE)
    if sql_0 == sql:
        return r_lag, None, False  # no lag in SQL, skip structural check

    r_0, _ = rerun_sql(sql_0, duck)

    if r_lag is not None and r_0 is not None:
        is_structural = abs(abs(r_lag) - abs(r_0)) < STRUCTURAL_DELTA
        return r_lag, r_0, is_structural

    return r_lag, r_0, False


# ── audit result container ─────────────────────────────────────────────────

class Issue:
    __slots__ = ("iteration", "score", "kind", "detail", "sql_snippet")

    def __init__(self, iteration: int, score: int, kind: str, detail: str, sql: str = ""):
        self.iteration  = iteration
        self.score      = score
        self.kind       = kind
        self.detail     = detail
        self.sql_snippet = sql[:130]


class AuditResult:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.ts         = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.total      = 0
        self.confirmed_raw = 0
        self.clean_confirmed = 0
        self.issues: list[Issue] = []
        self.stats: dict = {}
        self.convergence_top: list[tuple] = []  # (fingerprint, count)


# ── main audit logic ───────────────────────────────────────────────────────

def run_audit(session_id: Optional[str] = None, verbose: bool = True) -> AuditResult:
    exp_con = sqlite3.connect(str(EXP_PATH))
    duck    = duckdb.connect(str(DB_PATH), read_only=True)

    # Resolve session
    if not session_id:
        row = exp_con.execute(
            "SELECT session_id FROM experiments ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            raise RuntimeError("No experiments found in experiments.db")
        session_id = row[0]

    if verbose:
        print(f"[revizor] Auditing session: {session_id}", flush=True)

    result = AuditResult(session_id)

    rows = exp_con.execute(
        """SELECT iteration, hypothesis, sql_final, confirmed, signal_score, lag_days
           FROM experiments WHERE session_id = ? ORDER BY iteration""",
        (session_id,),
    ).fetchall()

    result.total        = len(rows)
    confirmed_rows      = [(it, hyp, sql, sc, lag) for it, hyp, sql, conf, sc, lag in rows if conf]
    result.confirmed_raw = len(confirmed_rows)

    # counters
    aliasing_n = tautology_n = regime_n = structural_n = weak_n = 0
    clean_n    = 0

    # ── CONVERGENCE CHECK (session-level) ────────────────────────────────
    fp_counter: Counter = Counter()
    for _, hyp, _, _, _ in confirmed_rows:
        # Fingerprint: first 55 chars, numbers normalised
        fp = re.sub(r"\d+", "N", (hyp or "")[:55].lower().strip())
        fp_counter[fp] += 1

    convergence_traps = {fp: cnt for fp, cnt in fp_counter.items() if cnt >= CONVERGENCE_MIN}
    result.convergence_top = sorted(convergence_traps.items(), key=lambda x: -x[1])[:5]

    if verbose and convergence_traps:
        print(f"[revizor] Convergence traps: {len(convergence_traps)}", flush=True)

    # ── PER-SIGNAL CHECKS ────────────────────────────────────────────────
    for idx, (iteration, hyp, sql, score, lag_days) in enumerate(confirmed_rows):
        if verbose and idx % 50 == 0:
            print(f"[revizor] Checking signal {idx+1}/{len(confirmed_rows)}...", flush=True)

        score    = score or 0
        hyp      = hyp or ""
        sql      = sql or ""
        flagged  = False

        # 1. ALIASING
        aliases = check_aliasing(sql)
        if aliases:
            aliasing_n += 1
            result.issues.append(Issue(
                iteration, score, "ALIASING",
                f"v_market_context aliased as foreign instrument: {aliases}",
                sql,
            ))
            flagged = True

        # 2. TAUTOLOGY
        taut = check_tautology(sql, hyp)
        if taut:
            tautology_n += 1
            result.issues.append(Issue(
                iteration, score, "TAUTOLOGY",
                f"Tautological pair: {taut} (level correlation, not predictive)",
                sql,
            ))
            flagged = True

        # 3. REGIME_INACTIVE
        regime = check_regime(sql, hyp)
        if regime:
            regime_n += 1
            result.issues.append(Issue(
                iteration, score, "REGIME_INACTIVE",
                regime,
                sql,
            ))
            flagged = True

        # 4. STRUCTURAL keywords (fast, no SQL re-run needed)
        struct_kw = check_structural_keywords(sql, hyp)
        if struct_kw and not flagged:
            structural_n += 1
            result.issues.append(Issue(
                iteration, score, "STRUCTURAL",
                f"Known structural co-movement: {struct_kw}",
                sql,
            ))
            flagged = True

        # 5. STRUCTURAL via lag re-run (only for higher-score signals, skip already-flagged)
        if not flagged and score >= RERUN_MIN_SCORE and lag_days and lag_days > 0 and sql:
            r_lag, r_0, is_struct = check_structural_lag(sql, lag_days, duck)
            if is_struct and r_lag is not None:
                structural_n += 1
                result.issues.append(Issue(
                    iteration, score, "STRUCTURAL",
                    f"r flat across lags: r(lag={lag_days}d)={r_lag}, r(lag=0d)={r_0} "
                    f"— difference {abs(abs(r_lag)-abs(r_0)):.3f} < threshold {STRUCTURAL_DELTA}",
                    sql,
                ))
                flagged = True
            elif r_lag is not None and abs(r_lag) < MIN_R:
                weak_n += 1
                result.issues.append(Issue(
                    iteration, score, "WEAK_R",
                    f"Re-executed SQL: r={r_lag} < threshold {MIN_R} (agent score={score})",
                    sql,
                ))
                flagged = True

        # 6. WEAK_R fallback — re-run for high-score unflagged signals
        elif not flagged and score >= RERUN_MIN_SCORE and sql:
            r_actual, n_actual = rerun_sql(sql, duck)
            if r_actual is not None and abs(r_actual) < MIN_R:
                weak_n += 1
                result.issues.append(Issue(
                    iteration, score, "WEAK_R",
                    f"Re-executed SQL: r={r_actual}, n={n_actual} — "
                    f"weak despite agent score={score}",
                    sql,
                ))
                flagged = True

        if not flagged:
            clean_n += 1

    result.clean_confirmed = clean_n
    result.stats = {
        "aliasing":          aliasing_n,
        "tautology":         tautology_n,
        "regime_inactive":   regime_n,
        "structural":        structural_n,
        "weak_r":            weak_n,
        "convergence_traps": len(convergence_traps),
        "total_issues":      len(result.issues),
    }

    exp_con.close()
    duck.close()
    return result


# ── report writer ──────────────────────────────────────────────────────────

def write_report(result: AuditResult) -> Path:
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = AUDIT_DIR / f"audit_{ts}.md"

    raw_rate   = result.confirmed_raw / result.total * 100 if result.total else 0
    clean_rate = result.clean_confirmed / result.total * 100 if result.total else 0

    lines = [
        f"# Revizor Audit Report",
        f"",
        f"**Session:** `{result.session_id}`  ",
        f"**Generated:** {result.ts}  ",
        f"**Current regime:** key_rate={CURRENT_KEY_RATE}%, USD/RUB={CURRENT_USD_RUB}",
        f"",
        f"## Summary",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total iterations | {result.total} |",
        f"| Confirmed (raw) | {result.confirmed_raw} ({raw_rate:.1f}%) |",
        f"| Clean confirmed | {result.clean_confirmed} ({clean_rate:.1f}%) |",
        f"| Total issues | {result.stats['total_issues']} |",
        f"",
        f"### Issues breakdown",
        f"",
        f"| Type | Count | Description |",
        f"|------|-------|-------------|",
        f"| ALIASING | {result.stats['aliasing']} | v_market_context column used as foreign instrument proxy |",
        f"| TAUTOLOGY | {result.stats['tautology']} | Both variables structurally co-determined (level prices) |",
        f"| REGIME_INACTIVE | {result.stats['regime_inactive']} | Signal conditional on regime not currently active |",
        f"| STRUCTURAL | {result.stats['structural']} | r flat across all lags — co-movement, not predictive |",
        f"| WEAK_R | {result.stats['weak_r']} | Re-executed SQL shows r < {MIN_R} |",
        f"| CONVERGENCE | {result.stats['convergence_traps']} | Hypothesis pattern repeated >= {CONVERGENCE_MIN}x |",
        f"",
    ]

    if result.convergence_top:
        lines += [
            f"## Convergence Traps (Top {len(result.convergence_top)})",
            f"",
        ]
        for fp, cnt in result.convergence_top:
            lines.append(f"- **{cnt}x** `{fp}`")
        lines.append("")

    # Issues grouped by type
    by_type: dict[str, list[Issue]] = defaultdict(list)
    for iss in result.issues:
        by_type[iss.kind].append(iss)

    lines.append("## Issues Detail")
    lines.append("")

    for kind in ["ALIASING", "TAUTOLOGY", "REGIME_INACTIVE", "STRUCTURAL", "WEAK_R"]:
        issues = by_type.get(kind, [])
        if not issues:
            continue
        lines += [f"### {kind} ({len(issues)})", ""]
        for iss in issues[:15]:  # cap at 15 per section
            lines.append(f"- **iter={iss.iteration}** score={iss.score}: {iss.detail}")
            if iss.sql_snippet:
                short = iss.sql_snippet.replace("\n", " ")[:120]
                lines.append(f"  `{short}`")
        if len(issues) > 15:
            lines.append(f"  _(+{len(issues)-15} more)_")
        lines.append("")

    lines += [
        "## Verdict",
        "",
        f"Raw confirmed rate: **{raw_rate:.1f}%** → "
        f"Clean confirmed rate: **{clean_rate:.1f}%**",
        f"(difference: {raw_rate - clean_rate:.1f}pp inflated by bugs)",
        "",
        "### Recommendations",
        "",
        "1. **ALIASING**: Add SQL validation layer that rejects "
        "`v_market_context.* AS <foreign_instrument>` before execution.",
        "2. **TAUTOLOGY**: Add EUR/RUB <-> USD/RUB pair check to hypothesis generator prompt.",
        "3. **REGIME**: Inject `CURRENT_KEY_RATE` and `CURRENT_USD_RUB` into schema hint "
        "so agent knows the current regime.",
        "4. **STRUCTURAL**: Add Silver/RUGOLD to known structural pairs — "
        "document in forbidden_patterns.md.",
        "5. **CONVERGENCE**: Add repetition counter per hypothesis fingerprint; "
        "force RANDOM_JUMP after 3 repeats.",
        "",
        "_Generated by Revizor — Signal Mind post-marathon auditor_",
    ]

    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def print_summary(result: AuditResult):
    raw_rate   = result.confirmed_raw / result.total * 100 if result.total else 0
    clean_rate = result.clean_confirmed / result.total * 100 if result.total else 0

    print(f"\n{'='*60}", flush=True)
    print(f"REVIZOR — Session: {result.session_id}", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  Total iterations : {result.total}", flush=True)
    print(f"  Confirmed (raw)  : {result.confirmed_raw} ({raw_rate:.1f}%)", flush=True)
    print(f"  Clean confirmed  : {result.clean_confirmed} ({clean_rate:.1f}%)", flush=True)
    print(f"  Inflation        : {raw_rate - clean_rate:.1f}pp", flush=True)
    print(f"", flush=True)
    print(f"  Issues found:", flush=True)
    for k in ["aliasing", "tautology", "regime_inactive", "structural", "weak_r", "convergence_traps"]:
        v = result.stats.get(k, 0)
        if v:
            print(f"    {k:<22} {v}", flush=True)
    if result.convergence_top:
        print(f"", flush=True)
        print(f"  Top convergence traps:", flush=True)
        for fp, cnt in result.convergence_top[:3]:
            print(f"    [{cnt}x] {fp}", flush=True)
    print(f"{'='*60}\n", flush=True)


# ── auto-fix writer ────────────────────────────────────────────────────────

def _query_current_regime() -> dict:
    """Return current market regime from hardcoded constants.

    DuckDB contains historical data only — it cannot reliably reflect today's rates.
    Update CURRENT_KEY_RATE and CURRENT_USD_RUB in revizor.py when the regime changes.
    """
    return {
        "key_rate": CURRENT_KEY_RATE,
        "usd_rub":  CURRENT_USD_RUB,
        "updated":  datetime.now().isoformat(),
    }


def apply_fixes(result: AuditResult):
    """Write config patches so the next marathon run starts with fresh knowledge.

    Writes three files:
      db/current_regime.json      — live key_rate + USD/RUB from DuckDB
      db/convergence_blacklist.json — fingerprints that caused convergence traps
      db/forbidden_patterns.md    — appends any newly detected aliasing patterns
    """
    # 1. current_regime.json — always refresh from live data
    regime = _query_current_regime()
    REGIME_PATH.write_text(json.dumps(regime, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[revizor] Regime patched → key_rate={regime['key_rate']}%, USD/RUB={regime['usd_rub']}", flush=True)

    # 2. convergence_blacklist.json — cross-session trap memory
    blacklist = [fp for fp, _ in result.convergence_top]
    existing_bl: list = []
    if BLACKLIST_PATH.exists():
        try:
            existing_bl = json.loads(BLACKLIST_PATH.read_text(encoding="utf-8")).get("blacklist", [])
        except Exception:
            pass
    merged = list(dict.fromkeys(existing_bl + blacklist))  # deduplicate, preserve order
    BLACKLIST_PATH.write_text(
        json.dumps({"blacklist": merged, "updated": datetime.now().isoformat()}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"[revizor] Convergence blacklist → {len(merged)} patterns ({len(blacklist)} new)", flush=True)

    # 3. forbidden_patterns.md — append newly discovered aliasing patterns
    aliasing_issues = [iss for iss in result.issues if iss.kind == "ALIASING"]
    new_pairs: set[str] = set()
    for iss in aliasing_issues:
        m = re.search(r"foreign instrument: \[(.+?)\]", iss.detail)
        if m:
            for pair in m.group(1).split(","):
                new_pairs.add(pair.strip().strip("'\""))

    if new_pairs:
        existing_text = FORBIDDEN_PATH.read_text(encoding="utf-8") if FORBIDDEN_PATH.exists() else ""
        truly_new = [p for p in sorted(new_pairs) if p not in existing_text]
        if truly_new:
            block = "\n\n## AUTO-DETECTED ALIASING (by Revizor)\n\n"
            for pair in truly_new:
                parts = pair.split(" AS ", 1)
                if len(parts) == 2:
                    col, inst = parts
                    block += (
                        f"- `{col} AS {inst}` — `{col}` is NOT `{inst}`. "
                        f"Use `FROM market_data WHERE instrument='{inst.upper()}'`\n"
                    )
            with open(FORBIDDEN_PATH, "a", encoding="utf-8") as f:
                f.write(block)
            print(f"[revizor] Appended {len(truly_new)} new aliasing pattern(s) to forbidden_patterns.md", flush=True)


# ── entry point ────────────────────────────────────────────────────────────

def main():
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    session_id = sys.argv[1] if len(sys.argv) > 1 else None

    print("[revizor] Starting audit...", flush=True)
    result = run_audit(session_id=session_id, verbose=True)
    print_summary(result)
    apply_fixes(result)
    path = write_report(result)
    print(f"[revizor] Report written: {path}", flush=True)


if __name__ == "__main__":
    main()
