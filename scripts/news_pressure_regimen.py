"""Run News Pressure Radar on repeatable date windows.

This script is the cron-ready entrypoint for the current spike. It keeps the
collector and clustering modules separate, but gives operations a single command
for daily, weekly, monthly, and 90-day historical reports.

Examples:
    python scripts/news_pressure_regimen.py --mode daily --as-of 2026-07-22
    python scripts/news_pressure_regimen.py --mode history --as-of 2026-07-22
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCES = "kommersant,interfax,lenta,vedomosti,ria"
DEFAULT_DB_PATH = ROOT / "data" / "news_pressure" / "news_pressure.db"
DEFAULT_STATUS_DIR = ROOT / "data" / "news_pressure" / "run_status"
SNAPSHOT_RUN_RE = re.compile(r"SQLite: .*\(run_id=([^,\s)]+), inserted=(\d+)\)")
TOPIC_RUN_RE = re.compile(r"Topic run: ([^\s(]+)")
ARTICLES_RE = re.compile(r"Articles: (\d+)")
CLUSTERS_RE = re.compile(r"Clusters: (\d+)")


@dataclass(frozen=True)
class Window:
    start: date
    end: date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run News Pressure Radar collection and reports")
    parser.add_argument(
        "--mode",
        choices=["daily", "weekly", "monthly", "history"],
        required=True,
        help="Date window to run. history means trailing 90 days.",
    )
    parser.add_argument(
        "--as-of",
        default=datetime.now(UTC).date().isoformat(),
        help="Anchor date YYYY-MM-DD. Default: today in UTC.",
    )
    parser.add_argument("--sources", default=DEFAULT_SOURCES)
    parser.add_argument("--label-mode", choices=["heuristic", "deepseek"], default="deepseek")
    parser.add_argument("--cluster-limit", type=int, default=20)
    parser.add_argument("--min-cluster-size", type=int, default=None)
    parser.add_argument("--min-shared-tokens", type=int, default=None)
    parser.add_argument("--jaccard-threshold", type=float, default=None)
    parser.add_argument("--max-token-df", type=int, default=None)
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite database path")
    parser.add_argument("--status-dir", default=str(DEFAULT_STATUS_DIR), help="Directory for JSON run status files")
    parser.add_argument(
        "--max-quality-flags",
        type=int,
        default=0,
        help="Maximum allowed cluster quality flags before automatic delivery is blocked.",
    )
    parser.add_argument("--skip-collect", action="store_true", help="Only rebuild cluster reports from existing SQLite")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing them")
    return parser.parse_args()


def window_for(mode: str, as_of: date) -> Window:
    # Reports use completed days. If run in the morning, yesterday is the last
    # complete day and today is still forming.
    end = as_of - timedelta(days=1)
    if mode == "daily":
        start = end
    elif mode == "weekly":
        start = end - timedelta(days=6)
    elif mode == "monthly":
        start = end - timedelta(days=29)
    elif mode == "history":
        start = end - timedelta(days=89)
    else:
        raise ValueError(f"Unsupported mode: {mode}")
    return Window(start=start, end=end)


def defaults_for(mode: str, args: argparse.Namespace) -> tuple[int, int, float, int]:
    min_cluster_size = args.min_cluster_size
    min_shared_tokens = args.min_shared_tokens
    jaccard_threshold = args.jaccard_threshold
    max_token_df = args.max_token_df
    if min_cluster_size is None:
        min_cluster_size = 2 if mode == "daily" else 5 if mode == "weekly" else 8
    if min_shared_tokens is None:
        min_shared_tokens = 2 if mode == "daily" else 3
    if jaccard_threshold is None:
        jaccard_threshold = 0.22 if mode == "daily" else 0.30 if mode == "weekly" else 0.34
    if max_token_df is None:
        max_token_df = 80 if mode == "daily" else 120 if mode == "weekly" else 180
    return min_cluster_size, min_shared_tokens, jaccard_threshold, max_token_df


def run_command(cmd: list[str], dry_run: bool) -> subprocess.CompletedProcess[str] | None:
    print("+ " + " ".join(cmd))
    if dry_run:
        return None
    result = subprocess.run(cmd, cwd=ROOT, check=True, text=True, capture_output=True)
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="" if result.stderr.endswith("\n") else "\n")
    return result


def ensure_regimen_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS regimen_runs (
            run_id TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            period_from TEXT NOT NULL,
            period_to TEXT NOT NULL,
            as_of TEXT NOT NULL,
            status TEXT NOT NULL,
            send_allowed INTEGER NOT NULL DEFAULT 0,
            block_reason TEXT NOT NULL DEFAULT '',
            snapshot_run_id TEXT,
            topic_run_id TEXT,
            fetched_count INTEGER NOT NULL DEFAULT 0,
            inserted_count INTEGER NOT NULL DEFAULT 0,
            source_row_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            article_count INTEGER NOT NULL DEFAULT 0,
            cluster_count INTEGER NOT NULL DEFAULT 0,
            quality_flag_count INTEGER NOT NULL DEFAULT 0,
            report_path TEXT,
            status_json_path TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            params_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_regimen_runs_mode_finished ON regimen_runs(mode, finished_at DESC)")
    conn.commit()


def parse_snapshot_run(stdout: str) -> str | None:
    match = SNAPSHOT_RUN_RE.search(stdout)
    return match.group(1) if match else None


def parse_topic_run(stdout: str) -> str | None:
    matches = TOPIC_RUN_RE.findall(stdout)
    return matches[-1] if matches else None


def parse_int(pattern: re.Pattern[str], stdout: str) -> int | None:
    match = pattern.search(stdout)
    return int(match.group(1)) if match else None


def snapshot_summary(conn: sqlite3.Connection, run_id: str | None) -> dict[str, object]:
    if not run_id:
        return {"snapshot_run_id": None, "fetched_count": 0, "inserted_count": 0, "error_count": 0}
    row = conn.execute(
        """
        SELECT run_id, fetched_count, inserted_count, error_count
        FROM snapshot_runs
        WHERE run_id=?
        """,
        (run_id,),
    ).fetchone()
    if not row:
        return {"snapshot_run_id": run_id, "fetched_count": 0, "inserted_count": 0, "error_count": 0}
    return {
        "snapshot_run_id": row[0],
        "fetched_count": int(row[1]),
        "inserted_count": int(row[2]),
        "error_count": int(row[3]),
    }


def source_counts(conn: sqlite3.Connection, start: date, end: date) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT source, COUNT(*)
        FROM headline_snapshots
        WHERE published_date BETWEEN ? AND ?
        GROUP BY source
        ORDER BY source
        """,
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    return {str(source): int(count) for source, count in rows}


def topic_summary(conn: sqlite3.Connection, topic_run_id: str | None) -> dict[str, object]:
    if not topic_run_id:
        return {"topic_run_id": None, "article_count": 0, "cluster_count": 0, "quality_flag_count": 0}
    row = conn.execute(
        "SELECT params_json FROM topic_runs WHERE run_id=?",
        (topic_run_id,),
    ).fetchone()
    params = json.loads(row[0]) if row else {}
    topic_rows = conn.execute(
        "SELECT summary FROM topics WHERE run_id=?",
        (topic_run_id,),
    ).fetchall()
    quality_flag_count = 0
    for (summary_json,) in topic_rows:
        try:
            summary = json.loads(summary_json or "{}")
        except json.JSONDecodeError:
            continue
        flags = summary.get("quality_flags")
        if flags:
            quality_flag_count += len([flag for flag in str(flags).split(",") if flag.strip()])
    return {
        "topic_run_id": topic_run_id,
        "article_count": int(params.get("article_count", 0)),
        "cluster_count": len(topic_rows),
        "quality_flag_count": quality_flag_count,
        "topic_params": params,
    }


def delivery_decision(status: dict[str, object], max_quality_flags: int) -> tuple[bool, str]:
    if status["status"] != "ok":
        return False, "run_failed"
    if int(status.get("error_count", 0)) > 0:
        return False, "collector_errors"
    if int(status.get("article_count", 0)) <= 0:
        return False, "no_articles"
    if int(status.get("cluster_count", 0)) <= 0:
        return False, "no_clusters"
    if int(status.get("quality_flag_count", 0)) > max_quality_flags:
        return False, "quality_flags"
    return True, ""


def save_status(db_path: Path, status_dir: Path, status: dict[str, object]) -> Path:
    status_dir.mkdir(parents=True, exist_ok=True)
    run_id = str(status["run_id"])
    status_path = status_dir / f"{run_id}.json"
    latest_path = status_dir / f"latest_{status['mode']}.json"
    status["status_json_path"] = str(status_path.relative_to(ROOT))
    payload = json.dumps(status, ensure_ascii=False, indent=2, sort_keys=True)
    status_path.write_text(payload + "\n", encoding="utf-8")
    latest_path.write_text(payload + "\n", encoding="utf-8")

    conn = sqlite3.connect(db_path)
    ensure_regimen_table(conn)
    conn.execute(
        """
        INSERT OR REPLACE INTO regimen_runs(
            run_id, mode, period_from, period_to, as_of, status, send_allowed,
            block_reason, snapshot_run_id, topic_run_id, fetched_count,
            inserted_count, source_row_count, error_count, article_count,
            cluster_count, quality_flag_count, report_path, status_json_path,
            started_at, finished_at, params_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            status["run_id"],
            status["mode"],
            status["period_from"],
            status["period_to"],
            status["as_of"],
            status["status"],
            1 if status["send_allowed"] else 0,
            status["block_reason"],
            status.get("snapshot_run_id"),
            status.get("topic_run_id"),
            status.get("fetched_count", 0),
            status.get("inserted_count", 0),
            sum(status.get("source_counts", {}).values()),
            status.get("error_count", 0),
            status.get("article_count", 0),
            status.get("cluster_count", 0),
            status.get("quality_flag_count", 0),
            status.get("report_path"),
            status.get("status_json_path"),
            status["started_at"],
            status["finished_at"],
            json.dumps(status.get("params", {}), ensure_ascii=False, sort_keys=True),
        ),
    )
    conn.commit()
    conn.close()
    return status_path


def main() -> int:
    args = parse_args()
    started_at = datetime.now(UTC)
    regimen_run_id = "regimen_" + started_at.strftime("%Y%m%dT%H%M%S%fZ")
    as_of = date.fromisoformat(args.as_of)
    window = window_for(args.mode, as_of)
    min_cluster_size, min_shared_tokens, jaccard_threshold, max_token_df = defaults_for(args.mode, args)
    db_path = Path(args.db)
    status_dir = Path(args.status_dir)

    print(
        f"News Pressure Radar mode={args.mode} "
        f"window={window.start.isoformat()}..{window.end.isoformat()}"
    )

    snapshot_run_id = None
    topic_run_id = None
    cluster_stdout = ""
    status_name = "ok"
    error_message = ""
    if not args.skip_collect:
        try:
            collect_result = run_command(
                [
                    sys.executable,
                    "analytics/news_pressure_snapshot.py",
                    "--from",
                    window.start.isoformat(),
                    "--to",
                    window.end.isoformat(),
                    "--sources",
                    args.sources,
                    "--cluster-limit",
                    str(args.cluster_limit),
                    "--db",
                    str(db_path),
                ],
                args.dry_run,
            )
            snapshot_run_id = parse_snapshot_run(collect_result.stdout) if collect_result else None
        except subprocess.CalledProcessError as exc:
            status_name = "error"
            error_message = f"collect_failed:{exc.returncode}"
            if exc.stdout:
                print(exc.stdout, end="" if exc.stdout.endswith("\n") else "\n")
            if exc.stderr:
                print(exc.stderr, file=sys.stderr, end="" if exc.stderr.endswith("\n") else "\n")

    if status_name == "ok":
        try:
            cluster_result = run_command(
                [
                    sys.executable,
                    "analytics/news_pressure_cluster.py",
                    "--from",
                    window.start.isoformat(),
                    "--to",
                    window.end.isoformat(),
                    "--cluster-limit",
                    str(args.cluster_limit),
                    "--min-cluster-size",
                    str(min_cluster_size),
                    "--min-shared-tokens",
                    str(min_shared_tokens),
                    "--jaccard-threshold",
                    str(jaccard_threshold),
                    "--max-token-df",
                    str(max_token_df),
                    "--label-mode",
                    args.label_mode,
                    "--report-mode",
                    args.mode,
                    "--db",
                    str(db_path),
                ],
                args.dry_run,
            )
            cluster_stdout = cluster_result.stdout if cluster_result else ""
            topic_run_id = parse_topic_run(cluster_stdout)
        except subprocess.CalledProcessError as exc:
            status_name = "error"
            error_message = f"cluster_failed:{exc.returncode}"
            cluster_stdout = exc.stdout or ""
            if exc.stdout:
                print(exc.stdout, end="" if exc.stdout.endswith("\n") else "\n")
            if exc.stderr:
                print(exc.stderr, file=sys.stderr, end="" if exc.stderr.endswith("\n") else "\n")

    if args.dry_run:
        return 0

    finished_at = datetime.now(UTC)
    conn = sqlite3.connect(db_path)
    ensure_regimen_table(conn)
    snapshot = snapshot_summary(conn, snapshot_run_id)
    if args.skip_collect:
        snapshot = {"snapshot_run_id": None, "fetched_count": 0, "inserted_count": 0, "error_count": 0}
    topics = topic_summary(conn, topic_run_id)
    counts = source_counts(conn, window.start, window.end)
    conn.close()

    article_count = int(topics.get("article_count") or parse_int(ARTICLES_RE, cluster_stdout) or 0)
    cluster_count = int(topics.get("cluster_count") or parse_int(CLUSTERS_RE, cluster_stdout) or 0)
    report_path = ROOT / "data" / "news_pressure" / f"agenda_pulse_v2_{window.start.isoformat()}_{window.end.isoformat()}.md"
    status = {
        "run_id": regimen_run_id,
        "mode": args.mode,
        "period_from": window.start.isoformat(),
        "period_to": window.end.isoformat(),
        "as_of": as_of.isoformat(),
        "status": status_name,
        "error_message": error_message,
        **snapshot,
        "topic_run_id": topic_run_id,
        "article_count": article_count,
        "cluster_count": cluster_count,
        "quality_flag_count": int(topics.get("quality_flag_count", 0)),
        "source_counts": counts,
        "report_path": str(report_path.relative_to(ROOT)) if report_path.exists() else None,
        "started_at": started_at.isoformat(timespec="seconds"),
        "finished_at": finished_at.isoformat(timespec="seconds"),
        "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
        "params": {
            "sources": args.sources,
            "label_mode": args.label_mode,
            "cluster_limit": args.cluster_limit,
            "min_cluster_size": min_cluster_size,
            "min_shared_tokens": min_shared_tokens,
            "jaccard_threshold": jaccard_threshold,
            "max_token_df": max_token_df,
            "skip_collect": args.skip_collect,
            "max_quality_flags": args.max_quality_flags,
        },
    }
    send_allowed, block_reason = delivery_decision(status, args.max_quality_flags)
    status["send_allowed"] = send_allowed
    status["block_reason"] = block_reason
    status_path = save_status(db_path, status_dir, status)

    print(f"Regimen run: {regimen_run_id}")
    print(f"Status: {status_name}")
    print(f"Status JSON: {status_path}")
    print(f"Send allowed: {send_allowed}" + (f" ({block_reason})" if block_reason else ""))
    if status_name != "ok":
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
