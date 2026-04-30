"""Session metrics tracker — Ouroboros engineering layer."""
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

METRICS_FILE = Path(__file__).parents[2] / "db" / "metrics.jsonl"


@dataclass
class SessionMetrics:
    started_at: str = field(default_factory=lambda: datetime.now().isoformat())
    total: int = 0
    confirmed: int = 0
    rejected: int = 0
    partial: int = 0
    sql_errors: int = 0
    scores: list = field(default_factory=list)
    confirmed_scores: list = field(default_factory=list)
    chain_depth: int = 0
    max_chain: int = 0
    top_signals: list = field(default_factory=list)

    def record(self, result: dict, sql_error: bool = False):
        self.total += 1
        if sql_error:
            self.sql_errors += 1

        confirmed = result.get("confirmed")
        score = result.get("signal_score") or 0
        self.scores.append(score)

        if confirmed is True:
            self.confirmed += 1
            self.chain_depth += 1
            self.confirmed_scores.append(score)
        elif confirmed is False:
            self.rejected += 1
            self.chain_depth = 0
        else:
            self.partial += 1
            self.chain_depth += 1

        self.max_chain = max(self.max_chain, self.chain_depth)

        self.top_signals.append({
            "score": score,
            "hypothesis": result.get("hypothesis", ""),
            "confirmed": confirmed,
        })
        self.top_signals = sorted(self.top_signals, key=lambda x: x["score"], reverse=True)[:3]

    @property
    def confirmation_rate(self) -> float:
        return round(self.confirmed / self.total * 100, 1) if self.total else 0.0

    @property
    def avg_score(self) -> float:
        return round(sum(self.scores) / len(self.scores), 1) if self.scores else 0.0

    @property
    def avg_confirmed_score(self) -> float:
        return round(sum(self.confirmed_scores) / len(self.confirmed_scores), 1) if self.confirmed_scores else 0.0

    @property
    def sql_error_rate(self) -> float:
        return round(self.sql_errors / self.total * 100, 1) if self.total else 0.0

    def report(self) -> str:
        lines = [
            f"\n{'='*60}",
            f"SESSION METRICS — {self.total} iterations",
            f"{'='*60}",
            f"  Confirmation rate : {self.confirmation_rate}%"
            f"  ({self.confirmed}C / {self.partial}P / {self.rejected}R)",
            f"  Avg signal score  : {self.avg_score}/100",
            f"  Avg confirmed     : {self.avg_confirmed_score}/100",
            f"  SQL error rate    : {self.sql_error_rate}%  ({self.sql_errors} errors)",
            f"  Max chain depth   : {self.max_chain}",
            "",
            "  TOP SIGNALS:",
        ]
        for s in self.top_signals:
            conf = "Y" if s["confirmed"] else ("?" if s["confirmed"] is None else "N")
            lines.append(f"    [{conf}] {s['score']:>3}/100  {s['hypothesis'][:55]}")
        lines.append(f"{'='*60}")
        return "\n".join(lines)

    def save(self):
        METRICS_FILE.parent.mkdir(exist_ok=True)
        record = {
            "session_ts": self.started_at,
            "ended_at": datetime.now().isoformat(),
            "total": self.total,
            "confirmed": self.confirmed,
            "rejected": self.rejected,
            "partial": self.partial,
            "confirmation_rate": self.confirmation_rate,
            "avg_score": self.avg_score,
            "avg_confirmed_score": self.avg_confirmed_score,
            "sql_error_rate": self.sql_error_rate,
            "max_chain_depth": self.max_chain,
            "top_signals": self.top_signals,
        }
        with open(METRICS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
