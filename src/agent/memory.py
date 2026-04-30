"""Three-tier memory for Signal Mind Ouroboros agent.

Working  → context string passed between iterations (in agent.py)
Short-term → db/journals/YYYY-MM-DD.md  (one entry per iteration)
Long-term  → db/knowledge.md             (LLM-synthesized, updated each session)
"""
from datetime import datetime
from pathlib import Path

from src.agent.llm import chat

_ROOT = Path(__file__).parents[2]
JOURNALS_DIR = _ROOT / "db" / "journals"
KNOWLEDGE_FILE = _ROOT / "db" / "knowledge.md"
PRINCIPLES_FILE = _ROOT / "analysis_principles.md"


def load_principles() -> str:
    if PRINCIPLES_FILE.exists():
        return PRINCIPLES_FILE.read_text(encoding="utf-8").strip()
    return ""


def load_knowledge() -> str:
    if KNOWLEDGE_FILE.exists():
        return KNOWLEDGE_FILE.read_text(encoding="utf-8").strip()
    return ""


def save_journal_entry(result: dict, iteration: int):
    JOURNALS_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    ts = datetime.now().strftime("%H:%M:%S")
    journal_file = JOURNALS_DIR / f"{today}.md"

    score = result.get("signal_score", 0) or 0
    confirmed = result.get("confirmed")
    status = "CONFIRMED" if confirmed is True else ("PARTIAL" if confirmed is None else "REJECTED")

    entry = (
        f"\n## Iteration {iteration} — {today}T{ts} | {score}/100 {status}\n\n"
        f"**Hypothesis:** {result.get('hypothesis', '—')}\n"
        f"**Rationale:** {result.get('rationale', '—')}\n"
        f"**Expected signal:** {result.get('expected_signal', '—')}\n\n"
        f"**Finding:** {result.get('finding', '—')}\n"
        f"**Signal brief:** {result.get('signal_brief', '—')}\n\n"
        f"**SQL:**\n```sql\n{result.get('sql', '—')}\n```\n\n"
        f"**Next hypothesis:** {result.get('next_hypothesis', '—')}\n\n"
        f"---\n"
    )

    with open(journal_file, "a", encoding="utf-8") as f:
        f.write(entry)


def update_knowledge(signals: list[dict]) -> bool:
    """LLM synthesizes confirmed findings and rewrites knowledge.md. Returns True if written."""
    confirmed = [s for s in signals if s.get("confirmed") is True]
    if not confirmed:
        return False

    findings_text = "\n".join(
        f"- [{s.get('signal_score', 0)}/100] {s.get('hypothesis', '')}: {s.get('finding', '')}"
        for s in confirmed
    )
    existing = load_knowledge()

    prompt = (
        "You are a financial analyst maintaining a knowledge base of confirmed market signals "
        "for the Russian financial market.\n\n"
        f"Existing knowledge:\n{existing if existing else '(empty)'}\n\n"
        f"New confirmed findings from this session:\n{findings_text}\n\n"
        "Write a concise synthesis (3-8 bullet points in Russian) of what we now know, "
        "combining existing and new. Do not repeat well-established points verbatim — "
        "refine or expand them if new evidence adds nuance.\n"
        "Format: each point starts with '- '\n"
        "Output only the bullet points, nothing else."
    )

    synthesis = chat([{"role": "user", "content": prompt}], temperature=0.2)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    KNOWLEDGE_FILE.parent.mkdir(exist_ok=True)
    with open(KNOWLEDGE_FILE, "w", encoding="utf-8") as f:
        f.write(f"# Signal Mind — Accumulated Knowledge\n")
        f.write(f"_Last updated: {ts}_\n\n")
        f.write(synthesis.strip() + "\n")
    return True
