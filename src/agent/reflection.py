"""Meta-reflection layer — runs every N iterations to synthesize patterns.

Temporally separated from execution (Ouroboros principle):
the agent reflects only AFTER completing a batch, never mid-cycle.
"""
import json
import re

from src.agent.llm import chat


def reflect(recent_signals: list[dict]) -> dict:
    """LLM analyzes a batch of signals and returns strategic direction."""
    if not recent_signals:
        return {}

    signals_text = "\n".join(
        "[{status}] {score}/100 | {hyp} → {finding}".format(
            status="Y" if s.get("confirmed") is True else ("?" if s.get("confirmed") is None else "N"),
            score=s.get("signal_score", 0) or 0,
            hyp=s.get("hypothesis", ""),
            finding=s.get("finding", ""),
        )
        for s in recent_signals
    )

    prompt = (
        "You are a senior quantitative analyst reviewing a batch of hypothesis tests "
        "on Russian financial market data.\n\n"
        f"Recent results ({len(recent_signals)} iterations):\n{signals_text}\n\n"
        "Analyze these results. Respond with JSON only:\n"
        "{\n"
        '  "key_findings": ["confirmed insight 1 in Russian", "confirmed insight 2 in Russian"],\n'
        '  "weak_areas": ["what data is missing or insufficient (in Russian)"],\n'
        '  "bias_warnings": ["any systematic bias in the hypotheses (in Russian)"],\n'
        '  "next_focus": "most promising direction for the next batch (1 sentence in Russian)"\n'
        "}"
    )

    response = chat([{"role": "user", "content": prompt}], temperature=0.2)

    match = re.search(r"\{.*\}", response, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return {"next_focus": response[:300]}


def format_reflection(r: dict) -> str:
    lines = ["\n" + "="*60, "META-REFLECTION", "="*60]
    for item in r.get("key_findings", []):
        lines.append(f"  [+] {item}")
    for item in r.get("weak_areas", []):
        lines.append(f"  [~] {item}")
    for item in r.get("bias_warnings", []):
        lines.append(f"  [!] {item}")
    if r.get("next_focus"):
        lines.append(f"\n  Next focus: {r['next_focus']}")
    lines.append("="*60)
    return "\n".join(lines)
