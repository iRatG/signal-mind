# Ticket 06 - Evaluation Harness

Status: open
Type: task
Labels: `wayfinder:task`
Claim: unclaimed
Blocked By: Ticket 02 - Signal Ontology, Ticket 03 - Quality Gate Contract, Ticket 05 - Source Strategy (all three closed 2026-07-31 — this ticket is now formally unblocked, though its forward note ties the "reference correlation" work to Ticket 10 closing first — see below)
Blocks: Ticket 08 - OpenSpec Bridge, Ticket 09 - Repository Boundary

## Question

How will we know the radar is getting better rather than just producing plausible prose?

## Why This Matters

LLM summaries can sound confident while hiding weak evidence. The project needs an evaluation layer that judges signal detection, report usefulness, and false positives across daily and longer windows.

## Task Scope

Define and, if needed, prototype:

- a small labeled set of past reports;
- review rubric for main/rising/persistent/anomaly/noise classifications;
- metrics for quality flags and delivery blocks;
- regression checks for cluster stability;
- review notes format that can feed future improvements.

## Decision Shape

The ticket resolves when there is a concrete evaluation harness spec with enough detail for implementation.

## Forward Note (2026-07-31, captured while grilling Ticket 01, not resolved)

Airat's "success after 30 days" answer on Ticket 01 depends on this ticket delivering more than a labeled-report rubric: he wants at least one statistically backtested reference correlation (an "эталон") between a synthesized news-tonality index and a real market/composite signal at a defined lag, validated against historical headlines pulled from as far back as the start of the year. That reference correlation is meant to become the benchmark other candidate signals get checked against — an objective replacement for gut-feel judgment of "is this working." Whatever historical headline backfill this needs should check `db/hf_news.db` first (already described in `CLAUDE.md` as the 1M+-article master news source) before treating it as a new data-gathering task.

Also carries Ticket 11's expectation that the metrics-ensemble itself may be window-specific (some metrics only hold at D+1, others at D+3) — the harness should be able to evaluate a metric per-window, not just pass/fail overall.

Forward note (2026-07-31, captured while grilling Ticket 02 - Signal Ontology, not resolved): Airat named a concrete future use for the versioned parameter registry being introduced across Ticket 02's signal types (see Ticket 11's forward note) — scenario/stress-test analysis: replay historical data through different threshold configurations and observe how indices actually reacted under each. This is this ticket's territory, not a Ticket 02 concern; resolve it here once the parameter registry exists.
