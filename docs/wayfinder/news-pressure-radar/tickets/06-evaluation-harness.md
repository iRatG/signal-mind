# Ticket 06 - Evaluation Harness

Status: blocked
Type: task
Labels: `wayfinder:task`
Claim: unclaimed
Blocked By: Ticket 02 - Signal Ontology, Ticket 03 - Quality Gate Contract, Ticket 05 - Source Strategy
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
