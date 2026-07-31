# Runner Brief 01: MADPAC Market-First Anomaly Backtrace v0

Status: draft
Created: 2026-07-28
Project: MADPAC

**Status note (2026-07-31):** predates [Ticket 10 - Market Coupling Model](../tickets/10-market-coupling-model.md), grilled live 2026-07-31 without using this brief as an input. Ticket 10 is authoritative; corrections below fix the instrument-list mismatch (missing `EUR/RUB`). The specific 10-metric list and control-window details were never grilled — Ticket 10 confirmed only "several metrics, raw vector, no hard gate" as a principle. Do not launch this brief as-is; it needs a pass against Ticket 10's actual Working Decision before any Runner agent uses it.

## Role Boundary

You are the Hypothesis Runner for the first MADPAC News-Market Coupling cycle.

Do not run this brief until the Loop Controller explicitly launches you.

The main planning session has already decided:

- no trading advice;
- no causal claims;
- first cycle is market-first anomaly backtrace;
- first Runner should be separate from the planning session;
- no code should begin before this brief is accepted.

## Source Spec

Read first:

- `docs/wayfinder/news-pressure-radar/mini-specs/01-market-first-anomaly-backtrace-v0.md`
- `openspec/changes/news-market-coupling-agentic-loop/design.md`
- `openspec/changes/news-market-coupling-agentic-loop/specs/agentic-research-loop/spec.md`

## Hypothesis

When selected MOEX indices or ruble FX pairs show unusual movements, meaningful news pressure may be visible in the event window `D-3..D+3`.

The cycle tests candidate correspondence, not causality.

## Instruments

Use the first instrument universe (registry-driven per Ticket 10 — read instrument rows from the registry table filtered by `active_in_pilot_v0`, do not hardcode this list in code):

- `IMOEX` / broad Moscow Exchange index;
- oil and gas sector index;
- financial sector index;
- `USD/RUB`;
- `EUR/RUB`;
- `CNY/RUB`.

If exact local symbols differ, record the mapping and uncertainty instead of guessing silently.

## Calibration Window

Use short-regime calibration.

- Maximum calibration depth: one quarter.
- Compare shorter contexts where useful: day, week, month, two months, quarter.
- Longer history may be inspected as context, but not used as the primary v0 baseline.

## Candidate Metric Pool

Treat this as a predeclared candidate pool, not as a claim that these are the best metrics.

The goal is to compare behavior and let Auditor/Synthesizer promote, keep, penalize, or reject metrics.

Initial 10 candidate metrics:

1. top 5% absolute daily move over one month;
2. top 5% absolute daily move over one quarter;
3. `2 sigma` daily move over one month;
4. `2 sigma` daily move over one quarter;
5. rolling z-score of daily return;
6. realized-volatility breakout;
7. relative move versus `IMOEX`;
8. gap from previous close;
9. 3-day momentum persistence;
10. reversal after abnormal move.

Record raw metric vectors before applying any hard pass/fail gate.

Do not invent an ensemble rule such as `3 of 5 metrics passed` in this run.

## Control Windows

Use two v0 controls:

- random same-quarter days;
- adjacent calm days near the event, excluding anomaly days.

Matched-volatility controls are out of v0 scope.

## News Window

For each market event date `D`, inspect:

```text
D-3, D-2, D-1, D, D+1, D+2, D+3
```

Start with headline-level data.

Descriptions/leads are optional only if already available.

Full article text is out of v0 scope unless the Loop Controller explicitly narrows a later cycle to a small candidate case.

## Required Outputs

Write artifacts under a cycle-specific directory:

```text
data/news_market_coupling/cycle_runs/<cycle_id>/
```

Required artifacts:

- `experiment_manifest.json`
- `market_events.csv`
- `metric_vectors.parquet` or `metric_vectors.csv`
- `candidate_relations.jsonl`
- `hypothesis_runner_report.md`
- `metrics.json`

If SQLite registry implementation is not ready, still write the manifest and tabular artifacts so the registry can be backfilled.

## Report Requirements

The Runner report must include:

- exact instruments used;
- exact data period;
- metric definitions and parameters;
- how many times each metric fired;
- metrics that always fired or never fired;
- overlap between metrics;
- event cases with news pressure;
- negative cases with no clear news pressure;
- comparison with control windows;
- caveats and uncertainty.

Use language such as:

- `coincides with`;
- `candidate relation`;
- `news pressure was visible near the event`;
- `market trace`.

Do not use language such as:

- `caused`;
- `proved`;
- `predicts`;
- `will happen`.

## Stop Conditions

Stop and report instead of improvising if:

- market data for fewer than 3 instruments is available;
- dates cannot be aligned between market and news data;
- no reproducible input snapshot can be named;
- the data source mapping is unclear;
- the implementation would require changing production code.

## Handoff To Auditor

The Auditor must receive:

- this Runner brief;
- all output artifacts;
- exact commands used, if any;
- known caveats;
- list of decisions the Runner made independently.
