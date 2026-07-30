# Design: MADPAC Agentic Loop

Project codename: MADPAC.

MADPAC combines:

- news pressure collection/reporting;
- market-trace research across MOEX indices and ruble FX pairs;
- a reusable agentic research loop with experiment registry and audit discipline.

## Current State

News Pressure Radar already has a data-oriented pipeline:

- news collection;
- SQLite storage;
- clustering;
- pressure scoring;
- daily/weekly/monthly/history reports;
- quality flags;
- run status.

The new work adds a market-coupling research loop and, more importantly, a reusable agentic process.

## Design Principles

### Plan Before Code

Use Wayfinder and mini-specs before implementation.

Code starts only when the active mini-spec defines:

- objective;
- inputs;
- method;
- outputs;
- acceptance criteria;
- review checklist.

### Three Agents First

Start with three roles:

1. Hypothesis Runner;
2. Independent Auditor;
3. Synthesizer / Loop Controller.

Do not add more roles until the first manual cycle produces useful artifacts.

### Decision Tickets Are Not Build Tickets

Wayfinder tickets resolve decisions.

Build tickets implement accepted decisions.

Do not debate and implement in the same ticket.

### Evidence Before Story

The Runner computes. The Auditor checks. The Synthesizer explains only after the evidence is inspected.

Avoid causal wording unless the method supports it.

### Anti-Loop By Default

Every cycle must check prior work before selecting a next hypothesis.

Repeated failed ideas are blocked unless:

- the data changed;
- the method changed;
- the question changed;
- Airat explicitly asks to retry.

## Cycle Architecture

```text
Cycle Brief
  -> Hypothesis Runner
  -> Runner Artifacts
  -> Independent Auditor
  -> Audit Report
  -> Synthesizer / Loop Controller
  -> Synthesis + State Update
  -> Next Cycle Brief or Stop
```

For the first MADPAC News-Market Coupling cycle, the Runner should be a separate agent. The main session acts as Wayfinder/Grill mentor and Loop Controller until a Runner brief exists. No code should run before the mini-spec and Runner brief are clear enough to hand off.

## Artifact Layout

Suggested local layout:

```text
docs/wayfinder/news-pressure-radar/
  map.md
  tickets/
  agentic-research-loop-spec.md
  external-skill-research.md
  mini-specs/
    README.md
    01-market-first-anomaly-backtrace-v0.md

data/news_market_coupling/
  cycle_state/
    current.json
    hypothesis_registry.jsonl
    failure_patterns.md
    experiment_registry.sqlite
  cycle_runs/
    <cycle_id>/
      cycle_brief.md
      hypothesis_runner_report.md
      audit_report.md
      synthesis.md
      metrics.json
      metric_vectors.parquet
      experiment_manifest.json
```

The `data/` paths are for later implementation; this proposal does not create them yet.

Storage decision for v0: use hybrid storage. Markdown/CSV/JSONL/Parquet files remain the reproducible evidence bundle for each cycle, while SQLite is the practical registry/index for querying experiment history and process quality.

## Experiment Registry

The loop should treat every run as data. The registry records the process, not only the market result.

Logical entities:

- `experiments`: cycle id, mini-spec id, status, timestamps, data snapshot, owner role;
- `instruments`: local symbol, market family, frequency, source;
- `metric_definitions`: metric name, version, parameters, intended use;
- `metric_results`: experiment id, instrument, event date, raw value, fire flag, rank/score;
- `candidate_relations`: market event, news window, topic/entity evidence, relation strength;
- `audits`: verdict, findings, leakage/cherry-pick/overclaim flags;
- `synthesis_decisions`: accept/iterate/reject/park, next action, metric status changes;
- `failure_patterns`: reusable warnings and blocked repeats.

Implementation should favor functional Python transformations with explicit inputs and outputs. A fast dataframe engine such as Polars may be a good candidate for experiment runs, but dependency choice remains a build decision after checking the existing repository.

## First Pilot: Market-First Anomaly Backtrace v0

Question:

```text
When MOEX indices or ruble FX pairs show rare moves, what news pressure appeared in D-3..D+3?
```

Initial constraints:

- one year of market data;
- 5 first instruments:
  - `IMOEX` / broad Moscow Exchange index;
  - oil and gas sector index;
  - financial sector index;
  - `USD/RUB`;
  - `CNY/RUB`;
- initial pool of 10 anomaly metrics, rather than one fixed threshold;
- metric selection may be instrument-specific rather than universal;
- raw metric vectors are stored first; hard ensemble/voting rules come only after calibration;
- metric/baseline calibration should not look deeper than one quarter in v0;
- compare shorter contexts where useful: day, week, month, two months, quarter;
- v0 control windows: random days in the same quarter plus adjacent calm days near the event;
- existing headlines first;
- descriptions/leads only after a signal appears;
- no full-text dependency in the first run;
- no causal claims.

## Review Model

The Auditor checks:

- lookahead leakage;
- baseline quality;
- cherry-picking;
- missing negative cases;
- duplicate hypothesis;
- weak source spread;
- overclaiming.

For metric-discovery cycles, the Auditor also checks:

- metrics that always fire;
- metrics that never fire;
- thresholds that are too strict or too loose;
- duplicated metrics that add no new information;
- whether a metric improves news/market alignment versus controls.
- whether a metric is only useful for a specific instrument, which is allowed.

Metric quality is judged by:

- fire rate;
- lift versus control windows;
- stability across month and quarter contexts;
- complementarity with other metrics;
- instrument specificity;
- interpretability.

Verdicts:

- `accept`;
- `accept_with_caveats`;
- `iterate`;
- `reject`.

## Open Questions

- Which exact local symbols/names correspond to the oil/gas and financial sector indices?
- Which FX series exist locally and at what frequency?
- Should the first audit be performed by a separate Auditor subagent or by the main agent in review stance?
