# Mini-Spec 01: MADPAC Market-First Anomaly Backtrace v0

Status: draft
Created: 2026-07-28
Project: MADPAC

**Status note (2026-07-31):** this mini-spec predates [Ticket 10 - Market Coupling Model](../tickets/10-market-coupling-model.md), which has since been grilled live with Airat from a clean slate (this draft was not used as an input). Ticket 10 is now authoritative. Corrections: the instrument list below is missing `EUR/RUB` and undercounts sector candidates — Ticket 10 decided instruments are registry-driven (all MOEX ISS API candidates loaded, an `active_in_pilot_v0` flag marks the v0 subset), not a fixed 5-item list. The 10-metric pool, one-quarter calibration depth, and control-window specifics below were never actually grilled — Ticket 10 confirmed only the *principle* (several metrics, raw vector, no hard gate); treat the specifics here as an unconfirmed candidate menu for calibration/build time, not a decision.

## Objective

Run the first manual research cycle for MADPAC's News-Market Coupling layer.

Question:

```text
When selected MOEX indices or ruble FX pairs show rare moves, what news pressure appeared around the event window D-3..D+3?
```

This cycle tests both:

- the domain idea: news pressure may leave a measurable market trace;
- the process idea: Runner -> Auditor -> Synthesizer can produce useful learning.

Execution boundary: do not run code from this mini-spec yet. First finish the planning chain:

```text
Wayfinder/Grill decisions -> OpenSpec draft -> accepted mini-spec -> Runner brief -> separate Runner agent -> Auditor -> Synthesizer
```

## Inputs

Market data:

- selected instruments — registry-driven per Ticket 10, not a fixed list; v0-active subset:
  - `IMOEX` / broad Moscow Exchange index;
  - oil and gas sector index;
  - financial sector index;
  - `USD/RUB`;
  - `EUR/RUB`;
  - `CNY/RUB`;
- approximately one year of daily observations;
- close price or equivalent daily value.

News data:

- existing headline-level News Pressure Radar data first;
- source, date, title, URL;
- cluster/topic labels if available.

Do not require full article text in v0.

## Method

1. Compute daily returns for each instrument.
2. Compute baseline movement with a short-regime bias:
   - maximum calibration lookback: one quarter;
   - compare shorter contexts when useful: day, week, month, two months, quarter.
3. Mark anomaly days using an initial pool of anomaly metrics, not one fixed rule.
4. For each anomaly day D, build an event window:

```text
D-3, D-2, D-1, D, D+1, D+2, D+3
```

5. Attach news clusters/headlines from the window.
6. Score news pressure by:
   - source spread;
   - headline count;
   - volume growth versus baseline;
   - presence of market-relevant entities.
7. Calibrate anomaly metrics:
   - store the raw metric vector for every instrument/date before making any pass/fail ensemble decision;
   - record how often each metric fires;
   - detect metrics that always fire or never fire;
   - measure overlap between metrics;
   - compare event windows against control windows;
   - check whether the metric behaves differently by instrument;
   - assign metric status: promote, keep, penalize, or reject.
8. Produce candidate relations.
9. Include negative cases where market anomaly has no clear news pressure.
10. Compare candidate event windows with v0 control windows:
   - random days within the same quarter;
   - adjacent calm days near the event, excluding anomaly days.

## Output

Expected artifacts for the Runner:

```text
cycle_runs/<cycle_id>/cycle_brief.md
cycle_runs/<cycle_id>/market_events.csv
cycle_runs/<cycle_id>/candidate_relations.jsonl
cycle_runs/<cycle_id>/hypothesis_runner_report.md
cycle_runs/<cycle_id>/metrics.json
cycle_runs/<cycle_id>/metric_vectors.parquet
cycle_runs/<cycle_id>/experiment_manifest.json
```

The same logical records should be stored in a database-backed experiment registry, not only as loose files. v0 uses a practical hybrid:

- immutable file artifacts for reproducibility and debugging;
- SQLite registry rows for querying experiment history, metric behavior, audits, and synthesis decisions.

The first design target is:

- experiment id;
- cycle id;
- run timestamp;
- input data snapshot;
- instrument;
- event date;
- metric name/version/parameters;
- raw metric value;
- metric fire/no-fire flag;
- candidate relation id;
- audit verdict;
- synthesis decision;
- rejected/promoted/penalized reasons.

Implementation note for later coding: Python is acceptable, but data operations should be written as small functional transformations with explicit inputs and outputs. Prefer a fast dataframe engine such as Polars for tabular experiment runs if it fits the existing repo better than pandas. Do not decide this in v0 design without checking current dependencies.

Expected output row:

```json
{
  "instrument": "MOEXOG",
  "event_date": "YYYY-MM-DD",
  "return_1d": -0.024,
  "anomaly_rank": "top_5_pct",
  "window": "D-3..D+3",
  "candidate_topic": "sanctions / export / oil",
  "source_count": 4,
  "headline_count": 23,
  "news_peak_timing": "D-1",
  "relation_strength": "weak|moderate|strong",
  "claim": "news pressure coincided with abnormal market movement; causality not claimed"
}
```

## Acceptance Criteria

- At least 3 instruments processed.
- At least 10 anomaly events inspected.
- At least 3 negative/no-clear-news cases included.
- At least 5 anomaly metrics compared.
- Each metric receives a quality note: useful, noisy, too sparse, too broad, or reject.
- Metric thresholds are treated as calibratable, not final constants.
- Raw metric vectors are stored before any hard ensemble or "N of M metrics passed" rule.
- Metrics that always pass or never pass are explicitly flagged.
- Metrics are compared against control windows, not only against interesting cases.
- v0 control windows include random days and adjacent calm days.
- v0 metric/baseline calibration does not look deeper than one quarter.
- Every candidate relation includes visible news evidence.
- No causal wording in Runner conclusions.
- Auditor can reproduce the main event list.
- Auditor verdict is not `reject`.
- Experiment metadata is captured well enough to compare cycles later.

## Risks

- Lookahead leakage: explaining pre-event movement with post-event news.
- Cherry-picking: showing only pretty cases.
- Common-factor confusion: indices and currencies may move from the same hidden factor.
- Weak news clustering: headlines may be too shallow for interpretation.
- Calendar mismatch: trading days and news days differ.

## Non-Goals

- No trading advice.
- No individual stocks.
- No full-text article dependency.
- No dashboard.
- No automated daily run yet.

## Review Checklist

Auditor MUST check:

- anomaly metrics were defined before looking at news;
- metric comparison includes weak/failed metrics, not only winners;
- event window does not misuse future data;
- negative cases exist;
- source spread is visible;
- claims say "coincides with" or "candidate relation", not "caused";
- output can be rerun from commands/scripts.

## Open Questions For Grill

- Review Runner brief before launch.
- Decide whether first Auditor is a separate agent or main-session review stance.

## Working Decision

Do not pick a single anomaly rule too early.

Use an ensemble/pool of candidate metrics and let the cycle evaluate them. The first cycle should use a deliberately diverse pool of 10 metrics across month and quarter contexts, then the Auditor/Synthesizer should penalize metrics that produce noise and preserve metrics that create useful, reproducible signals.

Do not assume one metric fits all instruments. The process should allow different metrics to win for different instruments: broad index, oil/gas index, financial index, USD/RUB, and CNY/RUB may each need a different anomaly lens.

Initial candidate pool:

- top 5% absolute daily move over 1 month;
- top 5% absolute daily move over 3 months;
- `2 sigma` move over 1 month;
- `2 sigma` move over 3 months;
- move versus rolling volatility;
- gap from previous close;
- sector relative move versus `IMOEX`;
- FX relative move versus recent range;
- direction persistence over 3 days;
- reversal after abnormal move.

Metric evaluation should be empirical. A metric is not good because it sounds mathematically serious; it is good if, over many historical samples, it produces a useful event set: not always firing, not never firing, interpretable, and showing better news/market alignment than control windows.

Prefer soft scoring before hard gates. Instead of immediately saying "event passed if 3 of 5 metrics pass", first record the vector of metric results and let the calibration report discover useful combinations.

Initial metric scoring rubric:

- fire rate: does the metric fire too often, too rarely, or in a usable band?
- lift versus controls: do metric-selected windows align with news pressure better than control windows?
- stability: does the signal survive month and quarter contexts, or only one arbitrary window?
- complementarity: does the metric add information, or duplicate another metric?
- instrument specificity: is the metric useful for at least one instrument, even if weak elsewhere?
- interpretability: can a human explain why the day was marked as unusual?

Trader-inspired candidate families are allowed as inputs to the pool, but outputs remain analytical and non-advisory. Candidate families include rolling z-score, realized-volatility breakout, ATR/range-style move, Bollinger-band breach, relative strength versus `IMOEX`, drawdown, gap, short momentum persistence, and reversal after abnormal move.

Do not lock an ensemble rule in v0. `3 of 5 metrics passed` may become a later rule only after the calibration report shows that the combination behaves better than single metrics and controls.

The experiment system should be analyzable by itself. Later cycles should be able to answer:

- which metrics were tried;
- which metrics failed and why;
- which criteria rejected an approach;
- where auditors most often found problems;
- whether process quality is improving across cycles.

Persistent storage decision: use hybrid storage from the first designed cycle. File artifacts preserve exact evidence; SQLite acts as the registry/index that makes the lab queryable.

Control-window decision: v0 uses two practical controls, random days plus adjacent calm days. Matched-volatility controls are postponed until a later cycle. Calibration windows should stay within the current quarter, with shorter day/week/month/two-month contexts available for comparison.

Agent-boundary decision: the first cycle should be executed by a separate Runner agent, not by the main mentoring session. The main session remains Wayfinder/Loop Controller until the Runner brief is written and accepted.
