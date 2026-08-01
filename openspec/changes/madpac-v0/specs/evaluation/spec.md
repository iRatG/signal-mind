# Spec: Evaluation

## Purpose

Defines how MADPAC checks whether its own news-market hypothesis is statistically real, whether its classification pipeline is trustworthy, and the one unified mechanism (Metric Card + Decision Block) that turns any persistent mismatch into a documented, loop-safe calibration decision.

## ADDED Requirements

### Requirement: Two Independent Evaluation Tracks

The system SHALL run two independent evaluation tracks, never blended into one composite score: Track 1 (statistical backtest against a frozen historical snapshot) and Track 2 (qualitative rubric review of live classification output). They answer different questions — "is the hypothesis statistically real" versus "is the pipeline producing trustworthy output" — and a failure in one MUST NOT be interpreted as evidence about the other.

#### Scenario: Reporting system health

- **WHEN** asked whether the radar is improving over time
- **THEN** the system reports a Track 1 result and a Track 2 result separately, not one composite score

### Requirement: Frozen Portable Snapshot For Track 1

Track 1 SHALL NOT depend on a live cross-machine connection to `db/hf_news.db`. It SHALL use a versioned, read-only extraction (headline text, date, source, language only — no full article text) at 1-year depth, matching the market-side pilot depth. This same extraction serves both Track 1's backtest and Ticket 12's historical calibration.

#### Scenario: Production runs without dev-machine access

- **WHEN** production evaluation runs on a server with no access to the original `hf_news.db` host
- **THEN** it reads only from the frozen versioned snapshot, and correctness does not depend on any specific machine being online

### Requirement: Independent Sentiment Index

Track 1's sentiment index SHALL be computed fresh via a per-headline LLM score against a fixed, versioned prompt template, aggregated by a versioned formula. It SHALL NOT reuse or be derived from `pressure_score_v2`, since correlating a metric against a signal derived from itself would be tautological.

#### Scenario: Computing the эталон correlation

- **WHEN** the эталон correlation is computed
- **THEN** the sentiment values used come only from the independent prompt-template pipeline, never from `pressure_score_v2`

### Requirement: Pre-Registered Statistical Method

The correlation method MUST be locked and documented before any calibration pass runs, to prevent post-hoc method changes. It reuses the existing D-3..D+3 (optional D+5) lag grid; computes Spearman correlation per lag × per registry-driven instrument (never one blended market-wide number); applies Benjamini-Hochberg FDR (not Bonferroni) across all lag × instrument tests; uses block bootstrap (moving blocks of approximately 5-10 trading days) for significance and confidence intervals rather than naive parametric p-values, to account for daily sentiment autocorrelation. Exactly one verification run against the snapshot is permitted to confirm the procedure runs correctly — not to revise the method based on its result. This general, full-sample эталон correlation SHALL become the baseline/null that the `analysis` capability's anomaly-triggered, event-window market-coupling signals are checked against.

#### Scenario: Calibration pass runs

- **WHEN** the pre-registered method runs against the frozen snapshot
- **THEN** it produces one Spearman coefficient, BH-adjusted significance, and a block-bootstrap confidence interval per lag×instrument pair, using the method exactly as pre-registered regardless of how the results look

### Requirement: Known-Answer Calibration Anchors

A registry of known, scheduled economic-calendar events (CB rate decisions, Rosstat releases, quarterly earnings, MOEX corporate events) SHALL serve as known-answer test checkpoints. The method MUST correctly recover the expected relationship at each known anchor before it is trusted on unlabeled dates elsewhere in the year. Once validated, each anchor's measured coefficients become a distinguished high-confidence reference inside the relevant Metric Card's `interpretation_bands`, never folded anonymously into general population statistics or extrapolated to unrelated topics without separately checking generalization.

#### Scenario: Method fails a known anchor

- **WHEN** the method is applied to a known CB rate-decision date and fails to detect elevated FX volatility in the surrounding window
- **THEN** the method is treated as broken and MUST be fixed before being trusted on any unlabeled date

### Requirement: Metric Card Registry

Every metric in the system (the эталон correlation, the anomaly-pool metrics, the quality-gate flags — retrofitted when convenient, not forced immediately) SHALL be represented as a registry object with: `id`/`version`; `purpose`; `computation` (a fully formalized, deterministic procedure, including any fixed LLM prompt version and aggregation formula); `inputs`; `observed_distribution` (descriptive only — median/mean/min/max, derived empirically from calibration on the 1-year snapshot); `interpretation_bands` (normative, versioned separately from `observed_distribution`); `window_applicability`; and `llm_notes` (interpretation guidance only — never authorizes overriding a quality-gate hard blocker). If retrofitting an existing metric (e.g. a quality-gate flag) into this schema surfaces a real conceptual conflict, that conflict MUST be raised and discussed explicitly — it MUST NOT be silently resolved either way.

#### Scenario: LLM consults a Metric Card

- **WHEN** an LLM component needs to judge whether a metric value is contextually appropriate
- **THEN** it reads that metric's `llm_notes` for guidance, and cannot use that guidance to bypass a quality-gate hard blocker

### Requirement: Generator, Evaluator, And Airat-Audit Stay Separate Roles

Track 2 SHALL use three separate roles: the Generator (the existing classification pipeline), the Evaluator (a separate LLM context — never the same call or context as the Generator), and Airat's periodic spot-audit of the Evaluator's own judgments. A checker MUST NOT check its own work.

#### Scenario: Report scored

- **WHEN** a report is generated
- **THEN** it is scored by a distinct Evaluator context, and Airat only periodically audits whether the Evaluator itself remains trustworthy

### Requirement: Four-State Handoff Protocol

Every work item moving between Generator, Evaluator, and audit MUST be logged through four explicit states — `taken`, `in_progress`, `done`, `handed_off` — each with a timestamp and an explicit from/to reference.

#### Scenario: Evaluator finishes scoring

- **WHEN** the Evaluator finishes scoring a report
- **THEN** the item transitions from `in_progress` to `done` and then `handed_off`, each transition timestamped and attributed

### Requirement: Weekly Audit Cadence

Airat's audit of Evaluator judgments SHALL occur on a weekly sample cadence, not per-run.

#### Scenario: Week completes

- **WHEN** a week of reports has accumulated
- **THEN** a weekly audit sample is presented to Airat rather than surfacing individual reports as they are produced

### Requirement: Bounded-Choice Escalation

Any point where the system reaches out to Airat SHALL present at most 3 concrete options with reasoning, plus an implicit slot for his own answer — never an open-ended question.

#### Scenario: Decision Block escalates

- **WHEN** the Decision Block surfaces a proposed calibration to Airat
- **THEN** it presents up to 3 labeled options with reasoning, not an open prompt

### Requirement: Domain-Relative Historical-Baseline Check

The Evaluator MUST judge whether a cluster's volume/frequency is unusual relative to the calibrated normal distribution for clusters of its own topic domain, not against one global threshold. A manufactured-distraction case (state media synchronizing around a trivial pretext while independent media cover the real story at lower volume) MUST be cross-checked against the `collection` capability's `category_spread` field, since a sharp state/independent divergence is itself worth surfacing regardless of raw volume.

#### Scenario: Domain-normal high volume

- **WHEN** a cluster shows high absolute volume that is normal for its own topic domain
- **THEN** it is compared against that domain's historical baseline, not a global volume threshold, so it does not automatically outrank a lower-volume but domain-unusual cluster

### Requirement: Reuse The Ouroboros SQL-Hypothesis Engine

The domain-relative baseline computation, and any similar historical-precedent check, SHALL reuse the existing Ouroboros SQL-hypothesis pattern (LLM composes a SQL query, retries across versions on failure, executes, logs a structured finding), applied to historical cluster frequency rather than news↔market correlation. No parallel bespoke query-writer SHALL be built for this purpose.

#### Scenario: Checking historical precedent

- **WHEN** the Evaluator needs to know whether a cluster's volume level has occurred before for its domain
- **THEN** it invokes the existing Ouroboros SQL-hypothesis mechanism rather than a new bespoke query tool

### Requirement: Evaluator Disagreement Is Logged, Not Retroactive

When the Evaluator disagrees with the Generator's original label, the disagreement is logged as an evaluation signal (verdict plus reasoning) and MUST NOT retroactively alter an already-sent report. A single disagreement is data only; a persistent, previously-unseen pattern is what triggers the Decision Block.

#### Scenario: Single disagreement

- **WHEN** the Evaluator scores a report's label as wrong on one occasion
- **THEN** the disagreement is recorded in the evaluation log, and the already-delivered report is not modified or re-gated

### Requirement: Decision Block Trigger And Required Reconciliation

The Decision Block triggers only on a mismatch between system expectation and Evaluator judgment that persists across an observation window AND has no precedent in calibration history — never on a single disagreement. Before proposing anything, it MUST document three things: the affected metric's documented purpose (from its Metric Card), the documented decision algorithm that produced the original label, and the specific way the Evaluator's judgment diverged.

#### Scenario: Persistent unprecedented mismatch

- **WHEN** the same classification mismatch recurs across the observation window with no precedent in history
- **THEN** the Decision Block produces a written reconciliation of metric purpose, decision algorithm, and divergence before proposing any calibration

### Requirement: Decision Block Distinguishes Label Vs. Metric Calibration

The Decision Block MUST be explicit about whether it is calibrating the classification label/threshold or the metric's own computation/`interpretation_bands`, since these are different fixes for different root causes. Every calibration decision self-documents via its own ticket and database record, preserving the justification, not just the new value.

#### Scenario: Calibration applied

- **WHEN** a calibration is applied
- **THEN** a new ticket and database record states whether the label logic or the metric's `interpretation_bands` were changed, and why

### Requirement: Persistent Quality-Gate Block Diagnosis

A persistent quality-gate block (the `quality-gate` capability's hard or soft blocks firing repeatedly) SHALL be watched with two thresholds: 7 days as a soft watch period, 30 days as a hard ceiling after which it is treated as a confirmed calibration gap, not noise. The diagnostic mechanism SHALL be a one-time, off-cycle (e.g. overnight) invocation of the Ouroboros SQL-hypothesis agent, checking whether this exact pattern occurred before within available historical depth, following the same self-documenting trace as a classification-mismatch Decision Block case (observed anomaly → suspected gap → agent invoked → query composed → historical depth checked → found/not-found → resulting decision). If the diagnostic concludes available historical depth or source breadth is too shallow to judge honestly, that outcome SHALL be routed to the `analysis` capability's calibration follow-ups or the `collection` capability's source-strategy backlog, rather than solved directly here.

#### Scenario: Block persists past the hard ceiling

- **WHEN** a report has been blocked by the same quality-gate flag for 30 consecutive days
- **THEN** the diagnostic mechanism has already run overnight to check historical precedent, and the issue is confirmed as a calibration gap with a self-documented ticket filed, rather than left as unexplained recurring noise

### Requirement: Decision Block Closes The Loop Via Weekly Audit

A Decision Block proposal MUST surface through the existing weekly bounded-choice audit, not a separate approval flow. After a calibration is applied, a fresh observation window MUST check whether the specific conflict actually stopped recurring, confirming the fix worked rather than displacing the discrepancy elsewhere.

#### Scenario: Post-calibration confirmation

- **WHEN** a calibration has been applied
- **THEN** a fresh observation window monitors whether the specific conflict stops recurring before the calibration is considered confirmed

### Requirement: Decision Block Loop-Prevention (3 Distinct Attempts)

Before proposing a calibration, the Decision Block MUST check its own calibration history via the Metric Card's version log. A recurrence after a prior calibration is not treated as a fresh, unprecedented pattern. After 3 failed calibration attempts on the same recurring issue — each attempt required to be a genuinely different approach, never the same fix retried, and each explicitly informed by why the previous attempt failed — the Decision Block MUST stop and escalate to Airat as an explicitly flagged unresolved case.

#### Scenario: Third distinct attempt fails

- **WHEN** a third genuinely different calibration approach also fails to resolve a recurring mismatch
- **THEN** the Decision Block stops attempting further recalibration and escalates to Airat with an explicit "not self-resolving" flag

### Requirement: Champion/Challenger Shadow Deployment

Cluster-stability regression SHALL be handled as a champion/challenger pattern: champion is the current live, stable pipeline actually producing delivered reports; challenger is any candidate change (code, threshold, prompt) running in shadow on real or historical data, writing to a separate store, never touching delivered output. Target metrics are this capability's existing Metric Cards plus a cluster-stability diff against a fixed historical sample. A challenger is promoted only if it does not regress the target metrics relative to the current champion. This capability owns the target-metric definitions; the `agentic-research-loop` capability owns the shadow-run execution mechanics (which mode runs, where output is stored, observation duration before promotion).

#### Scenario: Cluster-stability check

- **WHEN** a challenger configuration is run against a fixed historical sample of a known cluster (e.g. a CB-rate cluster of about a dozen articles)
- **THEN** the check confirms whether the articles still group together the same way; if the cluster fragments or merges unexpectedly, the challenger is not promoted

#### Scenario: Regression detected

- **WHEN** a challenger's shadow-run shows regressed target metrics relative to the champion
- **THEN** the challenger is not promoted

### Requirement: Decision Block Unification Across Q5/Q6/Q7

Rather than inventing a distinct decision-making pathway for promotion, the champion/challenger promotion decision SHALL reuse the same Decision Block already required for classification-mismatch calibration and persistent quality-gate diagnosis. The Decision Block MUST support, at minimum, three trigger types: a persistent, previously-unseen classification mismatch; a persistent quality-gate block; and a challenger's target-metric evaluation ready for a promotion decision.

#### Scenario: Promotion decision ready

- **WHEN** a challenger's shadow-run metrics are ready for a promotion decision
- **THEN** the same Decision Block engine used for classification-mismatch and quality-gate diagnosis processes it, applying the same self-documentation, history-check, and bounded-choice escalation rules

### Requirement: Weekly System-Health Record

A lightweight system-health record SHALL be written every week regardless of whether anything triggered escalation, capturing: date, number of reports/clusters checked, Evaluator/Airat agreement rate, and current values of the key target metrics. This record preserves the raw trend only — it does not interpret whether a long calm streak reflects genuine stabilization or data stagnation.

#### Scenario: Quiet week

- **WHEN** a week passes with no triggered calibration or promotion decision
- **THEN** a health record is still written with the standard fields, preserving the trend line even during quiet periods

### Requirement: Scenario Replay Reuses Champion/Challenger

Running historical data through alternative threshold configurations (scenario/stress-test replay) SHALL introduce no new mechanism — it MUST be the champion/challenger pattern applied to the frozen snapshot as the arena, with each alternative configuration scored against the same target metrics as a live comparison.

#### Scenario: Testing an alternative threshold set

- **WHEN** evaluating whether a different registry configuration would have behaved differently over the past year
- **THEN** it is run as a challenger against the frozen snapshot and scored against the same target metrics as any live champion/challenger comparison
