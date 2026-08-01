# Spec: MADPAC Agentic Research Loop

MADPAC is the project codename for the combined news-pressure, market-coupling, experiment-registry, and agentic research-loop effort.

## Purpose

Defines the disciplined process MADPAC is built and operated with — both the meta-development process (idea → wayfinder → grilling → openspec → implement) and the production research cycle (Runner → Auditor → Synthesizer) — including which roles must run in separate agent contexts, which model tier builds versus operates the system, and the single feedback mechanism every calibration decision in the project routes through.

## ADDED Requirements

### Requirement: Role-Taxonomy Altitude Separation

The project distinguishes three altitudes of "agentic role" and MUST NOT merge or rename across them: (a) the meta-development process for building MADPAC itself (wayfinder/researcher/spec-writer/implementer/tester/reviewer/summarizer); (b) the production coupling-analysis cycle (Runner/Auditor/Synthesizer, defined below); (c) the production quality-evaluation cycle (Generator/Evaluator, `evaluation` capability). A proposal to reuse a role name across altitudes (e.g. calling the meta-process's reviewer stage "Auditor" to match altitude (b)) MUST be rejected as a taxonomy conflation.

#### Scenario: Naming conflict proposed

- **WHEN** a future change proposes reusing a role name from one altitude for a role in another altitude
- **THEN** the proposal MUST be rejected and the existing altitude-specific name kept

### Requirement: Generation-Vs-Verification Context Boundary

Across all three altitudes, a stage MUST be classified as generation (clarify → research → spec-writing; a Runner's hypothesis formation) or verification (implement → test/review; an Auditor's independent check; an Evaluator's independent check). Generation stages MAY share one continuous agent context. Verification stages MUST run in a genuinely separate agent context from the generation stage they verify — a checker must never check its own work.

#### Scenario: Meta-process moves from spec-writing to review

- **WHEN** the meta-process moves from implementation to review
- **THEN** the reviewer MUST be a separate agent context from the implementer, even though the spec-writer and implementer stages before it were allowed to share context

### Requirement: Wayfinder/Grilling/OpenSpec Is The Fixed Decision-Making Core

`wayfinder → grilling → openspec` MUST remain the unchanged, undiluted core for all decision and spec work in the project — it MUST NOT be replaced, diversified, or diluted by any other skill during the planning/decision phase. Every other skill (`implement`, `tdd`, `code-review`, `simplify`, `security-review`, `diagnosing-bugs`, `handoff`, etc.) MUST activate only after `openspec` has produced a real `tasks.md`. Once `tasks.md` exists, the fixed mapping is: spec artifact work → `to-spec`/`openspec-propose`; implementation → `implement`; test discipline → `tdd` (generation context); independent review → `code-review` (plus `simplify`/`security-review` as needed) in a separate context from the implementer, per the generation/verification boundary above; diagnostic/support → `diagnosing-bugs` restricted to Phases 1-4 only (never Phase 5's auto-fix) — it files a ticket with a confirmed root cause instead, and any approved fix re-enters through normal `implement` against the module's actual spec, never as an ad-hoc patch; cross-context handoff → `handoff`; memory/spec fixation → the project `memory/` directory plus `openspec-archive-change`/`openspec-sync-specs`.

#### Scenario: No tasks.md yet

- **WHEN** a ticket is still in the planning/decision phase and no `tasks.md` exists for it
- **THEN** only `wayfinder`, `grilling`, and `openspec` MAY be used to make decisions on it

#### Scenario: Diagnostic agent finds a root cause

- **WHEN** a diagnostic run identifies a confirmed root cause for a failure
- **THEN** it files a ticket describing the root cause and stops before Phase 5, rather than applying a fix itself

#### Scenario: Idea reaches implementation

- **WHEN** a task has not yet appeared in a `tasks.md` produced by `openspec`
- **THEN** it MUST NOT move to code

### Requirement: Temporal Model-Selection Split

Model selection is split by construction phase, not by call-site content. A high-capability model (Claude Code) MUST be used for all building and extending of the system, for as long as construction/extension continues, regardless of cost. A cheaper model (DeepSeek) MAY be used only for roles that are routine, frequent, and check-against-what-already-exists, and only once the relevant part of the system already works — specifically the diagnostic/support agent (diagnose-only, per the Phase 1-4 restriction above) and the `evaluation` capability's Evaluator role. Pre-existing DeepSeek-routed internal pipeline calls (Ouroboros SQL hypotheses, headline-sentiment labeling) are unaffected by this rule.

#### Scenario: Implementing a fix

- **WHEN** a diagnostic agent has identified why a run failed and a fix is being implemented
- **THEN** the implementation MUST run on the high-capability model, never on the cheaper operate-time model, even though diagnosis itself may have run on the cheaper model

**Provenance note:** the requirements from this point through "Experiment Registry Supports Process Analysis" below carry forward structural detail (the mini-spec's 7 fields, the Synthesizer's 6-state taxonomy, the Anti-Loop Registry's fingerprint/`next_allowed_if` mechanics, the Runner's control-window guidance, the Auditor's checklist) from the original process design written 2026-07-28, before the disciplined wayfinder→grilling process existed. Ticket 11 confirmed the outer loop these requirements implement (idea → wayfinder → grilling → openspec → implement, with generation/verification context separation) but did not individually re-litigate every structural detail inside it. They remain the project's actual operating defaults — no session has revisited or contradicted them — but are flagged here as carried-forward process design rather than presented as individually ticket-grilled decisions, so a future session knows to treat them as revisitable if they stop fitting.

### Requirement: Cycle Has A Mini-Spec

Every research cycle MUST have a mini-spec before implementation begins.

The mini-spec MUST define:

- objective;
- inputs;
- method;
- outputs;
- acceptance criteria;
- review checklist;
- non-goals.

#### Scenario: Agent Starts A Cycle

- GIVEN a Runner agent is asked to begin work
- WHEN no mini-spec exists
- THEN the agent MUST stop and request or create the mini-spec instead of running code.

### Requirement: Runner Produces Reproducible Artifacts

The Hypothesis Runner MUST record its evidence using the `analysis` capability's Market-Coupling Output Row Schema (`instrument_id, event_date, window, metric_vector, news_source_count, news_headline_count, news_sync_flag, news_peak_timing, relation_note, negative_case`) as the decided shape — not an ad hoc field list — plus whatever additional reproducibility metadata (commands, parameters, caveats) is needed to rerun the cycle.

For the first News-Market Coupling cycle, the Runner MUST be launched as a separate agent, only after the mini-spec and Runner brief are accepted (see the Runner/Auditor Context Separation requirement below for the general rule this instantiates).

The main session SHOULD remain in Wayfinder/Grill/Loop Controller mode until the brief is ready; it MUST NOT silently switch into implementation.

The Hypothesis Runner MUST record structured experiment metadata sufficient to compare this cycle with later cycles.

For metric-discovery cycles, the Runner MUST compare a predeclared metric pool rather than report only the best-looking metric.

For metric-discovery cycles, the Runner MUST store the raw metric vector for each instrument/date before any hard pass/fail ensemble rule is applied.

For each metric, the Runner MUST record whether it is too strict, too loose, duplicated by another metric, or useful for at least one instrument.

The Runner SHOULD include control-window comparisons so metric quality is judged against ordinary periods, not only against selected market events.

For the first News-Market Coupling cycle, the Runner SHOULD use random same-quarter days and adjacent calm days as control windows.

For the first News-Market Coupling cycle, metric/baseline calibration SHOULD not look deeper than one quarter, because market regimes can change quickly.

#### Scenario: Runner Completes Experiment

- GIVEN the Runner finishes a cycle
- WHEN artifacts are written
- THEN the Runner MUST include an experiment manifest with cycle id, mini-spec id, data snapshot, metric versions, parameters, and output paths.

#### Scenario: Runner Finds A Candidate Relation

- GIVEN an index anomaly is detected
- WHEN the Runner attaches news pressure as candidate explanation
- THEN the Runner MUST populate the output row's `window`, `news_source_count`, `relation_note`, and `news_peak_timing` fields per the `analysis` capability's schema.

### Requirement: Auditor Reviews Independently

The Independent Auditor MUST review Runner artifacts before a result becomes durable knowledge.

The Auditor MUST check:

- lookahead leakage;
- cherry-picking;
- baseline quality;
- duplicate hypotheses;
- negative cases;
- causal overclaiming.

#### Scenario: Runner Uses Causal Language

- GIVEN the Runner says a news event caused a market move
- WHEN the method only supports correlation or event-window association
- THEN the Auditor MUST flag overclaiming and return `iterate` or `reject`.

### Requirement: Runner/Auditor Context Separation And Handoff

The Runner and the Auditor MUST be genuinely separate agent instances/contexts — never the same context wearing two hats — a direct instance of the Generation-Vs-Verification Context Boundary requirement above. The Runner MUST hand off its findings to the Auditor via a compact, redaction-aware artifact produced by the `handoff` skill, not via a shared chat thread the Auditor merely continues.

#### Scenario: Runner hands off to Auditor

- **WHEN** the Runner finishes a coupling-analysis cycle
- **THEN** it produces a `handoff` artifact, and the Auditor reviews that artifact in a fresh agent context rather than continuing the Runner's own conversation thread

### Requirement: Synthesizer Closes The Cycle

The Synthesizer MUST decide the next state of the cycle:

- accept;
- accept with caveats;
- iterate;
- reject;
- park;
- ask Airat.

The Synthesizer MUST update durable memory/state only after reading the audit verdict.

For metric-discovery cycles, the Synthesizer MUST update metric status: promote, keep, penalize, or reject.

The Synthesizer MUST NOT turn a metric threshold into a permanent constant until it has been compared across enough samples or explicitly accepted as a temporary heuristic. "Enough samples" and the actual comparison mechanism are defined by the Unified Feedback Contour requirement below — the Synthesizer does not invent its own ad hoc comparison.

The Synthesizer MUST NOT lock an ensemble rule, such as an "N of M metrics passed" gate, until calibration shows that the rule behaves better than single metrics and control windows. The exact N and M are not finalized by any ticket and MUST NOT be presented as decided numbers until calibration work sets them.

#### Scenario: Hypothesis Fails

- GIVEN a hypothesis is rejected
- WHEN synthesis closes the cycle
- THEN the Synthesizer MUST record a fingerprint and a `next_allowed_if` condition.

### Requirement: Anti-Loop Registry

The process MUST keep a hypothesis registry.

The registry MUST prevent repeated failed ideas unless data, method, or question changed.

#### Scenario: Repeated hypothesis with no change

- **WHEN** a new cycle proposes a hypothesis whose fingerprint matches a previously rejected one, and no data, method, or question has changed
- **THEN** the registry MUST block the cycle before it runs

### Requirement: Experiment Registry Supports Process Analysis

The process MUST persist enough structured records to analyze not only the domain result, but also the quality of the research method over time.

The first practical storage pattern SHOULD be hybrid: immutable file artifacts for reproducibility, plus a SQLite registry for querying cycles, metrics, audits, and synthesis decisions.

At minimum, the registry MUST support:

- experiments;
- metric definitions;
- metric results;
- audit verdicts;
- synthesis decisions;
- failure patterns.

#### Scenario: Synthesizer Reviews Prior Work

- GIVEN multiple cycles have run
- WHEN the Synthesizer plans the next cycle
- THEN it MUST be able to inspect which metrics, methods, and failure patterns were previously promoted, penalized, rejected, or blocked.

#### Scenario: Duplicate Failed Hypothesis

- GIVEN a new cycle proposes the same fingerprint as a rejected hypothesis
- WHEN no changed method/data/question is supplied
- THEN the cycle MUST be blocked before Runner execution.

### Requirement: Unified Feedback Contour

Every metric-or-threshold calibration decision in the project — whether it originates in this capability's Synthesizer, the `evaluation` capability's rubric review, or the `delivery` capability's threshold tuning — MUST route through one contour, not separate ad hoc mechanisms: run → record metrics as a Metric Card (`observed_distribution`) → compare against `interpretation_bands` → the Decision Block (`evaluation` capability) reconciles and decides → the decision and its history feed the next iteration. Champion/challenger testing and delivery-threshold recalibration are two entry points into this same contour, not separately invented systems.

#### Scenario: New threshold candidate evaluated

- **WHEN** a new clustering threshold candidate is evaluated for adoption
- **THEN** it produces a Metric Card, is compared against `interpretation_bands`, and passes through the Decision Block exactly as a delivery-threshold recalibration proposal would

### Requirement: Champion/Challenger Shadow-Run Mechanics

For a change that alters the analysis itself (a clustering threshold, a new source), the process MUST test it via (A) replay against the `evaluation` capability's frozen historical snapshot when a historical equivalent exists, or (B) a live parallel shadow-run when no historical equivalent exists. Both modes MUST write results into the `regimen_runs` table (see below), tagged `run_type='shadow'`, rather than into a new table.

#### Scenario: New source with no history

- **WHEN** a new news source is proposed for the coupling pipeline and has no historical data to replay
- **THEN** the process runs a live shadow-run and logs the outcome to `regimen_runs` with `run_type='shadow'`, rather than skipping evaluation or inventing a new comparison table

### Requirement: Delivery-Threshold Recalibration Cadence

For delivery-side-only parameters that change what is surfaced but not what is computed (e.g. the `delivery` capability's `K`), no shadow-run is required. After N=30 real operational days since the parameter's last calibration, the process MUST perform a statistical read of accumulated `regimen_runs` history and route a proposed new value through the Decision Block. N=30 is an explicit, concrete provisional starting value, not a guess left unstated — it MAY be revised, but MUST NOT be silently replaced without recording why.

#### Scenario: 30 operational days accumulate

- **WHEN** 30 operational days of `regimen_runs` history have accumulated since a delivery threshold's last calibration
- **THEN** the process generates a statistical read of that history and routes a proposed new value through the Decision Block, rather than leaving the threshold fixed indefinitely or recalibrating on an ad hoc schedule

### Requirement: Regimen Runs Is The Metrics/Audit-Log Artifact

The metrics/audit-log artifact for production runs MUST be the existing `regimen_runs` table (in `scripts/news_pressure_regimen.py`), extended with the threshold value used, whether a diagnostic-agent ticket was filed, the champion/challenger comparison result, and `run_type` — never a new store, and never `db/knowledge.md` (which is overwritten wholesale each session and has no per-run granularity, a poor fit for an append-only audit log). The relationship between `regimen_runs` and the SQLite experiment registry required above is an open question, not yet resolved — see Open Questions in `design.md`.

#### Scenario: Production run completes

- **WHEN** a production run of the news-pressure regimen completes
- **THEN** its `regimen_runs` row includes the threshold value used and, if applicable, the champion/challenger comparison result and `run_type`, rather than recording these facts only in prose or a separate file

### Requirement: Mechanical Closure Of Meta-Process Decision Points

A task MUST NOT move from idea to code until it has arrived in a `tasks.md` produced by `openspec`. A spike SHALL be accepted once `code-review` passes both its axes (Standards and Spec). When a review fails, the process MUST iterate back through `implement` — no separate escalation mechanism exists for a plain review failure.

#### Scenario: Review fails on the Spec axis

- **WHEN** `code-review` reports a Spec-axis failure on a spike
- **THEN** the work routes back through `implement`, not through a separate escalation path
