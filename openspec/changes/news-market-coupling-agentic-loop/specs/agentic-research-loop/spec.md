# Spec: MADPAC Agentic Research Loop

MADPAC is the project codename for the combined news-pressure, market-coupling, experiment-registry, and agentic research-loop effort.

## ADDED Requirements

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

The Hypothesis Runner MUST record the hypothesis, data window, commands, parameters, metrics, positive cases, negative cases, and caveats.

For the first News-Market Coupling cycle, the Runner SHOULD be a separate agent launched only after the mini-spec and Runner brief are accepted.

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
- THEN the Runner MUST include event window, source spread, topic evidence, and timing.

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

The Synthesizer MUST NOT turn a metric threshold into a permanent constant until it has been compared across enough samples or explicitly accepted as a temporary heuristic.

The Synthesizer MUST NOT lock an ensemble rule, such as `3 of 5 metrics passed`, until calibration shows that the rule behaves better than single metrics and control windows.

#### Scenario: Hypothesis Fails

- GIVEN a hypothesis is rejected
- WHEN synthesis closes the cycle
- THEN the Synthesizer MUST record a fingerprint and a `next_allowed_if` condition.

### Requirement: Anti-Loop Registry

The process MUST keep a hypothesis registry.

The registry MUST prevent repeated failed ideas unless data, method, or question changed.

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
