# MADPAC Agentic Research Loop Spec

Status: draft
Created: 2026-07-28
Project: MADPAC

MADPAC is the project codename for the combined effort:

- News Pressure Radar: news-pressure collection and reporting;
- News-Market Coupling: market-trace research over MOEX indices and ruble FX pairs;
- Agentic Research Loop: the repeatable process for turning foggy ideas into specs, experiments, audits, synthesis, and memory.

## Purpose

Build a repeatable process that turns Airat's foggy research ideas into tested knowledge through independent agent roles.

The first training task is News-Market Coupling:

```text
news pressure / public speech -> market trace in MOEX indices and ruble FX pairs
```

But the deeper goal is methodological:

```text
foggy idea -> spec -> experiment -> audit -> synthesis -> memory -> next cycle
```

## Non-Goals

- Do not build a trading bot.
- Do not claim causality from correlation.
- Do not optimize for impressive prose before evidence.
- Do not let an agent repeat failed ideas without new evidence or a changed method.
- Do not create a large agent swarm before a three-role loop proves useful.

## Minimal Three-Agent System

The first version uses three independent agent roles.

### Agent 1: Hypothesis Runner

Mission: turn one approved question into a reproducible experiment.

For News-Market Coupling, this agent runs the market-first spike:

```text
instrument anomaly -> news backtrace -> candidate relation
```

Inputs:

- active cycle brief;
- accepted mini-spec;
- data inventory;
- prior rejected hypotheses;
- current hypothesis registry.

Startup checklist:

- Read the active mini-spec.
- Read the last synthesis summary.
- Check the hypothesis registry for duplicates.
- Confirm data windows and instruments.
- State the exact hypothesis before running code.

Procedure:

1. Select one instrument or small instrument set.
2. Compute baseline movement over the agreed period, initially one year.
3. Detect rare moves, such as top 5% absolute daily returns or `2 sigma` events.
4. For each market event, inspect news clusters in `D-3..D+3`.
5. Record cross-source topic pressure, source count, volume change, and key entities.
6. Compare against simple control windows when possible.
7. Produce a machine-readable result plus a short human-readable report.

Output artifact:

```text
cycle_runs/<cycle_id>/hypothesis_runner_report.md
cycle_runs/<cycle_id>/market_events.csv
cycle_runs/<cycle_id>/candidate_relations.jsonl
```

Required report fields:

- hypothesis id;
- data period;
- instruments;
- anomaly definition;
- news window;
- number of market events checked;
- number of candidate relations found;
- negative cases;
- caveats;
- exact commands/scripts used.

Quality metrics:

- `coverage`: how many intended instruments/events were checked;
- `reproducibility`: whether another agent can rerun the same command;
- `novelty`: whether this is not a duplicate of a rejected run;
- `evidence_strength`: weak, moderate, strong;
- `claim_discipline`: whether the report avoids causal overclaiming.

Failure conditions:

- no explicit hypothesis;
- duplicate hypothesis without new method;
- no baseline;
- no negative cases;
- no reproducible command;
- causal language without evidence.

### Agent 2: Independent Auditor

Mission: attack the Hypothesis Runner result before it is trusted.

Inputs:

- mini-spec;
- Hypothesis Runner artifacts;
- raw/generated metrics;
- prior failure patterns;
- acceptance checklist.

Startup checklist:

- Do not assume the result is good.
- Re-read the original question.
- Identify the claim being made.
- Check whether evidence supports that exact claim.

Procedure:

1. Verify reproducibility: can the stated command be rerun?
2. Check for lookahead leakage: did news after the event explain a pre-event claim?
3. Check baseline quality: is the anomaly threshold meaningful?
4. Check cherry-picking: are negative cases reported?
5. Check statistical weakness: is the relation based on one pretty case?
6. Check wording: does the report say "caused" when it only has correlation?
7. Assign a verdict.

Verdicts:

- `accept`: evidence is strong enough for the next stage.
- `accept_with_caveats`: useful, but limitations must travel with it.
- `iterate`: promising but method/data must be improved.
- `reject`: not useful or misleading.

Output artifact:

```text
cycle_runs/<cycle_id>/audit_report.md
```

Required report fields:

- verdict;
- top findings by severity;
- data risks;
- method risks;
- claim-language risks;
- required fixes;
- whether the cycle may update durable knowledge.

Penalty registry updates:

- duplicate hypothesis;
- weak baseline;
- unsupported causal claim;
- no control window;
- missing negative cases;
- overfitted explanation;
- poor data coverage.

### Agent 3: Synthesizer / Loop Controller

Mission: close the cycle and decide what happens next.

This agent does not rerun the experiment. It reads the runner and auditor outputs, then turns them into durable state.

Inputs:

- cycle brief;
- Hypothesis Runner report;
- Auditor report;
- current wayfinder map;
- current OpenSpec/mini-spec;
- memory notes;
- hypothesis registry.

Startup checklist:

- Read the auditor verdict first.
- Separate result, interpretation, and next action.
- Do not bury a failed cycle.

Procedure:

1. Summarize what was attempted.
2. Summarize what was found.
3. Summarize what failed or remained unclear.
4. Decide one next action:
   - accept and expand;
   - iterate with a narrower method;
   - reject and avoid repeating;
   - park until better data exists;
   - ask Airat for a decision.
5. Update durable artifacts:
   - wayfinder ticket;
   - cycle registry;
   - hypothesis registry;
   - memory;
   - OpenSpec if the decision is stable.
6. Prepare the next cycle brief.

Output artifacts:

```text
cycle_runs/<cycle_id>/synthesis.md
cycle_state/current.json
cycle_state/hypothesis_registry.jsonl
cycle_state/failure_patterns.md
```

Quality metrics:

- `decision_clarity`: one clear next action exists;
- `memory_quality`: useful learning was preserved;
- `anti_loop_strength`: rejected/failed ideas are fingerprinted;
- `process_value`: the cycle improved the method, not only the result.

## Cycle Lifecycle

### 0. Intake

Airat gives a foggy idea.

The main agent restates:

- subject goal;
- process goal;
- first unknown;
- recommended next artifact.

### 1. Wayfinder

Use wayfinder when the path is bigger than one session.

Output:

- destination;
- decision tickets;
- current frontier;
- blocked tickets;
- fog;
- out of scope.

### 2. Mini-Spec

Before code, write a mini-spec.

Template:

```markdown
# Mini-Spec: <name>

## Objective
## Inputs
## Method
## Output
## Acceptance Criteria
## Risks
## Non-Goals
## Review Checklist
```

### 3. Cycle Brief

Each cycle starts with a small brief:

```markdown
# Cycle <id>: <title>

## Question
## Hypothesis
## Data
## Method
## Stop Conditions
## Expected Artifacts
```

### 4. Run

Agent 1 runs the experiment and records artifacts.

### 5. Audit

Agent 2 reviews the output independently.

### 6. Synthesis

Agent 3 closes the cycle and decides the next action.

### 7. Memory

Durable state is updated only after synthesis.

## Anti-Loop System

The loop must not repeat itself blindly.

Maintain a hypothesis registry:

```json
{
  "hypothesis_id": "market_backtrace_moexog_sanctions_v0",
  "fingerprint": "instrument=MOEXOG|topic=sanctions|window=D-3..D+3|method=top5_abs_return",
  "status": "accepted_with_caveats",
  "last_cycle_id": "2026-07-28-001",
  "evidence_strength": "moderate",
  "failure_reason": null,
  "next_allowed_if": "add descriptions/leads or change baseline method"
}
```

Rules:

- Do not rerun the same fingerprint unless method, data, or question changed.
- Rejected hypotheses require a `next_allowed_if`.
- Negative results are first-class knowledge.
- If three cycles produce the same blocker, stop and ask Airat or change the destination.
- Every new cycle must cite what it learned from the previous cycle.

## Metrics

### Process Metrics

- cycles completed;
- accepted / iterated / rejected count;
- repeated-hypothesis attempts blocked;
- time per cycle;
- artifacts produced;
- unresolved blockers.

### Data Metrics

- market instruments covered;
- date coverage;
- missing market days;
- news source coverage;
- source spread per event;
- headline/description/full-text depth.

### Signal Metrics

- anomaly count;
- candidate relation count;
- repeated relation count;
- event-window timing;
- abnormal move size;
- relative move versus broad market;
- topic pressure growth;
- control-window comparison.

### Quality Metrics

- leakage risk;
- cherry-picking risk;
- baseline quality;
- reproducibility;
- claim discipline;
- audit verdict.

## First Pilot Cycle

Name:

```text
Market-first anomaly backtrace v0
```

Question:

```text
When MOEX indices or ruble FX pairs show rare moves, what news pressure appeared in the previous three days and around the event?
```

Inputs:

- one year of market data;
- broad MOEX index;
- selected sector indices;
- USD/RUB, EUR/RUB, CNY/RUB;
- existing news headlines first.

Method:

1. Compute daily returns.
2. Detect top 5% absolute moves by instrument.
3. Build event windows `D-3..D+3`.
4. Join news clusters by date.
5. Rank clusters by source spread, volume growth, and relevant entities.
6. Produce candidate relations.
7. Audit for leakage, cherry-picking, and overclaiming.

Acceptance criteria:

- At least 3 instruments processed.
- At least 10 market events inspected.
- Negative cases included.
- Every candidate relation has visible evidence.
- No causal wording in final output.
- Auditor verdict is not `reject`.

## Agent Launch Order

For the first manual run:

1. Main agent creates mini-spec and cycle brief.
2. Hypothesis Runner executes the spike.
3. Independent Auditor reviews artifacts.
4. Synthesizer closes the cycle.

For later automated/daily runs:

1. Scheduler starts a cycle only if the previous cycle is closed.
2. Loop Controller checks the hypothesis registry.
3. Runner tests one approved next hypothesis.
4. Auditor scores quality.
5. Synthesizer updates state and writes next brief.

## OpenSpec Integration

Use OpenSpec after the first cycle proves the shape.

OpenSpec should capture:

- data model changes;
- scripts/commands;
- output contract;
- quality gates;
- agent role contracts;
- accepted terminology.

Do not create a large OpenSpec before the first spike. Use a mini-spec first.

## TDD Integration

TDD applies when implementation begins.

Initial tests:

- anomaly detector returns stable events on fixture data;
- event windows do not include future data when running backtrace mode;
- duplicate hypothesis fingerprinting works;
- audit fails causal wording;
- synthesis blocks a repeated rejected hypothesis.

## Recommended Next Step

Create the mini-spec for `Market-first anomaly backtrace v0`, then run one full manual cycle with the three roles.
