# Proposal: MADPAC Agentic Loop

Status: draft
Created: 2026-07-28

## Why

Airat wants to use MADPAC as a learning playground for disciplined agentic work.

MADPAC is the project codename for combining news pressure, market traces, experiment registry, and agentic research-loop methodology.

The domain idea is:

```text
news pressure / public speech -> market trace in MOEX indices and ruble FX pairs
```

The methodological idea is:

```text
foggy human intent -> wayfinder -> spec -> spike -> audit -> synthesis -> memory -> next cycle
```

The current risk is jumping from a promising conversation directly into code. This change defines the planning and execution contracts first.

## What Changes

- Define a minimal three-agent research loop:
  - Hypothesis Runner;
  - Independent Auditor;
  - Synthesizer / Loop Controller.
- Define artifacts for each cycle.
- Define anti-loop rules so failed ideas are not repeated blindly.
- Define quality metrics and acceptance gates.
- Define the first mini-spec-driven pilot: `Market-first anomaly backtrace v0`.

## Scope

In scope:

- process design;
- mini-specs;
- agent role contracts;
- metrics and cycle state;
- first market-first research spike specification.

Out of scope for this proposal:

- production Telegram delivery;
- public dashboard;
- trading recommendations;
- full automation before the first manual cycle works;
- broad multi-agent swarm beyond the first three roles.

## Success Criteria

- A human can read the spec and understand how the cycle starts and ends.
- Each agent has a clear role, inputs, outputs, and failure conditions.
- The first spike can be handed to a Runner agent without rereading the whole Telegram thread.
- The Auditor can reject weak work using explicit criteria.
- The Synthesizer can update memory/state and choose the next cycle without repeating failed ideas.
