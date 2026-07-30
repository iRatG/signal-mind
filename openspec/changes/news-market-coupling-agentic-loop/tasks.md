# Tasks: MADPAC Agentic Loop

Status: draft

## Planning

- [x] Name the combined project `MADPAC`.
- [x] Capture Wayfinder map.
- [x] Add `Ticket 10 - Market Coupling Model`.
- [x] Add `Ticket 11 - Agentic Delivery Process`.
- [x] Create detailed project process spec.
- [x] Create external skill research notes.
- [x] Create OpenSpec draft change.
- [x] Resolve whether the first cycle uses 3 or 5 instruments.
- [x] Resolve exact first instrument list.
- [x] Resolve first anomaly-threshold strategy: use a metric pool/ensemble, not one fixed rule.
- [x] Resolve first metric-pool size: 10 metrics.
- [x] Move exact first metric list out of Grill: Runner brief carries a predeclared candidate pool; Auditor/Synthesizer judge it.
- [x] Resolve metric scoring principle: metric thresholds are calibratable, not final constants.
- [x] Resolve metric gating principle: store raw metric vectors first; postpone hard ensemble/voting rules until calibration.
- [ ] Resolve metric calibration report format.
- [x] Resolve metric scoring rubric: fire rate, lift vs controls, stability, complementarity, instrument specificity, interpretability.
- [x] Resolve experiment logging principle: every run stores enough structured metadata to analyze the research process itself.
- [x] Resolve persistent experiment storage approach: hybrid file artifacts + SQLite registry from the first designed cycle.
- [ ] Resolve dataframe/runtime choice after checking repository dependencies.
- [x] Resolve first control-window method: random same-quarter days + adjacent calm days; matched-volatility later.
- [x] Resolve v0 max calibration depth: one quarter, with shorter contexts for comparison.

## Mini-Specs

- [x] Create mini-spec index.
- [x] Draft `Market-first anomaly backtrace v0`.
- [ ] Review mini-spec with Airat.
- [x] Draft Runner brief for `Market-first anomaly backtrace v0`.
- [ ] Review Runner brief before launching Runner.

## Agent Roles

- [x] Resolve first Runner boundary: separate Runner agent, but only after planning/brief is ready.
- [ ] Write Runner role prompt.
- [ ] Write Auditor role prompt.
- [ ] Write Synthesizer role prompt.
- [ ] Define exact artifact handoff format.
- [ ] Define cycle state JSON schema.
- [ ] Define hypothesis registry schema.
- [ ] Define experiment registry schema.

## First Manual Cycle

- [ ] Run Hypothesis Runner.
- [ ] Run Independent Auditor.
- [ ] Run Synthesizer.
- [ ] Update memory/spec after synthesis.

## Later Implementation

- [ ] Add tests for anomaly detection.
- [ ] Add tests for no lookahead leakage.
- [ ] Add tests for hypothesis fingerprint blocking.
- [ ] Add tests for audit claim-language checks.
