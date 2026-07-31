# Tasks: MADPAC Agentic Loop

Status: draft

## Planning

- [x] Name the combined project `MADPAC`.
- [x] Capture Wayfinder map.
- [x] Add `Ticket 10 - Market Coupling Model`.
- [x] Resolve `Ticket 10 - Market Coupling Model` via a live grill (2026-07-31), independent of this draft: market-first for v0, news-first/bidirectional and pairwise signals deferred to stage 2; instruments and windows registry/config-driven, not hardcoded. Surfaced two follow-ups routed to other tickets rather than decided here: an Editor/rendering role (→ Ticket 04) and Runner/Auditor agent separation (→ Ticket 11).
- [x] Add `Ticket 11 - Agentic Delivery Process`.
- [x] Create detailed project process spec.
- [x] Create external skill research notes.
- [x] Create OpenSpec draft change.
- [x] Resolve whether the first cycle uses 3 or 5 instruments. — **updated 2026-07-31:** superseded by Ticket 10's registry-driven approach (no fixed instrument count in code; a flag on the registry table marks the active v0 subset).
- [x] Resolve exact first instrument list. — **updated 2026-07-31 per [Ticket 10](../../../docs/wayfinder/news-pressure-radar/tickets/10-market-coupling-model.md)'s live grill:** `IMOEX` + oil&gas + financial sector indices + all 3 FX pairs (`USD/EUR/CNY`, not just USD/CNY as this draft originally had); `RTSI` inactive. Instruments live in a registry table, not hardcoded.
- [x] Resolve first anomaly-threshold strategy: use a metric pool/ensemble, not one fixed rule. — confirmed independently by Ticket 10's live grill 2026-07-31 (raw vector, no hard gate).
- [ ] Resolve first metric-pool size: 10 metrics. — **unchecked 2026-07-31:** this specific count was never grilled with Airat; Ticket 10 confirmed only "several metrics" as a principle. Treat the 10-item list in the mini-spec/design.md as an unconfirmed candidate menu, not a decision.
- [x] Move exact first metric list out of Grill: Runner brief carries a predeclared candidate pool; Auditor/Synthesizer judge it.
- [x] Resolve metric scoring principle: metric thresholds are calibratable, not final constants.
- [x] Resolve metric gating principle: store raw metric vectors first; postpone hard ensemble/voting rules until calibration.
- [ ] Resolve metric calibration report format.
- [x] Resolve metric scoring rubric: fire rate, lift vs controls, stability, complementarity, instrument specificity, interpretability.
- [x] Resolve experiment logging principle: every run stores enough structured metadata to analyze the research process itself.
- [x] Resolve persistent experiment storage approach: hybrid file artifacts + SQLite registry from the first designed cycle.
- [ ] Resolve dataframe/runtime choice after checking repository dependencies.
- [ ] Resolve first control-window method: random same-quarter days + adjacent calm days; matched-volatility later. — **unchecked 2026-07-31:** never grilled with Airat on Ticket 10; this draft's proposal, not a decision. Revisit at calibration/build time.
- [ ] Resolve v0 max calibration depth: one quarter, with shorter contexts for comparison. — **unchecked 2026-07-31:** same as above, not grilled.

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
