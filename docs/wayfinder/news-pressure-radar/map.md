# Wayfinder Map: MADPAC

Status: charted
Tracker: local markdown
Created: 2026-07-28
Owner: Airat + Logos
Project Codename: MADPAC

## Destination

Reach a handoff-ready OpenSpec for MADPAC: a reliable personal analytical loop that connects news pressure, public speech, market traces, experiment logs, and agentic research process into one practical project.

This map ends when the route from current spike to implementation spec is clear: product contract, signal model, quality gate, report shape, delivery policy, evaluation method, and repository boundary are decided.

## Notes

- Domain: `signal-mind`, branch `news-pressure-radar-spike`.
- Project name clarified on 2026-07-28: call the whole project `MADPAC`. Existing terms remain descriptive subdomains: `News Pressure Radar` for news-pressure collection/reporting and `News-Market Coupling` for market-trace research.
- Current system: 5 active sources, SQLite store, daily/weekly/monthly/history regimen, graph clustering, DeepSeek labeling, `pressure_score_v2`, Markdown reports, JSON run status, systemd timer.
- Current operating stance: analytical contour first, no public product or dashboard yet.
- Product direction clarified on 2026-07-28: the first MVP is for understanding the world through digital traces, not for trading recommendations. Market data is evidence of social/economic reaction, not a buy/sell target.
- Market universe clarified on 2026-07-28: start with Moscow Exchange indices, sector indices, and ruble FX pairs against USD, EUR, and CNY. Individual stocks are out of the pilot unless a later ticket proves they are needed.
- Meta-goal clarified on 2026-07-28: the project is also a training ground for a disciplined multi-agent process. The desired process is: clarify a foggy idea, write a technical/spec artifact, implement a spike, independently review quality, decide whether to accept or iterate, then preserve learnings.
- Delivery is downstream from analysis. Telegram publication remains disabled until explicitly approved.
- Use these skills in later sessions: `wayfinder`, `grilling`/`grill-me` (one question at a time — no `grill-up` skill exists in `mattpocock/skills`, confirmed against the upstream repo tree on 2026-07-31), `cultural-research-story` only if the work touches media/source research, and OpenSpec/spec-driven workflow once the map has enough decisions.
- Work one decision ticket at a time. Research tickets may run in parallel; grilling tickets require Airat in the loop.
- Known current status on 2026-07-28 UTC: daily run completed but `send_allowed=false` due to 1 quality flag; weekly has 7 quality flags; history has 1 quality flag.
- Standing preferences clarified 2026-07-31:
  - The coupling idea deliberately allows both causal directions — news pressure shaping market traces, and markets moving first with news tone/cadence (e.g. top-10 headlines/hour) shifting afterward. Direction and news-granularity are open questions, not a decision — they feed Ticket 01's "primary value" and Ticket 10's "news-first vs market-first vs both" question, not a standing assumption to build against yet.
  - Airat weighs functionality and the shape of the implementation approach above code polish at this stage — this is a planning-heavy effort, not a production-hardening one.
  - Working order is idea → plan → tools → implementation, in that order; don't let tooling setup or code get ahead of an unresolved decision ticket.
- **Session resume point (2026-07-31, commit `894bc43`, pushed to `origin/madpac-openspec-wayfinder`):** Tickets 01 and 02 closed this session — see their `## Working Decision` + `## Grilling Transcript` for the full record, not just the one-line gists below. Next session should claim [Ticket 03 - Quality Gate Contract](tickets/03-quality-gate-contract.md) first (see `## Current Frontier`), and check Tickets 04, 05, 06, 07, 10, 11 for forward-notes left mid-interview before re-deciding anything on them from scratch.

## Decisions So Far

- [Ticket 01 - Product Contract](tickets/01-product-contract.md) — solo/Airat-only, daily from day one, retrospective-only correlation framing (no directional forecasts), success measured by a backtested reference correlation rather than gut feel; report/quality-gate/metrics-core kept as three separate layers (Tickets 01+04 / 03 / 11).
- [Ticket 02 - Signal Ontology](tickets/02-signal-ontology.md) — 6 canonical signal types (Main pressure, Rising impulse, Persistent background, Synchronized story, One-source anomaly, Noise/routine) each grounded in existing code, sharing 4 cross-cutting principles: versioned parameter registry, no empty modules, state transitions over permanent labels, and a reused news-plane×market-plane cross-check.

## Current Frontier

- [Ticket 03 - Quality Gate Contract](tickets/03-quality-gate-contract.md) — **resume here next session**
- [Ticket 05 - Source Strategy](tickets/05-source-strategy.md)
- [Ticket 10 - Market Coupling Model](tickets/10-market-coupling-model.md)
- [Ticket 11 - Agentic Delivery Process](tickets/11-agentic-delivery-process.md)

## Blocked Tickets

- [Ticket 04 - Report Prototype](tickets/04-report-prototype.md) depends on Product Contract and Signal Ontology.
- [Ticket 06 - Evaluation Harness](tickets/06-evaluation-harness.md) depends on Signal Ontology and Quality Gate Contract.
- [Ticket 07 - Delivery Policy](tickets/07-delivery-policy.md) depends on Product Contract, Quality Gate Contract, and Report Prototype.
- [Ticket 08 - OpenSpec Bridge](tickets/08-openspec-bridge.md) depends on Product Contract, Signal Ontology, Quality Gate Contract, Report Prototype, and Delivery Policy.
- [Ticket 09 - Repository Boundary](tickets/09-repository-boundary.md) depends on OpenSpec Bridge and Evaluation Harness.

## Not Yet Specified

- Whether the system should later become a multi-user product, a private intelligence assistant, or a publishing workflow.
- Whether to add a dashboard, and what exact dashboard decisions become necessary after reports prove useful.
- Whether to use embeddings, lemmatization, or both as the next semantic clustering layer.
- Whether to keep Russian-only scope or add foreign news sources for geopolitical context.
- Whether the final project should integrate with Obsidian, Telegram channel publishing, email reports, or all of them.
- Whether agent-written interpretations should be treated as prose summaries only or as structured claims that must pass stricter evidence checks.
- Whether the market coupling layer should use event-study statistics, simple association tables, supervised models, or all of them in stages.
- Which parts of the work should be handled by separate agents versus one main agent with explicit review phases.

## Out Of Scope

- Trading signals or investment advice. This project measures agenda pressure, not buy/sell decisions.
- Individual stock-picking in the first pilot. Sector and broad indices are the first market lens.
- Public automatic publishing before a delivery policy and quality gate are closed.
- Full article scraping as a first-class dependency. The current MVP works from headlines; full text can return only if a later decision proves it necessary.
- A polished SaaS dashboard. Dashboard work starts only after the report contract is useful for repeated use.
- Rewriting the existing spike from scratch. The map should preserve the working regimen and clarify the next decisions around it.

## Suggested Session Order

1. ~~Resolve [Ticket 01 - Product Contract](tickets/01-product-contract.md) with `grill-me`.~~ Closed 2026-07-31.
2. ~~Resolve [Ticket 02 - Signal Ontology](tickets/02-signal-ontology.md) with domain modeling.~~ Closed 2026-07-31.
3. **← Next.** Resolve [Ticket 03 - Quality Gate Contract](tickets/03-quality-gate-contract.md) using current run failures as examples.
4. Resolve [Ticket 05 - Source Strategy](tickets/05-source-strategy.md) with research.
5. Resolve [Ticket 10 - Market Coupling Model](tickets/10-market-coupling-model.md) before any market-code implementation.
6. Resolve [Ticket 11 - Agentic Delivery Process](tickets/11-agentic-delivery-process.md) so the project teaches the workflow, not only the market idea.
7. Build [Ticket 04 - Report Prototype](tickets/04-report-prototype.md) as a concrete markdown/email/Telegram prototype.
8. Turn the settled decisions into OpenSpec through [Ticket 08 - OpenSpec Bridge](tickets/08-openspec-bridge.md).
