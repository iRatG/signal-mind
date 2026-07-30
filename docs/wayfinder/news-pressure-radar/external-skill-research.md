# External Skill Research Notes

Status: draft
Created: 2026-07-28

## Sources Checked

- Matt Pocock `skills`: https://github.com/mattpocock/skills
- Matt Pocock skills v1.1 changelog: https://www.aihero.dev/skills/skills-changelog-v1-1-wayfinder-to-spec-to-tickets-grilling-improvements
- Wayfinder docs: https://github.com/mattpocock/skills/blob/main/docs/engineering/wayfinder.md
- OpenSpec: https://github.com/Fission-AI/OpenSpec and https://openspec.pro/

## What Matches Our Direction

- Skills should be small, composable, and adaptable rather than a rigid framework.
- Wayfinder is upstream of spec work: use it when the route is still foggy.
- Wayfinder tickets should resolve decisions, not implementation slices.
- After wayfinder, the flow should move toward spec and tickets.
- OpenSpec is useful when requirements should live in the repo before code.
- TDD should keep implementation focused; broader refactoring/review belongs after implementation.

## Missing Pieces To Add

### 1. Explicit `to-spec` Stage

Our current loop says "mini-spec", but it should have a named stage:

```text
closed/accepted decision -> mini-spec -> OpenSpec proposal when durable
```

The spec should constrain the implementation, not decorate it afterward.

### 2. Explicit `to-tickets` Stage

After a spec is accepted, implementation should be sliced into buildable tickets.

Wayfinder tickets are decision tickets. Build tickets are different: they are implementation tasks.

### 3. Research Ticket Type

For unknown external facts, use AFK research.

For News-Market Coupling examples:

- available MOEX sector index symbols;
- reliable ruble FX data sources;
- source RSS/description availability;
- event-study methods for short windows.

### 4. Prototype Ticket Type

Before a full spec, create cheap concrete artifacts.

For this project:

- one sample anomaly backtrace table;
- one sample cycle report;
- one mock audit report;
- one hypothesis registry example.

### 5. Tracker Wiring

Wayfinder works best when maps/tickets live on an issue tracker with blocking relationships.

Current state is local markdown. This is fine for the first training pass, but later we should choose:

- local markdown only;
- GitHub issues;
- OpenSpec changes plus local tickets.

### 6. Verify Stage

OpenSpec encourages plan-before-code and verifiable implementation.

Add a verify gate after implementation:

```text
implementation -> tests -> spec conformance check -> audit
```

### 7. Context Hygiene

Large multi-agent workflows need clean handoffs.

Each agent should receive:

- role prompt;
- active spec/ticket;
- relevant artifacts only;
- explicit stop condition.

Avoid dumping the whole chat into every agent.

### 8. Separate Decision Tickets From Build Tickets

Decision tickets answer questions.

Build tickets implement accepted decisions.

Do not let the same ticket both debate and build.

## Recommended Process Update

Use this full ladder:

```text
Grill / Wayfinder
-> Research or Prototype where needed
-> to-spec / mini-spec
-> to-tickets
-> TDD implementation
-> Verify
-> Independent review
-> Grill-up synthesis
-> Memory / next cycle
```

For the first run, keep it smaller:

```text
mini-spec
-> one market-first spike
-> independent audit
-> synthesis
```

Then expand the workflow only after the first cycle has real artifacts.
