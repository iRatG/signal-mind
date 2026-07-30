# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root, or
- **`CONTEXT-MAP.md`** at the repo root if it exists — it points at one `CONTEXT.md` per context. Read each one relevant to the topic.
- **`docs/adr/`** — read ADRs that touch the area you're about to work in.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

This is a single-context repo:

```
/
├── CONTEXT.md          (not yet created)
├── docs/adr/            (not yet created)
└── src/
```

Existing domain context lives elsewhere until `/domain-modeling` runs:

- `CLAUDE.md` — protected data files, static Habr-article assets, working style
- `docs/wayfinder/news-pressure-radar/map.md` — the MADPAC project's terminology and decisions so far (Signal Mind / News Pressure Radar / News-Market Coupling)
- `memory/project_signal_mind.md` — accumulated cross-session project memory

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md` once it exists — until then, stay consistent with the sources above. Don't drift to synonyms the project's docs explicitly avoid (e.g. this project measures "agenda pressure," not "sentiment"; it studies "coupling," not "signals" for trading).

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (event-sourced orders) — but worth reopening because…_
