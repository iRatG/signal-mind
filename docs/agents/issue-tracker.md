# Issue tracker: Local Markdown

Issues and specs (you may know a spec as a PRD) for this repo live as markdown files — no GitHub Issues, even though `origin` points at GitHub.

## Wayfinder efforts

This repo already runs `/wayfinder` under its own convention, established before this skill set was installed. Wayfinder efforts do **not** use `.scratch/` — they live at:

- **Map**: `docs/wayfinder/<effort-slug>/map.md`
- **Ticket**: `docs/wayfinder/<effort-slug>/tickets/NN-<slug>.md`, numbered from `01`

Ticket header lines, in this order (omit any that don't apply):

```
Status: open | blocked | closed
Type: task | research | grilling | prototype
Labels: `wayfinder:<type>`
Claim: unclaimed | claimed by <name>
Blocked By: <ticket titles>
Blocks: <ticket titles>
```

Body sections, in this order: `## Question`, `## Why This Matters`, `## Decision Shape`, `## Starting Assumption` (optional), `## Working Decision` (appended once the ticket is resolved).

- **Frontier**: scan `docs/wayfinder/<effort>/tickets/` for tickets that are `Status: open`, `Claim: unclaimed`, and unblocked (every ticket listed in `Blocked By:` is `Status: closed`).
- **Claim**: set `Claim: claimed by <name>` and save before starting work.
- **Resolve**: append the answer under `## Working Decision`, set `Status: closed`, then append a one-line decision summary + link to the map's "Decisions So Far" section.
- **Grilling tickets additionally get a `## Grilling Transcript` section**, appended after `## Working Decision`: the actual questions asked (with the recommendation offered), and the answer given, close to how it was actually said — not only the synthesized conclusion above it. Airat asked for this explicitly on 2026-07-31: the synthesis can lose a phrase or framing he introduced (e.g. a term like "эталон"), and he wants the raw exchange recoverable from the ticket alone, without depending on chat history. Keep it readable, not a byte-for-byte transcript — trim filler, keep every distinct point and any term the human coined.
- **Active effort**: `news-pressure-radar` (project codename MADPAC) at `docs/wayfinder/news-pressure-radar/`. Continue this map rather than starting a new one unless the user names a genuinely separate effort.

For a brand-new wayfinder effort unrelated to MADPAC, still use `docs/wayfinder/<new-effort-slug>/` — not `.scratch/` — to keep one convention across the repo.

## Everything else (to-tickets, to-spec, qa, triage)

Non-wayfinder issues and specs use the generic local-markdown layout:

- One feature per directory: `.scratch/<feature-slug>/`
- The spec is `.scratch/<feature-slug>/spec.md`
- Implementation issues are one file per ticket at `.scratch/<feature-slug>/issues/<NN>-<slug>.md`, numbered from `01` — never a single combined tickets file
- Triage state is recorded as a `Status:` line near the top of each issue file (see `triage-labels.md` for the role strings)
- Comments and conversation history append to the bottom of the file under a `## Comments` heading

### When a skill says "publish to the issue tracker"

Create a new file under `.scratch/<feature-slug>/` (creating the directory if needed).

### When a skill says "fetch the relevant ticket"

Read the file at the referenced path. The user will normally pass the path or the issue number directly.
