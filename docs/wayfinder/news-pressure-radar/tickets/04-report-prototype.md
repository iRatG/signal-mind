# Ticket 04 - Report Prototype

Status: open
Type: prototype
Labels: `wayfinder:prototype`
Claim: unclaimed
Blocked By: Ticket 01 - Product Contract, Ticket 02 - Signal Ontology (both closed 2026-07-31 — this ticket is now unblocked)
Blocks: Ticket 07 - Delivery Policy, Ticket 08 - OpenSpec Bridge

## Question

What should the first excellent News Pressure Radar report look like?

## Why This Matters

The report is the product surface. Before dashboard or delivery automation, Airat needs a format that is quick to scan and trustworthy enough to read repeatedly.

## Prototype Scope

Create one concrete report prototype in Markdown with:

- title and period;
- executive summary;
- main pressure signal;
- rising signals;
- synchronized stories;
- one-source anomalies;
- noise section;
- source/evidence links;
- watch-next list;
- quality status block.

## Decision Shape

The ticket resolves when Airat accepts one report shape as the target for OpenSpec.

## Forward Note (2026-07-31, captured while grilling Ticket 01, not resolved)

Airat sketched a three-section daily structure, richer than the Prototype Scope list above — reconcile the two when this ticket is actually claimed:

1. **General** — what happened in the news, which index/instrument looked more correlated or volatile. Terminology for this section still needs picking (ties to Ticket 02 - Signal Ontology).
2. **Process/technical** (decided 2026-07-31: lives OUTSIDE the daily product report — a separate internal log, candidate home `db/knowledge.md`, not a report section) — which of *our own* metrics fired or misfired this run, an evolutionary log of what's kept vs dropped in the method itself. This is Ticket 11's audit/iterate loop, deliberately kept out of the product surface so the daily report stays about news/market, not method-navel-gazing.
3. **Overall progress** — a meta read on whether the system is advancing at all, "metrics of our functionality" rather than metrics of the market. Whether this stays in the daily report (as a short takeaway) or also moves out with #2 is still open — only #2's placement was decided so far.

Weekly cadence (once added, see Ticket 01) could add a word-cloud / most-frequent-terms view across the week on top of the daily structure.
