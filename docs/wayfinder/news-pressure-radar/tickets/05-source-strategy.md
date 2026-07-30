# Ticket 05 - Source Strategy

Status: open
Type: research
Labels: `wayfinder:research`
Claim: unclaimed
Blocks: Ticket 06 - Evaluation Harness, Ticket 08 - OpenSpec Bridge

## Question

Which sources should the MVP trust, compare, exclude, or keep as disabled candidates?

## Why This Matters

The current active set is Kommersant, Interfax, Lenta, Vedomosti, and RIA. RBC is disabled because of Qrator blocking; TASS returned 403 from the server. Source mix directly shapes the radar's view of agenda pressure.

## Research Scope

Investigate:

- whether RBC has a stable RSS/API/archive route that avoids brittle scraping;
- whether TASS has a legitimate accessible route;
- whether additional sources should cover business, state media, opposition/independent media, regional signals, or international wires;
- what each source contributes and what bias/noise it adds;
- whether source categories should become part of scoring.

## Decision Shape

Produce a source policy:

- active sources for MVP;
- disabled candidates with reasons;
- source categories;
- available text depth by source: headline only, headline plus description/lead, or full article candidate;
- minimum source diversity for each signal type;
- future source backlog.
