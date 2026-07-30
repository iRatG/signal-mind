# Ticket 02 - Signal Ontology

Status: open
Type: grilling
Labels: `wayfinder:grilling`
Claim: unclaimed
Blocks: Ticket 04 - Report Prototype, Ticket 06 - Evaluation Harness, Ticket 08 - OpenSpec Bridge

## Question

What exactly counts as a "news pressure signal" in this project?

## Why This Matters

Without a shared ontology, every score, section, and quality rule becomes arbitrary. The code already has clusters, pressure score components, anomaly scores, and report sections; this ticket turns those into a product language.

## Decision Shape

Define the canonical signal types and their evidence:

- Main pressure: high volume plus broad source spread.
- Rising impulse: unusually recent acceleration versus baseline.
- Persistent background: a theme that stays active for many days.
- Synchronized story: several sources converge on the same story.
- One-source anomaly: narrow source concentration that may be early signal or noise.
- Noise/routine: recurring rubrics, calendar fillers, market table churn, and unsupported LLM claims.

For each type, decide:

- required evidence;
- typical false positives;
- what the report should say;
- whether it can trigger delivery.

## Starting Assumption

The system should prioritize interpretable evidence over model cleverness: every signal needs visible source count, representative headlines, period, and reason for ranking.

## Working Decision

Cross-source repetition is a core signal of news pressure. If several independent sources talk more often about the same theme, the system may treat the theme as statistically meaningful agenda pressure. This alone does not prove market influence; it only qualifies the news side of the relationship. Market influence or market trace requires a separate abnormal/relative market movement check in the event window.
