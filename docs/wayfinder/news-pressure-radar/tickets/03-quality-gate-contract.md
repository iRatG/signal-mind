# Ticket 03 - Quality Gate Contract

Status: open
Type: grilling
Labels: `wayfinder:grilling`
Claim: unclaimed
Blocks: Ticket 06 - Evaluation Harness, Ticket 07 - Delivery Policy, Ticket 08 - OpenSpec Bridge

## Question

What quality gate should decide whether a report is allowed to be sent?

## Why This Matters

The regimen already writes `send_allowed`, `block_reason`, and quality flag counts. On 2026-07-28 the daily run had 1 flag and was blocked. The product decision is whether that behavior is correct or too strict.

## Decision Shape

Set policy for:

- collector errors;
- empty article or cluster counts;
- unsupported numeric claims;
- one-source persistent clusters;
- thin clusters in daily versus weekly/monthly/history modes;
- LLM fallback to heuristic labels;
- maximum tolerated quality flags by mode;
- whether a blocked report should be silently stored, sent with warning, or escalated to Airat.

## Starting Assumption

Daily reports should tolerate a small number of low-severity flags only if the flagged cluster is excluded from the main signal. Weekly/monthly/history reports can remain dry-review artifacts until evaluation is stronger.
