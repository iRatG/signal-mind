# Ticket 11 - Agentic Delivery Process

Status: open
Type: grilling
Labels: `wayfinder:grilling`
Claim: unclaimed
Blocks: Ticket 08 - OpenSpec Bridge, Ticket 09 - Repository Boundary

## Question

What multi-agent process should this project use so the work itself becomes a reusable methodology?

## Why This Matters

Airat clarified that News-Market Coupling is not only a data idea. It is also a training ground for learning how to use skills and independent agents correctly: one agent clarifies a foggy idea, another turns it into a technical/spec artifact, another implements, another checks quality, and the cycle repeats if the result is weak.

## Working Decision

The desired process is iterative:

- clarify the foggy idea;
- create a decision map;
- write a technical/spec artifact;
- implement a narrow spike;
- review quality independently;
- accept, reject, or iterate;
- preserve the decision and learning in memory/spec/docs.

## Decision Shape

Define:

- which roles exist: wayfinder, researcher, spec writer, implementer, tester, reviewer, summarizer;
- which artifacts each role produces;
- when a task is allowed to move from idea to code;
- what quality gate accepts a spike;
- what happens when a review fails;
- how to keep the process lightweight enough for a solo project.

## Starting Assumption

Use a small process first: `wayfinder/grill -> spike spec -> implementation -> independent review -> grill-up summary -> memory/spec update`. Add more agents only when parallel work is genuinely useful.
