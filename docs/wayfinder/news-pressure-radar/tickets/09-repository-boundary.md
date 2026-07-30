# Ticket 09 - Repository Boundary

Status: blocked
Type: grilling
Labels: `wayfinder:grilling`
Claim: unclaimed
Blocked By: Ticket 06 - Evaluation Harness, Ticket 08 - OpenSpec Bridge

## Question

When should News Pressure Radar remain inside `signal-mind`, and when should it become its own repository?

## Why This Matters

`signal-mind` is a good incubator, but the radar can become a separate product or service. Extracting too early creates overhead; extracting too late mixes project identities and slows deployment.

## Decision Shape

Define extraction criteria:

- stable report contract;
- stable daily regimen for several days;
- accepted quality gate;
- source strategy settled;
- delivery policy settled;
- OpenSpec exists;
- repo access/push flow restored.

## Starting Assumption

Keep it inside `signal-mind` until the MVP report and delivery policy are stable. Extract only when the first repeatable user value is proven.
