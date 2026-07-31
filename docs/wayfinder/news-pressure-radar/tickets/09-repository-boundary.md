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

## Forward Note (2026-07-31, captured after Ticket 07 closed, not resolved)

Airat raised this ticket's exact question unprompted right after closing Ticket 07 ("мы в итоге этот проект будем совмещать с signal-mind или будем выводить... это надо конкретно хорошо продумать"), independently of this ticket's formal block. Claude checked the Decision Shape criteria against current state and found **4 of 6 already satisfied**: stable report contract (Ticket 04), accepted quality gate (Ticket 03), source strategy settled (Ticket 05), delivery policy settled (Ticket 07) — all closed the same session this question was raised. Still unmet: **OpenSpec exists** (Ticket 08, open) and an **unexplained criterion, "repo access/push flow restored"** — this phrase predates this session's detailed memory (from the 2026-07-28 map creation) and nobody could verify what it refers to; ask Airat directly rather than guessing. A third, implicit criterion from the Starting Assumption — a stable daily regimen proven over several real days of operation — isn't in the Decision Shape checklist but can't be verified from this checkout either (see the second-session snapshot's note on runtime data). Airat was offered a 3-way bounded choice (grill this ticket now despite the formal block / capture a quick note and wait for Ticket 08 / his own answer) and explicitly deferred the choice itself to the next session rather than picking one — do not assume an answer, re-offer the same choice.
