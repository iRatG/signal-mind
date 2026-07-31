# Ticket 07 - Delivery Policy

Status: open
Type: grilling
Labels: `wayfinder:grilling`
Claim: unclaimed
Blocked By: Ticket 01 - Product Contract, Ticket 03 - Quality Gate Contract, Ticket 04 - Report Prototype (all three closed 2026-07-31 — this ticket is now unblocked)
Blocks: Ticket 08 - OpenSpec Bridge

## Question

When, where, and how should News Pressure Radar send reports automatically?

## Why This Matters

The timer is running, but delivery is intentionally not enabled. Delivery changes the risk profile: a bad report becomes an interruption, and a public report becomes reputational output.

## Decision Shape

Decide:

- channel: Telegram DM, MetaStore, email, Obsidian note, or none;
- timing: daily morning, weekly review, or manual pull;
- failure behavior: silence, warning, or status ping;
- content length by channel;
- whether blocked reports should be summarized;
- who can receive reports later.

## Starting Assumption

Start with Telegram DM to Airat only, daily, only when `send_allowed=true`; send blocked-status only after repeated failures or explicit request.

## Forward Note (2026-07-31, captured while grilling Ticket 01, not resolved)

Airat mentioned either email or Telegram as acceptable daily channels, without ranking one over the other — confirms the Starting Assumption's direction but doesn't lock the specific channel yet. Decide for real when this ticket is claimed.
