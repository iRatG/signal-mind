# Ticket 07 - Delivery Policy

Status: blocked
Type: grilling
Labels: `wayfinder:grilling`
Claim: unclaimed
Blocked By: Ticket 01 - Product Contract, Ticket 03 - Quality Gate Contract, Ticket 04 - Report Prototype
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
