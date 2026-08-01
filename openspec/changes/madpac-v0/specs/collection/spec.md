# Spec: Collection

## Purpose

Defines which Russian news sources MADPAC collects from, how each is reached, how legal/network constraints on a source are handled, and the shape every collected row must conform to before it reaches analysis.

## ADDED Requirements

### Requirement: Active Source Set

The system SHALL collect from the following 14 active sources, each configured with a `kind` (`html_archive` | `rss` | `telegram` | `dataset`) and a `route` (`direct` | `vpn`): kommersant, interfax, lenta, vedomosti, ria (`html_archive`, direct); rbc (`rss` via `rssexport.rbc.ru`, direct — route pending verification from the collector's production host); gazeta.ru, iz.ru, forbes.ru, banki.ru, novayagazeta.ru, agents.media (`rss`, direct); meduza.io (`rss`, vpn); svoboda.org (`rss`, vpn — conditionally active pending feed URL confirmation before being wired into the regimen).

Scope SHALL remain Russian-language sources only. English-language wire services (Bloomberg, Reuters, AP) SHALL stay parked in backlog until a dedicated English-language tokenization/clustering pipeline is justified.

#### Scenario: Source with a confirmed RSS route

- **WHEN** the collector polls a source configured with `kind: rss` and `route: direct`
- **THEN** it fetches the source's RSS feed directly, with no VPN routing

#### Scenario: Candidate source not yet verified

- **WHEN** a candidate source's RSS feed URL or legal/foreign-agent status has not been confirmed (e.g. fontanka.ru, Verstka, iStories, Novaya Gazeta Europe, or The Bell)
- **THEN** the system SHALL keep it in backlog rather than activating it, until a manual verification step is performed

#### Scenario: MOEX itself as a news source

- **WHEN** MOEX's own corporate/economic press releases are proposed as a 15th source, distinct from its existing role as the `analysis` capability's market-data API
- **THEN** the system SHALL treat this as its own forward-note item requiring the same feasibility check (RSS/API route, terms of use, reachability) as any other candidate, not as generic backlog speculation

### Requirement: Explicit Contractual Prohibition Excludes A Source Outright

The system SHALL NOT collect from a source whose published terms of use explicitly forbid RSS/feed ingestion without a written agreement, regardless of whether a technical bypass route exists. This applies today to tass.ru/tass.com.

#### Scenario: Terms of use forbid feed ingestion

- **WHEN** a source's terms of use explicitly prohibit RSS/feed ingestion without a written agreement
- **THEN** the system SHALL exclude that source outright, even if a technical workaround (e.g. a different subdomain or scraping route) is available

### Requirement: Network-Blocked Sources Route Through VPN, Not Exclusion

Where a source is blocked at the network level from Russia (a Roskomnadzor block) but carries no other legal exclusion, the system SHALL set that source's `route` to `vpn` in `config/news_pressure_sources.yaml` and continue collecting it. VPN routing SHALL be applied per-source, never as a collector-wide toggle.

#### Scenario: RKN-blocked source with no legal exclusion

- **WHEN** a source is Roskomnadzor-blocked at the network level and has no explicit contractual prohibition
- **THEN** the system SHALL route only that source's requests through the existing VPN capability and continue collecting it

### Requirement: Legal Designation Is A Descriptive Label, Not An Inclusion Gate

Foreign-agent, undesirable-organization, and Roskomnadzor-block designations SHALL be recorded as descriptive per-source facts (with country of registration where foreign), never as a reason to exclude a source, for as long as the project stays private/Airat-only and does not publish or redistribute collected content.

This policy MUST be re-checked whenever the project's publishing/redistribution posture changes. As of 2026-07-31 the check was performed against Ticket 07's delivery decision (all three delivery surfaces confirmed private, including VPS hosting motivated by uptime only) and the policy was confirmed unchanged.

#### Scenario: Source carries a legal designation

- **WHEN** a source carries a foreign-agent, undesirable-organization, or RKN-blocked designation
- **THEN** the system SHALL record it as a descriptive tag on that source and SHALL still include the source in collection, unless the project has begun publishing or redistributing content to an outside audience

### Requirement: Source Categorization

Every active source SHALL carry three recorded axes: (1) alignment — `state-aligned` | `independent-opposition` | `business`; (2) a `foreign` tag with country of registration, where applicable; (3) its legal-designation tags per the label-not-gate policy above. Any source added after v0 MUST be categorized on all three axes, and MUST come with a short coverage description, at the time it is added, not deferred.

#### Scenario: New source added post-v0

- **WHEN** a new source is added to the active set after v0
- **THEN** it SHALL be assigned an alignment category, a foreign/country tag if applicable, its legal-designation tags, and a short coverage description before it starts contributing to collected data

### Requirement: Category Spread Report Field

Every cluster SHALL receive a non-numeric, descriptive `category_spread` field with value `state-only` | `independent-only` | `mixed`, computed from which active sources' alignment categories covered that cluster. This field carries no threshold and no gating logic — it exists so a reader can see whether a "synchronized story" reflects genuine cross-spectrum agreement or only state-aligned unanimity.

#### Scenario: Mixed-alignment coverage

- **WHEN** a cluster is covered by both state-aligned and independent-opposition sources
- **THEN** the system SHALL tag that cluster's `category_spread` as `mixed`, with no threshold applied to gate or filter based on this value

### Requirement: Pluggable Ingestion Contract

Every `kind` handler MUST produce output in the same shape — `(url, title, published_date, source, lead_text?)` — regardless of collection mechanism, so the analysis layer never needs to know which mechanism produced a row. `telegram` and `dataset` kinds are reserved in the contract for future ingestion mechanisms not yet implemented.

#### Scenario: New ingestion mechanism added later

- **WHEN** a new `kind` (e.g. `telegram`) is implemented in the future
- **THEN** it SHALL emit rows in the existing `(url, title, published_date, source, lead_text?)` shape, requiring no changes to the downstream analysis layer

### Requirement: Text Depth By Source

Headline-only SHALL remain the default text depth. Where a source's collection mechanism already provides a description/lead field at no extra cost (all `rss`-based sources in the active set), the system SHALL capture it as `headline+lead` text depth with no additional scrape request. Full article text SHALL NOT be collected as part of the live daily/weekly/monthly/history regimen; it remains the separate, out-of-band `ru_news_archive_loader.py` backfill path into `db/hf_news.db` (read-only).

#### Scenario: RSS source with a lead field

- **WHEN** an `rss`-based source's feed response includes a description/lead field
- **THEN** the system SHALL store it as `headline+lead` without issuing an additional scrape request

#### Scenario: Archive source with no lead field

- **WHEN** a source's collection mechanism (`html_archive`) does not provide a description/lead field
- **THEN** its text depth SHALL remain headline-only

### Requirement: RSS Polling Cadence And Deduplication

RSS-based sources SHALL be polled hourly (not once daily) to avoid losing items on a source whose feed rolls over faster than the polling interval. Deduplication SHALL apply both a URL-based check and a fuzzy-title check, so that breaking-news republication across successive polls does not inflate counts as false duplicates or, conversely, silently drop genuinely new items.

#### Scenario: Fast-rolling feed

- **WHEN** an RSS source's feed rolls over more than once between successive hourly polls
- **THEN** the combination of hourly polling, URL dedup, and fuzzy-title dedup SHALL ensure no items are silently lost and no false duplicate-count inflation occurs from breaking-news republication

### Requirement: Source Diversity Data Must Support Downstream Computation

Every collected row MUST retain a recoverable per-source identity (not a shared re-syndication domain) so that the `analysis` capability's `source_spread` metric can be computed meaningfully. The numeric threshold applied to that metric is defined and calibrated in the `analysis` capability, not here; this requirement only obligates collection to preserve the data `analysis` depends on.

#### Scenario: Source identity preserved

- **WHEN** a cluster is built from articles collected across multiple active sources
- **THEN** each article's `source` field SHALL identify the actual originating outlet, never a shared syndication front, so that `source_spread` reflects real newsroom diversity
