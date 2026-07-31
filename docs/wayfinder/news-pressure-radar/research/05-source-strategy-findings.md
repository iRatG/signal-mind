# Ticket 05 — Source Strategy: Primary-Source Research Findings

Research date: 2026-07-31. This document gathers grounded facts for Airat to decide from; it does **not** recommend a source policy. Every claim is tagged:

- **Confirmed** — verified directly by this research (fetched the URL, read the primary doc, checked a live feed).
- **Reported but not independently verified** — credible secondary source, but the primary source could not be checked directly here.
- **Uncertain / needs manual follow-up** — thin, conflicting, or environment-limited evidence.

**Important environment caveat that applies to every "Confirmed" fetch below:** all fetches in this research were made from this research environment's network location, which is very likely outside Russia. A URL loading successfully here does **not** guarantee it is reachable from the collector's actual server (whose location/IP range determines what Qrator, Roskomnadzor DPI blocks, or geo-fencing will do to it). Conversely, a 403/401 seen here does not by itself confirm the collector's server would see the same thing (though for RBC and TASS it matches the ticket's own reported observations, which is corroborating). Every "works" finding below should be re-verified from the collector's actual host before being relied on operationally.

---

## 1. RBC (rbc.ru) — RSS/API route avoiding the Qrator-blocked front end

**Confirmed:**
- `https://rssexport.rbc.ru/rbcnews/news/30/full.rss` returned a valid RSS 2.0 feed on fetch: `<language>ru</language>`, 20+ items, full text/description fields, author, category tags, a custom `rbc_news:*` namespace, images/video links, and timestamps in both human-readable and Unix form. Most recent items at fetch time were dated 2026-07-31 (same day), covering domestic/military/political/market topics — i.e., this is a live, current feed, not a stale or decommissioned one.
- This is a **different subdomain** (`rssexport.rbc.ru`) from the Qrator-protected front end (`www.rbc.ru`). Directly re-confirmed the front-end block is real and generic: `https://www.rbc.ru/rss/` returned HTTP 401 Unauthorized on fetch (same failure mode the ticket already reports for the collector).
- RBC's general copyright/terms page (found via search, not fetched directly — `www.rbc.ru/privacy/`, `www.rbc.ru/restrictions`, `www.rbc.ru/legal`, all behind the same 401 as the rest of `www.rbc.ru`) is summarized by secondary sources as: citation is allowed free of charge "in a volume justified by the purpose of citation" with a hyperlink to `www.rbc.ru` or the specific article; any other copying/reproduction/redistribution requires written permission from JSC "ROSBUSINESSCONSULTING" (contact: `copyright@rbc.ru`). No RBC-specific clause about RSS specifically was found (contrast with Interfax and TASS below, which do explicitly ban RSS redistribution in their terms).

**Reported but not independently verified:**
- `static.feed.rbc.ru/rbc/logical/footer/news.rss` was cited by search results as another RBC RSS endpoint, but the domain did not resolve from this environment (`ENOTFOUND`) — could be a genuinely dead/renamed host, or a DNS quirk of this environment. Not confirmed either way.
- No explicit written RBC/data-partnership API terms (e.g. a documented commercial API product) were found; absence of evidence is not evidence of absence — RBC may have a commercial data product not indexed by search.

**Bottom line for this question:** a working, current, Russian-language RSS feed exists at `rssexport.rbc.ru` that appears to sidestep the Qrator wall entirely, at least from outside Russia. Whether it is reachable from the collector's server, and whether RBC's terms treat automated RSS ingestion into a downstream analysis product as acceptable "citation" use, are both open items (RBC's terms as summarized are silent on RSS specifically, unlike Interfax/TASS).

---

## 2. TASS (tass.ru) — RSS/API route and usage policy vs. scraping

**Confirmed:**
- `https://tass.ru/rss/v2.xml` (the Russian-domain feed) returned HTTP 403 Forbidden on fetch — consistent with the ticket's existing report that `tass.ru` returns 403.
- `https://tass.com/rss/v2.xml` (the separate international/English-language domain) returned a valid RSS 2.0 feed: channel title "TASS", 50+ items, English-language content (geopolitics, military, economics, sports), copyright tag `"TASS"` per item. This is a **different site** (`tass.com`, English) from `tass.ru` (Russian) — it does not solve the Russian-language ingestion need, but shows TASS does operate at least one live public RSS endpoint elsewhere in its domain family.
- `https://tass.com/terms-of-use` was fetched directly. Section 4.5 explicitly states materials may not be used "by their inclusion into sms or email newsletters, RSS feeds ... and cases of direct commercial use without written consent from TASS or concluding a relevant agreement with TASS." Non-commercial use (personal pages, educational sites) is permitted only up to "30% of the amount of the quoted Text Materials" with attribution; a site monetized by advertising or paid access does not count as non-commercial under these terms.

**Reported but not independently verified:**
- TASS is widely reported (via search, e.g. Feeder.co discovery pages) to sell commercial/licensed API and wire-feed access to institutional clients (this matches the ticket's own background note that "TASS is known to sell commercial API access to some clients"), but no specific commercial API product page or pricing was located/fetched in this research.

**Bottom line for this question:** even where TASS RSS is technically reachable (`tass.com`, English only), TASS's own published terms explicitly classify RSS ingestion as licensable use requiring a written agreement, not a free/public route. `tass.ru` itself (Russian, the domain actually wanted) returned 403 directly, matching the ticket's existing finding.

---

## 3. Common Crawl candidate domains not yet live: legal designation + RSS availability

Russian legal designations checked: "нежелательная организация" (undesirable organization, Prosecutor General's Office / Ministry of Justice) and "иностранный агент" (foreign agent, Ministry of Justice registry). All findings below are via search of secondary reporting (Interfax, RBC, Forbes.ru, Kommersant, Wikipedia's Russian-language "Список иностранных агентов" page, and Minjust's own registry page for foreign-media-agents at `minjust.gov.ru/ru/documents/7755/`), not by pulling the raw Minjust registry entry for each name — treat designation claims as **reported, not independently verified against the primary registry**, except where noted.

| Domain | Designation status (as reported) | RSS feed |
|---|---|---|
| gazeta.ru | No foreign-agent/undesirable designation found for the outlet itself in this research. | **Confirmed live**: `https://www.gazeta.ru/export/rss/first.xml` — valid RSS 2.0, `windows-1251` encoding, Russian, 10 items, most recent dated 2026-07-31. |
| fontanka.ru | No designation found. | **Uncertain** — guessed URL `fontanka.ru/fontanka.rss` returned 404; secondary sources confirm Fontanka has offered an RSS feed since 2007 and third-party RSS catalogs list it, but the current correct URL was not confirmed by direct fetch in this research. Needs manual lookup on the live site. |
| iz.ru (Izvestia) | No designation found. | **Confirmed live**: `https://iz.ru/xml/rss/all.xml` — valid RSS 2.0, Russian, 50 items, most recent 2026-07-31. |
| novayagazeta.ru | Important distinction: this is the **original Moscow-based Novaya Gazeta** (Dmitry Muratov), not "Novaya Gazeta Europe." Its Russian print license was revoked by a Moscow court on 2022-09-05 and its online-media license was revoked by Russia's Supreme Court on 2022-09-15 (reported via CPJ, Moscow Times, RSF — not independently verified against the court record here), officially for failing to label "foreign agent" content. It is **not itself** listed as "undesirable" or "foreign agent" as an organization in what was found here, but continues to publish online without a formal media license; as of April 2026 its Moscow office was reportedly searched by police (CPJ). This is a distinct entity from "Novaya Gazeta Europe" (BDR Novaja Gazeta-Europe, Latvia-based), which **was** designated an undesirable organization by the Prosecutor General's Office (reported via Interfax, Forbes.ru, Current Time). | **Confirmed live**: `https://novayagazeta.ru/feed/rss` — valid RSS 2.0, Russian, ~18 items, most recent 2026-07-31, with several items carrying "foreign agent" disclaimers on quoted individuals — i.e. the outlet itself is still publishing and distinguishing labeled content. |
| meduza.io | **Confirmed via direct fetch of secondary reporting**: designated "нежелательная организация" (undesirable organization) by Russia's Prosecutor General's Office on 2023-01-26 (per Interfax, RBC, and Meduza's own article about the designation — `meduza.io/feature/2023/01/26/...`), on top of an earlier Ministry of Justice "foreign agent" listing. Its website was also separately blocked by Roskomnadzor from Russian networks starting 2022-03-04 (network-level block, distinct from the legal designation) per multiple outlets reporting the same March 2022 blocking wave. | **Confirmed live** (from this environment): `https://meduza.io/rss/all` — valid RSS 2.0, Russian, 20 items, most recent 2026-07-31. Reachability from inside Russia is a separate question given the 2022 RKN block — not verified here (see environment caveat above). |
| forbes.ru | No designation found for the outlet. | **Confirmed live**: `https://www.forbes.ru/newrss.xml` — valid RSS 2.0, Russian, 15 items, most recent 2026-07-31. |
| banki.ru | No designation found. | **Confirmed live**: `https://www.banki.ru/xml/news.rss` — valid RSS 2.0, Russian, 25 items, most recent 2026-07-31. |

**Uncertain / needs manual follow-up:** none of the designation checks above were cross-checked against the primary Minjust registry page (`minjust.gov.ru/ru/documents/7755/`) or the Prosecutor General's undesirable-organizations list directly — only against secondary news reporting of those registries. For a decision ticket with legal consequences, that primary-registry cross-check is worth doing before finalizing.

---

## 4. Radio Svoboda (svoboda.org, RFE/RL Russian service)

**Confirmed (via secondary reporting, cross-referenced across independent outlets — RBC, Lenta.ru, Wikipedia RU):**
- Russia's Ministry of Justice added Radio Free Europe/Radio Liberty (RFE/RL) to the "undesirable organizations" registry on 2024-02-20.
- Separately and earlier, Roskomnadzor blocked the svoboda.org website from Russian networks starting the night of 2022-03-04, as part of a wave that also hit Meduza, BBC Russian, and others, citing "deliberately false publicly significant information" about the invasion of Ukraine. This is a network-level block distinct from the 2024 legal designation.
- svoboda.org itself published an article documenting the network block: `https://www.svoboda.org/a/sayt-radio-svoboda-zablokirovan-v-rossii/31732804.html` (title translates to "Radio Svoboda's site has begun to be blocked in Russia").

**Uncertain / needs manual follow-up:**
- An RSS feed listing page exists at `https://www.svoboda.org/rssfeeds` (found via search) but returned HTTP 403 Forbidden on direct fetch from this environment — could not confirm current feed URLs or content structure directly. A related legacy path `archive.svoboda.org/rss.asp` also surfaced in search but was not fetched.

**Bottom line for this question:** svoboda.org carries both the "undesirable organization" legal designation (2024, applies to the RFE/RL parent) and a standing Roskomnadzor network block (2022, applies to the domain). For a Russia-run collector this is a double barrier — legal risk and (likely) network unreachability — making it a materially different case from, say, Meduza (undesirable-org designated but this research could not confirm whether the RKN block still resolves the same way today).

---

## 5. Other independent/opposition-leaning outlets as backlog candidates

| Outlet | Legal status (as reported) | RSS |
|---|---|---|
| Verstka (verstka.media) | Russia's Ministry of Justice added Verstka to the foreign-agent registry (reported via Forbes.ru, alongside "Можем объяснить" and several individuals including Oleg Kuvaev); reporting did not specify the exact 2023 date. Continues publishing — Wikipedia RU cites 2025/2026-dated articles. No "undesirable organization" designation for the outlet was found. | Not confirmed by direct fetch in this research; not checked. |
| iStories / Важные истории (istories.media) | The Latvia-registered "IStories fonds" that founded Important Stories was designated an undesirable organization by the Ministry of Justice (reported via Forbes.ru: "«Важные истории» внесли в реестр «нежелательных» организаций"), on top of an earlier foreign-agent designation for the outlet/individuals. | Guessed URL `istories.media/feed/` returned 404 on direct fetch — correct feed path not found in this research. |
| The Bell (thebell.io) | **Reported but not independently verified against a primary registry**: multiple 2021-era Russian outlets (Kommersant, Vedomosti, Forbes.ru, Inc.) reported a private complaint asking the Prosecutor General's Office to designate The Bell a foreign agent, and The Bell (via founder Elizaveta Osetinskaya) publicly disputed the basis, stating its management company is Russia-registered and not foreign-funded. Search results in this research did **not** turn up confirmation that The Bell (the outlet, as opposed to individual staff) was ever formally added to the registry — status as of 2026 should be treated as unresolved/needs a fresh check rather than assumed either way. | `en.thebell.io/feed` returned 403 on direct fetch; not confirmed. |
| Agentstvo (agents.media) | Formed by ex-"Proyekt" (Project Media) journalists after Proyekt's US entity was designated an undesirable organization and most of its journalists were designated foreign agents (2021). Agentstvo itself was added to Russia's foreign-agent registry on 2023-04-14 (reported via OVD-Info: "В реестр «иноагентов» внесли «Агентство»..."). No undesirable-organization designation for Agentstvo itself was found. | **Confirmed live**: `https://www.agents.media/feed/` — valid RSS 2.0 with full namespace set (content, dc, atom, sy, slash, wfw), Russian, items dated July 2026. |
| Novaya Gazeta Europe (novayagazeta.eu) | Designated an undesirable organization by the Prosecutor General's Office (reported via Interfax: "«Новая газета Европа» признана нежелательной в России", and Forbes.ru). Distinct legal entity from the Moscow-based novayagazeta.ru discussed in section 3. | Not checked directly in this research. |

**Bottom line for this question:** of the five named, Agentstvo has a confirmed-live RSS feed; the others need direct feed-URL confirmation. Legally, iStories and Novaya Gazeta Europe carry the stronger "undesirable organization" designation (highest-risk tier), Verstka and Agentstvo carry "foreign agent" (lower tier, still consequential), and The Bell's status is genuinely unresolved in what this research could find — worth a dedicated check before categorizing it either way.

---

## 6. International wire services for geopolitical context (Reuters, AP, Bloomberg)

**Confirmed:**
- **Reuters**: no working official public RSS feed. Multiple sources (including a substack post specifically about this: "Returning the 'killed' RSS of Reuters from the dead") report Reuters discontinued its public RSS feeds. As a workaround check, `https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com&ceid=US:en&hl=en-US&gl=US` was fetched directly and does return a valid RSS wrapper — but it is a **Google News aggregation feed**, not an official Reuters channel, and its own metadata states it is "made available solely for the purpose of rendering Google News results within a personal feed reader for personal, non-commercial use" — i.e. not intended for ingestion into an analysis pipeline. Reuters' real institutional distribution today is the paid LSEG "Headlines Direct" product, not a free RSS feed.
- **Bloomberg**: `https://feeds.bloomberg.com/markets/news.rss` redirects (301) to `https://www.bloomberg.com/feeds/markets/news.rss`, which is a **valid, live, free, public RSS feed** — confirmed by direct fetch: RSS 2.0, English, 20 items, most recent dated 2026-07-31, with `Copyright 2026 BLOOMBERG L.P. ALL RIGHTS RESERVED` in the feed. Content is headline + description; linked articles may hit Bloomberg's paywall, but the feed itself (headline-only ingestion) is not paywalled. Bloomberg also documented (via its own press release, found via search, not fetched directly) a newer paid "Real-Time News Feeds" product for institutional/trading clients with deeper machine-readable analytics — the free RSS feed above is a separate, more limited public offering.
- **AP (Associated Press)**: no working public RSS feed found. Search results (GitHub project `rererecursive/associated-press-rss` built specifically to scrape AP into RSS because none exists; multiple RSS-generator/aggregator sites offering "AP feeds" that are actually third-party scrapes) indicate AP discontinued public RSS at some point after offering it in 2005. Direct fetch of `apnews.com` was not possible from this research environment ("Claude Code is unable to fetch from apnews.com" — an environment-level restriction, not necessarily proof of AP's own feed status, but consistent with the search findings).

**Bottom line for this question:** of the three, only Bloomberg currently has a confirmed, free, public, English-language RSS feed suitable for headline-only ingestion. Reuters' and AP's official public RSS routes are effectively gone; any "Reuters via RSS" or "AP via RSS" a scraper might find today is a third-party or Google-wrapper reconstruction, not an authoritative feed, and may carry its own terms-of-use or reliability risk. All three are English-only at these endpoints — language-mixing with the Russian-language corpus (Cyrillic vs. Latin script, and translation/framing differences) is a real integration question, not just an access one, if wire-service headlines are added for geopolitical context.

---

## Open questions for Airat

- Does the interface being designed for this ticket need to distinguish "confirmed-live RSS feed, network-reachable from outside Russia" (what this research checked) from "reachable from the collector's actual server" (what actually matters operationally)? All findings above should be re-verified from the collector's host before being relied on.
- For RBC: is ingesting `rssexport.rbc.ru` (bypassing the Qrator wall) something you're comfortable doing given RBC's terms are silent on RSS specifically (unlike Interfax/TASS, which explicitly forbid it)? Interfax is already an active scraped source under terms that explicitly ban RSS — is the ticket's existing HTML-archive scraping treated differently from RSS ingestion under those terms, or is that inconsistency worth resolving either way?
- For TASS: is a written licensing conversation with TASS (implied by their terms-of-use) in scope at all for this project, or is TASS effectively out unless a free route is found later?
- Which legal-designation tier(s) — foreign agent vs. undesirable organization vs. Roskomnadzor network block (these are three independent axes, not one) — should gate inclusion at all, versus just being a labeled/scored category per the "state-aligned / independent-opposition / foreign" split from Ticket 02's forward note?
- Should The Bell's unresolved designation status be checked against the primary Minjust registry before it's placed in any backlog category, given the ambiguity found here?
- Is the Moscow-based novayagazeta.ru (license-revoked but still publishing, no undesirable-org tag found) meant to be treated the same as, or differently from, "Novaya Gazeta Europe" (foreign-registered, undesirable-org designated) in any future source-category scheme? They are easy to conflate by name alone.
- For the international-wire question: is English-only, headline-level Bloomberg content actually useful for a Russian-agenda-pressure radar, or does it only make sense if/when a Russian-language geopolitical wire source is found?
- Do any of the "Confirmed live" RSS URLs found here need to be revisited periodically (feed URLs on Russian news sites have historically moved — e.g. the fontanka.ru and static.feed.rbc.ru dead-ends found in this same research), i.e. should the pluggable-ingestion interface from Ticket 05's forward note treat RSS URLs as configuration that needs a health check rather than a one-time hardcoded value?
