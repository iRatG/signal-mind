# Ticket 09 - Repository Boundary

Status: closed
Type: grilling
Labels: `wayfinder:grilling`
Claim: claimed by Claude (wayfinder session, 2026-07-31)
Blocked By: Ticket 06 - Evaluation Harness (closed 2026-07-31 — satisfied), Ticket 08 - OpenSpec Bridge (still open — decision resolved this session without waiting on it; see `## Working Decision`). Formal block overridden at Airat's explicit request.

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

**Resolved 2026-07-31 (this ticket's own grilling session):** re-offered the same 3-way choice at the start of the next session; Airat chose "grill now." Full resolution, including what "repo access/push flow restored" actually meant, is in `## Working Decision` below.

## Working Decision

Resolved 2026-07-31 (`/grilling` session). Full raw Q&A is in `## Grilling Transcript` below; this section is the synthesis.

**1. Target: MADPAC extracts into its own independent repository.** Not a permanent merge into `signal-mind`. Two independent lines of reasoning converge on this, not one: (a) Airat's own — `signal-mind` is a large, undocumented, "by-feel" legacy project (multiple marathons, an experiments DB, a big data/expert layer); MADPAC is a deliberately more disciplined line of work (wayfinder + grilling + OpenSpec, every decision documented and justified) and deserves its own home before the two styles blur together; (b) Claude's — MADPAC has its own delivery lifecycle (Ticket 07: Telegram channel, email, local page, eventual VPS) independent of `signal-mind`'s Ouroboros loop, and mixing two governance styles (documented-first vs. by-feel) in one `CLAUDE.md`/one repo risks MADPAC's discipline eroding under the older project's habits.

**2. The mystery criterion "repo access/push flow restored" is retired, not carried forward.** Traced to its origin (`git show` on the original 2026-07-28 map-creation commit, `fb21494`, authored by an earlier assistant persona — no further explanation existed anywhere in history). Airat confirmed it was never a literal git/access problem — it was his own unresolved uncertainty at the time about how to relate a large, undocumented legacy project to a newly-disciplined effort built on top of it. That uncertainty is what this ticket's grilling session resolves; the phrase itself is dropped from the criteria list.

**3. Git-repository boundaries and data-source coupling are independent axes — this was the key unlock.** Initial framing (Claude's) conflated them: "don't extract the repo until the historical-snapshot extraction (Ticket 12) is done and materialized, because otherwise you'd need cross-repo access to `hf_news.db`." Airat correctly pushed back: a one-time read of a DB file by path (the same `ATTACH`-style read-only connection Ticket 12 already planned) does not require git-monorepo colocation — a script in a brand-new repository can open `hf_news.db` by path exactly as easily as a script in this checkout can. **Verified by grep before accepting this, not assumed:** `analytics/news_pressure_cluster.py` and `scripts/news_pressure_regimen.py` (MADPAC's actual code) already have zero references to `db/signal_mind.duckdb`, `db/experiments.db`, or `src/agent/*` — the only files that touch those are `analytics/verify_signals.py`, `analytics/signal_scan.py`, `analytics/generate_report.py`, which belong to the older, unrelated Ouroboros/marathon reporting line, not MADPAC. So MADPAC already has zero *live* coupling to `signal-mind` internals today, before Ticket 12 even runs. The only remaining touch point is a single, one-time, read-only file open of `hf_news.db` — an operational detail (a configurable source path), not a structural dependency.

**4. Revised Decision Shape criteria (replaces the original 7-item list):**
- stable report contract — met (Ticket 04) ✅
- accepted quality gate — met (Ticket 03) ✅
- source strategy settled — met (Ticket 05) ✅
- delivery policy settled — met (Ticket 07) ✅
- no live code coupling to `signal-mind` internals — met today, confirmed by grep (see point 3) ✅
- OpenSpec exists (Ticket 08) — downgraded from hard blocker to "should exist in parallel," does not gate extraction
- stable daily regimen over several real days — downgraded from a pre-extraction gate to a post-extraction, in-new-repo work item (see point 5); does not gate the extraction *event* itself
- ~~"repo access/push flow restored"~~ — retired (point 2)

**5. Operational-stability proof does not need to happen inside `signal-mind` before extracting.** If bugs surface during real daily operation, they get fixed in MADPAC's own code — which (per point 3) has no dependency on `signal-mind` internals to fix them. There is no debugging advantage to staying in the shared repo while proving stability, so this proof is reclassified as ongoing work to continue *after* the move, not a precondition for it.

**6. Sequencing of the actual git extraction, relative to Ticket 12:** implement and run Ticket 12 (year-deep snapshot from `hf_news.db`) **first, inside this checkout** — the read-only path to `hf_news.db` is already confirmed reachable here — then perform the repository split as a single clean cut-over, moving documentation, code, and the now-materialized snapshot together in one step, rather than standing up an empty new repository and writing the extraction script inside it separately. Reasoning: fewer moving parts at once (not simultaneously bootstrapping a new repo *and* writing new extraction code), and the new repository is born already self-contained with real data from day one instead of an empty shell waiting on a follow-up migration.

**7. Extraction mechanics, when the cut-over happens:** use `git filter-repo` (not `git subtree`, not a fresh empty repo with a plain file copy) against the relevant paths (`docs/wayfinder/news-pressure-radar/`, `openspec/changes/news-market-coupling-agentic-loop/`, `analytics/news_pressure_cluster.py`, `scripts/news_pressure_regimen.py`, `data/news_pressure/`, Ticket 12's extraction script and output) so the new repository preserves commit history for those paths — losing the documented decision trail (the whole point of the wayfinder+grilling discipline) would be a real loss, not a cosmetic one. One clean cut-over event, not a dual-maintenance window. `signal-mind`'s existing `CLAUDE.md` protected-files list is unaffected (those files belong to `signal-mind`, not MADPAC); the new repository gets its own fresh `CLAUDE.md` once it has its own data assets worth protecting (the extracted snapshot, its own future experiment logs).

## Grilling Transcript

Full record of the `/grilling` session behind the Working Decision above, per the convention on `docs/agents/issue-tracker.md`. Session opened by re-presenting the 3-way bounded choice deferred from the prior session (per `map.md`'s resume point); Airat chose to grill now.

**Q1 — What did "repo access/push flow restored" actually mean?**
Claude offered three guesses (a stale technical access problem / a forward-looking "how will push/CI work post-extraction" question / a dead note safe to drop), having already confirmed via `git log`/`git show` that no commit history or message explains the phrase.
Airat explained at length: the note reflected his own unresolved uncertainty at the time, not a technical git problem. `signal-mind` is a large, undocumented legacy project (multiple "marathons," an experiments DB, a large collected dataset, extensive expert/experimentation work) built "by feel," with no documentation discipline. MADPAC is conceptually a continuation of that lineage but built with a fundamentally more rigorous method (wayfinder + grilling + OpenSpec, everything documented before code). He hadn't known how to relate the two. He now sees the actual answer: once MADPAC pulls a year-deep snapshot from `hf_news.db` and stops comparing/relating anything to `signal-mind`'s ongoing data, a clean split becomes possible — and he wants Claude's professional opinion on how to do it, with his own position stated as: separate/independent, but only after the year-deep data-extraction module is built.

**Q2 — Claude's professional opinion on the target and the gating condition.**
Claude agreed with the "separate repository" target for two independent reasons (data-coupling logic from Airat's own framing, plus a second reason — governance/discipline mixing risk — from its own analysis), then proposed sharpening "after the data-extraction module" into a more precise, verifiable gate: Ticket 12 done and its snapshot materialized, **plus** a grep-verifiable confirmation that no live code path touches `signal-mind` internals, **plus** downgrading OpenSpec (Ticket 08) from a hard blocker to a parallel-track item, and retiring the "repo access/push flow" criterion outright. Also proposed extraction mechanics (`git filter-repo`, history-preserving, single clean cut-over).
Airat accepted the framing without objection, restating his preference for a documented, non-confusing boundary "пока не поздно" (while it's not too late).

**Q3 — Does the repository split itself need to wait for Ticket 12, or can data extraction and repo extraction happen independently?**
Airat pushed back on Claude's own proposed sequencing: if there's no disagreement about the target being a separate project, the repository split and the data transfer/DB-connection/snapshot work could be treated as two separate needs, not one blocking the other — a DB connection doesn't require monorepo colocation.
Claude agreed this was a real correction, not a preference call — verified it with a grep before accepting (see Working Decision point 3), and confirmed MADPAC's actual code has zero live references to `signal-mind` internals already, which fully supports decoupling data-access from repo-boundary timing.

**Q4 — Given both threads (target confirmed, coupling reasoning confirmed by grep), what's the actual decision — asked as an explicit decision tree, not a flat answer.**
Airat asked directly for "карту принятия решения. дерево. логичное... давай примем решение" (a logical decision map/tree — let's actually decide), continuing the pattern from Ticket 07 of wanting mechanism-grounded branches rather than open questions.
Claude built a two-node tree grounded in the just-verified facts: **Node A** (does proving operational stability need to happen before extraction, inside `signal-mind`?) — resolved by noting that any bugs found would be fixed in MADPAC's own decoupled code regardless of which repo hosts it, so there's no debugging advantage to waiting in place; stability-proving is deferred to after the move, not a precondition. **Node B** (extraction now, with Ticket 12's script written inside the new repo, vs. extraction after Ticket 12 runs inside the current checkout) — recommended the latter, fewer simultaneous moving parts, new repo starts self-contained with real data.
Airat confirmed the full tree and the final synthesized decision in one word: **"верно."**
