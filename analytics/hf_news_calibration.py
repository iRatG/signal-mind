"""Ticket 12 - Historical Cluster Calibration Study.

One-off, read-only analysis over db/hf_news.db (NEVER written to) to derive
empirical cluster-size / persistence / source-spread distributions for the
production Union-Find/jaccard clustering algorithm (analytics/news_pressure_cluster.py),
applied to a headline-equivalent field per Ticket 12's Algorithm-fit=(b) decision.

Two passes, reported separately per the ticket's Starting Assumption:
  - English pass: source='data', 2021-01 .. 2025-09 (the only real multi-year slice)
  - Russian pass: source LIKE 'ru_archive:%', 2025-09 .. 2026-05 (provisional, short window)

Progress and per-cluster results are logged to a SQLite table (NOT under db/) so the
run is inspectable and resumable if interrupted. Each month has a hard time budget;
a month that blows it is logged as skipped_dense_month rather than allowed to hang
the whole run.

Usage:
    python analytics/hf_news_calibration.py
"""
from __future__ import annotations

import json
import re
import time
from collections import Counter, defaultdict
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
HF_NEWS_DB = ROOT / "db" / "hf_news.db"
OUT_DIR = ROOT / "docs" / "wayfinder" / "news-pressure-radar" / "calibration"
LOG_DB = OUT_DIR / "progress.db"

MIN_SHARED_TOKENS = 2
JACCARD_THRESHOLD = 0.24
MAX_TOKEN_DF = 320
MIN_CLUSTER_SIZE = 5
PER_MONTH_TIME_BUDGET_S = 400  # "не зацикливайся" — a dense month gets skipped, not looped on
TEXT_PREFIX_CHARS = 300

STOPWORDS = {
    "что", "как", "это", "или", "для", "при", "над", "под", "без", "после",
    "было", "будет", "более", "менее", "себя", "свои", "свой", "также",
    "из-за", "изза", "россии", "россия", "российский", "российские",
    "заявил", "сообщил", "рассказал", "назвал", "стало", "стали",
    "the", "for", "and", "with", "from", "that", "this", "are", "was",
    "were", "has", "have", "had", "will", "would", "could", "should",
    "its", "their", "his", "her", "you", "your", "not", "but", "who",
    "what", "when", "where", "how", "why", "all", "new", "more", "than",
    "into", "over", "after", "before", "about", "amid", "amp",
}


def canonical_token(word: str) -> str:
    if not re.search(r"[а-яё]", word):
        return word
    for suffix in (
        "иями", "ями", "ами", "ого", "ему", "ыми", "ими", "ией", "ия", "иях",
        "ах", "ях", "ов", "ев", "ом", "ем", "ой", "ей", "ым", "им", "ую",
        "юю", "ая", "яя", "ое", "ее", "ые", "ие", "ый", "ий", "его",
        "а", "я", "ы", "и", "у", "ю", "е", "о",
    ):
        if len(word) - len(suffix) >= 4 and word.endswith(suffix):
            return word[: -len(suffix)]
    return word


def analysis_tokens(text: str) -> list[str]:
    words = re.findall(r"[A-Za-zА-Яа-яЁё0-9-]{3,}", text.lower())
    return [canonical_token(w) for w in words if w not in STOPWORDS and not w.isdigit()]


URL_DOMAIN_RE = re.compile(r"https?://(?:www\.)?([^/]+)")


def extract_outlet(source_domain, news_outlet, publisher, publication, url, source_field, dataset) -> str:
    """Best-effort per-article outlet identity for source_spread, for the
    English 'data' source where the `source` column is a flat constant.
    Priority chain justified per sub-dataset field audit, 2026-08-01:
    source_domain/news_outlet (yahoo_finance_felixdrinkall, american_news_jonasbecker)
    > publisher (yahoo_finance_articles, cnbc) > publication (headlines_10sites)
    > url domain (fnspid_news fallback, no clean field at all)
    > source (nyt/huffpost/wikinews/cnbc/sentarl/finsen — single-outlet datasets)
    > dataset name (worst case, e.g. reddit_finance_sp500's per-subreddit identity lost)."""
    for val in (source_domain, news_outlet, publisher, publication):
        if val:
            return val
    if url:
        m = URL_DOMAIN_RE.match(url)
        if m:
            return m.group(1)
    if source_field:
        return source_field
    return dataset or "unknown"


def extract_headline(title: str | None, dataset: str | None, text_prefix: str) -> tuple[str, bool]:
    """Returns (headline, is_degraded). is_degraded flags the known
    headlines_10sites_2007_2022 no-separator case (Ticket 12 Algorithm-fit caveat)."""
    if title:
        return title, False
    if "\n\n" in text_prefix:
        return text_prefix.split("\n\n", 1)[0], False
    degraded = dataset == "headlines_10sites_2007_2022"
    return text_prefix[:200], degraded


class UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1


def init_log_db(path: Path) -> None:
    import sqlite3

    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS progress_log (
            pass TEXT, month TEXT, articles_in INTEGER, headlines_used INTEGER,
            degraded_headline_count INTEGER, unique_tokens INTEGER, pair_count INTEGER,
            clusters_found INTEGER, elapsed_s REAL, status TEXT, note TEXT, logged_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clusters (
            pass TEXT, month TEXT, cluster_id INTEGER, volume INTEGER,
            persistence_days INTEGER, source_spread INTEGER, degraded_share REAL,
            label TEXT
        )
    """)
    conn.commit()
    conn.close()


def log_progress(row: dict) -> None:
    import sqlite3
    from datetime import UTC, datetime

    conn = sqlite3.connect(LOG_DB)
    conn.execute(
        """
        INSERT INTO progress_log(pass, month, articles_in, headlines_used, degraded_headline_count,
            unique_tokens, pair_count, clusters_found, elapsed_s, status, note, logged_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            row["pass"], row["month"], row.get("articles_in", 0), row.get("headlines_used", 0),
            row.get("degraded_headline_count", 0), row.get("unique_tokens", 0),
            row.get("pair_count", 0), row.get("clusters_found", 0), row.get("elapsed_s", 0.0),
            row["status"], row.get("note", ""), datetime.now(UTC).isoformat(timespec="seconds"),
        ),
    )
    conn.commit()
    conn.close()


def log_clusters(pass_name: str, month: str, clusters: list[dict]) -> None:
    import sqlite3

    conn = sqlite3.connect(LOG_DB)
    for c in clusters:
        conn.execute(
            "INSERT INTO clusters(pass, month, cluster_id, volume, persistence_days, source_spread, degraded_share, label) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (pass_name, month, c["cluster_id"], c["volume"], c["persistence_days"],
             c["source_spread"], c["degraded_share"], c["label"]),
        )
    conn.commit()
    conn.close()


def cluster_month(rows: list[tuple], pass_name: str, month: str) -> dict:
    """rows: list of (id, source, date, headline, is_degraded)."""
    t0 = time.monotonic()
    tokenized = []
    for rid, source, date_, headline, degraded in rows:
        toks = tuple(dict.fromkeys(analysis_tokens(headline)))
        if toks:
            tokenized.append((rid, source, date_, toks, degraded))

    postings: dict[str, list[int]] = defaultdict(list)
    for idx, (_, _, _, toks, _) in enumerate(tokenized):
        for tok in set(toks):
            postings[tok].append(idx)
    usable = {tok: ids for tok, ids in postings.items() if 2 <= len(ids) <= MAX_TOKEN_DF}

    pair_counts: Counter = Counter()
    deadline = t0 + PER_MONTH_TIME_BUDGET_S
    timed_out = False
    for ids in usable.values():
        if time.monotonic() > deadline:
            timed_out = True
            break
        for i, left in enumerate(ids):
            for right in ids[i + 1:]:
                pair_counts[(left, right)] += 1

    if timed_out:
        elapsed = time.monotonic() - t0
        return {
            "status": "skipped_dense_month",
            "note": f"pair-counting exceeded {PER_MONTH_TIME_BUDGET_S}s budget, month too dense for in-process pairwise pass",
            "articles_in": len(rows), "headlines_used": len(tokenized),
            "degraded_headline_count": sum(1 for *_, d in tokenized if d),
            "unique_tokens": len(postings), "pair_count": len(pair_counts),
            "clusters_found": 0, "elapsed_s": elapsed, "clusters": [],
        }

    uf = UnionFind(len(tokenized))
    token_sets = [set(toks) for _, _, _, toks, _ in tokenized]
    for (left, right), shared in pair_counts.items():
        if shared < MIN_SHARED_TOKENS:
            continue
        union_size = len(token_sets[left] | token_sets[right])
        jaccard = shared / max(1, union_size)
        if jaccard >= JACCARD_THRESHOLD or shared >= MIN_SHARED_TOKENS + 2:
            uf.union(left, right)

    components: dict[int, list[int]] = defaultdict(list)
    for idx in range(len(tokenized)):
        components[uf.find(idx)].append(idx)

    global_df: Counter = Counter()
    for _, _, _, toks, _ in tokenized:
        global_df.update(set(toks))

    clusters = []
    for comp_idx, member_idxs in components.items():
        if len(member_idxs) < MIN_CLUSTER_SIZE:
            continue
        members = [tokenized[i] for i in member_idxs]
        sources = {m[1] for m in members}
        dates = {m[2] for m in members}
        degraded_share = sum(1 for m in members if m[4]) / len(members)
        cluster_df: Counter = Counter()
        for _, _, _, toks, _ in members:
            cluster_df.update(set(toks))
        total_docs = max(1, sum(global_df.values()))
        scored = sorted(
            ((count * __import__("math").log(1 + total_docs / max(1, global_df[tok])), tok)
             for tok, count in cluster_df.items()),
            reverse=True,
        )
        label = " / ".join(tok for _, tok in scored[:4])
        clusters.append({
            "cluster_id": comp_idx,
            "volume": len(members),
            "persistence_days": len(dates),
            "source_spread": len(sources),
            "degraded_share": round(degraded_share, 3),
            "label": label,
        })

    elapsed = time.monotonic() - t0
    return {
        "status": "done", "note": "",
        "articles_in": len(rows), "headlines_used": len(tokenized),
        "degraded_headline_count": sum(1 for *_, d in tokenized if d),
        "unique_tokens": len(postings), "pair_count": len(pair_counts),
        "clusters_found": len(clusters), "elapsed_s": elapsed, "clusters": clusters,
    }


def fetch_pass(con, pass_name: str) -> dict[str, list[tuple]]:
    """Fetch id, outlet-identity, date, headline, is_degraded for one pass, grouped by month.
    'outlet-identity' is the real per-article publisher for English (source column is a flat
    'data' constant there — see extract_outlet), and the existing source column for Russian
    (ru_archive:interfax/lenta/kommersant are already real per-article identities)."""
    if pass_name == "english":
        where = "source = 'data'"
    else:
        where = "source LIKE 'ru_archive:%'"

    print(f"[{pass_name}] fetching rows ({where}) ...")
    t0 = time.monotonic()
    if pass_name == "english":
        rows = con.execute(
            f"""
            SELECT id, date,
                   json_extract_string(extra_fields, '$.title') AS title,
                   json_extract_string(extra_fields, '$.dataset') AS dataset,
                   json_extract_string(extra_fields, '$.source_domain') AS source_domain,
                   json_extract_string(extra_fields, '$.news_outlet') AS news_outlet,
                   json_extract_string(extra_fields, '$.publisher') AS publisher,
                   json_extract_string(extra_fields, '$.publication') AS publication,
                   json_extract_string(extra_fields, '$.url') AS url,
                   json_extract_string(extra_fields, '$.source') AS source_field,
                   substr(text, 1, {TEXT_PREFIX_CHARS}) AS text_prefix
            FROM news.articles
            WHERE {where} AND date IS NOT NULL
            """
        ).fetchall()
        print(f"[{pass_name}] fetched {len(rows)} rows in {time.monotonic() - t0:.1f}s")

        by_month: dict[str, list[tuple]] = defaultdict(list)
        for (rid, date_, title, dataset, source_domain, news_outlet, publisher,
             publication, url, source_field, text_prefix) in rows:
            headline, degraded = extract_headline(title, dataset, text_prefix or "")
            outlet = extract_outlet(source_domain, news_outlet, publisher, publication,
                                     url, source_field, dataset)
            ym = str(date_)[:7]
            by_month[ym].append((rid, outlet, date_, headline, degraded))
        return by_month

    rows = con.execute(
        f"""
        SELECT id, source, date,
               json_extract_string(extra_fields, '$.title') AS title,
               json_extract_string(extra_fields, '$.dataset') AS dataset,
               substr(text, 1, {TEXT_PREFIX_CHARS}) AS text_prefix
        FROM news.articles
        WHERE {where} AND date IS NOT NULL
        """
    ).fetchall()
    print(f"[{pass_name}] fetched {len(rows)} rows in {time.monotonic() - t0:.1f}s")

    by_month: dict[str, list[tuple]] = defaultdict(list)
    for rid, source, date_, title, dataset, text_prefix in rows:
        headline, degraded = extract_headline(title, dataset, text_prefix or "")
        ym = str(date_)[:7]
        by_month[ym].append((rid, source, date_, headline, degraded))
    return by_month


def run_pass(con, pass_name: str) -> None:
    by_month = fetch_pass(con, pass_name)
    for ym in sorted(by_month):
        rows = by_month[ym]
        print(f"[{pass_name}] {ym}: {len(rows)} articles -> clustering...")
        result = cluster_month(rows, pass_name, ym)
        log_progress({"pass": pass_name, "month": ym, **result})
        if result["clusters"]:
            log_clusters(pass_name, ym, result["clusters"])
        print(f"[{pass_name}] {ym}: {result['status']} clusters={result['clusters_found']} "
              f"elapsed={result['elapsed_s']:.1f}s {result.get('note', '')}")


def main() -> None:
    init_log_db(LOG_DB)
    con = duckdb.connect()
    con.execute(f"ATTACH '{HF_NEWS_DB}' AS news (TYPE sqlite, READ_ONLY)")

    run_pass(con, "english")
    run_pass(con, "russian")

    print("=== calibration run complete ===")
    print(f"Log DB: {LOG_DB}")


if __name__ == "__main__":
    main()
