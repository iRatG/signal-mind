"""Ticket 12 timing probe: measure headline extraction + tokenize + cluster cost
on two known-volume months from hf_news.db, to extrapolate a full-history estimate
before committing to the real calibration run.

Disposable script, not the final Ticket 12 deliverable. Writes results to
probe_results.json in this same folder; prints only a short summary.
"""
import json
import re
import time
from collections import Counter, defaultdict

import duckdb

DB_PATH = "db/hf_news.db"

STOPWORDS = {
    "что", "как", "это", "или", "для", "при", "над", "под", "без", "после",
    "было", "будет", "более", "менее", "себя", "свои", "свой", "также",
}


def analysis_tokens(text: str) -> list[str]:
    words = re.findall(r"[A-Za-zА-Яа-яЁё0-9-]{3,}", text.lower())
    return [w for w in words if w not in STOPWORDS and not w.isdigit()]


def extract_headline(text: str) -> str:
    if "\n\n" in text:
        return text.split("\n\n", 1)[0]
    return text[:200]


class UnionFind:
    def __init__(self, n):
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
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


def run_month(con, ym: str, min_shared_tokens=2, jaccard_threshold=0.24, max_token_df=320):
    t0 = time.time()
    rows = con.execute(
        """
        SELECT id, text
        FROM news.articles
        WHERE source = 'data' AND strftime(CAST(date AS DATE), '%Y-%m') = ?
        """,
        [ym],
    ).fetchall()
    t_fetch = time.time() - t0

    t0 = time.time()
    headlines = [(rid, extract_headline(text)) for rid, text in rows]
    tokenized = [(rid, tuple(dict.fromkeys(analysis_tokens(h)))) for rid, h in headlines]
    tokenized = [(rid, toks) for rid, toks in tokenized if toks]
    t_tokenize = time.time() - t0

    t0 = time.time()
    postings = defaultdict(list)
    for idx, (rid, toks) in enumerate(tokenized):
        for tok in set(toks):
            postings[tok].append(idx)
    usable = {tok: ids for tok, ids in postings.items() if 2 <= len(ids) <= max_token_df}

    pair_counts = Counter()
    for ids in usable.values():
        for i, left in enumerate(ids):
            for right in ids[i + 1:]:
                pair_counts[(left, right)] += 1

    uf = UnionFind(len(tokenized))
    token_sets = [set(toks) for _, toks in tokenized]
    for (left, right), shared in pair_counts.items():
        if shared < min_shared_tokens:
            continue
        union_size = len(token_sets[left] | token_sets[right])
        jaccard = shared / max(1, union_size)
        if jaccard >= jaccard_threshold or shared >= min_shared_tokens + 2:
            uf.union(left, right)

    components = defaultdict(int)
    for idx in range(len(tokenized)):
        components[uf.find(idx)] += 1
    t_cluster = time.time() - t0

    return {
        "ym": ym,
        "row_count": len(rows),
        "tokenized_count": len(tokenized),
        "unique_tokens": len(postings),
        "usable_tokens": len(usable),
        "pair_count": len(pair_counts),
        "cluster_count": len(components),
        "largest_cluster": max(components.values()) if components else 0,
        "t_fetch_s": round(t_fetch, 2),
        "t_tokenize_s": round(t_tokenize, 2),
        "t_cluster_s": round(t_cluster, 2),
        "t_total_s": round(t_fetch + t_tokenize + t_cluster, 2),
    }


def main():
    con = duckdb.connect()
    con.execute(f"ATTACH '{DB_PATH}' AS news (TYPE sqlite, READ_ONLY)")

    results = []
    for ym in ["2023-01", "2022-06"]:
        print(f"--- probing {ym} ---")
        r = run_month(con, ym)
        print(json.dumps(r, indent=2))
        results.append(r)

    with open(".scratch/ticket-12-timing-probe/probe_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()
