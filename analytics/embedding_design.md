# Embedding-Based News Features: Design Document

**Date:** 2026-05-19  
**Phase:** 2a (Research)  
**Status:** Draft (awaiting Phase 1 results)

---

## Problem Statement

Current keyword-based features (news_daily columns) are simple ILIKE match counts:
- `oil`: count of articles matching ["oil", "Brent", "crude", "energy", "нефть", ...]
- `rate`: count of articles matching ["interest rate", "key rate", "CBR", "ставка", ...]
- etc.

**Limitations:**
1. **Ambiguity:** "rate" matches both "interest rate" and "exchange rate" (no semantic distinction)
2. **Coarse signal:** Binary keyword match (1 if match, 0 if not) doesn't distinguish:
   - Relevance: minor mention vs. major focus
   - Sentiment: positive ("rate cut supports growth") vs. negative ("rate hike risks recession")
   - Authority: rumor vs. official CBR statement
3. **Noise:** Every article with the word "rate" counts equally

**Expected outcome:** Semantic embeddings will improve signal-to-noise ratio, allowing M5/M6 to detect weaker but genuine relationships.

---

## Architecture

### Input Data
**Source:** `hf_news.db` (2.56M articles)
- Columns: `id`, `date`, `text` (title + body), `source`, `extra_fields` (JSON)
- Encoding: UTF-8, mixed EN+RU

### Processing Pipeline

```
1. Load articles from hf_news.db, grouped by date
2. For each (date, topic) pair:
   a. Match articles loosely (soft filter, not just ILIKE)
   b. Compute embedding for each article title using sentence-transformers
   c. Compute embedding for topic keywords
   d. Measure similarity (cosine) between article and topic
   e. Aggregate daily: mean/max/weighted-sum of similarities
3. Output: daily scores [0, 1] or raw similarity values
4. Store in news_daily as new columns: oil_emb, rate_emb, ..., gold_emb
```

### Key Decisions

#### Model Choice
- **Reuse existing:** `paraphrase-multilingual-MiniLM-L12-v2` (from rag.py)
- **Rationale:** 
  - Already loaded in project
  - Good for EN+RU mixed corpora
  - Fast (~128 dimensions, small model)
  - Trade-off: smaller than large models, but sufficient for financial domain

#### Feature Aggregation

For each (date, topic), given set of article embeddings E_i and topic embedding T:

**Option A: Simple Mean**
```
score = mean([cosine(e_i, T) for e_i in E_i])
```
- Pros: Stable, interpretable
- Cons: Outliers wash out

**Option B: Max**
```
score = max([cosine(e_i, T) for e_i in E_i])
```
- Pros: Catches one strong mention
- Cons: Single-article noise

**Option C: Weighted Mean (Recommended)**
```
scores = [cosine(e_i, T) for e_i in E_i]
# Weight higher scores more heavily
weights = softmax(scores / tau), where tau ~ 0.1
score = sum(weights * scores)
```
- Pros: Balances mean (stable) + max (peaks matter)
- Cons: One extra hyperparameter (tau)

**Recommended:** Option C with tau=0.1 (to be validated empirically)

#### Article Selection

Given a topic with keywords K = ["oil", "Brent", ...] and an article A:
- Soft filter: use any article where A.text contains **any K** (ILIKE match)
- Rationale: avoids the "what if article mentions 'rate' in passing" problem by being inclusive

#### Encoding Input
- Use **article title** (short, high signal-to-noise)
- Fallback: title + first 200 chars of content if title is short
- Rationale: title is concise and authored by journalists; full text is noisy

#### Missing Data Handling
- If no articles match a (date, topic): score = 0.0
- If embedding fails for an article: skip it, aggregate remaining
- If all articles fail: fallback to keyword count for that day

---

## Implementation

### Module: `src/pipeline_v2/embedding_feature_builder.py`

**Class:** `EmbeddingFeatureBuilder`

```python
class EmbeddingFeatureBuilder:
    def __init__(self, 
                 model_name="paraphrase-multilingual-MiniLM-L12-v2",
                 aggregation="weighted_mean",
                 tau=0.1):
        self.model = SentenceTransformer(model_name)
        self.aggregation = aggregation
        self.tau = tau
        self.topic_keywords = {...}  # from news_precompute.py
        self.topic_embeddings = precompute topic embeddings
        
    def build_daily_scores(self, 
                          conn: duckdb.DuckDBPyConnection,
                          date_start: date = None,
                          date_end: date = None) -> pd.DataFrame:
        """
        Load articles from hf_news.db, compute embeddings, aggregate by date+topic.
        Returns: DataFrame with columns [news_date, oil_emb, rate_emb, ..., gold_emb]
        """
        # Append-only: only compute for dates > MAX(news_date) in existing table
        
    def save_to_duckdb(self, df: pd.DataFrame):
        """Write to signal_mind.duckdb, append-only (or create new table if missing)"""
```

---

## Validation Strategy

### Phase 1 (Baseline)
After big_scanner with loosened gates:
- Document: how many signals found? Which topics/instruments dominate?
- Hypothesis: expect ~40–60 signals (research tier)

### Phase 2 (Embeddings)
1. **Sanity check:** Embedding scores in [0, 1], no NaN, reasonable variance
2. **Correlation check:** Embedding scores should correlate with keyword counts (not orthogonal)
3. **Phase A.5 shuffle test:** Re-run with embedding columns
   - Expected: 0% FPR (shuffle breaks any real signal)
   - Success: if FPR still ~ 0–5%, embeddings don't overfit
4. **Big scanner comparison:**
   - Run on same 672 hypotheses with embedding columns
   - Compare: keyword IC vs. embedding IC
   - Success: embedding IC should improve (e.g., 0.04 → 0.10+)

---

## Success Criteria

1. **Build:** Embeddings computed without crashes; stored in news_daily
2. **Quality:** Embedding features show improvement on Phase A.5 shuffle test
3. **Signal:** At least 10 new production-grade signals appear when re-running big_scanner
4. **Interpretability:** Top signals are economically sensible (e.g., "MOEX reacts to CBR news")

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Embeddings are orthogonal to market returns | High | Phase A.5 shuffle test + IC comparison |
| Embedding computation slow (2.56M articles) | Medium | Batch vectorisation, cache embeddings |
| Model drift if fine-tuned later | Medium | Freeze model choice, document version |
| Memory overflow on large batch | Low | Process in chunks (date ranges) |

---

## Timeline

- **2a (Research):** 1 hour — finalize spec, choose aggregation method
- **2b (Build):** 1 day — implement embedding_feature_builder.py
- **2c (Integrate):** 4 hours — merge into news_precompute.py pipeline
- **2d (Rerun):** 2 hours — execute big_scanner with embeddings
- **2e (Compare):** 2 hours — generate report, interpret results

**Total Phase 2:** 2–3 days

---

## Next Steps

1. Review Phase 1 (Option A) results
2. Finalize aggregation method (simple mean vs. weighted mean)
3. Implement embedding_feature_builder.py
4. Validate on Phase A.5 shuffle test
5. Rerun big_scanner with embeddings
6. Compare and decide: proceed to Phase B real run or iterate further?
