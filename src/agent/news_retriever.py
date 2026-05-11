"""News context retriever from hf_news.db (read-only).

Mixed corpus: 2.52M English articles from HF (2021-09-06 and earlier) +
~28k Russian articles from ru_archive: namespace (2025-09-01 → present).
Each mapping entry below carries BOTH English and Russian search terms so the
same query reaches both halves of the corpus.

Note on SQLite LIKE with Cyrillic: SQLite's LIKE is case-insensitive for ASCII
only; Cyrillic is byte-exact. We use lowercase RU stems — most occurrences in
article bodies are lowercase, so this catches the bulk in practice without the
cost of LOWER(text).
"""
import sqlite3
from pathlib import Path

NEWS_DB = Path(__file__).parents[2] / "db" / "hf_news.db"

# Russian hypothesis stem -> [EN keyword 1, EN keyword 2, RU stem].
# All three are used in LIKE so Russian articles also match.
_KEYWORD_MAP: dict[str, list[str]] = {
    "ставк":      ["interest rate", "key rate",      "ставк"],
    "цб":         ["central bank", "CBR",            "центробанк"],
    "нефть":      ["oil", "crude",                   "нефть"],
    "нефт":       ["oil", "Brent",                   "нефт"],
    "газ":        ["natural gas", "Gazprom",         "газ"],
    "рубл":       ["ruble", "RUB",                   "рубл"],
    "санкц":      ["sanctions", "Russia",            "санкци"],
    "банк":       ["bank", "banking",                "банк"],
    "финанс":     ["financial", "finance",           "финанс"],
    "акци":       ["stock", "equity",                "акци"],
    "инфляц":     ["inflation", "CPI",               "инфляц"],
    "экспорт":    ["export", "commodity",            "экспорт"],
    "зарплат":    ["wages", "salary",                "зарплат"],
    "золото":     ["gold", "precious metals",        "золото"],
    "золот":      ["gold", "metals",                 "золот"],
    "недвижим":   ["real estate", "housing",         "недвижим"],
    "ипотек":     ["mortgage", "housing loan",       "ипотек"],
    "moexfn":     ["financial sector", "banking sector", "moexfn"],
    "moexog":     ["oil gas", "energy sector",       "moexog"],
    "imoex":      ["Moscow Exchange", "Russian stock", "imoex"],
    "sp500":      ["S&P 500", "US market",           "sp500"],
    "brent":      ["Brent", "crude oil",             "Brent"],
    "usd":        ["dollar", "USD",                  "доллар"],
    "корреляц":   ["correlation", "market",          "корреляц"],
    "сектор":     ["sector", "industry",             "сектор"],
    "индекс":     ["index", "benchmark",             "индекс"],
    "волатильн":  ["volatility", "market risk",      "волатильн"],
    "режим":      ["regime", "policy",               "режим"],
    "дивиденд":   ["dividend", "yield",              "дивиденд"],
}

_FALLBACK_KEYWORDS = ["Russia", "financial market", "economy"]


def _extract_keywords(hypothesis: str) -> list[str]:
    """Map Russian hypothesis text to mixed EN+RU search keywords."""
    h = hypothesis.lower()
    matched: list[str] = []
    for ru_stem, search_terms in _KEYWORD_MAP.items():
        if ru_stem in h:
            matched.extend(search_terms)  # all 3: 2 EN + 1 RU
    if not matched:
        matched = _FALLBACK_KEYWORDS[:]
    # Deduplicate while preserving order, cap at 8 (was 6 — bigger budget for RU+EN)
    seen: set[str] = set()
    result: list[str] = []
    for kw in matched:
        if kw not in seen:
            seen.add(kw)
            result.append(kw)
        if len(result) >= 8:
            break
    return result


def get_news_context(
    hypothesis: str,
    date_from: str = "2022-01-01",
    date_to: str = "2027-12-31",
    top_n: int = 5,
) -> str:
    """
    Search hf_news.db for articles relevant to the hypothesis.
    Returns a formatted block for LLM prompt injection, or "" on failure.
    Never raises — news retrieval is optional.
    """
    keywords = _extract_keywords(hypothesis)
    # Use up to 5 keywords in LIKE — covers 2 EN + 1 RU per matched topic plus a few extras
    like_conditions = " OR ".join(
        f"text LIKE '%{kw.replace(chr(39), '')}%'" for kw in keywords[:5]
    )

    try:
        conn = sqlite3.connect(f"file:{NEWS_DB}?mode=ro", uri=True)
        rows = conn.execute(
            f"""
            SELECT date, text FROM articles
            WHERE date BETWEEN ? AND ?
              AND ({like_conditions})
            LIMIT ?
            """,
            (date_from, date_to, top_n * 4),
        ).fetchall()
        conn.close()
    except Exception:
        return ""

    if not rows:
        return ""

    # Re-rank by keyword coverage, keep top_n
    def _score(text: str) -> int:
        t = text.lower()
        return sum(1 for kw in keywords if kw.lower() in t)

    rows.sort(key=lambda r: _score(r[1]), reverse=True)
    rows = rows[:top_n]

    blocks = []
    for date, text in rows:
        excerpt = text[:280].replace("\n", " ").strip()
        blocks.append(f"[{date}] {excerpt}…")

    return (
        f"=== News context ({len(blocks)} articles, {date_from}–{date_to}) ===\n\n"
        + "\n\n".join(blocks)
    )
