"""News context retriever from hf_news.db (read-only).

2.52M English articles, 2021-2025, SQLite with idx_date index.
Extracts English keywords from Russian hypothesis text and returns
a formatted news block for LLM prompt injection.
"""
import sqlite3
from pathlib import Path

NEWS_DB = Path(__file__).parents[2] / "db" / "hf_news.db"

# Russian financial terms → English search keywords (first 2 are used in LIKE)
_KEYWORD_MAP: dict[str, list[str]] = {
    "ставк":      ["interest rate", "key rate"],
    "цб":         ["central bank", "CBR"],
    "нефть":      ["oil", "crude"],
    "нефт":       ["oil", "Brent"],
    "газ":        ["natural gas", "Gazprom"],
    "рубл":       ["ruble", "RUB"],
    "санкц":      ["sanctions", "Russia"],
    "банк":       ["bank", "banking"],
    "финанс":     ["financial", "finance"],
    "акци":       ["stock", "equity"],
    "инфляц":     ["inflation", "CPI"],
    "экспорт":    ["export", "commodity"],
    "зарплат":    ["wages", "salary"],
    "золото":     ["gold", "precious metals"],
    "золот":      ["gold", "metals"],
    "недвижим":   ["real estate", "housing"],
    "ипотек":     ["mortgage", "housing loan"],
    "moexfn":     ["financial sector", "banking sector"],
    "moexog":     ["oil gas", "energy sector"],
    "imoex":      ["Moscow Exchange", "Russian stock"],
    "sp500":      ["S&P 500", "US market"],
    "brent":      ["Brent", "crude oil"],
    "usd":        ["dollar", "USD"],
    "корреляц":   ["correlation", "market"],
    "сектор":     ["sector", "industry"],
    "индекс":     ["index", "benchmark"],
    "волатильн":  ["volatility", "market risk"],
    "режим":      ["regime", "policy"],
    "дивиденд":   ["dividend", "yield"],
}

_FALLBACK_KEYWORDS = ["Russia", "financial market", "economy"]


def _extract_keywords(hypothesis: str) -> list[str]:
    """Map Russian hypothesis text to English search keywords."""
    h = hypothesis.lower()
    matched: list[str] = []
    for ru_stem, en_words in _KEYWORD_MAP.items():
        if ru_stem in h:
            matched.extend(en_words[:2])
    if not matched:
        matched = _FALLBACK_KEYWORDS[:]
    # Deduplicate while preserving order, cap at 6
    seen: set[str] = set()
    result: list[str] = []
    for kw in matched:
        if kw not in seen:
            seen.add(kw)
            result.append(kw)
        if len(result) >= 6:
            break
    return result


def get_news_context(
    hypothesis: str,
    date_from: str = "2022-01-01",
    date_to: str = "2025-12-31",
    top_n: int = 5,
) -> str:
    """
    Search hf_news.db for articles relevant to the hypothesis.
    Returns a formatted block for LLM prompt injection, or "" on failure.
    Never raises — news retrieval is optional.
    """
    keywords = _extract_keywords(hypothesis)
    # Use up to 3 keywords in LIKE to balance speed and coverage
    like_conditions = " OR ".join(
        f"text LIKE '%{kw.replace(chr(39), '')}%'" for kw in keywords[:3]
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
