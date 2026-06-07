"""Phase C — RagHypothesis dataclass and result types.

A RagHypothesis is extracted from a corporate or regulatory document via RAG+LLM.
Every hypothesis has a traceable source (company, year, document page).

Flow:
  RagHypothesis  →  HypothesisResult  →  ledger_c.jsonl
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Optional
import json


# ──────────────────────────────────────────────────────────────────────────────
# Known instruments — must match signal_mind.duckdb
# ──────────────────────────────────────────────────────────────────────────────

VALID_INSTRUMENTS = {
    # Forex / global
    "USD_RUB", "EUR_RUB",
    # Commodities
    "BRENT", "GOLD", "SILVER",
    # Global indices
    "SP500", "MSCI_WORLD", "DXY", "MSCI_INDIA",
    "CHINA_H_SHARES", "FTSE_CHINA_50",
    "DJ_SOUTH_AFRICA",
    # MOEX sectors (v_*_sectors table)
    "IMOEX",    # Broad market
    "MOEXFN",   # Financials
    "MOEXOG",   # Oil & Gas
    "MOEX10",   # Blue-chip
    "RUGOLD",   # Gold (MOEX)
}

# MOEX sectors map — instrument name → column in v_{split}_sectors
MOEX_SECTOR_COLS = {
    "IMOEX":  "imoex",
    "MOEXFN": "moexfn_finance",
    "MOEXOG": "moexog_oil_gas",
    "MOEX10": "moex10_bluechip",
    "RUGOLD": "rugold",
}

# News topics available as features (keyword_z90 or embedding_z90)
VALID_TOPICS = {
    "oil", "rate", "ruble", "sanctions", "inflation", "banking", "gold",
}

VALID_DIRECTIONS = {"positive", "negative", "unknown"}
VALID_FEATURE_TYPES = {"keyword_z90", "embedding_z90"}


# ──────────────────────────────────────────────────────────────────────────────
# Hypothesis
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class RagHypothesis:
    """One testable hypothesis extracted from a document.

    Fields set by rag_extractor:
      instrument   — what to predict (e.g. "MOEXFN")
      topic        — news topic feature (e.g. "rate")
      lag_days     — list of lags to test, e.g. [7, 14, 30]
      direction    — expected direction: "positive" | "negative" | "unknown"
      rationale    — economic reasoning from the document (1-2 sentences)
      source_company — e.g. "sberbank"
      source_year    — document year, e.g. 2023
      source_page    — page in the document
      source_section — section title, e.g. "Risk factors"
      confidence     — extractor confidence [0.0, 1.0]
      feature_type   — "keyword_z90" | "embedding_z90"
      hypothesis_id  — unique slug, auto-generated if empty

    Fields set after testing:
      tested       — True once hypothesis_tester ran
      results      — list of HypothesisResult (one per lag)
    """
    instrument:      str
    topic:           str
    lag_days:        list
    direction:       str
    rationale:       str
    source_company:  str
    source_year:     int
    source_page:     int
    source_section:  str        = ""
    confidence:      float      = 0.5
    feature_type:    str        = "keyword_z90"
    hypothesis_id:   str        = ""
    tested:          bool       = False
    results:         list       = field(default_factory=list)

    def __post_init__(self):
        # Normalise
        self.instrument = self.instrument.upper().strip()
        self.topic      = self.topic.lower().strip()
        self.direction  = self.direction.lower().strip()
        if not self.lag_days:
            self.lag_days = [7, 14, 30]
        # Auto-generate ID
        if not self.hypothesis_id:
            ft_suffix = "emb" if "embedding" in self.feature_type else "kw"
            self.hypothesis_id = (
                f"{self.source_company}_{self.source_year}"
                f"_{self.instrument}_{self.topic}_{ft_suffix}"
            ).lower().replace(" ", "_")

    def is_valid(self) -> tuple[bool, str]:
        """Validate fields. Returns (ok, reason)."""
        if self.instrument not in VALID_INSTRUMENTS:
            return False, f"unknown instrument '{self.instrument}'"
        if self.topic not in VALID_TOPICS:
            return False, f"unknown topic '{self.topic}'"
        if self.direction not in VALID_DIRECTIONS:
            return False, f"unknown direction '{self.direction}'"
        if self.feature_type not in VALID_FEATURE_TYPES:
            return False, f"unknown feature_type '{self.feature_type}'"
        if not self.lag_days:
            return False, "lag_days is empty"
        if not self.rationale or len(self.rationale) < 10:
            return False, "rationale too short"
        return True, "ok"

    def source_label(self) -> str:
        return f"{self.source_company}/{self.source_year}/p.{self.source_page}"

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    @classmethod
    def from_dict(cls, d: dict) -> "RagHypothesis":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ──────────────────────────────────────────────────────────────────────────────
# Test result (one per lag)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class HypothesisResult:
    """Result of testing one (hypothesis, lag) combination.

    Fields:
      hypothesis_id  — FK to RagHypothesis.hypothesis_id
      instrument     — copy for convenience
      topic          — copy for convenience
      lag            — the specific lag tested
      split          — "train" | "val" | "test"
      m5_confirmed   — Mann-Whitney / VAR-IRF passed
      m5_pvalue      — p-value
      m5_score       — effect size or IRF score
      m6_confirmed   — LightGBM walk-forward IC passed
      m6_ic          — information coefficient
      m6_pvalue      — bootstrap p-value
      n_obs          — number of observations
      ensemble_pass  — m5 AND m6 both confirmed
      sign_flip      — direction on val/test differs from train
      source_label   — "sberbank/2023/p.47"
      error          — non-empty string if evaluation crashed
    """
    hypothesis_id:  str
    instrument:     str
    topic:          str
    lag:            int
    split:          str
    m5_confirmed:   bool  = False
    m5_pvalue:      float = 1.0
    m5_score:       float = 0.0
    m6_confirmed:   bool  = False
    m6_ic:          float = 0.0
    m6_pvalue:      float = 1.0
    n_obs:          int   = 0
    ensemble_pass:  bool  = False
    sign_flip:      bool  = False
    source_label:   str   = ""
    error:          str   = ""

    def to_dict(self) -> dict:
        return asdict(self)
