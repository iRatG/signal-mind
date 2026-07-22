#!/usr/bin/env bash
set -euo pipefail

# Run from signal-mind root. Environment variables can override defaults:
# FROM_DATE=2025-09-01 TO_DATE=2025-09-30 DELAY=5 ./run_news_loader.sh

PYTHON="${PYTHON:-python3}"
FROM_DATE="${FROM_DATE:-2025-09-01}"
TO_DATE="${TO_DATE:-2025-09-30}"
OUT="${OUT:-data/news_${FROM_DATE}_${TO_DATE}_rf_context.jsonl}"
DELAY="${DELAY:-5}"
ARCHIVE_DELAY="${ARCHIVE_DELAY:-3}"
SOURCES="${SOURCES:-bbc,guardian,aljazeera,euronews}"
CONFIG="${CONFIG:-config/news_loader.yaml}"
MAX_ARTICLES="${MAX_ARTICLES:-0}"

mkdir -p data

"$PYTHON" src/parsers/en_news_archive_loader.py \
  --config "$CONFIG" \
  --from "$FROM_DATE" \
  --to "$TO_DATE" \
  --sources "$SOURCES" \
  --match-scope article \
  --save-policy all \
  --delay "$DELAY" \
  --archive-delay "$ARCHIVE_DELAY" \
  --max-articles "$MAX_ARTICLES" \
  --out "$OUT"

echo "Done: $OUT"
