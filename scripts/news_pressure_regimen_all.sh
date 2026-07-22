#!/usr/bin/env bash
set -euo pipefail

ROOT="/root/.openclaw/workspace/signal-mind"
LOG_DIR="$ROOT/data/news_pressure/regimen_logs"
LOCK_FILE="/tmp/news_pressure_regimen.lock"
AS_OF="${1:-$(date -u +%F)}"

mkdir -p "$LOG_DIR"

if [[ -f /root/.openclaw/.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source /root/.openclaw/.env
  set +a
fi

cd "$ROOT"

run_mode() {
  local mode="$1"
  shift
  local log_file="$LOG_DIR/${mode}_${AS_OF}.log"
  {
    echo "== $(date -u --iso-8601=seconds) mode=${mode} as_of=${AS_OF} =="
    python3 scripts/news_pressure_regimen.py \
      --mode "$mode" \
      --as-of "$AS_OF" \
      --label-mode deepseek \
      "$@"
  } 2>&1 | tee -a "$log_file"
}

(
  flock -n 9
  run_mode daily
  run_mode weekly --skip-collect
  run_mode monthly --skip-collect
  run_mode history --skip-collect
) 9>"$LOCK_FILE"
