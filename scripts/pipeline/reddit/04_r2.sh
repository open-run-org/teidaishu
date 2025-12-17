#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$ROOT_DIR/scripts/lib/yaml.sh"
source "$ROOT_DIR/scripts/lib/log.sh"

CFG="${CFG:-$ROOT_DIR/config/pipeline/reddit/04_r2.yaml}"
[[ -f "$CFG" ]] || { log_error "config not found: $CFG"; exit 1; }

STAGED_ROOT="$(yaml_get "$CFG" "staged_root")"
LOOKBACK_DAYS="$(yaml_get "$CFG" "lookback_days")"
R2_BUCKET="$(yaml_get "$CFG" "r2_bucket")"
R2_PREFIX="$(yaml_get "$CFG" "r2_prefix")"
MAX_CHARS="$(yaml_get "$CFG" "max_chars")"
MAX_OBJECTS="$(yaml_get "$CFG" "max_objects_per_run")"
CHECK_EXISTS="$(yaml_get "$CFG" "check_exists")"
PUT_SLEEP_MS="$(yaml_get "$CFG" "put_sleep_ms")"
PUT_JITTER_MS="$(yaml_get "$CFG" "put_jitter_ms")"

STAGED_ROOT="${STAGED_ROOT:-data/reddit/02_staged}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-0}"
R2_PREFIX="${R2_PREFIX:-reddit/v1}"
MAX_CHARS="${MAX_CHARS:-20000}"
MAX_OBJECTS="${MAX_OBJECTS:-0}"
CHECK_EXISTS="${CHECK_EXISTS:-true}"
PUT_SLEEP_MS="${PUT_SLEEP_MS:-0}"
PUT_JITTER_MS="${PUT_JITTER_MS:-0}"

[[ "$LOOKBACK_DAYS" =~ ^[0-9]+$ ]] || { log_error "bad lookback_days=$LOOKBACK_DAYS"; exit 1; }
[[ "$MAX_CHARS" =~ ^[0-9]+$ ]] || { log_error "bad max_chars=$MAX_CHARS"; exit 1; }
[[ "$MAX_OBJECTS" =~ ^[0-9]+$ ]] || { log_error "bad max_objects_per_run=$MAX_OBJECTS"; exit 1; }
[[ "$PUT_SLEEP_MS" =~ ^[0-9]+$ ]] || { log_error "bad put_sleep_ms=$PUT_SLEEP_MS"; exit 1; }
[[ "$PUT_JITTER_MS" =~ ^[0-9]+$ ]] || { log_error "bad put_jitter_ms=$PUT_JITTER_MS"; exit 1; }

[[ -n "${R2_BUCKET:-}" ]] || { log_error "missing config: r2_bucket"; exit 1; }
[[ -n "${R2_ACCESS_KEY_ID:-}" ]] || { log_error "missing env: R2_ACCESS_KEY_ID"; exit 1; }
[[ -n "${R2_SECRET_ACCESS_KEY:-}" ]] || { log_error "missing env: R2_SECRET_ACCESS_KEY"; exit 1; }

PY="${PYTHON:-$ROOT_DIR/.venv/bin/python}"
[[ -x "$PY" ]] || { log_error "missing venv python: $PY"; exit 1; }

mapfile -t subs < <(yaml_list "$CFG" "subreddits")
TOTAL="${#subs[@]}"
[[ "$TOTAL" -gt 0 ]] || { log_error "no subreddits found in $CFG"; exit 1; }

task_start "reddit:04_r2"
log_info "cfg=$CFG staged_root=$STAGED_ROOT lookback_days=$LOOKBACK_DAYS bucket=$R2_BUCKET prefix=$R2_PREFIX subs=$TOTAL max_objects_per_run=$MAX_OBJECTS check_exists=$CHECK_EXISTS"

"$PY" "$ROOT_DIR/apps/reddit/r2/cmd/uploader/main.py" \
  --staged-root "$ROOT_DIR/$STAGED_ROOT" \
  --lookback-days "$LOOKBACK_DAYS" \
  --bucket "$R2_BUCKET" \
  --prefix "$R2_PREFIX" \
  --max-chars "$MAX_CHARS" \
  --max-objects-per-run "$MAX_OBJECTS" \
  --check-exists "$CHECK_EXISTS" \
  --put-sleep-ms "$PUT_SLEEP_MS" \
  --put-jitter-ms "$PUT_JITTER_MS" \
  $(printf -- "--sub %s " "${subs[@]}")

task_end "reddit:04_r2"
