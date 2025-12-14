#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

source "$ROOT_DIR/scripts/lib/yaml.sh"
source "$ROOT_DIR/scripts/lib/log.sh"

CFG="${CFG:-$ROOT_DIR/config/reddit/00_raw.yaml}"

: "${REDDIT_CLIENT_ID:?REDDIT_CLIENT_ID not set}"
: "${REDDIT_CLIENT_SECRET:?REDDIT_CLIENT_SECRET not set}"
: "${REDDIT_USER_AGENT:?REDDIT_USER_AGENT not set}"

if [[ ! -f "$CFG" ]]; then
  log_error "config not found: $CFG"
  exit 1
fi

RAW_ROOT="$(yaml_get "$CFG" "raw_root")"
APPS_ROOT="$(yaml_get "$CFG" "apps_root")"
LOOKBACK_DAYS="$(yaml_get "$CFG" "lookback_days")"
PAGE_LIMIT="$(yaml_get "$CFG" "page_limit")"
SLEEP_SEC="$(yaml_get "$CFG" "sleep_sec")"

RAW_ROOT="${RAW_ROOT:-data/reddit/00_raw}"
APPS_ROOT="${APPS_ROOT:-apps/reddit/harvest}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-7}"
PAGE_LIMIT="${PAGE_LIMIT:-100}"
SLEEP_SEC="${SLEEP_SEC:-0}"

mapfile -t subs < <(yaml_list "$CFG" "subreddits")
TOTAL="${#subs[@]}"
if [[ "$TOTAL" -eq 0 ]]; then
  log_error "no subreddits found in $CFG"
  exit 1
fi

mkdir -p "$ROOT_DIR/$RAW_ROOT"

run_tagged() {
  "$@" 2>&1 | while IFS= read -r ln; do
    printf "[INFO] %s\n" "$ln" >&2
  done
}

task_start "reddit:00_raw"
log_info "cfg=$CFG raw_root=$RAW_ROOT lookback_days=$LOOKBACK_DAYS page_limit=$PAGE_LIMIT subs=$TOTAL"

errs=0
for s in "${subs[@]}"; do
  sub="${s#r/}"
  sub="${sub#r_}"

  log_info "sub=$sub phase=submissions"
  if ! run_tagged bash -lc "cd '$ROOT_DIR/$APPS_ROOT' && go run ./cmd/submissions -sub '$sub' -days '$LOOKBACK_DAYS' -root '$ROOT_DIR/$RAW_ROOT' -limit '$PAGE_LIMIT'"; then
    log_error "sub=$sub phase=submissions failed"
    errs=$((errs+1))
    continue
  fi

  log_info "sub=$sub phase=comments"
  if ! run_tagged bash -lc "cd '$ROOT_DIR/$APPS_ROOT' && go run ./cmd/comments -sub '$sub' -days '$LOOKBACK_DAYS' -root '$ROOT_DIR/$RAW_ROOT'"; then
    log_error "sub=$sub phase=comments failed"
    errs=$((errs+1))
    continue
  fi

  if [[ "$SLEEP_SEC" != "0" ]]; then
    sleep "$SLEEP_SEC"
  fi
done

if [[ "$errs" -gt 0 ]]; then
  log_error "errors=$errs"
  task_end "reddit:00_raw"
  exit 2
fi

log_info "ok"
task_end "reddit:00_raw"
