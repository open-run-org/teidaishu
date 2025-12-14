#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

source "$ROOT_DIR/scripts/lib/yaml.sh"
source "$ROOT_DIR/scripts/lib/log.sh"

CFG="${CFG:-$ROOT_DIR/config/pipeline/reddit/02_staged.yaml}"

iter_days() {
  local n="$1" i=0
  while [[ $i -le $n ]]; do
    date -u -d "-${i} days" +%Y/%m%d
    i=$((i+1))
  done
}

list_thread_dirs() {
  local base="$1" lookback="$2"
  if [[ "$lookback" -le 0 ]]; then
    find "$base" -mindepth 3 -maxdepth 3 -type d 2>/dev/null | sort
    return 0
  fi
  while IFS= read -r ym; do
    local y="${ym%/*}" md="${ym#*/}"
    local day_dir="$base/$y/$md"
    [[ -d "$day_dir" ]] || continue
    find "$day_dir" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | sort
  done < <(iter_days "$lookback")
}

capture14() {
  local cap12="$1"
  duckdb :memory: -noheader -batch -c "
    SELECT strftime(to_timestamp(epoch(strptime('${cap12}', '%y%m%d%H%M%S'))), '%Y%m%d%H%M%S');
  " 2>/dev/null | tr -d '\r\n'
}

[[ -f "$CFG" ]] || { log_error "config not found: $CFG"; exit 1; }

PARQUET_ROOT="$(yaml_get "$CFG" "parquet_root")"
STAGED_ROOT="$(yaml_get "$CFG" "staged_root")"
LOOKBACK_DAYS="$(yaml_get "$CFG" "lookback_days")"

PARQUET_ROOT="${PARQUET_ROOT:-data/reddit/01_parquet}"
STAGED_ROOT="${STAGED_ROOT:-data/reddit/02_staged}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-0}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-0}"
[[ "$LOOKBACK_DAYS" =~ ^[0-9]+$ ]] || { log_error "bad lookback_days=$LOOKBACK_DAYS"; exit 1; }

mapfile -t subs < <(yaml_list "$CFG" "subreddits")
TOTAL="${#subs[@]}"
[[ "$TOTAL" -gt 0 ]] || { log_error "no subreddits found in $CFG"; exit 1; }

task_start "reddit:02_staged"
log_info "cfg=$CFG parquet_root=$PARQUET_ROOT staged_root=$STAGED_ROOT lookback_days=$LOOKBACK_DAYS subs=$TOTAL"

g_threads=0
g_wrote=0
g_skip=0
g_empty=0

for sub in "${subs[@]}"; do
  log_info "subreddit=$sub begin"

  base_01="$ROOT_DIR/$PARQUET_ROOT/r_${sub}"
  [[ -d "$base_01" ]] || { log_warn "subreddit=$sub skip reason=no_01_dir path=$base_01"; continue; }

  for kind in submissions comments; do
    in_root="$base_01/$kind"
    [[ -d "$in_root" ]] || { log_warn "subreddit=$sub kind=$kind missing_dir path=$in_root"; continue; }

    threads=0
    wrote=0
    skipped=0
    empty=0

    while IFS= read -r td; do
      [[ -d "$td" ]] || continue
      threads=$((threads+1)); g_threads=$((g_threads+1))

      latest="$(find "$td" -maxdepth 1 -type f -name '*.parquet' 2>/dev/null | sort | tail -n 1 || true)"
      y="$(basename "$(dirname "$(dirname "$td")")")"
      md="$(basename "$(dirname "$td")")"
      thread="$(basename "$td")"

      if [[ -z "$latest" ]]; then
        empty=$((empty+1)); g_empty=$((g_empty+1))
        log_info "subreddit=$sub kind=$kind action=skip scope=thread thread=$y/$md/$thread reason=no_parquet"
        continue
      fi

      fname="$(basename "$latest")"
      stem="${fname%.parquet}"
      capture_ts="${stem%%_*}"
      hash="${stem#*_}"
      capture_ts14="$(capture14 "$capture_ts")"
      [[ -n "$capture_ts14" ]] || { log_error "subreddit=$sub kind=$kind action=fail reason=bad_capture_ts file=$fname"; exit 1; }

      out_dir="$ROOT_DIR/$STAGED_ROOT/r_${sub}/${kind}/${y}/${md}"
      out="$out_dir/${thread}__${capture_ts14}_${hash}.parquet"

      if [[ -f "$out" ]]; then
        skipped=$((skipped+1)); g_skip=$((g_skip+1))
        log_info "subreddit=$sub kind=$kind action=skip scope=thread thread=$y/$md/$thread reason=exists out=$(basename "$out") src=$fname"
        continue
      fi

      mkdir -p "$out_dir"
      cp -f "$latest" "$out"
      wrote=$((wrote+1)); g_wrote=$((g_wrote+1))
      log_info "subreddit=$sub kind=$kind action=write thread=$y/$md/$thread latest=$fname out=$(basename "$out")"
    done < <(list_thread_dirs "$in_root" "$LOOKBACK_DAYS")

    log_info "subreddit=$sub kind=$kind stats threads=$threads wrote=$wrote skipped=$skipped empty=$empty"
  done

  log_info "subreddit=$sub end"
done

log_info "done totals threads=$g_threads wrote=$g_wrote skipped=$g_skip empty=$g_empty"
task_end "reddit:02_staged"
