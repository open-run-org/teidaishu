#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

source "$ROOT_DIR/scripts/lib/yaml.sh"
source "$ROOT_DIR/scripts/lib/log.sh"

CFG="${CFG:-$ROOT_DIR/config/pipeline/reddit/01_parquet.yaml}"

need_bin() { command -v "$1" >/dev/null 2>&1 || { log_error "missing binary: $1"; exit 1; }; }
esc_sql() { printf "%s" "$1" | sed "s/'/''/g"; }

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
need_bin duckdb

RAW_ROOT="$(yaml_get "$CFG" "raw_root")"
PARQUET_ROOT="$(yaml_get "$CFG" "parquet_root")"
COMPRESSION="$(yaml_get "$CFG" "compression")"
LOOKBACK_DAYS="$(yaml_get "$CFG" "lookback_days")"

RAW_ROOT="${RAW_ROOT:-data/reddit/00_raw}"
PARQUET_ROOT="${PARQUET_ROOT:-data/reddit/01_parquet}"
COMPRESSION="${COMPRESSION:-zstd}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-0}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-0}"
[[ "$LOOKBACK_DAYS" =~ ^[0-9]+$ ]] || { log_error "bad lookback_days=$LOOKBACK_DAYS"; exit 1; }

mapfile -t subs < <(yaml_list "$CFG" "subreddits")
TOTAL="${#subs[@]}"
[[ "$TOTAL" -gt 0 ]] || { log_error "no subreddits found in $CFG"; exit 1; }

task_start "reddit:01_parquet"
log_info "cfg=$CFG raw_root=$RAW_ROOT parquet_root=$PARQUET_ROOT lookback_days=$LOOKBACK_DAYS compression=$COMPRESSION subs=$TOTAL"

g_threads=0
g_files=0
g_wrote=0
g_skip=0

for sub in "${subs[@]}"; do
  log_info "subreddit=$sub begin"

  base_raw="$ROOT_DIR/$RAW_ROOT/r_${sub}"
  [[ -d "$base_raw" ]] || { log_warn "subreddit=$sub skip reason=no_raw_dir path=$base_raw"; continue; }

  for kind in submissions comments; do
    in_root="$base_raw/$kind"
    [[ -d "$in_root" ]] || { log_warn "subreddit=$sub kind=$kind missing_dir path=$in_root"; continue; }

    threads=0
    files=0
    wrote=0
    skipped=0

    while IFS= read -r td; do
      [[ -d "$td" ]] || continue
      threads=$((threads+1)); g_threads=$((g_threads+1))

      thread="$(basename "$td")"
      y="$(basename "$(dirname "$(dirname "$td")")")"
      md="$(basename "$(dirname "$td")")"
      hms="${thread%%_*}"
      sid="${thread#*_}"
      created_str="${y}${md}${hms}"

      out_thread_dir="$ROOT_DIR/$PARQUET_ROOT/r_${sub}/${kind}/${y}/${md}/${thread}"

      while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        files=$((files+1)); g_files=$((g_files+1))

        base="$(basename "$f" .jsonl)"
        capture_ts="${base%%_*}"
        hash="${base#*_}"
        capture_ts14="$(capture14 "$capture_ts")"
        [[ -n "$capture_ts14" ]] || { log_error "subreddit=$sub kind=$kind action=fail reason=bad_capture_ts file=$(basename "$f")"; exit 1; }

        out="$out_thread_dir/${capture_ts14}_${hash}.parquet"

        if [[ -f "$out" ]]; then
          skipped=$((skipped+1)); g_skip=$((g_skip+1))
          log_info "subreddit=$sub kind=$kind action=skip scope=file thread=$y/$md/$thread file=$(basename "$f") reason=exists out=$out"
          continue
        fi

        mkdir -p "$out_thread_dir"

        in_esc="$(esc_sql "$f")"
        out_esc="$(esc_sql "$out")"

        if [[ "$kind" == "submissions" ]]; then
          duckdb :memory: -c "
            COPY (
              SELECT
                coalesce(author, '') AS author,
                '${sid}' AS submission_id,
                CAST(epoch(strptime('${created_str}', '%Y%m%d%H%M%S')) AS BIGINT) AS created_utc,
                CAST(epoch(strptime('${capture_ts}', '%y%m%d%H%M%S')) AS BIGINT) AS capture_utc,
                coalesce(title, '') AS title,
                coalesce(selftext, '') AS body
              FROM read_json('${in_esc}', format='newline_delimited')
              LIMIT 1
            ) TO '${out_esc}' (FORMAT parquet, COMPRESSION '${COMPRESSION}');
          " >/dev/null
        else
          duckdb :memory: -c "
            COPY (
              SELECT
                coalesce(author, '') AS author,
                '${sid}' AS submission_id,
                coalesce(id, '') AS comment_id,
                coalesce(parent_id, '') AS parent_id,
                CAST(created_utc AS BIGINT) AS created_utc,
                CAST(epoch(strptime('${capture_ts}', '%y%m%d%H%M%S')) AS BIGINT) AS capture_utc,
                coalesce(body, '') AS body
              FROM read_json('${in_esc}', format='newline_delimited')
              WHERE id IS NOT NULL
            ) TO '${out_esc}' (FORMAT parquet, COMPRESSION '${COMPRESSION}');
          " >/dev/null
        fi

        wrote=$((wrote+1)); g_wrote=$((g_wrote+1))
        log_info "subreddit=$sub kind=$kind action=write thread=$y/$md/$thread file=$(basename "$f") out=$out"
      done < <(find "$td" -maxdepth 1 -type f -name '*.jsonl' 2>/dev/null | sort)
    done < <(list_thread_dirs "$in_root" "$LOOKBACK_DAYS")

    log_info "subreddit=$sub kind=$kind stats threads=$threads files=$files wrote=$wrote skipped=$skipped"
  done

  log_info "subreddit=$sub end"
done

log_info "done totals threads=$g_threads files=$g_files wrote=$g_wrote skipped=$g_skip"
task_end "reddit:01_parquet"
