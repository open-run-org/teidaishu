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

is_yyyymmddhhmmss() { [[ "$1" =~ ^[0-9]{14}$ ]]; }

batch_init() {
  local f="$1" threads="$2"
  : > "$f"
  if [[ "$threads" -gt 0 ]]; then
    printf "SET threads=%s;\n" "$threads" >> "$f"
  fi
}

batch_flush() {
  local sql_file="$1" errf="$2" meta_name="$3" out_name="$4"
  local -n meta_arr="$meta_name"
  local -n out_arr="$out_name"

  if [[ "${#out_arr[@]}" -eq 0 ]]; then
    return 0
  fi

  if ! duckdb :memory: -batch -bail < "$sql_file" >/dev/null 2>"$errf"; then
    tail -n 200 "$errf" >&2 || true
    return 1
  fi

  local i
  for i in "${!out_arr[@]}"; do
    if [[ -f "${out_arr[$i]}" ]]; then
      log_info "${meta_arr[$i]}"
    else
      log_error "${meta_arr[$i]} reason=missing_out"
      return 1
    fi
  done

  meta_arr=()
  out_arr=()
  return 0
}

[[ -f "$CFG" ]] || { log_error "config not found: $CFG"; exit 1; }
need_bin duckdb

RAW_ROOT="$(yaml_get "$CFG" "raw_root")"
PARQUET_ROOT="$(yaml_get "$CFG" "parquet_root")"
COMPRESSION="$(yaml_get "$CFG" "compression")"
LOOKBACK_DAYS="$(yaml_get "$CFG" "lookback_days")"
THREADS="$(yaml_get "$CFG" "duckdb_threads")"
BATCH_SIZE="$(yaml_get "$CFG" "batch_size")"

RAW_ROOT="${RAW_ROOT:-data/reddit/00_raw}"
PARQUET_ROOT="${PARQUET_ROOT:-data/reddit/01_parquet}"
COMPRESSION="${COMPRESSION:-zstd}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-0}"
THREADS="${THREADS:-0}"
BATCH_SIZE="${BATCH_SIZE:-64}"

[[ "$LOOKBACK_DAYS" =~ ^[0-9]+$ ]] || { log_error "bad lookback_days=$LOOKBACK_DAYS"; exit 1; }
[[ "$THREADS" =~ ^[0-9]+$ ]] || { log_error "bad duckdb_threads=$THREADS"; exit 1; }
[[ "$BATCH_SIZE" =~ ^[0-9]+$ ]] || { log_error "bad batch_size=$BATCH_SIZE"; exit 1; }
[[ "$BATCH_SIZE" -gt 0 ]] || { log_error "bad batch_size=$BATCH_SIZE"; exit 1; }

mapfile -t subs < <(yaml_list "$CFG" "subreddits")
TOTAL="${#subs[@]}"
[[ "$TOTAL" -gt 0 ]] || { log_error "no subreddits found in $CFG"; exit 1; }

copy_opts="FORMAT parquet, COMPRESSION '${COMPRESSION}'"
if [[ "${COMPRESSION,,}" == "zstd" ]]; then
  copy_opts="FORMAT parquet, COMPRESSION '${COMPRESSION}', COMPRESSION_LEVEL 22"
fi

task_start "reddit:01_parquet"
log_info "cfg=$CFG raw_root=$RAW_ROOT parquet_root=$PARQUET_ROOT lookback_days=$LOOKBACK_DAYS compression=$COMPRESSION duckdb_threads=$THREADS batch_size=$BATCH_SIZE subs=$TOTAL"

sql_file="$(mktemp)"
err_file="$(mktemp)"
batch_init "$sql_file" "$THREADS"

declare -a write_metas=()
declare -a out_paths=()
queued=0

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
      sid="${thread#*_}"

      out_thread_dir="$ROOT_DIR/$PARQUET_ROOT/r_${sub}/${kind}/${y}/${md}/${thread}"

      while IFS= read -r f; do
        [[ -f "$f" ]] || continue
        files=$((files+1)); g_files=$((g_files+1))

        base="$(basename "$f" .jsonl)"
        cap14="${base%%_*}"
        hash="${base#*_}"
        hash16="${hash:0:16}"

        if ! is_yyyymmddhhmmss "$cap14"; then
          log_error "subreddit=$sub kind=$kind action=fail thread=$y/$md/$thread file=$(basename "$f") reason=bad_capture_ts capture=$cap14"
          rm -f "$sql_file" "$err_file"
          task_end "reddit:01_parquet"
          exit 2
        fi

        out="$out_thread_dir/${cap14}_${hash16}.parquet"

        if [[ -f "$out" ]]; then
          skipped=$((skipped+1)); g_skip=$((g_skip+1))
          log_info "subreddit=$sub kind=$kind action=skip scope=file thread=$y/$md/$thread file=$(basename "$f") reason=exists out=$(basename "$out")"
          continue
        fi

        mkdir -p "$out_thread_dir"

        log_info "subreddit=$sub kind=$kind action=plan thread=$y/$md/$thread file=$(basename "$f") out=$(basename "$out")"

        in_esc="$(esc_sql "$f")"
        out_esc="$(esc_sql "$out")"

        if [[ "$kind" == "submissions" ]]; then
          cat >> "$sql_file" <<SQL
COPY (
  SELECT
    coalesce(author, '') AS author,
    coalesce(selftext, '') AS body,
    coalesce(title, '') AS title
  FROM read_json('${in_esc}', format='newline_delimited')
  LIMIT 1
) TO '${out_esc}' (${copy_opts});
SQL
        else
          cat >> "$sql_file" <<SQL
COPY (
  SELECT
    coalesce(author, '') AS author,
    coalesce(body, '') AS body,
    coalesce(id, '') AS comment_id,
    coalesce(parent_id, '') AS parent_id
  FROM read_json('${in_esc}', format='newline_delimited')
  WHERE id IS NOT NULL
) TO '${out_esc}' (${copy_opts});
SQL
        fi

        write_metas+=("subreddit=$sub kind=$kind action=write thread=$y/$md/$thread file=$(basename "$f") out=$(basename "$out")")
        out_paths+=("$out")
        queued=$((queued+1))

        if [[ "$queued" -ge "$BATCH_SIZE" ]]; then
          local_flush="$queued"
          if ! batch_flush "$sql_file" "$err_file" write_metas out_paths; then
            rm -f "$sql_file" "$err_file"
            task_end "reddit:01_parquet"
            exit 2
          fi
          batch_init "$sql_file" "$THREADS"
          wrote=$((wrote+local_flush)); g_wrote=$((g_wrote+local_flush))
          queued=0
        fi
      done < <(find "$td" -maxdepth 1 -type f -name '*.jsonl' 2>/dev/null | sort)
    done < <(list_thread_dirs "$in_root" "$LOOKBACK_DAYS")

    if [[ "$queued" -gt 0 ]]; then
      local_flush="$queued"
      if ! batch_flush "$sql_file" "$err_file" write_metas out_paths; then
        rm -f "$sql_file" "$err_file"
        task_end "reddit:01_parquet"
        exit 2
      fi
      batch_init "$sql_file" "$THREADS"
      wrote=$((wrote+local_flush)); g_wrote=$((g_wrote+local_flush))
      queued=0
    fi

    log_info "subreddit=$sub kind=$kind stats threads=$threads files=$files wrote=$wrote skipped=$skipped"
  done

  log_info "subreddit=$sub end"
done

rm -f "$sql_file" "$err_file"

log_info "done totals threads=$g_threads files=$g_files wrote=$g_wrote skipped=$g_skip"
task_end "reddit:01_parquet"
