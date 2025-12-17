#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$ROOT_DIR/scripts/lib/yaml.sh"
source "$ROOT_DIR/scripts/lib/log.sh"

CFG="${CFG:-$ROOT_DIR/config/pipeline/reddit/03_index.yaml}"

[[ -f "$CFG" ]] || { log_error "config not found: $CFG"; exit 1; }

STAGED_ROOT="$(yaml_get "$CFG" "staged_root")"
LOOKBACK_DAYS="$(yaml_get "$CFG" "lookback_days")"
INDEX_NAME="$(yaml_get "$CFG" "vectorize_index")"
VECTOR_DIM="$(yaml_get "$CFG" "vector_dim")"
GEMINI_MODEL="$(yaml_get "$CFG" "gemini_model")"
EMBED_DIM="$(yaml_get "$CFG" "embed_dim")"
TASK_TYPE="$(yaml_get "$CFG" "task_type")"
EMBED_BATCH_SIZE="$(yaml_get "$CFG" "embed_batch_size")"
GET_BATCH_SIZE="$(yaml_get "$CFG" "get_by_ids_batch_size")"
MAX_CHARS="$(yaml_get "$CFG" "max_chars")"
MAX_VECTORS="$(yaml_get "$CFG" "max_vectors_per_run")"

INDEX_ROOT="$(yaml_get "$CFG" "index_root")"
EMBED_SLEEP_MS="$(yaml_get "$CFG" "embed_sleep_ms")"
EMBED_JITTER_MS="$(yaml_get "$CFG" "embed_jitter_ms")"
EMBED_RETRY_MAX="$(yaml_get "$CFG" "embed_retry_max")"
EMBED_RETRY_BACKOFF_MS="$(yaml_get "$CFG" "embed_retry_backoff_ms")"
ON_EMBED_429="$(yaml_get "$CFG" "on_embed_429")"

STAGED_ROOT="${STAGED_ROOT:-data/reddit/02_staged}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-0}"
VECTOR_DIM="${VECTOR_DIM:-1536}"
GEMINI_MODEL="${GEMINI_MODEL:-gemini-embedding-001}"
EMBED_DIM="${EMBED_DIM:-1536}"
TASK_TYPE="${TASK_TYPE:-RETRIEVAL_DOCUMENT}"
EMBED_BATCH_SIZE="${EMBED_BATCH_SIZE:-128}"
GET_BATCH_SIZE="${GET_BATCH_SIZE:-200}"
MAX_CHARS="${MAX_CHARS:-20000}"
MAX_VECTORS="${MAX_VECTORS:-4096}"

INDEX_ROOT="${INDEX_ROOT:-data/reddit/03_index}"
EMBED_SLEEP_MS="${EMBED_SLEEP_MS:-900}"
EMBED_JITTER_MS="${EMBED_JITTER_MS:-400}"
EMBED_RETRY_MAX="${EMBED_RETRY_MAX:-6}"
EMBED_RETRY_BACKOFF_MS="${EMBED_RETRY_BACKOFF_MS:-1500}"
ON_EMBED_429="${ON_EMBED_429:-stop}"

[[ "$LOOKBACK_DAYS" =~ ^[0-9]+$ ]] || { log_error "bad lookback_days=$LOOKBACK_DAYS"; exit 1; }
[[ "$VECTOR_DIM" =~ ^[0-9]+$ ]] || { log_error "bad vector_dim=$VECTOR_DIM"; exit 1; }
[[ "$EMBED_DIM" =~ ^[0-9]+$ ]] || { log_error "bad embed_dim=$EMBED_DIM"; exit 1; }
[[ "$EMBED_BATCH_SIZE" =~ ^[0-9]+$ ]] || { log_error "bad embed_batch_size=$EMBED_BATCH_SIZE"; exit 1; }
[[ "$GET_BATCH_SIZE" =~ ^[0-9]+$ ]] || { log_error "bad get_by_ids_batch_size=$GET_BATCH_SIZE"; exit 1; }
[[ "$MAX_CHARS" =~ ^[0-9]+$ ]] || { log_error "bad max_chars=$MAX_CHARS"; exit 1; }
[[ "$MAX_VECTORS" =~ ^[0-9]+$ ]] || { log_error "bad max_vectors_per_run=$MAX_VECTORS"; exit 1; }

[[ -n "${CF_ACCOUNT_ID:-}" ]] || { log_error "missing env: CF_ACCOUNT_ID"; exit 1; }
[[ -n "${CF_API_TOKEN:-}" ]] || { log_error "missing env: CF_API_TOKEN"; exit 1; }
[[ -n "${GEMINI_API_KEY:-${GOOGLE_API_KEY:-}}" ]] || { log_error "missing env: GEMINI_API_KEY (or GOOGLE_API_KEY)"; exit 1; }
[[ -n "${INDEX_NAME:-}" ]] || { log_error "missing config: vectorize_index"; exit 1; }

[[ "$EMBED_SLEEP_MS" =~ ^[0-9]+$ ]] || { log_error "bad embed_sleep_ms=$EMBED_SLEEP_MS"; exit 1; }
[[ "$EMBED_JITTER_MS" =~ ^[0-9]+$ ]] || { log_error "bad embed_jitter_ms=$EMBED_JITTER_MS"; exit 1; }
[[ "$EMBED_RETRY_MAX" =~ ^[0-9]+$ ]] || { log_error "bad embed_retry_max=$EMBED_RETRY_MAX"; exit 1; }
[[ "$EMBED_RETRY_BACKOFF_MS" =~ ^[0-9]+$ ]] || { log_error "bad embed_retry_backoff_ms=$EMBED_RETRY_BACKOFF_MS"; exit 1; }

mapfile -t subs < <(yaml_list "$CFG" "subreddits")
TOTAL="${#subs[@]}"
[[ "$TOTAL" -gt 0 ]] || { log_error "no subreddits found in $CFG"; exit 1; }

PY="${PYTHON:-$ROOT_DIR/.venv/bin/python}"
[[ -x "$PY" ]] || { log_error "missing venv python: $PY"; exit 1; }

task_start "reddit:03_index"
log_info "cfg=$CFG staged_root=$STAGED_ROOT index_root=$INDEX_ROOT lookback_days=$LOOKBACK_DAYS index=$INDEX_NAME dim=$VECTOR_DIM subs=$TOTAL max_vectors_per_run=$MAX_VECTORS"

mkdir -p "$ROOT_DIR/$INDEX_ROOT"

did_any=0

while IFS= read -r ndjson_path; do
  [[ -n "${ndjson_path:-}" ]] || continue
  did_any=1
  log_info "action=upsert file=$(basename "$ndjson_path")"
  curl -fsS "https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/vectorize/v2/indexes/${INDEX_NAME}/upsert" \
    -H "Authorization: Bearer ${CF_API_TOKEN}" \
    -H "Content-Type: application/x-ndjson" \
    --data-binary "@${ndjson_path}" >/dev/null
  rm -f "$ndjson_path"
done < <(
  "$PY" "$ROOT_DIR/apps/reddit/index/cmd/indexer/main.py" \
    --staged-root "$ROOT_DIR/$STAGED_ROOT" \
    --index-root "$ROOT_DIR/$INDEX_ROOT" \
    --lookback-days "$LOOKBACK_DAYS" \
    --index-name "$INDEX_NAME" \
    --vector-dim "$VECTOR_DIM" \
    --gemini-model "$GEMINI_MODEL" \
    --embed-dim "$EMBED_DIM" \
    --task-type "$TASK_TYPE" \
    --embed-batch-size "$EMBED_BATCH_SIZE" \
    --get-by-ids-batch-size "$GET_BATCH_SIZE" \
    --max-chars "$MAX_CHARS" \
    --max-vectors-per-run "$MAX_VECTORS" \
    --embed-sleep-ms "$EMBED_SLEEP_MS" \
    --embed-jitter-ms "$EMBED_JITTER_MS" \
    --embed-retry-max "$EMBED_RETRY_MAX" \
    --embed-retry-backoff-ms "$EMBED_RETRY_BACKOFF_MS" \
    --on-embed-429 "$ON_EMBED_429" \
    $(printf -- "--sub %s " "${subs[@]}")
)

if [[ "$did_any" -eq 0 ]]; then
  log_info "action=skip reason=no_vectors"
fi

log_info "action=done"
task_end "reddit:03_index"
