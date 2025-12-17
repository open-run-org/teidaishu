#!/usr/bin/env bash
set -euo pipefail

URL="${URL:-}"
TOPK="${TOPK:-20}"
MAX_DOCS="${MAX_DOCS:-8}"
CTX_MAX_CHARS="${CTX_MAX_CHARS:-1200}"
TEMPERATURE="${TEMPERATURE:-0.4}"
MAX_OUTPUT_TOKENS="${MAX_OUTPUT_TOKENS:-800}"

if [[ -z "$URL" ]]; then
  echo "[ERROR] missing URL env" >&2
  exit 2
fi

q="$1"

curl -fsS "${URL%/}/ask" \
  -H "Content-Type: application/json" \
  -d "$(jq -cn \
    --arg q "$q" \
    --argjson topk "$TOPK" \
    --argjson max_docs "$MAX_DOCS" \
    --argjson ctx_max_chars "$CTX_MAX_CHARS" \
    --argjson temperature "$TEMPERATURE" \
    --argjson max_output_tokens "$MAX_OUTPUT_TOKENS" \
    '{q:$q, topk:$topk, max_docs:$max_docs, ctx_max_chars:$ctx_max_chars, temperature:$temperature, max_output_tokens:$max_output_tokens, dedup_sid:true}')"
