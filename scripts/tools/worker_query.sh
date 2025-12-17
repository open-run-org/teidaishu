#!/usr/bin/env bash
set -euo pipefail

URL="${URL:-}"
TOPK="${TOPK:-10}"
WITH_TEXT="${WITH_TEXT:-true}"
MAX_CHARS="${MAX_CHARS:-600}"

if [[ -z "$URL" ]]; then
  echo "[ERROR] missing URL env" >&2
  exit 2
fi

q="$1"

curl -fsS "${URL%/}/query" \
  -H "Content-Type: application/json" \
  -d "$(jq -cn --arg q "$q" --argjson topk "$TOPK" --argjson with_text "$WITH_TEXT" --argjson max_chars "$MAX_CHARS" '{q:$q, topk:$topk, with_text:$with_text, max_chars:$max_chars}')"
