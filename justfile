default:
    just --list

init:
    just init-py

init-py:
    just py-venv && \
    just py-lock && \
    just py-deps

py-venv:
    uv venv --clear

py-lock:
    uv pip compile requirements.in -o requirements.txt

py-deps:
    VIRTUAL_ENV=.venv uv pip sync requirements.txt

import-arctic:
    python3 scripts/tools/import_arctic.py --root data/reddit/00_raw data/import/arctic/*_posts.jsonl data/import/arctic/*_comments.jsonl

query-vec QUERY:
    bash scripts/tools/query_vectorize.sh \
      --index open-run-teidaishu-reddit-ja \
      --gemini-model gemini-embedding-001 \
      --embed-dim 1536 \
      --task-type RETRIEVAL_QUERY \
      --topk 16 \
      --return-metadata all \
      --return-values false \
      --with-text \
      --staged-root data/reddit/02_staged \
      --lookback-days 256 \
      --max-chars 4096 \
      "{{QUERY}}"

ask-rag QUERY:
    bash scripts/tools/ask_rag.sh \
      --index open-run-teidaishu-reddit-ja \
      --embed-model gemini-embedding-001 \
      --embed-dim 1536 \
      --embed-task-type RETRIEVAL_QUERY \
      --gen-model gemini-2.5-flash \
      --topk 16 \
      --max-docs 16 \
      --dedup-sid true \
      --staged-root data/reddit/02_staged \
      --lookback-days 16 \
      --ctx-max-chars 256 \
      --temperature 0.4 \
      --max-output-tokens 4096 \
      "{{QUERY}}"

worker-deploy:
    pnpm exec wrangler deploy --cwd apps/teidaishu/worker

worker-query q:
    URL={{env_var_or_default("WORKER_URL","")}} bash scripts/tools/worker_query.sh "{{q}}"

worker-ask q:
    URL={{env_var_or_default("WORKER_URL","")}} bash scripts/tools/worker_ask.sh "{{q}}"

worker-tail:
    cd apps/teidaishu/worker && pnpm exec wrangler tail teidaishu-api --format pretty

discord-cmds:
    bash scripts/tools/discord_register_commands.sh

pl-reddit:
    just pl-reddit-00 && \
    just pl-reddit-01 && \
    just pl-reddit-02 && \
    just pl-reddit-03 && \
    just pl-reddit-04

pl-reddit-00:
    bash scripts/pipeline/reddit/00_raw.sh

pl-reddit-01:
    bash scripts/pipeline/reddit/01_parquet.sh

pl-reddit-02:
    bash scripts/pipeline/reddit/02_staged.sh

pl-reddit-03:
    bash scripts/pipeline/reddit/03_index.sh

pl-reddit-04:
    bash scripts/pipeline/reddit/04_r2.sh
