default:
    just --list

init:

pl-reddit-00:
    bash scripts/pipeline/reddit/00_raw.sh

pl-reddit-01:
    bash scripts/pipeline/reddit/01_raw.sh
