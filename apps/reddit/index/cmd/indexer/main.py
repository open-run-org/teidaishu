#!/usr/bin/env python3
import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import tempfile
import time
import random

import duckdb
import requests
from google import genai
from google.genai import types
from urllib.parse import urljoin

RE_02 = re.compile(r"^(?P<hms>\d{6})_(?P<sid>[A-Za-z0-9]+)_(?P<cap14>\d{14})_(?P<h16>[0-9a-fA-F]+)\.parquet$")

def log_info(msg: str):
    sys.stderr.write(f"[INFO] {msg}\n")
    sys.stderr.flush()

def log_warn(msg: str):
    sys.stderr.write(f"[WARN] {msg}\n")
    sys.stderr.flush()

def log_error(msg: str):
    sys.stderr.write(f"[ERROR] {msg}\n")
    sys.stderr.flush()

def _iter_days(lookback_days: int):
    today = dt.datetime.now(dt.UTC).date()
    if lookback_days <= 0:
        return None
    days = []
    for i in range(lookback_days + 1):
        d = today - dt.timedelta(days=i)
        days.append((str(d.year), f"{d.month:02d}{d.day:02d}"))
    return days

def _sha16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()[:16]

def _cf_post_json(url, token, payload, timeout_s=30):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    r = requests.post(url, headers=headers, json=payload, timeout=timeout_s, allow_redirects=False)

    if 300 <= r.status_code < 400:
        loc = r.headers.get("Location", "")
        if loc:
            url2 = urljoin(url, loc)
            r = requests.post(url2, headers=headers, json=payload, timeout=timeout_s, allow_redirects=False)

    if r.status_code >= 400:
        try:
            body = r.text
        except Exception:
            body = "<no-body>"
        raise RuntimeError(f"cf_http_error status={r.status_code} method={getattr(r.request,'method','?')} url={r.url} location={r.headers.get('Location','')} body={body}")

    j = r.json()
    if not j.get("success", False):
        raise RuntimeError(f"cf_api_error method={getattr(r.request,'method','?')} url={r.url} resp={j}")
    return j

def _cf_get_by_ids(account_id: str, token: str, index_name: str, ids: list[str]):
    if not ids:
        return {}
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/vectorize/v2/indexes/{index_name}/get_by_ids"
    data = _cf_post_json(url, token, {"ids": ids})
    res = data.get("result") or []
    out = {}
    for v in res:
        vid = v.get("id")
        if vid:
            out[vid] = v.get("metadata") or {}
    return out

def _read_submission_row(path: str):
    con = duckdb.connect(database=":memory:")
    rows = con.execute("SELECT coalesce(author,''), coalesce(title,''), coalesce(body,'') FROM read_parquet(?) LIMIT 1", [path]).fetchall()
    con.close()
    if not rows:
        return None
    return rows[0]

def _read_comment_rows(path: str):
    con = duckdb.connect(database=":memory:")
    rows = con.execute(
        "SELECT coalesce(comment_id,''), coalesce(parent_id,''), coalesce(author,''), coalesce(body,'') "
        "FROM read_parquet(?) WHERE comment_id IS NOT NULL",
        [path],
    ).fetchall()
    con.close()
    return rows

def _embed(client: genai.Client, model: str, texts: list[str], task_type: str, embed_dim: int):
    cfg = types.EmbedContentConfig(task_type=task_type, output_dimensionality=embed_dim)
    res = client.models.embed_content(model=model, contents=texts, config=cfg)
    return [e.values for e in res.embeddings]

def _flush(items_buf, cf_account_id, cf_token, client, args, budget_left):
    if not items_buf or budget_left <= 0:
        return 0, False

    items_buf.sort(key=lambda x: x[0])

    if len(items_buf) > budget_left:
        items_buf = items_buf[:budget_left]

    step = min(20, max(1, args.get_by_ids_batch_size))
    log_info(f"get_by_ids plan batch_size={step} total_ids={len(items_buf)}")

    remote_h = {}
    for i in range(0, len(items_buf), step):
        batch = items_buf[i : i + step]
        ids = [it[0] for it in batch]
        got = _cf_get_by_ids(cf_account_id, cf_token, args.index_name, ids)
        for vid, md in got.items():
            hv = ""
            if isinstance(md, dict):
                hv = str(md.get("h") or "")
            if hv:
                remote_h[vid] = hv
        if (i // step) % 20 == 0:
            log_info(f"get_by_ids_progress batches={(i//step)+1} remote_known={len(remote_h)}")

    to_upsert = []
    for vid, text, meta in items_buf:
        if remote_h.get(vid) == meta.get("h"):
            continue
        to_upsert.append((vid, text, meta))

    if not to_upsert:
        return 0, False

    to_upsert.sort(key=lambda x: x[0])
    if len(to_upsert) > budget_left:
        to_upsert = to_upsert[:budget_left]

    log_info(f"plan to_upsert={len(to_upsert)} budget_left={budget_left}")

    os.makedirs(args.index_root, exist_ok=True)
    fd, out_path = tempfile.mkstemp(prefix="teidaishu_03_index_", suffix=".ndjson", dir=args.index_root)
    os.close(fd)

    written = 0
    bs = max(1, args.embed_batch_size)
    stop = False

    with open(out_path, "w", encoding="utf-8") as f:
        for i in range(0, len(to_upsert), bs):
            chunk = to_upsert[i : i + bs]
            texts = [c[1] for c in chunk]

            ok = False
            last_err = None

            for attempt in range(args.embed_retry_max + 1):
                try:
                    vals = _embed(client, args.gemini_model, texts, args.task_type, args.embed_dim)
                    ok = True
                    break
                except Exception as e:
                    last_err = e
                    s = str(e)
                    is_429 = ("429" in s) or ("RESOURCE_EXHAUSTED" in s)
                    if not is_429:
                        raise
                    if attempt >= args.embed_retry_max:
                        break
                    backoff = (args.embed_retry_backoff_ms / 1000.0) * (2 ** attempt)
                    log_warn(f"embed action=retry reason=429 attempt={attempt+1}/{args.embed_retry_max} sleep={backoff}s")
                    time.sleep(backoff)

            if not ok:
                s = str(last_err) if last_err is not None else ""
                log_warn(f"embed action=stop reason=429 on_embed_429={args.on_embed_429} written={written} err={s}")
                stop = True
                break

            for (vid, _, meta), v in zip(chunk, vals):
                f.write(json.dumps({"id": vid, "values": v, "metadata": meta}, ensure_ascii=False))
                f.write("\n")
                written += 1

            log_info(f"embed_progress done={written}/{len(to_upsert)}")

            sleep_ms = args.embed_sleep_ms + (random.randint(0, args.embed_jitter_ms) if args.embed_jitter_ms > 0 else 0)
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)

    if written <= 0:
        try:
            os.remove(out_path)
        except Exception:
            pass
        return 0, stop

    log_info(f"emit ndjson={out_path} vectors={written}")
    sys.stdout.write(out_path + "\n")
    sys.stdout.flush()
    return written, stop

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--staged-root", required=True)
    ap.add_argument("--lookback-days", type=int, required=True)
    ap.add_argument("--index-name", required=True)
    ap.add_argument("--vector-dim", type=int, required=True)
    ap.add_argument("--gemini-model", required=True)
    ap.add_argument("--embed-dim", type=int, required=True)
    ap.add_argument("--task-type", required=True)
    ap.add_argument("--embed-batch-size", type=int, required=True)
    ap.add_argument("--get-by-ids-batch-size", type=int, required=True)
    ap.add_argument("--max-chars", type=int, required=True)
    ap.add_argument("--max-vectors-per-run", type=int, required=True)
    ap.add_argument("--sub", action="append", default=[])
    ap.add_argument("--index-root", required=True)
    ap.add_argument("--embed-sleep-ms", type=int, required=True)
    ap.add_argument("--embed-jitter-ms", type=int, required=True)
    ap.add_argument("--embed-retry-max", type=int, required=True)
    ap.add_argument("--embed-retry-backoff-ms", type=int, required=True)
    ap.add_argument("--on-embed-429", required=True)
    args = ap.parse_args()

    if args.embed_dim != args.vector_dim:
        log_error(f"embed_dim must equal vector_dim embed_dim={args.embed_dim} vector_dim={args.vector_dim}")
        raise SystemExit(2)

    cf_account_id = os.environ.get("CF_ACCOUNT_ID", "")
    cf_token = os.environ.get("CF_API_TOKEN", "")
    if not cf_account_id or not cf_token:
        log_error("missing CF_ACCOUNT_ID or CF_API_TOKEN")
        raise SystemExit(2)

    gemini_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or ""
    if not gemini_key:
        log_error("missing GEMINI_API_KEY (or GOOGLE_API_KEY)")
        raise SystemExit(2)

    client = genai.Client(api_key=gemini_key)
    days = _iter_days(args.lookback_days)

    candidates = []
    for sub in args.sub:
        for kind in ("submissions", "comments"):
            base = os.path.join(args.staged_root, f"r_{sub}", kind)
            if not os.path.isdir(base):
                log_warn(f"subreddit={sub} kind={kind} action=skip reason=missing_dir path={base}")
                continue

            if days is None:
                for root, _, files in os.walk(base):
                    for fn in files:
                        if fn.endswith(".parquet"):
                            candidates.append((sub, kind, os.path.join(root, fn)))
            else:
                for y, md in days:
                    ddir = os.path.join(base, y, md)
                    if not os.path.isdir(ddir):
                        continue
                    for fn in sorted(os.listdir(ddir)):
                        if fn.endswith(".parquet"):
                            candidates.append((sub, kind, os.path.join(ddir, fn)))

    if not candidates:
        return

    candidates.sort(key=lambda x: x[2])
    log_info(f"scan candidates={len(candidates)} lookback_days={args.lookback_days}")

    flush_size = max(1, args.get_by_ids_batch_size)

    items_buf = []
    parsed = 0
    total_written = 0

    for sub, kind, path in candidates:
        if total_written >= args.max_vectors_per_run:
            break

        fn = os.path.basename(path)
        m = RE_02.match(fn)
        if not m:
            continue
        sid = m.group("sid")
        parsed += 1

        if kind == "submissions":
            row = _read_submission_row(path)
            if row is not None:
                author, title, body = row
                text = (title or "").strip()
                b = (body or "").strip()
                if b:
                    text = f"{text}\n\n{b}" if text else b
                if text:
                    if len(text) > args.max_chars:
                        text = text[: args.max_chars]
                    h = _sha16(text)
                    vid = f"r:s:{sub}:{sid}"
                    meta = {"src": "r", "sub": sub, "t": "s", "sid": sid, "h": h}
                    items_buf.append((vid, text, meta))

                    if len(items_buf) >= flush_size:
                        w, stop = _flush(items_buf, cf_account_id, cf_token, client, args, args.max_vectors_per_run - total_written)
                        total_written += w
                        items_buf = []
                        if stop:
                            break
        else:
            rows = _read_comment_rows(path)
            for cid, pid, author, body in rows:
                if total_written >= args.max_vectors_per_run:
                    break
                body = (body or "").strip()
                if not body:
                    continue
                text = body
                if len(text) > args.max_chars:
                    text = text[: args.max_chars]
                h = _sha16(text)
                vid = f"r:c:{sub}:{cid}"
                meta = {"src": "r", "sub": sub, "t": "c", "sid": sid, "pid": pid or "", "h": h}
                items_buf.append((vid, text, meta))

                if len(items_buf) >= flush_size:
                    w, stop = _flush(items_buf, cf_account_id, cf_token, client, args, args.max_vectors_per_run - total_written)
                    total_written += w
                    items_buf = []
                    if stop:
                        break

        if parsed % 50 == 0:
            log_info(f"scan_progress files_parsed={parsed} items_buf={len(items_buf)} written={total_written} last={sub}/{kind}/{fn}")

    if items_buf and total_written < args.max_vectors_per_run:
        w, _ = _flush(items_buf, cf_account_id, cf_token, client, args, args.max_vectors_per_run - total_written)
        total_written += w

    if total_written <= 0:
        return

if __name__ == "__main__":
    main()
