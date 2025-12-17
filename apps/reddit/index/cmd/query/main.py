#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
import sys
from urllib.parse import urljoin

import requests
from google import genai
from google.genai import types

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
    if lookback_days <= 0:
        return None
    today = dt.datetime.now(dt.UTC).date()
    out = []
    for i in range(lookback_days + 1):
        d = today - dt.timedelta(days=i)
        out.append((str(d.year), f"{d.month:02d}{d.day:02d}"))
    return out

def _cf_post_json(url: str, token: str, payload: dict, timeout_s: int = 30):
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
        body = r.text if hasattr(r, "text") else "<no-body>"
        raise RuntimeError(f"cf_http_error status={r.status_code} url={r.url} body={body}")

    j = r.json()
    if not j.get("success", False):
        raise RuntimeError(f"cf_api_error url={r.url} resp={j}")
    return j

def _embed_query(client: genai.Client, model: str, q: str, task_type: str, dim: int):
    cfg = types.EmbedContentConfig(task_type=task_type, output_dimensionality=dim)
    res = client.models.embed_content(model=model, contents=[q], config=cfg)
    return res.embeddings[0].values

def _vectorize_query(account_id: str, token: str, index: str, vector: list[float], topk: int, return_metadata: str, return_values: bool, filt: dict | None, timeout_s: int):
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/vectorize/v2/indexes/{index}/query"
    payload = {
        "vector": vector,
        "topK": topk,
        "returnMetadata": return_metadata,
        "returnValues": return_values,
    }
    if filt is not None:
        payload["filter"] = filt
    data = _cf_post_json(url, token, payload, timeout_s=timeout_s)
    res = data.get("result") or {}
    return res.get("matches") or []

def _find_latest_02(staged_root: str, sub: str, kind: str, sid: str, lookback_days: int):
    base = os.path.join(staged_root, f"r_{sub}", kind)
    if not os.path.isdir(base):
        return None

    days = _iter_days(lookback_days)
    cands = []

    if days is None:
        for root, _, files in os.walk(base):
            for fn in files:
                if not fn.endswith(".parquet"):
                    continue
                m = RE_02.match(fn)
                if not m:
                    continue
                if m.group("sid") != sid:
                    continue
                cands.append(os.path.join(root, fn))
    else:
        for y, md in days:
            ddir = os.path.join(base, y, md)
            if not os.path.isdir(ddir):
                continue
            for fn in os.listdir(ddir):
                if not fn.endswith(".parquet"):
                    continue
                m = RE_02.match(fn)
                if not m:
                    continue
                if m.group("sid") != sid:
                    continue
                cands.append(os.path.join(ddir, fn))

    if not cands:
        return None
    cands.sort()
    return cands[-1]

def _excerpt_from_02(path: str, kind: str, cid: str | None, max_chars: int):
    try:
        import duckdb
    except Exception as e:
        return f"<duckdb_import_error {e}>"

    con = duckdb.connect(database=":memory:")
    try:
        if kind == "submissions":
            row = con.execute(
                "SELECT coalesce(title,''), coalesce(body,'') FROM read_parquet(?) LIMIT 1",
                [path],
            ).fetchone()
            if not row:
                return ""
            title, body = row
            text = (title or "").strip()
            b = (body or "").strip()
            if b:
                text = f"{text}\n\n{b}" if text else b
        else:
            if not cid:
                return ""
            row = con.execute(
                "SELECT coalesce(body,'') FROM read_parquet(?) WHERE comment_id = ? LIMIT 1",
                [path, cid],
            ).fetchone()
            if not row:
                return ""
            text = (row[0] or "").strip()

        text = (text or "").strip()
        if not text:
            return ""
        if max_chars > 0 and len(text) > max_chars:
            text = text[:max_chars]
        return text
    finally:
        con.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query", nargs="?", default="")
    ap.add_argument("--index", required=True)
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--gemini-model", required=True)
    ap.add_argument("--embed-dim", type=int, required=True)
    ap.add_argument("--task-type", default="RETRIEVAL_QUERY")
    ap.add_argument("--filter-json", default="")
    ap.add_argument("--timeout-s", type=int, default=30)
    ap.add_argument("--format", choices=["pretty", "jsonl"], default="pretty")

    ap.add_argument("--return-metadata", choices=["none", "indexed", "all"], default="all")
    ap.add_argument("--return-values", choices=["true", "false"], default="false")

    ap.add_argument("--with-text", action="store_true")
    ap.add_argument("--staged-root", default="data/reddit/02_staged")
    ap.add_argument("--lookback-days", type=int, default=7)
    ap.add_argument("--max-chars", type=int, default=600)
    args = ap.parse_args()

    q = (args.query or "").strip()
    if not q:
        q = sys.stdin.read().strip()
    if not q:
        log_error("missing query")
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

    topk = max(1, args.topk)
    if topk > 100:
        topk = 100
    if args.return_metadata == "all" or args.return_values == "true":
        if topk > 20:
            topk = 20

    filt = None
    if args.filter_json.strip():
        try:
            filt = json.loads(args.filter_json)
        except Exception:
            log_error("bad --filter-json")
            raise SystemExit(2)

    client = genai.Client(api_key=gemini_key)
    vec = _embed_query(client, args.gemini_model, q, args.task_type, args.embed_dim)
    matches = _vectorize_query(
        cf_account_id,
        cf_token,
        args.index,
        vec,
        topk,
        args.return_metadata,
        args.return_values == "true",
        filt,
        args.timeout_s,
    )

    log_info(f"query ok index={args.index} topk={topk} matches={len(matches)}")

    out_rows = []
    for m in matches:
        vid = str(m.get("id") or "")
        score = float(m.get("score") or 0.0)
        md = m.get("metadata") if isinstance(m.get("metadata"), dict) else {}
        row = {"id": vid, "score": score, "metadata": md}

        if args.with_text and isinstance(md, dict):
            sub = str(md.get("sub") or "")
            sid = str(md.get("sid") or "")
            t = str(md.get("t") or "")
            kind = "submissions" if t == "s" else "comments" if t == "c" else ""
            cid = vid.split(":")[-1] if (t == "c" and ":" in vid) else ""
            if sub and sid and kind:
                p = _find_latest_02(args.staged_root, sub, kind, sid, args.lookback_days)
                if p:
                    row["excerpt"] = _excerpt_from_02(p, kind, cid if kind == "comments" else None, args.max_chars)
                else:
                    row["excerpt"] = ""
            else:
                row["excerpt"] = ""

        out_rows.append(row)

    if args.format == "jsonl":
        for r in out_rows:
            sys.stdout.write(json.dumps(r, ensure_ascii=False) + "\n")
        return

    for i, r in enumerate(out_rows, 1):
        sys.stdout.write(f"{i}. score={r['score']:.6f} id={r['id']}\n")
        sys.stdout.write(json.dumps(r.get("metadata") or {}, ensure_ascii=False) + "\n")
        if args.with_text:
            ex = (r.get("excerpt") or "").strip()
            if ex:
                sys.stdout.write(ex + "\n")
        sys.stdout.write("\n")

if __name__ == "__main__":
    main()
