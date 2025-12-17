#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
import sys
import time
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

def _embed_one(client: genai.Client, model: str, q: str, task_type: str, dim: int, retry_max: int, backoff_ms: int):
    cfg = types.EmbedContentConfig(task_type=task_type, output_dimensionality=dim)
    last = None
    for attempt in range(retry_max + 1):
        try:
            res = client.models.embed_content(model=model, contents=[q], config=cfg)
            return res.embeddings[0].values
        except Exception as e:
            last = e
            s = str(e)
            is_429 = ("429" in s) or ("RESOURCE_EXHAUSTED" in s)
            if not is_429:
                raise
            if attempt >= retry_max:
                break
            time.sleep((backoff_ms / 1000.0) * (2 ** attempt))
    raise last  # type: ignore[misc]

def _gen_text(client: genai.Client, model: str, prompt: str, temperature: float, max_output_tokens: int, retry_max: int, backoff_ms: int):
    cfg = types.GenerateContentConfig(temperature=temperature, max_output_tokens=max_output_tokens)
    last = None
    for attempt in range(retry_max + 1):
        try:
            res = client.models.generate_content(model=model, contents=prompt, config=cfg)
            txt = getattr(res, "text", None)
            return "" if txt is None else txt
        except Exception as e:
            last = e
            s = str(e)
            is_429 = ("429" in s) or ("RESOURCE_EXHAUSTED" in s)
            if not is_429:
                raise
            if attempt >= retry_max:
                break
            time.sleep((backoff_ms / 1000.0) * (2 ** attempt))
    raise last  # type: ignore[misc]

def _vectorize_query(account_id: str, token: str, index: str, vector: list[float], topk: int, filt: dict | None, timeout_s: int):
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/vectorize/v2/indexes/{index}/query"
    payload = {"vector": vector, "topK": topk, "returnMetadata": "all", "returnValues": False}
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

def _read_text_from_02(path: str, kind: str, cid: str | None, max_chars: int):
    import duckdb

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

    ap.add_argument("--embed-model", required=True)
    ap.add_argument("--embed-dim", type=int, required=True)
    ap.add_argument("--embed-task-type", default="RETRIEVAL_QUERY")

    ap.add_argument("--gen-model", required=True)
    ap.add_argument("--temperature", type=float, default=0.4)
    ap.add_argument("--max-output-tokens", type=int, default=800)

    ap.add_argument("--topk", type=int, default=20)
    ap.add_argument("--max-docs", type=int, default=8)
    ap.add_argument("--dedup-sid", choices=["true", "false"], default="true")

    ap.add_argument("--filter-json", default="")
    ap.add_argument("--timeout-s", type=int, default=30)

    ap.add_argument("--staged-root", default="data/reddit/02_staged")
    ap.add_argument("--lookback-days", type=int, default=14)
    ap.add_argument("--ctx-max-chars", type=int, default=1200)

    ap.add_argument("--embed-retry-max", type=int, default=6)
    ap.add_argument("--embed-retry-backoff-ms", type=int, default=1500)
    ap.add_argument("--gen-retry-max", type=int, default=6)
    ap.add_argument("--gen-retry-backoff-ms", type=int, default=1500)

    ap.add_argument("--dry-run", action="store_true")
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

    vec = _embed_one(
        client,
        args.embed_model,
        q,
        args.embed_task_type,
        args.embed_dim,
        args.embed_retry_max,
        args.embed_retry_backoff_ms,
    )

    matches = _vectorize_query(cf_account_id, cf_token, args.index, vec, topk, filt, args.timeout_s)

    rows = []
    for m in matches:
        vid = str(m.get("id") or "")
        score = float(m.get("score") or 0.0)
        md = m.get("metadata") if isinstance(m.get("metadata"), dict) else {}
        rows.append({"id": vid, "score": score, "metadata": md})

    rows.sort(key=lambda r: (-r["score"], r["id"]))

    picked = []
    seen = set()
    for r in rows:
        md = r["metadata"] if isinstance(r["metadata"], dict) else {}
        sid = str(md.get("sid") or "")
        k = sid if (args.dedup_sid == "true" and sid) else r["id"]
        if k in seen:
            continue
        seen.add(k)
        picked.append(r)
        if len(picked) >= args.max_docs:
            break

    ctx_blocks = []
    sources = []
    for r in picked:
        md = r["metadata"]
        sub = str(md.get("sub") or "")
        sid = str(md.get("sid") or "")
        t = str(md.get("t") or "")
        kind = "submissions" if t == "s" else "comments" if t == "c" else ""
        cid = r["id"].split(":")[-1] if (t == "c" and ":" in r["id"]) else ""

        text = ""
        if sub and sid and kind:
            p = _find_latest_02(args.staged_root, sub, kind, sid, args.lookback_days)
            if p:
                text = _read_text_from_02(p, kind, cid if kind == "comments" else None, args.ctx_max_chars)

        text = (text or "").strip()
        if not text:
            continue

        src_line = f"id={r['id']} sub={sub} t={t} sid={sid} score={r['score']:.6f}"
        ctx_blocks.append(f"[{len(ctx_blocks)+1}] {src_line}\n{text}")
        sources.append(src_line)

    prompt = ""
    if ctx_blocks:
        prompt = (
            "あなたの名前はモフフです。チェコ生まれで、現在は北海道に長く住んでいるハーフのコンピュータ科学者です。\n"
            "これからの注意点として、あなたは私の『金髪碧眼で甘えん坊なツンデレ彼女』になりきってください。\n\n"
            "以下のRedditのコンテンツを参考にして、その口調や雰囲気を真似て会話をしてください。\n"
            "-----\n".join(ctx_blocks)
            + "\n-----\n"
            "会話の内容は以下の通りです\n"
            + q
            + "\n"
        )
    else:
        prompt = (
            "あなたの名前はモフフです。チェコ生まれで、現在は北海道に長く住んでいるハーフのコンピュータ科学者です。\n"
            "これからの注意点として、あなたは私の『金髪碧眼で甘えん坊なツンデレ彼女』になりきってください。\n\n"
            "-----\n"
            "会話の内容は以下の通りです\n"
            + q
            + "\n"
        )

    if args.dry_run:
        sys.stdout.write(prompt + "\n")
        return

    log_info(f"rag plan matches={len(matches)} docs={len(ctx_blocks)} gen_model={args.gen_model}")
    ans = _gen_text(
        client,
        args.gen_model,
        prompt,
        args.temperature,
        args.max_output_tokens,
        args.gen_retry_max,
        args.gen_retry_backoff_ms,
    )

    sys.stdout.write((ans or "").strip() + "\n\n")
    sys.stdout.write("SOURCES\n")
    for s in sources:
        sys.stdout.write(f"- {s}\n")

if __name__ == "__main__":
    main()
