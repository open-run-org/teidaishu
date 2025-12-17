#!/usr/bin/env python3
import argparse
import datetime as dt
import hashlib
import os
import random
import re
import sys
import time

import duckdb
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

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
    out = []
    for i in range(lookback_days + 1):
        d = today - dt.timedelta(days=i)
        out.append((str(d.year), f"{d.month:02d}{d.day:02d}"))
    return out

def _sha16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()[:16]

def _read_submission_row(path: str):
    con = duckdb.connect(database=":memory:")
    rows = con.execute(
        "SELECT coalesce(author,''), coalesce(title,''), coalesce(body,'') FROM read_parquet(?) LIMIT 1",
        [path],
    ).fetchall()
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

def _norm_prefix(p: str) -> str:
    p = (p or "").strip().strip("/")
    return p

def _key_for(typ: str, sub: str, sid_or_cid: str, h: str, prefix: str) -> str:
    prefix = _norm_prefix(prefix)
    base = f"r/{typ}/{sub}/{sid_or_cid}/{h}.txt"
    return f"{prefix}/{base}" if prefix else base

def _mk_s3():
    ak = os.environ.get("R2_ACCESS_KEY_ID", "")
    sk = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    if not ak or not sk:
        log_error("missing R2_ACCESS_KEY_ID or R2_SECRET_ACCESS_KEY")
        raise SystemExit(2)

    endpoint = os.environ.get("R2_ENDPOINT", "").strip()
    if not endpoint:
        acc = os.environ.get("CF_ACCOUNT_ID", "").strip()
        if acc:
            endpoint = f"https://{acc}.r2.cloudflarestorage.com"
    if not endpoint:
        log_error("missing R2_ENDPOINT (or CF_ACCOUNT_ID to derive endpoint)")
        raise SystemExit(2)

    cfg = Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"})
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="auto",
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        config=cfg,
    ), endpoint

def _exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def _put_text(s3, bucket: str, key: str, text: str):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--staged-root", required=True)
    ap.add_argument("--lookback-days", type=int, required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--max-chars", type=int, required=True)
    ap.add_argument("--max-objects-per-run", type=int, required=True)
    ap.add_argument("--check-exists", required=True)
    ap.add_argument("--put-sleep-ms", type=int, required=True)
    ap.add_argument("--put-jitter-ms", type=int, required=True)
    ap.add_argument("--sub", action="append", default=[])
    args = ap.parse_args()

    check_exists = str(args.check_exists).lower() == "true"
    days = _iter_days(args.lookback_days)

    s3, endpoint = _mk_s3()
    log_info(f"r2 endpoint={endpoint} bucket={args.bucket} prefix={_norm_prefix(args.prefix)} check_exists={check_exists}")

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

    put_ok = 0
    skip_exist = 0
    empty = 0
    parsed = 0
    files = 0

    for sub, kind, path in candidates:
        fn = os.path.basename(path)
        m = RE_02.match(fn)
        if not m:
            continue
        sid = m.group("sid")
        parsed += 1
        files += 1

        if kind == "submissions":
            row = _read_submission_row(path)
            if row is None:
                empty += 1
                continue
            author, title, body = row
            text = (title or "").strip()
            b = (body or "").strip()
            if b:
                text = f"{text}\n\n{b}" if text else b
            if not text:
                empty += 1
                continue
            if len(text) > args.max_chars:
                text = text[: args.max_chars]
            h = _sha16(text)
            key = _key_for("s", sub, sid, h, args.prefix)

            if check_exists and _exists(s3, args.bucket, key):
                skip_exist += 1
            else:
                _put_text(s3, args.bucket, key, text)
                put_ok += 1

        else:
            rows = _read_comment_rows(path)
            if not rows:
                empty += 1
                continue
            for cid, pid, author, body in rows:
                body = (body or "").strip()
                if not body:
                    continue
                text = body
                if len(text) > args.max_chars:
                    text = text[: args.max_chars]
                h = _sha16(text)
                key = _key_for("c", sub, cid, h, args.prefix)

                if check_exists and _exists(s3, args.bucket, key):
                    skip_exist += 1
                else:
                    _put_text(s3, args.bucket, key, text)
                    put_ok += 1

                if args.max_objects_per_run > 0 and put_ok >= args.max_objects_per_run:
                    log_info(f"stop reason=max_objects_per_run put_ok={put_ok}")
                    log_info(f"done files={files} parsed={parsed} put_ok={put_ok} skip_exist={skip_exist} empty={empty}")
                    return

                sleep_ms = args.put_sleep_ms + (random.randint(0, args.put_jitter_ms) if args.put_jitter_ms > 0 else 0)
                if sleep_ms > 0:
                    time.sleep(sleep_ms / 1000.0)

        if parsed % 50 == 0:
            log_info(f"progress files_parsed={parsed} put_ok={put_ok} skip_exist={skip_exist} empty={empty} last={sub}/{kind}/{fn}")

    log_info(f"done files={files} parsed={parsed} put_ok={put_ok} skip_exist={skip_exist} empty={empty}")

if __name__ == "__main__":
    main()
