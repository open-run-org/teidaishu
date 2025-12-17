import argparse
import datetime
import hashlib
import json
import os
import sys
from pathlib import Path

def log(level, msg):
    sys.stderr.write(f"[{level}] {msg}\n")

def ts_fmt(unix_ts):
    try:
        v = int(float(unix_ts))
    except Exception:
        return ""
    if v <= 0:
        return ""
    return datetime.datetime.fromtimestamp(v, datetime.timezone.utc).strftime("%Y%m%d%H%M%S")

def split_created(unix_ts: int):
    dt = datetime.datetime.fromtimestamp(int(unix_ts), datetime.timezone.utc)
    return dt.strftime("%Y"), dt.strftime("%m%d"), dt.strftime("%H%M%S")

def norm_sub(s):
    s = (s or "").strip()
    if s.startswith(("r/", "R/")):
        s = s[2:]
    if s.startswith(("r_", "R_")):
        s = s[2:]
    return s.strip()

def has_hash_file(dir_path: Path, h: str) -> bool:
    if not dir_path.is_dir():
        return False
    needle = "_" + h
    with os.scandir(dir_path) as it:
        for e in it:
            if e.is_file() and e.name.endswith(".jsonl") and needle in e.name:
                return True
    return False

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def hash_post(obj):
    data = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8", "ignore")
    return sha256_bytes(data)

def comment_sort_key(r):
    pid = r.get("parent_id") or ""
    try:
        cu = float(r.get("created_utc") or 0)
    except Exception:
        cu = 0.0
    cid = r.get("id") or ""
    return (pid, cu, cid)

def hash_comments(rows):
    buf = []
    for r in rows:
        buf.append(json.dumps(r, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    data = ("\n".join(buf)).encode("utf-8", "ignore")
    return sha256_bytes(data)

def write_one_jsonl(path: Path, line: str) -> bool:
    try:
        with path.open("x", encoding="utf-8") as w:
            w.write(line)
            if not line.endswith("\n"):
                w.write("\n")
        return True
    except FileExistsError:
        return False

def write_many_jsonl(path: Path, rows) -> bool:
    try:
        with path.open("x", encoding="utf-8") as w:
            for r in rows:
                w.write(json.dumps(r, ensure_ascii=False))
                w.write("\n")
        return True
    except FileExistsError:
        return False

def thread_dir(root: Path, sub: str, kind: str, created_unix: int, pid: str) -> Path:
    y, md, hms = split_created(created_unix)
    return root / f"r_{sub}" / kind / y / md / f"{hms}_{pid}"

def import_posts(path: Path, root: Path, post_index: dict, report_every: int):
    wrote = 0
    scanned = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            scanned += 1
            if report_every > 0 and scanned % report_every == 0:
                log("INFO", f"posts progress file={path} scanned={scanned} wrote={wrote}")
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue

            sub = norm_sub(obj.get("subreddit") or obj.get("subreddit_name_prefixed") or "")
            if not sub:
                continue

            pid = obj.get("id")
            created = obj.get("created_utc")
            if not pid or created is None:
                continue
            try:
                created_int = int(float(created))
            except Exception:
                continue
            if created_int <= 0:
                continue

            k = (sub, pid)
            if k not in post_index or created_int < post_index[k]:
                post_index[k] = created_int

            retrieved = obj.get("retrieved_utc")
            if retrieved is None:
                retrieved = obj.get("retrieved_on")
            if retrieved is None:
                retrieved = datetime.datetime.now(datetime.timezone.utc).timestamp()

            capture_ts = ts_fmt(retrieved) or ts_fmt(created_int)
            h = hash_post(obj)

            subdir = thread_dir(root, sub, "submissions", created_int, pid)
            subdir.mkdir(parents=True, exist_ok=True)

            if has_hash_file(subdir, h):
                continue

            out_path = subdir / f"{capture_ts}_{h}.jsonl"
            if write_one_jsonl(out_path, json.dumps(obj, ensure_ascii=False)):
                wrote += 1

    log("INFO", f"posts done file={path} scanned={scanned} wrote={wrote}")

def import_comments(path: Path, root: Path, post_index: dict, report_every: int):
    groups = {}
    scanned = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            scanned += 1
            if report_every > 0 and scanned % report_every == 0:
                log("INFO", f"comments progress file={path} scanned={scanned} threads={len(groups)}")
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue

            sub = norm_sub(obj.get("subreddit") or "")
            if not sub:
                continue

            link_id = obj.get("link_id")
            if not isinstance(link_id, str) or not link_id.startswith("t3_"):
                continue
            pid = link_id.split("_", 1)[1]
            groups.setdefault((sub, pid), []).append(obj)

    now_ts = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    wrote_threads = 0
    skipped_threads = 0

    for (sub, pid), rows in groups.items():
        created_int = post_index.get((sub, pid))
        if created_int is None:
            log("WARN", f"comments skip sub={sub} post={pid} reason=no_post_created")
            skipped_threads += 1
            continue

        cap_ts = None
        for r in rows:
            v = r.get("retrieved_utc")
            if v is None:
                v = r.get("retrieved_on")
            if v is None:
                continue
            try:
                t = int(float(v))
            except Exception:
                continue
            if cap_ts is None or t > cap_ts:
                cap_ts = t
        if cap_ts is None:
            cap_ts = now_ts

        capture_ts = ts_fmt(cap_ts) or ts_fmt(created_int)

        seen = set()
        uniq = []
        for r in rows:
            cid = r.get("id")
            if not cid:
                continue
            if cid in seen:
                continue
            seen.add(cid)
            uniq.append(r)

        uniq.sort(key=comment_sort_key)
        h = hash_comments(uniq)

        subdir = thread_dir(root, sub, "comments", created_int, pid)
        subdir.mkdir(parents=True, exist_ok=True)

        if has_hash_file(subdir, h):
            skipped_threads += 1
            continue

        out_path = subdir / f"{capture_ts}_{h}.jsonl"
        if write_many_jsonl(out_path, uniq):
            wrote_threads += 1
        else:
            skipped_threads += 1

    log("INFO", f"comments done file={path} scanned={scanned} threads_wrote={wrote_threads} threads_skipped={skipped_threads}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/reddit/00_raw")
    ap.add_argument("--report-every", type=int, default=200000)
    ap.add_argument("paths", nargs="+")
    args = ap.parse_args()

    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)

    post_index = {}

    post_files = []
    comment_files = []
    for p in args.paths:
        name = os.path.basename(p)
        if "_posts" in name:
            post_files.append(Path(p))
        elif "_comments" in name:
            comment_files.append(Path(p))

    if not post_files:
        log("ERROR", "no *_posts files provided")
        return 2

    for pf in post_files:
        import_posts(pf, root, post_index, args.report_every)

    for cf in comment_files:
        import_comments(cf, root, post_index, args.report_every)

    log("INFO", f"done posts_index={len(post_index)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
