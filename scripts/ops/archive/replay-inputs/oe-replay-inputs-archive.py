#!/usr/bin/env python3
"""oe-replay-inputs-archive.py - retain the wire-exact inputs of a replay (DEV, read-only on Kafka).

WHY. On 2026-10-30 the VWAP / zone acceptance-rejection indicator could not be replayed: the VWAP
snapshots, tools/geometry sets, host break events and ES tape had been wiped from dev Kafka before
anything copied them out, and the existing nightly archivers decode Avro to JSON (the writer schema
and the exact bytes are gone) and do not carry these topics at all.

WHAT. A small incremental consumer. It assigns partitions itself (NO consumer group, nothing is ever
committed to the broker, so it cannot disturb a live consumer or a rebalance), reads
read_committed from the last archived offset to the end, and writes, per ET calendar day (taken from
the record's own timestamp, not from when the job ran):

  <root>/dt=YYYY-MM-DD/<topic>/<topic>.p<N>.e<epoch>.<first>-<last>.<archivedAt>.rec.jsonl.gz
        one JSON line per record: {p,o,ts,tt,k,v,h}; k/v are base64 of the EXACT bytes (Confluent
        wire format included - magic byte, 4-byte schema id, Avro body). Nothing is decoded.
  <root>/dt=YYYY-MM-DD/_schemas/<id>.json
        the writer schema for every Avro schema id that appears that day, with its registry
        subject(s)/version(s) and references, fetched from the Schema Registry at capture time.
        Per DAY on purpose: dev's registry has been wiped before, and ids are only unique within
        one registry lifetime.
  <root>/dt=YYYY-MM-DD/_manifest.jsonl     one line per file: sha256, md5, bytes, records, offsets...
  <root>/dt=YYYY-MM-DD/MANIFEST.sha256     sha256sum -c compatible, regenerated from the above
  <root>/dt=YYYY-MM-DD/MANIFEST.md5        md5 -c compatible
  <root>/_runs.jsonl                       append-only; THE checkpoint (one line per committed run)

CHECKPOINT = _runs.jsonl. A run is committed by appending its line AFTER its files are renamed into
place and listed in the day manifests. A crash earlier leaves unreferenced files (verify lists them
as orphans) and no checkpoint movement, so the range is simply read again. There is no second
checkpoint file to disagree with the data.

EPOCHS. A dev clean deletes and re-creates topics. A run notes the topic id (when the broker reports
one); a different id, or a high watermark below the checkpoint, starts a new epoch at the log start.
If the log start has moved PAST the checkpoint (retention or delete-records ate data we had not read)
the run records the exact gap and carries on - it never pretends the range is whole.

Subcommands:  capture | verify | check | show      (see --help)
Never deletes, alters or creates a topic. Never writes anywhere but --root.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import fcntl
import gzip
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from zoneinfo import ZoneInfo

VERSION = "2026-11-02.1"
ET = ZoneInfo("America/New_York")
DEFAULT_REGISTRY = "http://192.168.100.102:8082"
DEFAULT_BOOTSTRAP = "localhost:19092"
RUN_SECONDS = 240          # wall-clock budget per capture run; the next run continues from the checkpoint
MIN_FREE_GIB = 20          # refuse to write when the target filesystem has less free than this


def log(msg: str) -> None:
    print(dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), msg, flush=True)


def read_topics(path: Path) -> list[str]:
    out: list[str] = []
    for line in path.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.append(line)
    if not out or len(set(out)) != len(out):
        raise SystemExit(f"FATAL: {path} must list at least one topic, each once")
    return out


def et_date(ts_ms: int, fallback: str) -> str:
    if ts_ms and ts_ms > 0:
        return dt.datetime.fromtimestamp(ts_ms / 1000.0, ET).date().isoformat()
    return fallback


def file_digests(path: Path) -> tuple[str, str, int]:
    s, m, n = hashlib.sha256(), hashlib.md5(), 0
    with path.open("rb") as f:
        for blk in iter(lambda: f.read(1 << 20), b""):
            s.update(blk)
            m.update(blk)
            n += len(blk)
    return s.hexdigest(), m.hexdigest(), n


def schema_id_of(value: bytes | None) -> int | None:
    if value and len(value) >= 5 and value[0] == 0:
        return int.from_bytes(value[1:5], "big")
    return None


def b64(x: bytes | None) -> str | None:
    return None if x is None else base64.b64encode(x).decode("ascii")


def envelope(m) -> str:
    hdrs = [[k, b64(v)] for k, v in (m.headers() or [])]
    return json.dumps({"p": m.partition(), "o": m.offset(), "ts": m.timestamp()[1], "tt": m.timestamp()[0],
                       "k": b64(m.key()), "v": b64(m.value()), "h": hdrs}, separators=(",", ":"))


# ---------------------------------------------------------------- registry

def http_json(url: str, timeout: float = 10.0):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_schema(registry: str, sid: int) -> dict:
    body = http_json(f"{registry}/schemas/ids/{sid}")
    try:
        subjects = http_json(f"{registry}/schemas/ids/{sid}/versions")
    except Exception:  # noqa: BLE001 - older registries lack the endpoint; the schema text is what matters
        subjects = []
    refs: list[dict] = []
    seen: set[tuple] = set()

    def walk(references):
        for ref in references or []:
            ident = (ref["subject"], ref["version"])
            if ident in seen:
                continue
            seen.add(ident)
            txt = http_json(f"{registry}/subjects/{urllib.parse.quote(ref['subject'], safe='')}/versions/{ref['version']}")
            walk(txt.get("references"))          # dependencies first, so load order is parse order
            refs.append({"name": ref["name"], "subject": ref["subject"], "version": ref["version"],
                         "schema": txt.get("schema"), "id": txt.get("id")})

    walk(body.get("references"))
    schema = body["schema"]
    return {"id": sid, "schemaType": body.get("schemaType", "AVRO"), "schema": schema,
            "schema_sha256": hashlib.sha256(schema.encode("utf-8")).hexdigest(),
            "references": refs, "subjects": subjects, "registry": registry,
            "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat()}


# ---------------------------------------------------------------- state

def load_runs(root: Path, repair: bool = False) -> list[dict]:
    """Read the checkpoint. An INVALID final line with no newline is an interrupted append: it was never
    committed, so it is ignored (and, with repair=True, cut off). Any other malformed line is fatal."""
    p = root / "_runs.jsonl"
    if not p.exists():
        return []
    raw = p.read_text()
    lines = raw.split("\n")
    out = []
    for i, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            if i == len(lines) - 1 and not raw.endswith("\n"):
                if repair:
                    with p.open("r+") as f:
                        f.truncate(len(raw.encode("utf-8")) - len(line.encode("utf-8")))   # keep everything up to the last newline
                    log("repaired _runs.jsonl: cut an interrupted final append")
                break
            raise SystemExit(f"FATAL: _runs.jsonl line {i + 1} is corrupt; refusing to guess the checkpoint")
    return out


def last_state(runs: list[dict]) -> dict:
    """(topic, partition) -> the most recent committed run (file order is commit order)."""
    st: dict = {}
    for r in runs:
        st[(r["topic"], r["partition"])] = r
    return st


def append_line(path: Path, obj: dict) -> None:
    with path.open("a") as f:
        f.write(json.dumps(obj, separators=(",", ":")) + "\n")
        f.flush()
        os.fsync(f.fileno())


def read_jsonl(path: Path) -> tuple[list[dict], bool]:
    """(entries, tail_was_torn). An invalid, unterminated FINAL line is a torn append (never committed);
    any other corrupt line is fatal."""
    if not path.exists():
        return [], False
    raw = path.read_text()
    lines = raw.split("\n")
    out: list[dict] = []
    for i, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            if i == len(lines) - 1 and not raw.endswith("\n"):
                return out, True
            raise SystemExit(f"FATAL: {path} line {i + 1} is corrupt; refusing to guess") from None
    return out, False


def committed_files(runs: list[dict]) -> set[tuple[str, str]]:
    return {(f["dt"], f["path"]) for r in runs for f in r["files"]}


def recover(root: Path, runs: list[dict]) -> None:
    """Undo a crashed run: a file/manifest line is real only if a committed _runs line lists it.
    The commit order is files -> day manifest -> _runs, so anything not in _runs is the tail of a crash and
    its range will be read again."""
    committed = committed_files(runs)
    for ddir in sorted(root.glob("dt=*")):
        day = ddir.name[3:]
        mf = ddir / "_manifest.jsonl"
        ents, torn = read_jsonl(mf)
        keep = [e for e in ents if (day, e["path"]) in committed]
        kept = {e["path"] for e in keep} | {path for (d, path) in committed if d == day}   # never delete committed data
        dropped = 0
        for sub in ddir.iterdir():
            if sub.is_dir() and not sub.name.startswith("_"):
                for f in sub.iterdir():
                    if f"{sub.name}/{f.name}" not in kept:
                        f.unlink()
                        dropped += 1
        if len(keep) != len(ents) or dropped or torn:
            tmp = mf.with_suffix(".tmp")
            tmp.write_text("".join(json.dumps(e, separators=(",", ":")) + "\n" for e in keep))
            os.replace(tmp, mf)
        if mf.exists():
            write_day_manifests(ddir)       # sidecars are derived: always rebuild, a crash may have skipped them
        if len(keep) != len(ents) or dropped or torn:
            log(f"recovered {ddir.name}: dropped {len(ents) - len(keep)} uncommitted manifest line(s), {dropped} file(s)")
    for f in (root / "_state" / "tmp").glob("*.tmp"):
        f.unlink()


def write_day_manifests(day_dir: Path) -> None:
    ents = []
    mf = day_dir / "_manifest.jsonl"
    if mf.exists():
        ents = [json.loads(x) for x in mf.read_text().splitlines() if x.strip()]
    ents.sort(key=lambda e: e["path"])
    for name, key in (("MANIFEST.sha256", "sha256"), ("MANIFEST.md5", "md5")):
        tmp = day_dir / (name + ".tmp")
        tmp.write_text("".join(f"{e[key]}  {e['path']}\n" for e in ents))
        os.replace(tmp, day_dir / name)


# ---------------------------------------------------------------- guards

def nas_guard(root: Path) -> None:
    """Same refusals as dev-archive.py: right host, real NAS mount, sentinel present, room to write."""
    host = subprocess.check_output(["/bin/hostname"], text=True).strip()
    ifc = subprocess.check_output(["/sbin/ifconfig"], text=True)
    if host != "MacBook-Pro-2.local" or "inet 192.168.100.102 " not in ifc:
        raise SystemExit("REFUSING: target must be MacBook-Pro-2.local / 192.168.100.102 (dev Mac)")
    mount = subprocess.check_output(["/sbin/mount"], text=True)
    if not any(" on /Volumes/database (" in x and "smbfs" in x for x in mount.splitlines()):
        raise SystemExit("REFUSING: NAS SMB mount is absent; no local-disk fallback")
    if not Path("/Volumes/database/optionsedge/.oe_nas_sentinel").is_file():
        raise SystemExit("REFUSING: NAS sentinel is absent")
    base = Path("/Volumes/database/optionsedge").resolve()
    try:
        root.resolve().relative_to(base)
    except ValueError:
        raise SystemExit(f"REFUSING: --nas-guard requires --root to resolve under {base}") from None


def free_guard(root: Path) -> None:
    probe = root
    while not probe.exists():
        probe = probe.parent
    free_gib = shutil.disk_usage(probe).free / 2**30
    if free_gib < MIN_FREE_GIB:
        raise SystemExit(f"REFUSING: only {free_gib:.1f} GiB free under {probe} (floor {MIN_FREE_GIB} GiB)")


# ---------------------------------------------------------------- capture

class DayWriter:
    """Streams one (partition, day) chunk to a tmp file; finalised after the run."""

    def __init__(self, tmp: Path):
        self.tmp = tmp
        self.gz = gzip.GzipFile(filename="", mode="wb", fileobj=tmp.open("wb"), mtime=0)
        self.records = 0
        self.first = self.last = None
        self.ts_min = self.ts_max = None
        self.sids: set[int] = set()
        self.key_sids: set[int] = set()

    def add(self, m) -> None:
        self.gz.write((envelope(m) + "\n").encode("ascii"))
        self.records += 1
        o = m.offset()
        self.first = o if self.first is None else self.first
        self.last = o
        ts = m.timestamp()[1]
        if ts and ts > 0:
            self.ts_min = ts if self.ts_min is None else min(self.ts_min, ts)
            self.ts_max = ts if self.ts_max is None else max(self.ts_max, ts)
        sid = schema_id_of(m.value())
        if sid is not None:
            self.sids.add(sid)
        ksid = schema_id_of(m.key())
        if ksid is not None:
            self.key_sids.add(ksid)

    def close(self) -> None:
        fo = self.gz.fileobj
        self.gz.close()
        fo.flush()
        os.fsync(fo.fileno())
        fo.close()


def capture(args) -> int:
    from confluent_kafka import Consumer, KafkaError, TopicPartition, TopicCollection
    from confluent_kafka.admin import AdminClient

    root = Path(args.root)
    if args.nas_guard:
        nas_guard(root)
    free_guard(root)
    root.mkdir(parents=True, exist_ok=True)
    (root / "_state" / "tmp").mkdir(parents=True, exist_ok=True)
    lock = (root / "_state" / "capture.lock").open("w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("another capture holds the lock; skipping")
        return 0

    topics = read_topics(Path(args.topics))
    runs = load_runs(root, repair=True)
    recover(root, runs)
    state = last_state(runs)
    archived_at = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    today_et = dt.datetime.now(ET).date().isoformat()
    cons = Consumer({"bootstrap.servers": args.bootstrap, "group.id": "oe-replay-inputs-archive-nogroup",
                     "enable.auto.commit": False, "enable.auto.offset.store": False,
                     "enable.partition.eof": True, "isolation.level": "read_committed",
                     "auto.offset.reset": "error", "session.timeout.ms": 10000})
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    deadline = time.monotonic() + args.run_seconds
    rc = 0
    total_records = 0
    touched_days: set[str] = set()

    for topic in topics:
        if time.monotonic() > deadline:
            log(f"{topic}: run budget exhausted, left for the next run")
            continue
        try:
            md = cons.list_topics(topic, timeout=10).topics[topic]
        except Exception as e:  # noqa: BLE001
            log(f"{topic}: ERROR listing topic: {e}")
            rc = 1
            continue
        if md.error is not None:
            if args.verbose:
                log(f"{topic}: absent ({md.error.str()})")
            continue
        topic_id = None
        try:
            topic_id = str(admin.describe_topics(TopicCollection([topic]))[topic].result(timeout=10).topic_id)
        except Exception as e:  # noqa: BLE001
            log(f"{topic}: ERROR no topic id from the broker ({e}); refusing to guess whether it was recreated")
        if topic_id is None:
            rc = 1
            continue

        plan: dict[int, dict] = {}
        for p in sorted(md.partitions):
            lo, hi = cons.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            prev = state.get((topic, p))
            note = {}
            if prev is None:
                epoch, start = 0, lo
                if lo > 0:
                    note["initial_log_start"] = lo
            else:
                epoch, start = prev["epoch"], prev["to"]
                if (topic_id and prev.get("topic_id") and topic_id != prev["topic_id"]) or hi < prev["to"]:
                    epoch, start = prev["epoch"] + 1, lo
                    note["new_epoch"] = {"prev_topic_id": prev.get("topic_id"), "prev_to": prev["to"], "hi": hi}
                elif lo > prev["to"]:
                    start = lo
                    note["gap"] = {"from": prev["to"], "to": lo}
            if start < hi:
                plan[p] = {"epoch": epoch, "start": start, "note": note}
        if not plan:
            continue

        cons.assign([TopicPartition(topic, p, v["start"]) for p, v in plan.items()])
        writers: dict[tuple[int, str], DayWriter] = {}
        tdir = root / "_state" / "tmp"
        pending = set(plan)
        last_off: dict[int, int] = {}
        failed = False
        while pending and time.monotonic() < deadline:
            msgs = cons.consume(num_messages=1000, timeout=1.0)
            for m in msgs:
                if m.error():
                    if m.error().code() == KafkaError._PARTITION_EOF:
                        pending.discard(m.partition())
                    else:
                        log(f"{topic}: consumer error {m.error()}")
                        failed = True
                    continue
                key = (m.partition(), et_date(m.timestamp()[1], today_et))
                w = writers.get(key)
                if w is None:
                    w = writers[key] = DayWriter(tdir / f"{topic}.p{key[0]}.{key[1]}.{os.getpid()}.tmp")
                w.add(m)
                last_off[m.partition()] = m.offset()
            if failed:
                break
        ends: dict[int, int] = {}
        for p, v in plan.items():
            pos = cons.position([TopicPartition(topic, p)])[0].offset
            ends[p] = pos if pos >= v["start"] and pos >= 0 else last_off.get(p, v["start"] - 1) + 1
        for w in writers.values():
            w.close()
        if failed:
            for w in writers.values():
                w.tmp.unlink(missing_ok=True)
            rc = 1
            continue

        # ---- finalise: schemas, rename, day manifests, then the checkpoint lines
        sids_by_day: dict[str, dict[int, bool]] = {}      # day -> {schema id: seen as a VALUE framing}
        for (p, day), w in writers.items():
            d = sids_by_day.setdefault(day, {})
            for sid in w.key_sids:
                d.setdefault(sid, False)
            for sid in w.sids:
                d[sid] = True
        missing: dict[str, list[int]] = {}
        held = False
        for day, sids in sids_by_day.items():
            sdir = root / f"dt={day}" / "_schemas"
            sdir.mkdir(parents=True, exist_ok=True)
            for sid, is_value in sorted(sids.items()):
                sp = sdir / f"{sid}.json"
                if sp.exists():
                    continue
                try:
                    doc = fetch_schema(args.registry, sid)
                except urllib.error.HTTPError as e:
                    if e.code == 404:       # the registry does not have it: permanent, say so in the manifest
                        if is_value:
                            log(f"{topic}: schema id {sid} is not in the registry (404); recorded as missing")
                            missing.setdefault(day, []).append(sid)
                        continue            # a key whose first bytes merely look like framing
                    held = True
                    log(f"{topic}: registry HTTP {e.code} for schema {sid}; holding this topic's checkpoint")
                except Exception as e:  # noqa: BLE001 - transient: do NOT move the checkpoint past undecodable bytes
                    held = True
                    log(f"{topic}: registry unreachable for schema {sid} ({e}); holding this topic's checkpoint")
                else:
                    tmp = sp.with_suffix(".tmp")
                    tmp.write_text(json.dumps(doc, indent=1))
                    os.replace(tmp, sp)
        if held:
            for w in writers.values():
                w.tmp.unlink(missing_ok=True)
            rc = 1
            continue

        file_entries: dict[int, list[dict]] = {p: [] for p in plan}
        for (p, day), w in sorted(writers.items()):
            ddir = root / f"dt={day}" / topic
            ddir.mkdir(parents=True, exist_ok=True)
            name = f"{topic}.p{p}.e{plan[p]['epoch']}.{w.first}-{w.last}.{archived_at}.rec.jsonl.gz"
            os.replace(w.tmp, ddir / name)
            sha, md5, nbytes = file_digests(ddir / name)
            ent = {"path": f"{topic}/{name}", "topic": topic, "dt": day, "partition": p, "epoch": plan[p]["epoch"],
                   "offset_first": w.first, "offset_last": w.last, "records": w.records, "bytes": nbytes,
                   "sha256": sha, "md5": md5, "ts_min_ms": w.ts_min, "ts_max_ms": w.ts_max,
                   "schema_ids": sorted(w.sids), "key_schema_ids": sorted(w.key_sids), "schema_ids_missing": sorted(set(w.sids) & set(missing.get(day, []))),
                   "archived_at": archived_at, "archiver_version": VERSION}
            append_line(root / f"dt={day}" / "_manifest.jsonl", ent)
            file_entries[p].append(ent)
            touched_days.add(day)
            total_records += w.records
        for p, v in plan.items():
            if ends[p] <= v["start"]:
                continue
            run = {"topic": topic, "partition": p, "epoch": v["epoch"], "topic_id": topic_id, "from": v["start"],
                   "to": ends[p], "records": sum(e["records"] for e in file_entries[p]),
                   "files": [{"dt": e["dt"], "path": e["path"], "sha256": e["sha256"]} for e in file_entries[p]],
                   "archived_at": archived_at, "archiver_version": VERSION}
            run.update(v["note"])
            append_line(root / "_runs.jsonl", run)
            if v["note"]:
                log(f"{topic} p{p}: NOTE {json.dumps(v['note'])}")
        if args.verbose or any(file_entries[p] for p in plan):
            log(f"{topic}: " + " ".join(f"p{p}[{plan[p]['start']},{ends[p]})={sum(e['records'] for e in file_entries[p])}" for p in plan))

    for day in sorted(touched_days):
        write_day_manifests(root / f"dt={day}")
    cons.close()
    if args.verbose or total_records or rc:
        log(f"capture done: {total_records} records, days touched {sorted(touched_days)}, rc={rc}")
    return rc


# ---------------------------------------------------------------- read side

def iter_records(path: Path):
    with gzip.open(path, "rt", encoding="ascii") as f:
        for line in f:
            yield json.loads(line)


def day_entries(root: Path, day: str) -> list[dict]:
    ents, torn = read_jsonl(root / f"dt={day}" / "_manifest.jsonl")
    if torn:
        raise SystemExit(f"FATAL: dt={day}/_manifest.jsonl ends in a torn line; run capture to recover, then retry")
    return ents


def verify(args) -> int:
    root, day = Path(args.root), args.dt
    ddir = root / f"dt={day}"
    ents, torn = read_jsonl(ddir / "_manifest.jsonl")
    problems: list[str] = []
    if torn:
        problems.append("day _manifest.jsonl ends in a torn line (run capture to recover, then verify again)")
    if not ents:
        print(f"verify {day}: NO manifest entries under {ddir}")
        for x in problems:
            print("  PROBLEM:", x)
        return 2
    listed = set()
    per_topic: dict[str, int] = {}
    runs = load_runs(root)
    committed = committed_files(runs)
    for e in ents:
        if (day, e["path"]) not in committed:
            problems.append(f"manifest line not referenced by a committed run (crash tail?) {e['path']}")
        p = ddir / e["path"]
        listed.add(e["path"])
        if not p.is_file():
            problems.append(f"missing file {e['path']}")
            continue
        sha, md5, n = file_digests(p)
        if (sha, md5, n) != (e["sha256"], e["md5"], e["bytes"]):
            problems.append(f"digest/size mismatch {e['path']}")
            continue
        cnt, first, last = 0, None, None
        for r in iter_records(p):
            cnt += 1
            first = r["o"] if first is None else first
            last = r["o"]
        if (cnt, first, last) != (e["records"], e["offset_first"], e["offset_last"]):
            problems.append(f"content mismatch {e['path']}: {cnt}/{first}/{last} vs manifest")
        per_topic[e["topic"]] = per_topic.get(e["topic"], 0) + cnt
        for sid in e["schema_ids"]:
            if sid not in e.get("schema_ids_missing", []) and not (ddir / "_schemas" / f"{sid}.json").is_file():
                problems.append(f"schema {sid} file absent for {e['path']}")
        if e.get("schema_ids_missing"):
            problems.append(f"schema ids never fetched {e['schema_ids_missing']} for {e['path']}")
    for (d, path) in committed:
        if d == day and path not in listed:
            problems.append(f"committed run lists {path} but the day manifest does not")
    spans: dict[tuple, list[tuple]] = {}
    for e in ents:
        spans.setdefault((e["topic"], e["partition"], e["epoch"]), []).append((e["offset_first"], e["offset_last"], e["path"]))
    for k, sp in spans.items():
        sp.sort()
        for a, b in zip(sp, sp[1:]):
            if b[0] <= a[1]:
                problems.append(f"overlapping/duplicate offsets {k}: {a[2]} and {b[2]}")
    for sub in ddir.iterdir():
        if sub.is_dir() and not sub.name.startswith("_"):
            for f in sub.iterdir():
                if f"{sub.name}/{f.name}" not in listed:
                    problems.append(f"orphan file {sub.name}/{f.name} (uncommitted run?)")
    # sha256sum -c / md5 -c equivalents of the day manifests
    for name, key in (("MANIFEST.sha256", "sha256"), ("MANIFEST.md5", "md5")):
        mp = ddir / name
        want = "".join(f"{e[key]}  {e['path']}\n" for e in sorted(ents, key=lambda x: x["path"]))
        if not mp.is_file() or mp.read_text() != want:
            problems.append(f"{name} missing or stale")
    # continuity: committed runs chain with no hole (explicit gap/new_epoch notes excepted)
    chains: dict[tuple, list[dict]] = {}
    for r in runs:
        chains.setdefault((r["topic"], r["partition"], r["epoch"]), []).append(r)
    for (t, p, ep), rs in chains.items():
        for a, b in zip(rs, rs[1:]):
            if a["to"] != b["from"] and "gap" not in b:
                problems.append(f"discontinuity {t} p{p} e{ep}: {a['to']} -> {b['from']}")
    print(f"verify {day}: files={len(ents)} " + " ".join(f"{t}={n}" for t, n in sorted(per_topic.items())))
    for x in problems:
        print("  PROBLEM:", x)
    print("verify", "OK" if not problems else f"FAILED ({len(problems)} problems)")
    return 0 if not problems else 1


def load_schemas(ddir: Path):
    import fastavro
    docs = {int(p.stem): json.loads(p.read_text()) for p in (ddir / "_schemas").glob("*.json")}
    parsed = {}
    for sid, d in docs.items():
        if d.get("schemaType", "AVRO") != "AVRO":
            continue
        named: dict = {}
        for ref in d.get("references", []):
            fastavro.parse_schema(json.loads(ref["schema"]), named_schemas=named)
        parsed[sid] = fastavro.parse_schema(json.loads(d["schema"]), named_schemas=named)
    return parsed


def decode_value(parsed, v: bytes):
    import fastavro
    sid = schema_id_of(v)
    if sid is None:
        return "json", json.loads(v.decode("utf-8"))
    if sid not in parsed:
        raise KeyError(f"schema {sid} not archived")
    return sid, fastavro.schemaless_reader(io.BytesIO(v[5:]), parsed[sid])


def check(args) -> int:
    """Decode every archived record of a day with the ARCHIVED writer schemas; optionally compare to the broker."""
    root, day = Path(args.root), args.dt
    ddir = root / f"dt={day}"
    parsed = load_schemas(ddir)
    ents = day_entries(root, day)
    stats: dict[str, dict] = {}
    rc = 0
    if not ents:
        print(f"check {day}: NO manifest entries under {ddir}")
        return 2
    for e in ents:
        s = stats.setdefault(e["topic"], {"records": 0, "decoded": 0, "failed": 0, "avro": 0, "json": 0, "null": 0,
                                          "sids": set(), "first_err": None})
        for r in iter_records(ddir / e["path"]):
            s["records"] += 1
            try:
                ksid = schema_id_of(base64.b64decode(r["k"])) if r["k"] else None
                if ksid is not None and ksid in parsed:       # framed key with an archived schema: it must decode too
                    decode_value(parsed, base64.b64decode(r["k"]))
                if r["v"] is None:
                    s["null"] += 1
                    continue
                kind, _ = decode_value(parsed, base64.b64decode(r["v"]))
                s["decoded"] += 1
                s["avro" if kind != "json" else "json"] += 1
                if kind != "json":
                    s["sids"].add(kind)
            except Exception as ex:  # noqa: BLE001
                s["failed"] += 1
                s["first_err"] = s["first_err"] or f"{e['path']} o={r['o']}: {ex!r}"
    print(f"check {day}: decode with archived writer schemas")
    for t, s in sorted(stats.items()):
        print(f"  {t}: records={s['records']} decoded={s['decoded']} (avro={s['avro']} json={s['json']}) "
              f"tombstones={s['null']} failed={s['failed']} schema_ids={sorted(s['sids'])}")
        if s["failed"]:
            rc = 1
            print("    first failure:", s["first_err"])
    if args.broker:
        rc = max(rc, broker_compare(args, ents, root, day))
    print("check", "OK" if rc == 0 else "FAILED")
    return rc


def broker_compare(args, ents: list[dict], root: Path, day: str) -> int:
    """Re-read each archived [first,last] range from the live broker and compare count + chained sha256."""
    from confluent_kafka import Consumer, KafkaError, TopicPartition
    cons = Consumer({"bootstrap.servers": args.bootstrap, "group.id": "oe-replay-inputs-check-nogroup",
                     "enable.auto.commit": False, "enable.partition.eof": True, "isolation.level": "read_committed"})
    ddir = root / f"dt={day}"
    rc = 0
    groups: dict[tuple, list[dict]] = {}
    for e in ents:
        groups.setdefault((e["topic"], e["partition"]), []).append(e)
    print(f"  broker re-read of the archived offset ranges ({args.bootstrap}):")
    for (t, p), es in sorted(groups.items()):
        es.sort(key=lambda e: e["offset_first"])
        lo, hi = cons.get_watermark_offsets(TopicPartition(t, p), timeout=10)
        first, last = es[0]["offset_first"], es[-1]["offset_last"]
        if first < lo or last >= hi:
            print(f"    {t} p{p}: range [{first},{last}] no longer fully on the broker (log [{lo},{hi})) - skipped")
            continue
        arch_h, arch_n = hashlib.sha256(), 0
        for e in es:
            for r in iter_records(ddir / e["path"]):
                arch_h.update(f"{r['o']}:".encode() + (base64.b64decode(r["v"]) if r["v"] else b"") + b"\n")
                arch_n += 1
        brk_h, brk_n = hashlib.sha256(), 0
        cons.assign([TopicPartition(t, p, first)])
        done = False
        while not done:
            for m in cons.consume(num_messages=1000, timeout=1.0):
                if m.error():
                    if m.error().code() == KafkaError._PARTITION_EOF:
                        done = True
                    continue
                if m.offset() > last:
                    done = True
                    break
                if any(e["offset_first"] <= m.offset() <= e["offset_last"] for e in es):
                    brk_h.update(f"{m.offset()}:".encode() + (m.value() or b"") + b"\n")
                    brk_n += 1
        ok = (arch_n, arch_h.hexdigest()) == (brk_n, brk_h.hexdigest())
        print(f"    {t} p{p}: archived={arch_n} broker={brk_n} sha256 {'MATCH' if ok else 'DIFFER'}")
        rc = rc or (0 if ok else 1)
    cons.close()
    return rc


def show(args) -> int:
    root, day = Path(args.root), args.dt
    ddir = root / f"dt={day}"
    parsed = load_schemas(ddir)
    n = 0
    ents = day_entries(root, day)
    if not ents:
        print(f"show {day}: NO manifest entries under {ddir}")
        return 2
    for e in ents:
        if e["topic"] != args.topic:
            continue
        for r in iter_records(ddir / e["path"]):
            val = None if r["v"] is None else decode_value(parsed, base64.b64decode(r["v"]))[1]
            print(json.dumps({"p": r["p"], "o": r["o"], "ts": r["ts"], "value": val}, default=str)[:1500])
            n += 1
            if n >= args.n:
                return 0
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("capture", help="incremental capture of the listed topics")
    c.add_argument("--root", required=True)
    c.add_argument("--topics", required=True, help="file listing topics, one per line")
    c.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    c.add_argument("--registry", default=DEFAULT_REGISTRY)
    c.add_argument("--run-seconds", type=int, default=RUN_SECONDS)
    c.add_argument("--verbose", action="store_true", help="log idle topics too (default: only runs that moved data)")
    c.add_argument("--nas-guard", action="store_true", help="dev Mac + NAS mount/sentinel checks (launchd uses this)")
    c.set_defaults(fn=capture)
    for name, fn, h in (("verify", verify, "digests, counts, orphans, schemas, continuity for one day"),
                        ("check", check, "decode a day with the archived schemas; --broker compares to the topic"),
                        ("show", show, "print decoded records of one topic/day")):
        s = sub.add_parser(name, help=h)
        s.add_argument("--root", required=True)
        s.add_argument("--dt", required=True)
        s.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
        if name == "check":
            s.add_argument("--broker", action="store_true")
        if name == "show":
            s.add_argument("--topic", required=True)
            s.add_argument("-n", type=int, default=2)
        s.set_defaults(fn=fn)
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
