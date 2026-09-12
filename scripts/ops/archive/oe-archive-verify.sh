#!/usr/bin/env bash
# oe-archive-verify.sh — does a trading date's archive actually EXIST and is it WHOLE?
#
# WHY THIS EXISTS: on 2026-08-12 the prod heavy archive was found to have produced nothing since
# dt=2026-08-07. It had been exiting 0 every single weekday. Every piece of evidence anyone had
# been relying on — the cron exit status, the "done rc=0" log line, the growing archive size —
# was consistent with everything being fine, because all of them measured the RUN and none of them
# measured the DATA. Four sessions expired at 1-day Kafka retention and cannot be recovered.
#
# This script measures the data. It reads only the per-date completeness manifests on disk; it does
# not need a broker, and it does not care whether any job reported success. That independence is
# the whole point: it must be able to catch a daily job that never ran at all, so it runs from its
# OWN cron entry rather than from the tail of the job it is checking.
#
# USAGE
#   oe-archive-verify.sh [YYYY-MM-DD]        # default: today's New York trading date
#   ENV=prod ARCHIVE_DIR=/mnt/nas/optionsedge oe-archive-verify.sh 2026-08-12
#
# KNOBS
#   VERIFY_CHECKSUMS   sample (default, newest file per topic) | full | none
#   FORCE              true = verify even if the date is not a trading day
#
# EXIT: 0 = every policy topic OK. 1 = at least one MISSING / PARTIAL / CORRUPT (alert delivered).
#       2 = the verifier itself could not run (unmounted NAS, bad date) — also alerts, because a
#       verifier that cannot verify must never be mistaken for a verifier that found nothing wrong.
set -uo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

OE_DIR="$(cd "$(dirname "$0")" && pwd)"
ARCHIVE_DIR="${ARCHIVE_DIR:-/mnt/nas/optionsedge}"
ENV_NAME="${ENV:-prod}"
LOG="${LOG:-/home/abhinav/oe-ops/archive-verify.log}"
VERIFY_CHECKSUMS="${VERIFY_CHECKSUMS:-sample}"
FORCE="${FORCE:-false}"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG"; }

# shellcheck source=/dev/null
. "$OE_DIR/oe-alert.sh"
# shellcheck source=/dev/null
. "$OE_DIR/oe-trading-day.sh"

OE_TOPICS_ENV="${OE_TOPICS_ENV:-$OE_DIR/oe-topics.env}"
[ -r "$OE_TOPICS_ENV" ] || { log "FATAL: '$OE_TOPICS_ENV' missing — cannot know what a complete date looks like"; alert "🚨 archive verifier cannot run on $(hostname): $OE_TOPICS_ENV missing. Completeness is UNCHECKED."; exit 2; }
# shellcheck source=/dev/null
. "$OE_TOPICS_ENV"

DATE="${1:-$(TZ=America/New_York date +%Y-%m-%d)}"
case "$DATE" in
  [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]) : ;;
  *) log "FATAL: '$DATE' is not YYYY-MM-DD"; exit 2 ;;
esac

# A non-trading date has no expectation to check, so silence is correct there — but ONLY there.
# The gate is shared with the archiver (oe-trading-day.sh) on purpose: if the two disagreed about
# which days count, this would either shout every weekend or stay quiet on a missed weekday.
if [ "$FORCE" != "true" ] && [ "$(is_trading_day "$DATE")" != "yes" ]; then
  log "$DATE is not a New York trading day — nothing is expected, exiting clean"; exit 0
fi

eval "POLICY_TOPICS=\"\${OE_ALL_TOPICS_${ENV_NAME}:-}\""
if [ -z "${POLICY_TOPICS:-}" ]; then
  log "FATAL: oe-topics.env defines no OE_ALL_TOPICS_$ENV_NAME — no policy to verify against"
  alert "🚨 archive verifier has no topic policy for env=$ENV_NAME on $(hostname). Completeness is UNCHECKED."
  exit 2
fi

ROOT="$ARCHIVE_DIR/kafka/$ENV_NAME"
if [ ! -d "$ROOT" ]; then
  log "FATAL: '$ROOT' does not exist — NAS not mounted, or nothing was ever archived for env=$ENV_NAME"
  alert "🚨 archive verifier: '$ROOT' does not exist on $(hostname). env=$ENV_NAME completeness for $DATE is UNCHECKED — treat as NOT archived."
  exit 2
fi

log "=== verifying env=$ENV_NAME dt=$DATE root=$ROOT checksums=$VERIFY_CHECKSUMS ==="

REPORT_DIR="$ROOT/_manifest/completeness"
mkdir -p "$REPORT_DIR" 2>/dev/null

summary=$(
  ROOT="$ROOT" DATE="$DATE" ENV_NAME="$ENV_NAME" POLICY_TOPICS="$POLICY_TOPICS" \
  MIN_RECORDS="${OE_ARCHIVE_MIN_RECORDS:-}" MIN_DEFAULT="${OE_ARCHIVE_MIN_DEFAULT:-1}" \
  VERIFY_CHECKSUMS="$VERIFY_CHECKSUMS" REPORT_DIR="$REPORT_DIR" \
  python3 - <<'PY'
import hashlib, json, os, sys, glob

root   = os.environ["ROOT"]
date   = os.environ["DATE"]
envn   = os.environ["ENV_NAME"]
topics = os.environ["POLICY_TOPICS"].split()
mode   = os.environ["VERIFY_CHECKSUMS"]
rdir   = os.environ["REPORT_DIR"]

floors = {}
for pair in os.environ.get("MIN_RECORDS", "").split():
    if ":" in pair:
        t, _, v = pair.partition(":")
        try: floors[t] = int(v)
        except ValueError: pass
default_floor = int(os.environ.get("MIN_DEFAULT", "1") or 1)

def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

results = []
for topic in topics:
    d   = os.path.join(root, topic, "dt=" + date)
    man = os.path.join(d, "_manifest.jsonl")
    r = {"topic": topic, "dt": date, "dir": d, "records": 0, "files": 0,
         "partitions": [], "reasons": []}

    floor = floors.get(topic, default_floor)
    r["floor"] = floor

    if not os.path.isdir(d):
        # floor 0 == optional: silence is a real state for this topic (see oe-topics.env), so an
        # absent folder is reported, counted, and NOT treated as a failure. Everything else with no
        # folder was simply never archived.
        r["status"] = "EMPTY" if floor == 0 else "MISSING"
        r["reasons"].append("no records this date (topic is declared optional)" if floor == 0
                            else "no dt= folder — this date was never archived")
        results.append(r); continue

    data_files = sorted(glob.glob(os.path.join(d, "*.jsonl.gz")))
    if not os.path.isfile(man) or os.path.getsize(man) == 0:
        # Dates archived before the completeness manifest existed (< 2026-08-12) have data but no
        # manifest. Say so precisely: "cannot verify" is a different fact from "not archived", and
        # collapsing the two would either hide a real miss or cry wolf over every historical date.
        r["status"] = "LEGACY" if data_files else "MISSING"
        r["files"]  = len(data_files)
        r["reasons"].append("data present but no _manifest.jsonl — pre-2026-08-12 date, completeness cannot be proven"
                            if data_files else "no _manifest.jsonl and no data files")
        results.append(r); continue

    # A NONBLANK line that is not a JSON object is CORRUPT (deploy #1041 review round 5): the archiver appends a
    # line in one printf, so a partial line is a run that died mid-append — a no-progress attempt whose queried
    # end nobody can read, or a file claim nobody can check. What it declared is unknown; the loaders refuse the
    # window (MANIFEST_UNPARSEABLE), and this date must not be reported OK or merely PARTIAL over it.
    entries, bad_json = [], []
    with open(man, "r") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line: continue
            try: e = json.loads(line)
            except json.JSONDecodeError: e = None
            if isinstance(e, dict): entries.append(e)
            else: bad_json.append(i)
    if bad_json:
        r["reasons"].append(f"{len(bad_json)} unparseable manifest line(s) (not a JSON object; a run that died "
                            f"mid-append?) at line(s) {bad_json[:3]}")

    # A committed-read run that could capture NOTHING (the stable boundary still at its checkpoint) records the
    # ATTEMPT — "attempt":"no_progress", the end it queried, and NO file (deploy #1041 review round 2, MAJOR 3).
    # It is an obligation for the session loaders, not a file this verifier can find or checksum.
    # A line naming BOTH a file and an attempt is neither (deploy #1041 review round 3, MINOR): the archiver never
    # writes one, and treating it as an attempt would take a file the manifest claims out of the presence and
    # checksum checks — "attempt":"no_progress" pasted onto a missing file's line turned CORRUPT into OK. It is
    # reported as malformed AND kept among the file lines, so the file it names is still looked for; the loaders
    # (CommittedLedgerArchive, vpread.py) refuse such a line outright (MANIFEST_MISMATCH).
    contradictory = [e for e in entries if e.get("attempt") is not None and e.get("file") is not None]
    if contradictory:
        r["reasons"].append(f"{len(contradictory)} manifest line(s) name both a file and an attempt (malformed; "
                            f"checked as file lines): {[e.get('file') for e in contradictory][:3]}")
    attempts = [e for e in entries if e.get("attempt") is not None and e.get("file") is None]
    entries  = [e for e in entries if e.get("attempt") is None or e.get("file") is not None]
    r["files"]   = len(entries)
    r["records"] = sum(int(e.get("records", 0)) for e in entries)
    r["partitions"] = sorted({int(e["partition"]) for e in entries if "partition" in e})
    times = [e["min_event_time"] for e in entries if e.get("min_event_time", "unknown") != "unknown"]
    if times:
        r["min_event_time"] = min(times)
        r["max_event_time"] = max(e["max_event_time"] for e in entries
                                  if e.get("max_event_time", "unknown") != "unknown")
    schemas = sorted({str(e.get("schema_versions") or e.get("schema_version") or
                          e.get("schema_source", "unavailable")) for e in entries})
    r["schema"] = ",".join(schemas)

    # Every manifest line must point at a file that is still there. A missing file is the failure
    # mode a record count alone cannot see.
    missing = [e["file"] for e in entries if not os.path.isfile(os.path.join(d, e.get("file", "")))]
    if missing:
        r["reasons"].append(f"{len(missing)} manifest file(s) absent on disk: {missing[:3]}")

    # AND the other direction. Checking only that manifest lines have files leaves the crash residue
    # this job is meant to catch completely invisible: the archiver renames a data file into place and
    # THEN appends its manifest line, so a run that dies between the two leaves a .jsonl.gz that no
    # line names — records that are on disk, uncounted, and outside every completeness number here.
    named = {e.get("file") for e in entries}
    unnamed = sorted(os.path.basename(f) for f in data_files if os.path.basename(f) not in named)
    if unnamed:
        r["reasons"].append(f"{len(unnamed)} data file(s) no manifest line names: {unnamed[:3]}")

    # Offset continuity within the date, per partition. Overlaps are expected and harmless (a run
    # that failed before checkpointing re-reads its range next time — duplicates are recoverable).
    # Gaps are not: a gap is records that were never written down.
    gaps = []
    bypart = {}
    for e in entries:
        if "partition" in e and "offset_from" in e and "offset_to" in e:
            bypart.setdefault(int(e["partition"]), []).append((int(e["offset_from"]), int(e["offset_to"])))
    for p, rng in bypart.items():
        rng.sort()
        for i in range(1, len(rng)):
            if rng[i][0] > rng[i - 1][1]:
                gaps.append(f"p{p} {rng[i-1][1]}->{rng[i][0]}")
    if gaps:
        r["reasons"].append(f"offset discontinuity ({len(gaps)}): {gaps[:3]}")
    r["offset_gaps"] = len(gaps)

    # A committed-read capture that stopped BELOW the end its run queried is WHOLE for the range it names:
    # offsets [stable_boundary, queried_end) were inside a transaction that had not resolved, the checkpoint
    # stopped there, and the next run archives them under ITS OWN dt=. So this is reported, never counted as
    # PARTIAL — the date is not missing anything it ever claimed. Completeness for a SESSION is decided where
    # the session is read: the vol-premium loaders (CommittedLedgerArchive) read a session from its dt= AND the
    # following storage dates and refuse the session until a committed capture reaches this queried_end.
    withheld = []
    for e in entries + attempts:
        if e.get("capture") != "read_committed_stable_boundary":
            continue
        b, q = e.get("stable_boundary"), e.get("queried_end")
        if isinstance(b, int) and isinstance(q, int) and b < q:
            if e.get("attempt") is not None and e.get("file") is None:
                withheld.append(f"p{int(e.get('partition', 0))} [{b},{q}) — the run at {e.get('archived_at', '?')} "
                                f"queried {q} and found no committed record past its checkpoint {b} (attempt "
                                f"{e.get('attempt')}, no file published)")
            else:
                withheld.append(f"p{int(e.get('partition', 0))} [{b},{q}) in {e.get('file', '?')}")
    if withheld:
        r["withheld_by_open_transaction"] = withheld

    # Checksums. 'sample' verifies the newest file per topic — enough to catch a truncated or
    # bit-rotted copy without re-reading a quarter-terabyte archive every evening.
    checked = 0
    if mode != "none" and entries:
        targets = entries if mode == "full" else [max(entries, key=lambda e: e.get("archived_at", ""))]
        for e in targets:
            p = os.path.join(d, e.get("file", ""))
            if not os.path.isfile(p) or not e.get("sha256"):
                continue
            checked += 1
            if sha256(p) != e["sha256"]:
                r["reasons"].append(f"CHECKSUM MISMATCH on {e['file']}")
    r["checksums_verified"] = checked

    if floor > 0 and r["records"] < floor:
        r["reasons"].append(f"{r['records']} records is below the floor of {floor}")

    if any("CHECKSUM MISMATCH" in x or "absent on disk" in x or "unparseable manifest line" in x
           for x in r["reasons"]):
        r["status"] = "CORRUPT"
    elif r["reasons"]:
        r["status"] = "PARTIAL"
    else:
        r["status"] = "OK"
    results.append(r)

order  = {"CORRUPT": 0, "MISSING": 1, "PARTIAL": 2, "LEGACY": 3, "EMPTY": 4, "OK": 5}
counts = {}
for r in results:
    counts[r["status"]] = counts.get(r["status"], 0) + 1

report = {"env": envn, "dt": date, "checked_at_utc":
          __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
          "checksum_mode": mode, "counts": counts, "topics": results}
try:
    os.makedirs(rdir, exist_ok=True)
    tmp = os.path.join(rdir, f".{date}.json.partial")
    with open(tmp, "w") as f:
        json.dump(report, f, indent=1, sort_keys=True)
    os.replace(tmp, os.path.join(rdir, f"{date}.json"))
except OSError as exc:
    print(f"  WARN could not write completeness report: {exc}")

for r in sorted(results, key=lambda x: (order.get(x["status"], 9), x["topic"])):
    detail = f"records={r['records']} files={r['files']} parts={len(r['partitions'])}"
    if r.get("min_event_time"):
        detail += f" events={r['min_event_time']}..{r['max_event_time']}"
    if r.get("schema"):
        detail += f" schema={r['schema']}"
    print(f"  {r['status']:<8} {r['topic']:<45} {detail}")
    for why in r["reasons"]:
        print(f"           ^ {why}")
    for held in r.get("withheld_by_open_transaction", []):
        print(f"           ~ open transaction withheld {held} — the next run archives it under its own dt=; "
              "a session that needs it is refused until then, never loaded short")

bad = [r for r in results if r["status"] in ("MISSING", "PARTIAL", "CORRUPT")]
print("SUMMARY " + " ".join(f"{k}={v}" for k, v in sorted(counts.items())))
if bad:
    print("VERDICT INCOMPLETE " + ",".join(f"{r['topic']}:{r['status']}" for r in bad))
    sys.exit(1)
print("VERDICT COMPLETE")
PY
)
vrc=$?

printf '%s\n' "$summary" | tee -a "$LOG"

if [ "$vrc" -ne 0 ]; then
  bad_line=$(printf '%s' "$summary" | grep '^VERDICT INCOMPLETE' | sed 's/^VERDICT INCOMPLETE //')
  log "FAILURE: env=$ENV_NAME dt=$DATE is INCOMPLETE"
  alert "🚨 Archive INCOMPLETE — env=$ENV_NAME dt=$DATE on $(hostname)
$bad_line

Kafka retention on the heavy option topics is ONE DAY. Anything not archived today is unrecoverable tomorrow — there is no backfill. Check $LOG and $REPORT_DIR/$DATE.json now."
  exit 1
fi

log "=== env=$ENV_NAME dt=$DATE COMPLETE ==="
exit 0
