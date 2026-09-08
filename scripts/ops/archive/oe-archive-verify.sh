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

    entries, bad_json = [], 0
    with open(man, "r") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try: entries.append(json.loads(line))
            except json.JSONDecodeError: bad_json += 1
    if bad_json:
        r["reasons"].append(f"{bad_json} unparseable manifest line(s)")

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

    if any("CHECKSUM MISMATCH" in x or "absent on disk" in x for x in r["reasons"]):
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
