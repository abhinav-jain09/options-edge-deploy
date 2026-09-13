#!/usr/bin/env bash
# The vol-premium event calendar the deployment pins (Gate-1 §4.5), checked the way the engine checks it at
# boot — before the file can be published to the calendar ledger (Jenkinsfile.vol-premium-ledger-publish) and
# before a manifest that pins its version and content hash can merge.
#
# WHY A CONTENT HASH IS RECOMPUTED HERE, and not just read. The engine binds a calendar by
# VOL_PREMIUM_CALENDAR_VERSION + VOL_PREMIUM_CALENDAR_CONTENT_HASH and halts (70) when the ledger row's hash
# differs from the pin. The hash is NOT the sha256 of the file: it is the sha256 of the contract's CANONICAL
# BYTES of the hashable payload (options-edge-contracts CalendarArtifact.computeContentHash over CanonicalBytes —
# registry-tagged fields, uvarint lengths, big-endian longs, ISO dates; schemaVersion, hashVersion and the hash
# itself are excluded). The deploy repo has no JVM and no contracts jar, so that encoding is ported below for
# exactly the record shape a calendar is, and the port is held to the artefact: it must reproduce the declared
# calendarContentHash byte for byte. A 256-bit match is not a coincidence; a mismatch is either a corrupted file
# or a contract change, and both must stop the merge. The port is NOT the contract — when CanonicalBytes or the
# calendar registry changes, this file changes with it, and the mismatch is what says so.
#
# What is asserted (every rule names the contract constant it holds the file to):
#   1. the file is the exact artefact the owner signed off: its sha256 equals VP_CAL_EXPECT_SHA256
#   2. it parses as one JSON object with exactly the seven CalendarArtifact fields, no more
#   3. schemaVersion 1, hashVersion 1, calendarVersion == VP_CAL_EXPECT_VERSION (EVENT_CODE grammar, <= 64 chars)
#   4. validFromDate..validThroughDate == VP_CAL_EXPECT_FROM..VP_CAL_EXPECT_THROUGH, ISO, from <= through
#   5. exactly VP_CAL_EXPECT_ENTRIES entries (<= MAX_CALENDAR_ENTRIES 512); every entry carries exactly
#      eventCode / instantUtcMs / leadWindowMs / trailWindowMs; code grammar (<= 32 chars); instant in
#      (0, MAX_INSTANT_UTC_MS]; lead and trail in [0, MAX_EVENT_WINDOW_MS 3600000]; sorted by (instant, code);
#      no duplicate (eventCode, instantUtcMs); the file within MAX_CALENDAR_BYTES 65536
#   6. the recomputed canonical hash == the file's calendarContentHash == VP_CAL_EXPECT_HASH
#   7. the manifests that pin the calendar (VP_CAL_MANIFESTS) carry VOL_PREMIUM_CALENDAR_VERSION and
#      VOL_PREMIUM_CALENDAR_CONTENT_HASH equal to the file's — the pin and the artefact cannot drift apart
#
# Usage: scripts/ci/validate-vol-premium-calendar.sh [calendar.json]
#   Defaults describe calendar 2026.1. Override the VP_CAL_EXPECT_* variables to validate another artefact;
#   set VP_CAL_MANIFESTS to a space-separated list of manifests to cross-check, or to "" to skip rule 7.
set -uo pipefail
cd "$(dirname "$0")/../.."

FILE="${1:-scripts/vol-premium/calendars/calendar-2026.1.json}"
export VP_CAL_FILE="$FILE"
export VP_CAL_EXPECT_VERSION="${VP_CAL_EXPECT_VERSION:-2026.1}"
export VP_CAL_EXPECT_HASH="${VP_CAL_EXPECT_HASH:-cf209ff9ff428428988322928b27cd625556a2bb5a28131410b4d4631ea9c347}"
export VP_CAL_EXPECT_SHA256="${VP_CAL_EXPECT_SHA256:-eb7f76e5b3bfa7f0a2189a4014ada2ceb8b025cd744e6fbe09a34c1776142b81}"
export VP_CAL_EXPECT_ENTRIES="${VP_CAL_EXPECT_ENTRIES:-40}"
export VP_CAL_EXPECT_FROM="${VP_CAL_EXPECT_FROM:-2026-01-01}"
export VP_CAL_EXPECT_THROUGH="${VP_CAL_EXPECT_THROUGH:-2026-12-31}"
export VP_CAL_MANIFESTS="${VP_CAL_MANIFESTS-k8s/base/vol-premium-deployment.yaml k8s/services/vol-premium/overlays/production/manifest.yaml k8s/services/vol-premium/overlays/dev/manifest.yaml}"

command -v python3 >/dev/null 2>&1 || { echo "FAIL: python3 is required"; exit 1; }
[ -f "$FILE" ] || { echo "FAIL: calendar file not found: $FILE"; exit 1; }

python3 - <<'PY'
import datetime, hashlib, json, os, re, struct, sys

fails = 0
def ok(msg):  print("  ok   " + msg)
def bad(msg):
    global fails
    fails += 1
    print("  FAIL " + msg)

path = os.environ["VP_CAL_FILE"]
want_version = os.environ["VP_CAL_EXPECT_VERSION"]
want_hash = os.environ["VP_CAL_EXPECT_HASH"]
want_sha = os.environ["VP_CAL_EXPECT_SHA256"]
want_entries = int(os.environ["VP_CAL_EXPECT_ENTRIES"])
want_from = os.environ["VP_CAL_EXPECT_FROM"]
want_through = os.environ["VP_CAL_EXPECT_THROUGH"]
manifests = os.environ.get("VP_CAL_MANIFESTS", "").split()

# --- the contract's constants (options-edge-contracts CalendarArtifact / CanonicalBytes) -----------------------
MAX_CALENDAR_ENTRIES = 512
MAX_CALENDAR_BYTES = 65536
MAX_EVENT_WINDOW_MS = 3_600_000
MAX_EVENT_CODE_CHARS = 32
MAX_VERSION_CHARS = 64
MAX_INSTANT_UTC_MS = int(datetime.datetime(9999, 12, 31, tzinfo=datetime.timezone.utc).timestamp() * 1000) + 86_400_000 - 1
EVENT_CODE = re.compile(r"[A-Za-z0-9_.:-]+")
FIELDS = ["schemaVersion", "calendarVersion", "calendarContentHash", "hashVersion", "validFromDate", "validThroughDate", "entries"]
ENTRY_FIELDS = ["eventCode", "instantUtcMs", "leadWindowMs", "trailWindowMs"]

# --- 1. the exact artefact --------------------------------------------------------------------------------------
raw = open(path, "rb").read()
sha = hashlib.sha256(raw).hexdigest()
if sha == want_sha: ok(f"file sha256 {sha} is the signed-off artefact ({len(raw)} bytes)")
else: bad(f"file sha256 is {sha}, expected {want_sha}: this is not the artefact that was signed off")
if len(raw) <= MAX_CALENDAR_BYTES: ok(f"{len(raw)} bytes within MAX_CALENDAR_BYTES {MAX_CALENDAR_BYTES}")
else: bad(f"{len(raw)} bytes exceeds MAX_CALENDAR_BYTES {MAX_CALENDAR_BYTES}")

# --- 2. shape -----------------------------------------------------------------------------------------------------
try:
    doc = json.loads(raw.decode("utf-8"))
except Exception as e:
    bad(f"not valid JSON: {e}"); print(f"=== validate-vol-premium-calendar: FAILED ({fails}) ==="); sys.exit(1)
if not isinstance(doc, dict) or sorted(doc) != sorted(FIELDS):
    bad(f"top-level fields are {sorted(doc) if isinstance(doc, dict) else type(doc).__name__}, expected exactly {sorted(FIELDS)}")
    print(f"=== validate-vol-premium-calendar: FAILED ({fails}) ==="); sys.exit(1)
ok("one JSON object with exactly the seven CalendarArtifact fields")

# --- 3. identity --------------------------------------------------------------------------------------------------
def is_int(v): return isinstance(v, int) and not isinstance(v, bool)
if doc["schemaVersion"] == 1: ok("schemaVersion 1")
else: bad(f"schemaVersion {doc['schemaVersion']!r}, expected 1 (CURRENT_SCHEMA_VERSION)")
if doc["hashVersion"] == 1: ok("hashVersion 1")
else: bad(f"hashVersion {doc['hashVersion']!r}, expected 1 (CanonicalBytes.HASH_VERSION)")
version = doc["calendarVersion"]
if version == want_version and isinstance(version, str) and EVENT_CODE.fullmatch(version) and len(version) <= MAX_VERSION_CHARS:
    ok(f"calendarVersion {version}")
else:
    bad(f"calendarVersion {version!r}, expected {want_version!r} matching {EVENT_CODE.pattern} (<= {MAX_VERSION_CHARS} chars)")
declared = doc["calendarContentHash"]
if isinstance(declared, str) and re.fullmatch(r"[0-9a-f]{64}", declared): ok("calendarContentHash is 64 lowercase hex chars")
else: bad(f"calendarContentHash {declared!r} is not 64 lowercase hex chars")

# --- 4. validity window ---------------------------------------------------------------------------------------------
def iso_date(name, v):
    try:
        d = datetime.date.fromisoformat(v)
        if d.isoformat() != v: raise ValueError("not canonical ISO")
        return d
    except Exception as e:
        bad(f"{name} {v!r} is not a canonical ISO date: {e}"); return None
d_from, d_through = iso_date("validFromDate", doc["validFromDate"]), iso_date("validThroughDate", doc["validThroughDate"])
if d_from and d_through:
    if doc["validFromDate"] == want_from and doc["validThroughDate"] == want_through and d_from <= d_through:
        ok(f"valid {want_from}..{want_through}")
    else:
        bad(f"validity is {doc['validFromDate']}..{doc['validThroughDate']}, expected {want_from}..{want_through} with from <= through")

# --- 5. entries ----------------------------------------------------------------------------------------------------
entries = doc["entries"]
if not isinstance(entries, list):
    bad(f"entries is {type(entries).__name__}, expected a list"); entries = []
if len(entries) == want_entries and len(entries) <= MAX_CALENDAR_ENTRIES: ok(f"{len(entries)} entries (expected {want_entries}, cap {MAX_CALENDAR_ENTRIES})")
else: bad(f"{len(entries)} entries, expected {want_entries} (cap {MAX_CALENDAR_ENTRIES})")
entry_problems = 0
seen = set()
prev = None
for i, e in enumerate(entries):
    def p(msg):
        global entry_problems
        entry_problems += 1
        bad(f"entry {i}: {msg}")
    if not isinstance(e, dict) or sorted(e) != sorted(ENTRY_FIELDS):
        p(f"fields are {sorted(e) if isinstance(e, dict) else type(e).__name__}, expected exactly {sorted(ENTRY_FIELDS)}"); continue
    code, inst, lead, trail = e["eventCode"], e["instantUtcMs"], e["leadWindowMs"], e["trailWindowMs"]
    if not (isinstance(code, str) and EVENT_CODE.fullmatch(code) and len(code) <= MAX_EVENT_CODE_CHARS):
        p(f"eventCode {code!r} does not match {EVENT_CODE.pattern} within {MAX_EVENT_CODE_CHARS} chars")
    if not (is_int(inst) and 0 < inst <= MAX_INSTANT_UTC_MS):
        p(f"instantUtcMs {inst!r} outside (0, {MAX_INSTANT_UTC_MS}]")
    for n, v in (("leadWindowMs", lead), ("trailWindowMs", trail)):
        if not (is_int(v) and 0 <= v <= MAX_EVENT_WINDOW_MS):
            p(f"{n} {v!r} outside [0, {MAX_EVENT_WINDOW_MS}]")
    if is_int(inst) and isinstance(code, str):
        key = (inst, code)
        if key in seen: p(f"duplicate (eventCode, instantUtcMs) {code} @ {inst}")
        seen.add(key)
        if prev is not None and key < prev: p(f"not sorted by (instantUtcMs, eventCode): {key} after {prev}")
        prev = key
if entries and entry_problems == 0:
    codes = {}
    for e in entries: codes[e["eventCode"]] = codes.get(e["eventCode"], 0) + 1
    ok("every entry carries eventCode/instantUtcMs/leadWindowMs/trailWindowMs within the contract's domains, sorted, no duplicates ("
       + ", ".join(f"{k}={v}" for k, v in sorted(codes.items())) + ")")

# --- 6. the canonical content hash, recomputed -----------------------------------------------------------------------
# CanonicalBytes for the calendar record shape. Tags: INT 0x02, STRING 0x04, DATE 0x07, TIMESTAMP 0x08, ARRAY 0x09,
# RECORD 0x0B. A record payload is, per field in ascending id: uvarint(id) tag uvarint(len) payload; fields 0 and 1
# are the registry identity (id string, version long). An array payload is uvarint(count) then per element:
# tag uvarint(len) payload. Longs are 8 bytes big-endian; a date is its 10 ASCII bytes; strings are UTF-8 (NFC —
# the codes are ASCII by grammar). The hash is sha256 over the top-level RECORD PAYLOAD (unframed).
TAG_INT, TAG_STRING, TAG_DATE, TAG_TIMESTAMP, TAG_ARRAY, TAG_RECORD = 0x02, 0x04, 0x07, 0x08, 0x09, 0x0B
def uvarint(v):
    out = bytearray()
    while v & ~0x7F:
        out.append((v & 0x7F) | 0x80); v >>= 7
    out.append(v); return bytes(out)
def field(fid, tag, payload): return uvarint(fid) + bytes([tag]) + uvarint(len(payload)) + payload
def record(registry_id, registry_version, fields):
    out = field(0, TAG_STRING, registry_id.encode("utf-8")) + field(1, TAG_INT, struct.pack(">q", registry_version))
    for fid, tag, payload in fields: out += field(fid, tag, payload)
    return out
def content_hash(doc):
    arr = uvarint(len(doc["entries"]))
    for e in doc["entries"]:
        p = record("vol-premium.calendar.entry", 1, [
            (2, TAG_STRING, e["eventCode"].encode("utf-8")),
            (3, TAG_TIMESTAMP, struct.pack(">q", e["instantUtcMs"])),
            (4, TAG_INT, struct.pack(">q", e["leadWindowMs"])),
            (5, TAG_INT, struct.pack(">q", e["trailWindowMs"]))])
        arr += bytes([TAG_RECORD]) + uvarint(len(p)) + p
    rec = record("vol-premium.calendar", 1, [
        (2, TAG_STRING, doc["calendarVersion"].encode("utf-8")),
        (3, TAG_DATE, doc["validFromDate"].encode("ascii")),
        (4, TAG_DATE, doc["validThroughDate"].encode("ascii")),
        (5, TAG_ARRAY, arr)])
    return hashlib.sha256(rec).hexdigest()
if entry_problems == 0 and entries and d_from and d_through and isinstance(version, str):
    recomputed = content_hash(doc)
    if recomputed == declared == want_hash:
        ok(f"canonical content hash recomputed = declared = expected: {recomputed}")
    else:
        bad(f"canonical content hash: recomputed {recomputed}, declared {declared}, expected {want_hash} — the file, its hash and the pin must agree")
else:
    bad("content hash not recomputed: the entries above are not a valid payload")

# --- 7. the manifests that pin it ------------------------------------------------------------------------------------
def env_values(text, name):
    # `- name: NAME` followed by `value: V` on the next non-blank line; V may be quoted. One per container in a slice.
    out = []
    lines = text.split("\n")
    for i, ln in enumerate(lines):
        if re.match(r"\s*-\s*name:\s*" + re.escape(name) + r"\s*$", ln):
            j = i + 1
            while j < len(lines) and not lines[j].strip(): j += 1
            m = re.match(r"\s*value:\s*(.*?)\s*$", lines[j]) if j < len(lines) else None
            if m: out.append(m.group(1).strip().strip('"').strip("'"))
    return out
for mf in manifests:
    if not os.path.exists(mf):
        bad(f"{mf}: not found (VP_CAL_MANIFESTS names it)"); continue
    text = open(mf, encoding="utf-8").read()
    v, h = env_values(text, "VOL_PREMIUM_CALENDAR_VERSION"), env_values(text, "VOL_PREMIUM_CALENDAR_CONTENT_HASH")
    if v == [version] and h == [declared]:
        ok(f"{mf}: pins VOL_PREMIUM_CALENDAR_VERSION={version} and VOL_PREMIUM_CALENDAR_CONTENT_HASH={declared[:12]}…")
    else:
        bad(f"{mf}: VOL_PREMIUM_CALENDAR_VERSION={v} VOL_PREMIUM_CALENDAR_CONTENT_HASH={h}, expected exactly one of each, [{version}] and [{declared}]")

if fails == 0:
    print("=== validate-vol-premium-calendar: OK ===")
else:
    print(f"=== validate-vol-premium-calendar: FAILED ({fails} problem(s)) ===")
    sys.exit(1)
PY
