#!/usr/bin/env bash
# The vol-premium event calendar the deployment pins (Gate-1 §4.5), checked the way the engine checks it at
# boot — before the file can be published to the calendar ledger (Jenkinsfile.vol-premium-ledger-publish) and
# before a manifest that pins its version and content hash can merge.
#
# TWO LAYERS, and the distinction is the point:
#
#   1. PARITY — "what does the contract do with these bytes". A stdlib-Python reproduction of the engine's own
#      load path: LedgerPublisher.validateCalendar / ArtifactBinding — requireWithinWire on the bytes, then
#      Jackson (ObjectMapper + JavaTimeModule) into the CalendarArtifact record, whose constructor validates and
#      re-hashes with CanonicalBytes. That includes what Jackson ACCEPTS that a strict JSON reader would not
#      (a float or numeric string for an int, a number or boolean for a String, an int array for a date, a
#      missing or null primitive as 0, trailing tokens, duplicate keys last-wins) and the contract's bounded
#      normalisation (CanonicalValue.Str: BOM refused, then NFC, then the ASCII grammar). It is NOT the contract;
#      it is held to it by scripts/ci/fixtures/vol-premium-calendar: expected.tsv is GENERATED from the real
#      contract classes (CalendarVectors.java against the options-edge-contracts jar) over the vector files, and
#      `--vectors` requires this port to give every vector the same verdict and, when OK, the same hash. Positive
#      vectors, Jackson-leniency vectors and negative vectors alike. When CanonicalBytes, the calendar registry or
#      the reader change, the vectors change (regenerate-expected.sh) and this port is corrected to match.
#
#   2. ARTEFACT FORM — what the COMMITTED file must additionally be: the contract serializer's own output, and
#      the one the owner signed. The exact sha256, exactly the seven fields, exactly the four entry fields, JSON
#      integers for the integer fields, the pinned version / entries / validity window, and the manifests pinning
#      exactly this version and hash. Stricter than layer 1 on purpose: a file that only LOADS is not the artefact
#      that was reviewed. These are policy on the repository file, not a claim about the contract.
#
# Usage: scripts/ci/validate-vol-premium-calendar.sh [calendar.json]     (defaults describe calendar 2026.1)
#        scripts/ci/validate-vol-premium-calendar.sh --vectors           (layer-1 parity against expected.tsv)
#   VP_CAL_EXPECT_{VERSION,HASH,SHA256,ENTRIES,FROM,THROUGH} override the artefact pins; VP_CAL_MANIFESTS the
#   manifests to cross-check (space-separated; "" skips).
set -uo pipefail
cd "$(dirname "$0")/../.."

MODE=file
FILE="scripts/vol-premium/calendars/calendar-2026.1.json"
case "${1:-}" in --vectors) MODE=vectors ;; '') : ;; *) FILE="$1" ;; esac
export VP_CAL_MODE="$MODE" VP_CAL_FILE="$FILE"
export VP_CAL_EXPECT_VERSION="${VP_CAL_EXPECT_VERSION:-2026.1}"
export VP_CAL_EXPECT_HASH="${VP_CAL_EXPECT_HASH:-cf209ff9ff428428988322928b27cd625556a2bb5a28131410b4d4631ea9c347}"
export VP_CAL_EXPECT_SHA256="${VP_CAL_EXPECT_SHA256:-eb7f76e5b3bfa7f0a2189a4014ada2ceb8b025cd744e6fbe09a34c1776142b81}"
export VP_CAL_EXPECT_ENTRIES="${VP_CAL_EXPECT_ENTRIES:-40}"
export VP_CAL_EXPECT_FROM="${VP_CAL_EXPECT_FROM:-2026-01-01}"
export VP_CAL_EXPECT_THROUGH="${VP_CAL_EXPECT_THROUGH:-2026-12-31}"
export VP_CAL_MANIFESTS="${VP_CAL_MANIFESTS-k8s/base/vol-premium-deployment.yaml k8s/services/vol-premium/overlays/production/manifest.yaml k8s/services/vol-premium/overlays/dev/manifest.yaml}"
export VP_CAL_FIXTURES="scripts/ci/fixtures/vol-premium-calendar"

command -v python3 >/dev/null 2>&1 || { echo "FAIL: python3 is required"; exit 1; }
if [ "$MODE" = file ] && [ ! -f "$FILE" ]; then echo "FAIL: calendar file not found: $FILE"; exit 1; fi

python3 - <<'PY'
import datetime, hashlib, json, math, os, re, struct, sys, unicodedata

# =============================================================================================================
# LAYER 1 — PARITY: the contract's load path, reproduced. Every rule names what it reproduces.
# =============================================================================================================
class Reject(Exception):
    pass

# --- the contract's constants (options-edge-contracts CalendarArtifact / CanonicalValue / CanonicalBytes) -----
CURRENT_SCHEMA_VERSION = 1
HASH_VERSION = 1
MAX_CALENDAR_ENTRIES = 512
MAX_CALENDAR_BYTES = 65536
MAX_EVENT_WINDOW_MS = 3_600_000
MAX_EVENT_CODE_CHARS = 32
MAX_VERSION_CHARS = 64
MAX_INSTANT_UTC_MS = int(datetime.datetime(9999, 12, 31, tzinfo=datetime.timezone.utc).timestamp() * 1000) + 86_400_000 - 1
EVENT_CODE = re.compile(r"[A-Za-z0-9_.:-]+")
TOP_FIELDS = ("schemaVersion", "calendarVersion", "calendarContentHash", "hashVersion", "validFromDate", "validThroughDate", "entries")
ENTRY_FIELDS = ("eventCode", "instantUtcMs", "leadWindowMs", "trailWindowMs")
INT_MIN, INT_MAX = -2**31, 2**31 - 1
LONG_MIN, LONG_MAX = -2**63, 2**63 - 1

# --- Jackson 2.19 (default features + JavaTimeModule), as MEASURED by the vectors ---------------------------
class JNum(str):
    """A JSON number token kept as its text (see j_parse)."""
    __slots__ = ()

def j_parse(text):
    """One top-level JSON value, Jackson-style: leading whitespace skipped, trailing tokens IGNORED
    (FAIL_ON_TRAILING_TOKENS is off), duplicate keys last-wins, NaN/Infinity literals refused, a BOM refused."""
    if text.startswith("﻿"):
        raise Reject("JsonParseException: unexpected character U+FEFF")
    def bad_constant(c):
        raise Reject("JsonParseException: non-standard token " + c)
    # Numbers come back as JNum — their TEXT: Jackson coerces a number to a String field by that text (getText)
    # and parses it for an int/long field; json.loads would have lost the token ("2026.50" is not "2026.5").
    dec = json.JSONDecoder(parse_constant=bad_constant, parse_int=JNum, parse_float=JNum)
    stripped = text.lstrip(" \t\r\n")
    if not stripped:
        raise Reject("MismatchedInputException: no content to map")
    try:
        value, _end = dec.raw_decode(stripped)
    except ValueError as e:
        raise Reject("JsonParseException: " + str(e))
    return value

def j_object(v, what, known):
    if not isinstance(v, dict):
        raise Reject(f"MismatchedInputException: cannot deserialize {what} from {type(v).__name__}")
    for k in v:
        if k not in known:
            raise Reject(f"UnrecognizedPropertyException: unrecognized field {k!r} in {what}")
    return v

def j_primitive_number(v, name, lo, hi):
    """A Java int/long PRIMITIVE: missing or null -> 0; boolean refused; a NUMBER token with a fraction or exponent
    is truncated toward zero (ACCEPT_FLOAT_AS_INT); a STRING is coerced only when it is an integral literal
    (ALLOW_COERCION_OF_SCALARS: "600000" yes, "1.0" no — vector j17); out of range refused."""
    if v is None:
        return 0
    if isinstance(v, bool):
        raise Reject(f"MismatchedInputException: cannot deserialize {name} from Boolean")
    if isinstance(v, JNum):
        if re.fullmatch(r"-?[0-9]+", v):
            n = int(v)
        else:
            f = float(v)
            if not math.isfinite(f):
                raise Reject(f"InvalidFormatException: {name} is not finite")
            n = int(f)                               # truncation toward zero, as getValueAsInt/Long does
    elif isinstance(v, str):
        s = v.strip()
        if re.fullmatch(r"[+-]?[0-9]+", s):
            n = int(s)
        else:
            raise Reject(f"InvalidFormatException: {name} from String {v!r} is not a valid int value")
    else:
        raise Reject(f"MismatchedInputException: cannot deserialize {name} from {type(v).__name__}")
    if not (lo <= n <= hi):
        raise Reject(f"InvalidFormatException: {name} {n} out of range")
    return n

def j_string(v, name):
    """A Java String: null stays null (the constructor decides); a number token becomes its own TEXT and a boolean
    "true"/"false" (ALLOW_COERCION_OF_SCALARS); an array or object is refused."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, JNum):
        return str.__str__(v)
    if isinstance(v, str):
        return v
    raise Reject(f"MismatchedInputException: cannot deserialize {name} from {type(v).__name__}")

def j_local_date(v, name):
    """java.time.LocalDate via JavaTimeModule: an ISO string (strict yyyy-MM-dd) or an int array [y, m, d];
    null stays null (the constructor refuses it)."""
    if v is None:
        return None
    if isinstance(v, str):
        try:
            d = datetime.date.fromisoformat(v)
            if d.isoformat() != v:
                raise ValueError("not canonical ISO")
            return d
        except ValueError as e:
            raise Reject(f"InvalidFormatException: cannot deserialize LocalDate {name} from {v!r}: {e}")
    if isinstance(v, list):
        if len(v) != 3 or any(not isinstance(x, JNum) or not re.fullmatch(r"-?[0-9]+", x) for x in v):
            raise Reject(f"MismatchedInputException: LocalDate {name} array must be [year, month, day] ints")
        try:
            return datetime.date(*(int(x) for x in v))
        except ValueError as e:
            raise Reject(f"InvalidFormatException: LocalDate {name} {v}: {e}")
    raise Reject(f"MismatchedInputException: cannot deserialize LocalDate {name} from {type(v).__name__}")

# --- CanonicalValue.Str: the bounded normalisation, in the contract's order ------------------------------------
def canonical_str(s, what):
    if s.startswith("﻿"):
        raise Reject(f"CanonicalFormatException: {what} must not carry a BOM")
    return unicodedata.normalize("NFC", s)

# --- CalendarArtifact.CalendarEntry constructor ---------------------------------------------------------------
def build_entry(raw):
    o = j_object(raw, "CalendarEntry", ENTRY_FIELDS)
    code = j_string(o.get("eventCode"), "eventCode")
    inst = j_primitive_number(o.get("instantUtcMs"), "instantUtcMs", LONG_MIN, LONG_MAX)
    lead = j_primitive_number(o.get("leadWindowMs"), "leadWindowMs", LONG_MIN, LONG_MAX)
    trail = j_primitive_number(o.get("trailWindowMs"), "trailWindowMs", LONG_MIN, LONG_MAX)
    if code is None or not code.strip():
        raise Reject("IllegalArgumentException: eventCode is required")
    code = canonical_str(code, "eventCode")                  # normalised BEFORE the grammar, as the contract does
    if len(code) > MAX_EVENT_CODE_CHARS or not EVENT_CODE.fullmatch(code):
        raise Reject(f"IllegalArgumentException: eventCode must match {EVENT_CODE.pattern} within {MAX_EVENT_CODE_CHARS} chars, got {code!r}")
    if inst <= 0 or inst > MAX_INSTANT_UTC_MS:
        raise Reject(f"IllegalArgumentException: instantUtcMs {inst} for {code} is outside (0, {MAX_INSTANT_UTC_MS}]")
    for n, v in (("leadWindowMs", lead), ("trailWindowMs", trail)):
        if v < 0 or v > MAX_EVENT_WINDOW_MS:
            raise Reject(f"IllegalArgumentException: {n} {v} outside [0, {MAX_EVENT_WINDOW_MS}] for {code}")
    return {"eventCode": code, "instantUtcMs": inst, "leadWindowMs": lead, "trailWindowMs": trail}

# --- CanonicalBytes for the calendar record shape ---------------------------------------------------------------
# Tags: INT 0x02, STRING 0x04, DATE 0x07, TIMESTAMP 0x08, ARRAY 0x09, RECORD 0x0B. A record payload is, per field
# in ascending id: uvarint(id) tag uvarint(len) payload; fields 0 and 1 are the registry identity (id string,
# version long). An array payload is uvarint(count) then per element: tag uvarint(len) payload. Longs are 8 bytes
# big-endian; a date is its 10 ASCII bytes; strings are NFC UTF-8. The hash is sha256 over the top-level RECORD
# PAYLOAD (unframed) — computeContentHash(record, registry) does not frame the root.
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
def content_hash(version, d_from, d_through, entries):
    arr = uvarint(len(entries))
    for e in entries:
        p = record("vol-premium.calendar.entry", 1, [
            (2, TAG_STRING, e["eventCode"].encode("utf-8")),
            (3, TAG_TIMESTAMP, struct.pack(">q", e["instantUtcMs"])),
            (4, TAG_INT, struct.pack(">q", e["leadWindowMs"])),
            (5, TAG_INT, struct.pack(">q", e["trailWindowMs"]))])
        arr += bytes([TAG_RECORD]) + uvarint(len(p)) + p
    rec = record("vol-premium.calendar", 1, [
        (2, TAG_STRING, canonical_str(version, "calendarVersion").encode("utf-8")),
        (3, TAG_DATE, d_from.isoformat().encode("ascii")),
        (4, TAG_DATE, d_through.isoformat().encode("ascii")),
        (5, TAG_ARRAY, arr)])
    return hashlib.sha256(rec).hexdigest()

# --- LedgerPublisher.validateCalendar: requireWithinWire, Jackson, the CalendarArtifact constructor -------------
def java_load(raw):
    """Returns the loaded artefact (dict) with its content hash, or raises Reject — the contract's verdict."""
    if len(raw) > MAX_CALENDAR_BYTES:
        raise Reject(f"IllegalArgumentException: serialised calendar is {len(raw)} bytes, over MAX_CALENDAR_BYTES {MAX_CALENDAR_BYTES}")
    text = raw.decode("utf-8", errors="replace")            # new String(bytes, UTF_8) substitutes U+FFFD
    top = j_object(j_parse(text), "CalendarArtifact", TOP_FIELDS)
    schema = j_primitive_number(top.get("schemaVersion"), "schemaVersion", INT_MIN, INT_MAX)
    version = j_string(top.get("calendarVersion"), "calendarVersion")
    declared = j_string(top.get("calendarContentHash"), "calendarContentHash")
    hash_version = j_primitive_number(top.get("hashVersion"), "hashVersion", INT_MIN, INT_MAX)
    d_from = j_local_date(top.get("validFromDate"), "validFromDate")
    d_through = j_local_date(top.get("validThroughDate"), "validThroughDate")
    raw_entries = top.get("entries")
    entries = None
    if raw_entries is not None:
        if not isinstance(raw_entries, list):
            raise Reject("MismatchedInputException: entries is not an array")
        entries = []
        for e in raw_entries:
            if e is None:
                entries.append(None)                          # Jackson passes null through; the constructor refuses it
            else:
                entries.append(build_entry(e))
    # CalendarArtifact constructor, in its order
    if schema != CURRENT_SCHEMA_VERSION:
        raise Reject(f"IllegalArgumentException: schemaVersion must be {CURRENT_SCHEMA_VERSION}, got {schema}")
    if hash_version != HASH_VERSION:
        raise Reject(f"IllegalArgumentException: hashVersion must be {HASH_VERSION}, got {hash_version}")
    if version is None or not version.strip():
        raise Reject("IllegalArgumentException: calendarVersion is required")
    if len(version) > MAX_VERSION_CHARS or declared is None or not re.fullmatch(r"[0-9a-f]{64}", declared):
        raise Reject("IllegalArgumentException: calendarVersion is bounded and calendarContentHash is 64 lowercase hex chars")
    if not EVENT_CODE.fullmatch(version):                     # the RAW version: the grammar is checked before hashing normalises
        raise Reject(f"IllegalArgumentException: calendarVersion must be an ASCII identifier, got {version!r}")
    if d_from is None or d_through is None:
        raise Reject("IllegalArgumentException: validFromDate and validThroughDate are required")
    if d_through < d_from:
        raise Reject(f"IllegalArgumentException: validThroughDate {d_through} is before validFromDate {d_from}")
    if entries is None:
        raise Reject("IllegalArgumentException: entries must be present (empty, not null)")
    if len(entries) > MAX_CALENDAR_ENTRIES:
        raise Reject(f"IllegalArgumentException: calendar has {len(entries)} entries, over MAX_CALENDAR_ENTRIES {MAX_CALENDAR_ENTRIES}")
    seen = set(); prev = None
    for e in entries:
        if e is None:
            raise Reject("IllegalArgumentException: entries must not contain null")
        key = (e["instantUtcMs"], e["eventCode"])
        if (e["eventCode"] + "@" + str(e["instantUtcMs"])) in seen:
            raise Reject(f"IllegalArgumentException: duplicate calendar entry {e['eventCode']} at {e['instantUtcMs']}")
        seen.add(e["eventCode"] + "@" + str(e["instantUtcMs"]))
        if prev is not None and key < prev:                    # Long.compare, then String.compareTo (UTF-16 order; ASCII here)
            raise Reject(f"IllegalArgumentException: entries are not sorted by (instant, eventCode) at {e['eventCode']}@{e['instantUtcMs']}")
        prev = key
    computed = content_hash(version, d_from, d_through, entries)
    if computed != declared:
        raise Reject(f"IllegalArgumentException: calendarContentHash {declared} does not match the payload's canonical hash {computed}")
    return {"calendarVersion": version, "calendarContentHash": computed, "validFromDate": d_from, "validThroughDate": d_through, "entries": entries}

# =============================================================================================================
fails = 0
def ok(msg):  print("  ok   " + msg)
def bad(msg):
    global fails
    fails += 1
    print("  FAIL " + msg)
def finish(name):
    if fails == 0:
        print(f"=== {name}: OK ==="); sys.exit(0)
    print(f"=== {name}: FAILED ({fails} problem(s)) ==="); sys.exit(1)

mode = os.environ["VP_CAL_MODE"]
fixtures = os.environ["VP_CAL_FIXTURES"]

# --- --vectors: the port held to the contract's own verdicts -----------------------------------------------------
if mode == "vectors":
    exp_path = os.path.join(fixtures, "expected.tsv"); vec_dir = os.path.join(fixtures, "vectors")
    expected = {}
    for line in open(exp_path, encoding="utf-8"):
        if not line.strip() or line.startswith("#"): continue
        name, verdict, detail = line.rstrip("\n").split("\t", 2)
        expected[name] = (verdict, detail)
    names = sorted(n[:-5] for n in os.listdir(vec_dir) if n.endswith(".json"))
    if sorted(expected) != names:
        bad(f"expected.tsv and vectors/ disagree on the vector set: only in expected {sorted(set(expected) - set(names))}, only in vectors {sorted(set(names) - set(expected))} — run regenerate-expected.sh")
    n_ok = n_rej = 0
    for name in names:
        if name not in expected: continue
        raw = open(os.path.join(vec_dir, name + ".json"), "rb").read()
        try:
            got = ("OK", java_load(raw)["calendarContentHash"])
        except Reject as e:
            got = ("REJECT", str(e))
        want = expected[name]
        if got[0] == want[0] == "OK" and got[1] == want[1]:
            n_ok += 1; ok(f"{name}: OK, hash {got[1][:16]}… as the contract computes")
        elif got[0] == want[0] == "REJECT":
            n_rej += 1; ok(f"{name}: REJECT as the contract does (port: {got[1][:90]})")
        else:
            bad(f"{name}: port says {got[0]} {got[1][:100]!r}; the contract says {want[0]} {want[1][:100]!r}")
    if n_ok < 5 or n_rej < 5:
        bad(f"too few vectors of one kind to call this parity ({n_ok} OK, {n_rej} REJECT)")
    print(f"vectors: {len(names)} ({n_ok} OK, {n_rej} REJECT), contract verdicts from {exp_path}")
    finish("validate-vol-premium-calendar --vectors")

# --- the artefact file ---------------------------------------------------------------------------------------------
path = os.environ["VP_CAL_FILE"]
want_version = os.environ["VP_CAL_EXPECT_VERSION"]
want_hash = os.environ["VP_CAL_EXPECT_HASH"]
want_sha = os.environ["VP_CAL_EXPECT_SHA256"]
want_entries = int(os.environ["VP_CAL_EXPECT_ENTRIES"])
want_from = os.environ["VP_CAL_EXPECT_FROM"]
want_through = os.environ["VP_CAL_EXPECT_THROUGH"]
manifests = os.environ.get("VP_CAL_MANIFESTS", "").split()
raw = open(path, "rb").read()

# LAYER 1: would the engine load it, and to what hash
try:
    loaded = java_load(raw)
    ok(f"contract load path (Jackson + CalendarArtifact constructor + canonical re-hash) accepts the file: hash {loaded['calendarContentHash']}")
except Reject as e:
    bad(f"the contract's load path REJECTS this file: {e}")
    finish("validate-vol-premium-calendar")

# LAYER 2: the committed artefact's form and identity
sha = hashlib.sha256(raw).hexdigest()
if sha == want_sha: ok(f"file sha256 {sha} is the signed-off artefact ({len(raw)} bytes of MAX_CALENDAR_BYTES {MAX_CALENDAR_BYTES})")
else: bad(f"file sha256 is {sha}, expected {want_sha}: this is not the artefact that was signed off")
def is_json_int(v): return isinstance(v, int) and not isinstance(v, bool)   # strict json.loads: real ints here
try:
    strict = json.loads(raw.decode("utf-8"))                  # the serializer's output parses strictly, no trailing bytes
except Exception as e:
    strict = None; bad(f"artefact form: not strict JSON: {e}")
if isinstance(strict, dict):
    if sorted(strict) == sorted(TOP_FIELDS): ok("artefact form: exactly the seven CalendarArtifact fields")
    else: bad(f"artefact form: top-level fields are {sorted(strict)}, expected exactly {sorted(TOP_FIELDS)}")
    for k in ("schemaVersion", "hashVersion"):
        if is_json_int(strict.get(k)): ok(f"artefact form: {k} is a JSON integer ({strict[k]})")
        else: bad(f"artefact form: {k} is {strict.get(k)!r}, expected a JSON integer (the serializer writes int fields as integers)")
    for k in ("calendarVersion", "calendarContentHash", "validFromDate", "validThroughDate"):
        if not isinstance(strict.get(k), str): bad(f"artefact form: {k} is {type(strict.get(k)).__name__}, expected a JSON string")
    es = strict.get("entries")
    if isinstance(es, list):
        form = [i for i, e in enumerate(es) if not isinstance(e, dict) or sorted(e) != sorted(ENTRY_FIELDS)
                or not isinstance(e.get("eventCode"), str) or not all(is_json_int(e.get(k)) for k in ENTRY_FIELDS[1:])]
        if form: bad(f"artefact form: entries {form[:5]} do not carry exactly eventCode (string) + instantUtcMs/leadWindowMs/trailWindowMs (integers)")
        else: ok(f"artefact form: every entry carries exactly eventCode + instantUtcMs/leadWindowMs/trailWindowMs as JSON string/integers")
version, declared = loaded["calendarVersion"], loaded["calendarContentHash"]
if version == want_version: ok(f"calendarVersion {version}")
else: bad(f"calendarVersion {version!r}, expected {want_version!r}")
if loaded["validFromDate"].isoformat() == want_from and loaded["validThroughDate"].isoformat() == want_through: ok(f"valid {want_from}..{want_through}")
else: bad(f"validity is {loaded['validFromDate']}..{loaded['validThroughDate']}, expected {want_from}..{want_through}")
if len(loaded["entries"]) == want_entries:
    codes = {}
    for e in loaded["entries"]: codes[e["eventCode"]] = codes.get(e["eventCode"], 0) + 1
    ok(f"{want_entries} entries (" + ", ".join(f"{k}={v}" for k, v in sorted(codes.items())) + "), sorted, no duplicates, every window within the contract's domains")
else: bad(f"{len(loaded['entries'])} entries, expected {want_entries}")
if declared == want_hash: ok(f"canonical content hash recomputed = declared = expected: {declared}")
else: bad(f"canonical content hash {declared} (recomputed = declared) is not the pinned {want_hash}")

def env_values(text, name):
    out = []; lines = text.split("\n")
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
finish("validate-vol-premium-calendar")
PY
