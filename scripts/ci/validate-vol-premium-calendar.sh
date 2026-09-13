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
MAX_NUMBER_LENGTH = 1000
LONG_MIN, LONG_MAX = -2**63, 2**63 - 1

# --- Jackson 2.19 (default features + JavaTimeModule), as MEASURED by the vectors ---------------------------
class JNum(str):
    """A JSON number token kept as its text (see j_parse)."""
    __slots__ = ()

class JObj(list):
    """A JSON object as its ORDERED (key, value) pairs, duplicates kept: Jackson streams an object and reacts to
    each property as it arrives; a dict would have collapsed duplicates before anything could be judged."""
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
    def num(text):
        digits = sum(c in "0123456789" for c in text)   # StreamReadConstraints.maxNumberLength counts DIGITS (sign, '.', 'e' excluded): n38-n40
        if digits > MAX_NUMBER_LENGTH:
            raise Reject(f"StreamConstraintsException: number value length ({digits}) exceeds the maximum allowed ({MAX_NUMBER_LENGTH})")
        return JNum(text)
    dec = json.JSONDecoder(parse_constant=bad_constant, parse_int=num, parse_float=num, object_pairs_hook=JObj)
    stripped = text.lstrip(" \t\r\n")
    if not stripped:
        raise Reject("MismatchedInputException: no content to map")
    try:
        value, _end = dec.raw_decode(stripped)
    except ValueError as e:
        raise Reject("JsonParseException: " + str(e))
    return value

def j_record(v, what, known, coerce):
    """A Java RECORD read through its canonical constructor (creator properties), as Jackson does it, property by
    property in wire order: an unknown key is refused when met; a known key's value is deserialized when met (a
    type error is a refusal right there, even if a later duplicate would have been fine — vectors d01/d02/d10);
    creator values are BUFFERED and a repeated key overwrites the buffer (d04/d05/d06/d09 accept) until the LAST
    missing property arrives — the record is then instantiated at once, and any property after that is refused
    ("No fallback setter/field defined for creator property": d03/d07/d08/d12). Returns the coerced values with
    missing properties absent (the constructor sees the primitive default / null for those)."""
    if not isinstance(v, JObj):
        raise Reject(f"MismatchedInputException: cannot deserialize {what} from {type(v).__name__}")
    values = {}
    instantiated = False
    for k, raw in v:
        if k not in known:
            raise Reject(f"UnrecognizedPropertyException: unrecognized field {k!r} in {what}")
        if instantiated:
            raise Reject(f"InvalidDefinitionException: no fallback setter/field defined for creator property {k!r} of {what}")
        values[k] = coerce(k, raw)
        if len(values) == len(known):
            instantiated = True
    return values

def java_trim(s):
    """String.trim(): strips only characters <= U+0020 — NBSP, EM SPACE and the rest of Unicode stay (s04/s05)."""
    i, j = 0, len(s)
    while i < j and ord(s[i]) <= 0x20: i += 1
    while j > i and ord(s[j - 1]) <= 0x20: j -= 1
    return s[i:j]

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
        s = java_trim(v)
        if s == "" or s == "null":                   # a blank or the text "null" is null, and null is the default 0 (s01-s03)
            n = 0
        elif re.fullmatch(r"[+-]?\d+", s) and all(unicodedata.category(c) == "Nd" for c in s.lstrip("+-")):
            n = int(s)                               # Long.parseLong: sign, then Character.digit() digits of ANY script (s13-s16)
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

# --- java.time.LocalDate, kept as (y, m, d) so year 0000 exists (LocalDate has it; Python's datetime does not) ----
def is_leap(y): return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)
def valid_ymd(y, m, d):
    if not (1 <= m <= 12) or d < 1: return False
    return d <= [31, 29 if is_leap(y) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]
def ymd_from_epoch_day(z):
    """LocalDate.ofEpochDay: proleptic Gregorian (Howard Hinnant's civil_from_days)."""
    z += 719468
    era = (z if z >= 0 else z - 146096) // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    return (y + (1 if m <= 2 else 0), m, d)
def iso_date(ymd): return "%04d-%02d-%02d" % ymd

def j_local_date(v, name):
    """jackson-datatype-jsr310 2.18.2 LocalDateDeserializer, as its source reads and the vectors measure (t01-t53):
      * a string is String.trim()med; empty -> null;
      * length > 10 with 'T' at index 10: a trailing 'Z' is REMOVED (not parsed as an instant — so "T00:00Z" is
        fine and any other offset is not), then ISO_LOCAL_DATE_TIME: yyyy-MM-dd'T'HH:mm[:ss[.fraction 0-9 digits]] —
        a fraction only AFTER seconds, hours 00-23, minutes and seconds 00-59, ASCII digits, and the date resolved
        STRICT (no Feb 29 in 2026); the date part is kept;
      * otherwise LocalDate.parse (ISO_LOCAL_DATE): an unsigned 4-digit year, or a SIGNED 5-9 digit one;
      * an integer number is an epoch day (LocalDate.ofEpochDay); a float, a string of digits, an ISO week or
        ordinal date, a lowercase 't'/'z', an internal or non-ASCII space are refused;
      * an int array is [y, m, d].
    Returns (y, m, d); the CanonicalValue.Date bound (year 0000..9999) is applied where the hash is built, as the
    contract does — a +12026 date PARSES and is then refused by the Date constructor."""
    if v is None:
        return None
    if isinstance(v, JNum):
        if not re.fullmatch(r"-?[0-9]+", v):
            raise Reject(f"InvalidFormatException: LocalDate {name} from a non-integral number")
        return ymd_from_epoch_day(int(v))
    if isinstance(v, str):
        t = java_trim(v)
        if t == "":
            return None
        if len(t) > 10 and t[10] == "T":
            body = t[:-1] if t.endswith("Z") else t
            m = re.fullmatch(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]{0,9}))?)?", body)
            if not m:
                raise Reject(f"InvalidFormatException: LocalDate {name} from {v!r}: not an ISO local date-time")
            y, mo, d, hh, mi = (int(m.group(i)) for i in range(1, 6))
            ss = int(m.group(6)) if m.group(6) is not None else 0
            if hh > 23 or mi > 59 or ss > 59 or not valid_ymd(y, mo, d):
                raise Reject(f"InvalidFormatException: LocalDate {name} from {v!r}: out of range")
            return (y, mo, d)
        m = re.fullmatch(r"([0-9]{4}|[+-][0-9]{5,9})-([0-9]{2})-([0-9]{2})", t)
        if not m:
            raise Reject(f"InvalidFormatException: LocalDate {name} from {v!r}: not an ISO local date")
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if not valid_ymd(y, mo, d):
            raise Reject(f"InvalidFormatException: LocalDate {name} from {v!r}: invalid date")
        return (y, mo, d)
    if isinstance(v, list):
        if isinstance(v, JObj) or len(v) != 3 or any(not isinstance(x, JNum) or not re.fullmatch(r"-?[0-9]+", x) for x in v):
            raise Reject(f"MismatchedInputException: LocalDate {name} array must be [year, month, day] ints")
        y, mo, d = (int(x) for x in v)
        if not valid_ymd(y, mo, d):
            raise Reject(f"InvalidFormatException: LocalDate {name} {v}: invalid date")
        return (y, mo, d)
    raise Reject(f"MismatchedInputException: cannot deserialize LocalDate {name} from {type(v).__name__}")

def canonical_date(ymd, what):
    """CanonicalValue.Date: the year must have a yyyy-MM-dd form (0000..9999)."""
    if not (0 <= ymd[0] <= 9999):
        raise Reject(f"CanonicalFormatException: date year {ymd[0]} is outside 0000..9999 and has no yyyy-MM-dd form ({what})")
    return iso_date(ymd).encode("ascii")

# --- CanonicalValue.Str: the bounded normalisation, in the contract's order ------------------------------------
def canonical_str(s, what):
    if s.startswith("﻿"):
        raise Reject(f"CanonicalFormatException: {what} must not carry a BOM")
    if re.search(r"[\ud800-\udfff]", s):              # an unpaired surrogate: not well-formed UTF-16 (m05)
        raise Reject(f"CanonicalFormatException: {what} has an unpaired surrogate")
    return unicodedata.normalize("NFC", s)

# --- CalendarArtifact.CalendarEntry constructor ---------------------------------------------------------------
def build_entry(raw):
    def coerce(k, v):
        return j_string(v, k) if k == "eventCode" else j_primitive_number(v, k, LONG_MIN, LONG_MAX)
    o = j_record(raw, "CalendarEntry", ENTRY_FIELDS, coerce)
    code = o.get("eventCode")
    inst, lead, trail = o.get("instantUtcMs", 0), o.get("leadWindowMs", 0), o.get("trailWindowMs", 0)
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
        (3, TAG_DATE, canonical_date(d_from, "validFromDate")),
        (4, TAG_DATE, canonical_date(d_through, "validThroughDate")),
        (5, TAG_ARRAY, arr)])
    return hashlib.sha256(rec).hexdigest()

# --- LedgerPublisher.validateCalendar: requireWithinWire, Jackson, the CalendarArtifact constructor -------------
def java_load(raw):
    """Returns the loaded artefact (dict) with its content hash, or raises Reject — the contract's verdict."""
    if len(raw) > MAX_CALENDAR_BYTES:
        raise Reject(f"IllegalArgumentException: serialised calendar is {len(raw)} bytes, over MAX_CALENDAR_BYTES {MAX_CALENDAR_BYTES}")
    text = raw.decode("utf-8", errors="replace")            # new String(bytes, UTF_8) substitutes U+FFFD
    def coerce_entries(v):
        if v is None:
            return None
        if not isinstance(v, list) or isinstance(v, JObj):
            raise Reject("MismatchedInputException: entries is not an array")
        out = []
        for e in v:
            out.append(None if e is None else build_entry(e))   # Jackson passes null through; the constructor refuses it
        return out
    def coerce(k, v):
        if k in ("schemaVersion", "hashVersion"): return j_primitive_number(v, k, INT_MIN, INT_MAX)
        if k in ("calendarVersion", "calendarContentHash"): return j_string(v, k)
        if k in ("validFromDate", "validThroughDate"): return j_local_date(v, k)
        return coerce_entries(v)
    top = j_record(j_parse(text), "CalendarArtifact", TOP_FIELDS, coerce)
    schema, hash_version = top.get("schemaVersion", 0), top.get("hashVersion", 0)
    version, declared = top.get("calendarVersion"), top.get("calendarContentHash")
    d_from, d_through = top.get("validFromDate"), top.get("validThroughDate")
    entries = top.get("entries")
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
        raise Reject(f"IllegalArgumentException: validThroughDate {iso_date(d_through)} is before validFromDate {iso_date(d_from)}")
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
    return {"calendarVersion": version, "calendarContentHash": computed, "validFromDate": iso_date(d_from), "validThroughDate": iso_date(d_through), "entries": entries}

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
def no_dup_keys(pairs):
    keys = [k for k, _ in pairs]
    dups = sorted({k for k in keys if keys.count(k) > 1})
    if dups:
        raise ValueError(f"duplicate key(s) {dups} — the serializer never writes a key twice")
    return dict(pairs)
try:
    strict = json.loads(raw.decode("utf-8"), object_pairs_hook=no_dup_keys)   # strict JSON, no trailing bytes, no duplicate keys
    ok("artefact form: strict JSON with no duplicate keys at any depth")
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
    for k in ("validFromDate", "validThroughDate"):
        if isinstance(strict.get(k), str) and not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", strict[k]):
            bad(f"artefact form: {k} is {strict[k]!r}, expected the serializer's plain yyyy-MM-dd (a date-time or padded form loads, but is not the artefact's wire)")
    es = strict.get("entries")
    if isinstance(es, list):
        form = [i for i, e in enumerate(es) if not isinstance(e, dict) or sorted(e) != sorted(ENTRY_FIELDS)
                or not isinstance(e.get("eventCode"), str) or not all(is_json_int(e.get(k)) for k in ENTRY_FIELDS[1:])]
        if form: bad(f"artefact form: entries {form[:5]} do not carry exactly eventCode (string) + instantUtcMs/leadWindowMs/trailWindowMs (integers)")
        else: ok(f"artefact form: every entry carries exactly eventCode + instantUtcMs/leadWindowMs/trailWindowMs as JSON string/integers")
version, declared = loaded["calendarVersion"], loaded["calendarContentHash"]
if version == want_version: ok(f"calendarVersion {version}")
else: bad(f"calendarVersion {version!r}, expected {want_version!r}")
if loaded["validFromDate"] == want_from and loaded["validThroughDate"] == want_through: ok(f"valid {want_from}..{want_through}")
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
