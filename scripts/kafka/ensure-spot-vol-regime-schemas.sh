#!/usr/bin/env bash
# Registers the three canonical spot-vol-regime-service value schemas (options-edge-contracts schemas/spotvolregime/
# SpotVolRegimeSnapshot.avsc, SpotVolRegimeWindowState.avsc, SpotVolRegimeLastFrameState.avsc) under their Schema Registry subjects — each
# record's FULL NAME (RecordNameStrategy; one subject for every environment) — with FULL compatibility, BEFORE the Avro
# producer deploys. The service never registers (auto.register.schemas=false, use.latest.version=false): it frames the id the
# registry holds for EXACTLY the canonical schema. Idempotent: an already-registered identical schema is reported, never
# re-registered; FULL is enforced by read-back; the canonical schema must be the LATEST version of each subject.
#
#   SCHEMA_REGISTRY_URL=http://host:8082 SCHEMAS_DIR=<contracts>/schemas/spotvolregime scripts/kafka/ensure-spot-vol-regime-schemas.sh
# DEPLOY_DRY_RUN=true reads and reports, mutates nothing. Phase 1 validates every subject before any write; the writes are ordered
# so a mid-run failure leaves only idempotent canonical state (not atomic across subjects; a re-run converges).
set -euo pipefail
: "${SCHEMA_REGISTRY_URL:?SCHEMA_REGISTRY_URL is required (the registry of the deployed environment)}"
: "${SCHEMAS_DIR:?SCHEMAS_DIR is required (options-edge-contracts schemas/spotvolregime at the permitted sha)}"
exec python3 - "$SCHEMA_REGISTRY_URL" "$SCHEMAS_DIR" "${DEPLOY_DRY_RUN:-false}" <<'PY'
import hashlib, json, os, sys, urllib.error, urllib.request

# The exact bytes of each canonical file this deploy registers. A registration under FULL is irreversible, so no other
# file is ever sent: when a canonical schema changes, its hash changes in the same deploy change
# (tests/fixtures/spotvolregime/*.avsc are the pinned copies, hash-checked by the test).
CANONICAL = {
    "SpotVolRegimeSnapshot": ("com.optionsedge.contracts.spotvolregime.SpotVolRegimeSnapshot", "af166c74cc98c1619f67925f321be93c73450d0a173cf817acdb279e04de932b"),
    "SpotVolRegimeWindowState": ("com.optionsedge.contracts.spotvolregime.SpotVolRegimeWindowState", "09ecef1d5196fe5c7b9e0f698b9ca2855ba9cac767de51bdf9a2341303937821"),
    "SpotVolRegimeLastFrameState": ("com.optionsedge.contracts.spotvolregime.SpotVolRegimeLastFrameState", "717472443a0292d76b54f0f5ad3c4314a1d95d50eea0c860d2446956e9a16e07"),
}
url, directory, dry = sys.argv[1].rstrip("/"), sys.argv[2], sys.argv[3] == "true"
HDR = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

loaded = {}
for name, (subject, sha) in CANONICAL.items():
    path = os.path.join(directory, name + ".avsc")
    raw = open(path, "rb").read()
    if hashlib.sha256(raw).hexdigest() != sha:
        sys.exit(f"FATAL: {path} is not the canonical {name}.avsc this deploy pins (sha256 {hashlib.sha256(raw).hexdigest()})")
    loaded[subject] = json.dumps(json.loads(raw), separators=(",", ":"))


def call(method, route, body=None):
    req = urllib.request.Request(url + route, method=method, headers=HDR,
                                 data=None if body is None else json.dumps(body).encode())
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


# PHASE 1 - READ AND VALIDATE EVERYTHING, MUTATE NOTHING. For every subject: the version list, byte equality of the latest version
# with the canonical schema (a present subject whose latest is anything else is refused), the subject config, and the subject's
# effective mode (a READONLY or IMPORT subject cannot take a registration). Every condition detectable without writing is
# refused here, so such a run leaves the registry exactly as it found it. What a read cannot prove (registry outage or a write
# refusal mid-run) is handled by the ordering of PHASE 2.
plan = {}
for subject, schema in loaded.items():
    s, latest = call("GET", f"/subjects/{subject}/versions/latest")
    if s == 200:
        if json.loads(latest["schema"]) != json.loads(schema):
            sys.exit(f"FATAL: the latest version of {subject} is not the canonical schema (nothing was changed): {latest.get('version')} id={latest.get('id')}")
        plan[subject] = "present"
        s, b = call("GET", f"/config/{subject}")
        if s not in (200, 404):
            sys.exit(f"FATAL: cannot read the config of {subject} (nothing was changed): {s} {b}")
    elif s == 404:
        plan[subject] = "absent"
    else:
        sys.exit(f"FATAL: registry read failed for {subject} (nothing was changed): {s} {latest}")
    s, m = call("GET", f"/mode/{subject}?defaultToGlobal=true")
    if s != 200 or m.get("mode") != "READWRITE":
        sys.exit(f"FATAL: {subject} is not writable (mode read: {s} {m}); nothing was changed")

if dry:
    for subject in loaded:
        print(f"DRY RUN: {subject} is {plan[subject]}" + (" and would be registered under FULL" if plan[subject] == "absent" else "; FULL would be enforced"))
    sys.exit(0)

# PHASE 2 - mutate in an order whose every prefix is safe. (a) Register every absent subject: version 1 has nothing to be
# incompatible with, so registering before FULL is set holds it to nothing less. (b) Only then set FULL on every subject.
# A failure part-way (registry outage, a write refused) leaves only canonical v1 registrations and/or FULL configs: each
# idempotent, never a wrong schema, and a re-run completes the rest. It is NOT atomic across subjects, and does not claim to be.
for subject, schema in loaded.items():
    if plan[subject] == "absent":
        s, b = call("POST", f"/subjects/{subject}/versions", {"schema": schema})
        if s != 200:
            sys.exit(f"FATAL: registration refused (compatibility or content): {s} {b}; subjects registered earlier in this run stay registered (idempotent) - re-run")
for subject in loaded:
    s, b = call("PUT", f"/config/{subject}", {"compatibility": "FULL"})
    if s != 200:
        sys.exit(f"FATAL: could not set FULL on {subject}: {s} {b}; earlier subjects keep their canonical schema and FULL (idempotent) - re-run")

# PHASE 3 - verify by read-back.
for subject, schema in loaded.items():
    status, found = call("POST", f"/subjects/{subject}", {"schema": schema})
    if status != 200:
        sys.exit(f"FATAL: canonical schema is not found by lookup on {subject}: {status} {found}")
    print(f"registered: {subject} id={found['id']} version={found['version']}")
    s, b = call("GET", f"/config/{subject}")
    if s != 200 or b.get("compatibilityLevel") != "FULL":
        sys.exit(f"FATAL: {subject} compatibility is not FULL: {s} {b}")
    print(f"compatibility: {subject} FULL")
    s, latest = call("GET", f"/subjects/{subject}/versions/latest")
    if s != 200 or json.loads(latest["schema"]) != json.loads(schema):
        sys.exit(f"FATAL: the latest version of {subject} is not the canonical schema: {s} {latest}")
    print(f"latest: {subject} version={latest['version']} id={latest['id']} is the canonical schema")
PY
