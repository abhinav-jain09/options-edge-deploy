#!/usr/bin/env bash
# Registers the three canonical spot-vol-regime-service value schemas (options-edge-contracts schemas/spotvolregime/
# SpotVolRegimeSnapshot.avsc, SpotVolRegimeWindowState.avsc, SpotVolRegimeLastFrameState.avsc) under their Schema Registry subjects — each
# record's FULL NAME (RecordNameStrategy; one subject for every environment) — with FULL compatibility, BEFORE the Avro
# producer deploys. The service never registers (auto.register.schemas=false, use.latest.version=false): it frames the id the
# registry holds for EXACTLY the canonical schema. Idempotent: an already-registered identical schema is reported, never
# re-registered; FULL is enforced by read-back; the canonical schema must be the LATEST version of each subject.
#
#   SCHEMA_REGISTRY_URL=http://host:8082 SCHEMAS_DIR=<contracts>/schemas/spotvolregime scripts/kafka/ensure-spot-vol-regime-schemas.sh
# DEPLOY_DRY_RUN=true reads and reports, mutates nothing.
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
    "SpotVolRegimeWindowState": ("com.optionsedge.contracts.spotvolregime.SpotVolRegimeWindowState", "650b37eb36dcb5283957a296083a6d665e3b8ccbe141eb4a85988167ec7007ac"),
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


# PHASE 1 - READ EVERYTHING, MUTATE NOTHING. Every refusal (a non-canonical latest version, an unreadable subject, a
# canonical schema that is not the latest) happens here, so a refused run leaves the registry exactly as it found it.
plan = {}
for subject, schema in loaded.items():
    s, latest = call("GET", f"/subjects/{subject}/versions/latest")
    if s == 200:
        if json.loads(latest["schema"]) != json.loads(schema):
            sys.exit(f"FATAL: the latest version of {subject} is not the canonical schema (nothing was changed): {latest.get('version')} id={latest.get('id')}")
        plan[subject] = "present"
    elif s == 404:
        plan[subject] = "absent"
    else:
        sys.exit(f"FATAL: registry read failed for {subject} (nothing was changed): {s} {latest}")
    if plan[subject] == "present":
        s, b = call("GET", f"/config/{subject}")
        if s not in (200, 404):
            sys.exit(f"FATAL: cannot read the config of {subject} (nothing was changed): {s} {b}")

# PHASE 2 - mutate, only after every subject passed phase 1.
for subject, schema in loaded.items():
    if plan[subject] == "absent":
        if dry:
            print(f"DRY RUN: {subject} would be registered under FULL")
            continue
        # FULL is set first so even version 1 is held to it; a config on a not-yet-existing subject is legal.
        s, b = call("PUT", f"/config/{subject}", {"compatibility": "FULL"})
        if s != 200:
            sys.exit(f"FATAL: could not set FULL on {subject}: {s} {b}")
        s, b = call("POST", f"/subjects/{subject}/versions", {"schema": schema})
        if s != 200:
            sys.exit(f"FATAL: registration refused (compatibility or content): {s} {b}")
    elif not dry:
        s, b = call("PUT", f"/config/{subject}", {"compatibility": "FULL"})
        if s != 200:
            sys.exit(f"FATAL: could not set FULL on {subject}: {s} {b}")
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
