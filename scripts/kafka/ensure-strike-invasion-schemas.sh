#!/usr/bin/env bash
# Registers the strike-invasion family's canonical Avro schemas (options-edge-contracts schemas/strikeinvasion/*.avsc) on ONE
# Schema Registry, each under its subject = the record's FULL NAME (RecordNameStrategy), compatibility FULL, BEFORE any
# producer deploys (Kafka Serialization Rule, cutover order step 4a). The producer never registers (auto.register=false).
# Idempotent: an identical schema already present is reported, never re-registered; FULL is enforced by read-back.
#
#   SCHEMA_REGISTRY_URL=http://host:8082 SCHEMAS_DIR=<contracts>/schemas/strikeinvasion scripts/kafka/ensure-strike-invasion-schemas.sh
# DEPLOY_DRY_RUN=true reads and reports, mutates nothing. The directory must hold EXACTLY the 23 files below (a missing or
# an extra file is a refusal: the set is the contract), each declaring the record its file name says.
set -euo pipefail
: "${SCHEMA_REGISTRY_URL:?SCHEMA_REGISTRY_URL is required (the registry of the deployed environment)}"
: "${SCHEMAS_DIR:?SCHEMAS_DIR is required (options-edge-contracts schemas/strikeinvasion at the permitted sha)}"
exec python3 - "$SCHEMA_REGISTRY_URL" "$SCHEMAS_DIR" "${DEPLOY_DRY_RUN:-false}" <<'PY'
import json, os, sys, urllib.error, urllib.request

NS = "com.optionsedge.contracts.strikeinvasion."
NAMES = ["StrikeInvasionSnapshot"] + ["StrikeInvasionEvent" + t for t in (
    "NonFiniteSpotDropped", "OutOfOrderSpotDropped", "NonFiniteStrikeDropped", "SessionReset", "CandidateInvalidated",
    "Stage1Retired", "State", "Stage1", "Invasion", "Stage2Timeout", "DefenseBroken", "Stage2Verdict")] + [
    "StrikeInvasion" + t for t in ("SpotEvidence", "SrEvidence", "GexEvidence", "FlowEvidence", "BehaviorEvidence",
                                   "PaceEvidence", "PaceFrame", "WallFrame", "WallRow", "WallMeta")]
url, d, dry = sys.argv[1].rstrip("/"), sys.argv[2], sys.argv[3] == "true"
have = sorted(f for f in os.listdir(d) if f.endswith(".avsc"))
if have != sorted(n + ".avsc" for n in NAMES):
    sys.exit(f"FATAL: {d} must hold exactly the {len(NAMES)} strike-invasion schemas; missing={sorted(set(n + '.avsc' for n in NAMES) - set(have))} extra={sorted(set(have) - set(n + '.avsc' for n in NAMES))}")
schemas = {}
for n in NAMES:
    j = json.load(open(os.path.join(d, n + ".avsc")))
    if j.get("namespace", "") + "." + j.get("name", "") != NS + n:
        sys.exit(f"FATAL: {n}.avsc declares {j.get('namespace')}.{j.get('name')}, not {NS + n}")
    schemas[NS + n] = json.dumps(j, separators=(",", ":"))
HDR = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


def call(method, route, body=None):
    req = urllib.request.Request(url + route, method=method, headers=HDR, data=None if body is None else json.dumps(body).encode())
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


for subject, schema in schemas.items():
    status, found = call("POST", f"/subjects/{subject}", {"schema": schema})
    if status == 200:
        print(f"registered: {subject} id={found['id']} version={found['version']} (identical schema already present)")
    elif status in (404,):
        if dry:
            print(f"DRY RUN: {subject} would be registered under FULL")
            continue
        s, b = call("PUT", f"/config/{subject}", {"compatibility": "FULL"})   # FULL first, so even version 1 is held to it
        if s != 200:
            sys.exit(f"FATAL: could not set FULL on {subject}: {s} {b}")
        s, b = call("POST", f"/subjects/{subject}/versions", {"schema": schema})
        if s != 200:
            sys.exit(f"FATAL: registration refused (compatibility or content): {subject}: {s} {b}")
        status, found = call("POST", f"/subjects/{subject}", {"schema": schema})
        if status != 200:
            sys.exit(f"FATAL: registered schema is not found by lookup: {subject}: {status} {found}")
        print(f"registered: {subject} id={found['id']} version={found['version']}")
    else:
        sys.exit(f"FATAL: registry lookup failed for {subject}: {status} {found}")
    if not dry:
        s, b = call("PUT", f"/config/{subject}", {"compatibility": "FULL"})
        if s != 200:
            sys.exit(f"FATAL: could not set FULL on {subject}: {s} {b}")
    s, b = call("GET", f"/config/{subject}")
    if s != 200 or b.get("compatibilityLevel") != "FULL":
        sys.exit(f"FATAL: {subject} compatibility is not FULL: {s} {b}")
    s, latest = call("GET", f"/subjects/{subject}/versions/latest")
    if s != 200 or json.loads(latest["schema"]) != json.loads(schema):
        sys.exit(f"FATAL: the latest version of {subject} is not the canonical schema: {s} {latest}")
print(f"OK: {len(schemas)} subjects canonical, FULL" + (" (dry run)" if dry else ""))
PY
