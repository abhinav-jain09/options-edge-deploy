#!/usr/bin/env bash
# Registers the canonical underlying.spx.price value schema (options-edge-contracts schemas/hpsf/UnderlyingPrice.avsc,
# record com.optionsedge.contracts.hpsf.UnderlyingPrice) under its Schema Registry subject — the record's FULL NAME
# (RecordNameStrategy; one subject for every environment) — with FULL compatibility, BEFORE the Avro producer deploys.
# The producer (options-edge-databento-feed) never registers: it reads the latest version of this subject at start.
# Idempotent: an already-registered identical schema is reported, never re-registered; FULL is enforced by read-back.
#
#   SCHEMA_REGISTRY_URL=http://host:8082 SCHEMA_FILE=<contracts>/schemas/hpsf/UnderlyingPrice.avsc \
#       scripts/kafka/ensure-underlying-price-schema.sh
# DEPLOY_DRY_RUN=true reads and reports, mutates nothing.
set -euo pipefail
: "${SCHEMA_REGISTRY_URL:?SCHEMA_REGISTRY_URL is required (the registry of the deployed environment)}"
: "${SCHEMA_FILE:?SCHEMA_FILE is required (options-edge-contracts schemas/hpsf/UnderlyingPrice.avsc at the permitted sha)}"
SUBJECT="${SUBJECT:-com.optionsedge.contracts.hpsf.UnderlyingPrice}"
exec python3 - "$SCHEMA_REGISTRY_URL" "$SCHEMA_FILE" "$SUBJECT" "${DEPLOY_DRY_RUN:-false}" <<'PY'
import json, sys, urllib.error, urllib.request

url, path, subject, dry = sys.argv[1].rstrip("/"), sys.argv[2], sys.argv[3], sys.argv[4] == "true"
schema = json.dumps(json.load(open(path, encoding="utf-8")), separators=(",", ":"))
if json.loads(schema).get("namespace", "") + "." + json.loads(schema)["name"] != subject:
    sys.exit(f"FATAL: {path} defines {json.loads(schema).get('namespace')}.{json.loads(schema)['name']}, not the subject {subject}")
HDR = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


def call(method, route, body=None):
    req = urllib.request.Request(url + route, method=method, headers=HDR,
                                 data=None if body is None else json.dumps(body).encode())
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


status, found = call("POST", f"/subjects/{subject}", {"schema": schema})
if status == 200:
    print(f"registered: {subject} id={found['id']} version={found['version']} (identical schema already present)")
elif status == 404:
    if dry:
        print(f"DRY RUN: {subject} would be registered under FULL")
        sys.exit(0)
    # FULL is set first so even version 1 is held to it; a config on a not-yet-existing subject is legal.
    s, b = call("PUT", f"/config/{subject}", {"compatibility": "FULL"})
    if s != 200:
        sys.exit(f"FATAL: could not set FULL on {subject}: {s} {b}")
    s, b = call("POST", f"/subjects/{subject}/versions", {"schema": schema})
    if s != 200:
        sys.exit(f"FATAL: registration refused (compatibility or content): {s} {b}")
    status, found = call("POST", f"/subjects/{subject}", {"schema": schema})
    if status != 200:
        sys.exit(f"FATAL: registered schema is not found by lookup: {status} {found}")
    print(f"registered: {subject} id={found['id']} version={found['version']}")
else:
    sys.exit(f"FATAL: registry lookup failed: {status} {found}")
if not dry:
    s, b = call("PUT", f"/config/{subject}", {"compatibility": "FULL"})
    if s != 200:
        sys.exit(f"FATAL: could not set FULL on {subject}: {s} {b}")
s, b = call("GET", f"/config/{subject}")
if s != 200 or b.get("compatibilityLevel") != "FULL":
    sys.exit(f"FATAL: {subject} compatibility is not FULL: {s} {b}")
print(f"compatibility: {subject} FULL")
PY
