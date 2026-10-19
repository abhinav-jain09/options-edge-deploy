#!/usr/bin/env bash
# Registers the seventeen canonical delta-flow-service value schemas (options-edge-contracts schemas/deltaflow/*.avsc) under their Schema
# Registry subjects - each record's FULL NAME (RecordNameStrategy; one subject for every environment and every TOPIC_PREFIX) - with
# FULL compatibility, BEFORE the Avro producer deploys: six public output records (DeltaFlowEvent, DeltaFlowSnapshot,
# DeltaFlowStrikeSnapshot, DeltaFlowCorrectionEvent, DeltaFlowPositioningSnapshot, DeltaFlowCalibrationRecord) and the eleven private
# images of the service's two repartition topics and eight state-store changelogs / global stores (DeltaFlowTradeInput,
# DeltaFlowQuoteState, DeltaFlowIntradayState, DeltaFlowRollingState, DeltaFlowStrikeStoreValue, DeltaFlowReversibleContribution,
# DeltaFlowAcceptedWatermark, DeltaFlowIndexMarker, DeltaFlowGreekStoreValue, DeltaFlowOiState, DeltaFlowPositioningState).
# The service never registers (auto.register.schemas=false, use.latest.version=false): it frames the id the registry holds for EXACTLY the
# canonical schema, so a subject that is missing, not at the canonical bytes, or not FULL stops the Streams thread at its first write.
# Idempotent: an already-registered identical schema is reported, never re-registered; FULL is enforced by read-back; the canonical schema
# must be the LATEST version of each subject.
#
#   SCHEMA_REGISTRY_URL=http://host:8082 SCHEMAS_DIR=<contracts>/schemas/deltaflow scripts/kafka/ensure-delta-flow-schemas.sh
# DEPLOY_DRY_RUN=true reads and reports, mutates nothing. Phase 1 validates every subject before any write; the writes are ordered
# so a mid-run failure leaves only idempotent canonical state (not atomic across subjects; a re-run converges).
set -euo pipefail
: "${SCHEMA_REGISTRY_URL:?SCHEMA_REGISTRY_URL is required (the registry of the deployed environment)}"
: "${SCHEMAS_DIR:?SCHEMAS_DIR is required (options-edge-contracts schemas/deltaflow at the permitted sha)}"
exec python3 - "$SCHEMA_REGISTRY_URL" "$SCHEMAS_DIR" "${DEPLOY_DRY_RUN:-false}" <<'PY'
import hashlib, json, os, sys, urllib.error, urllib.request

# The exact bytes of each canonical file this deploy registers. A registration under FULL is irreversible, so no other
# file is ever sent: when a canonical schema changes, its hash changes in the same deploy change
# (tests/fixtures/deltaflow/*.avsc are the pinned copies, hash-checked by the test).
CANONICAL = {
    "DeltaFlowAcceptedWatermark": ("com.optionsedge.contracts.deltaflow.DeltaFlowAcceptedWatermark", "5c25802ff72baeccc75b6e739a857e97538541cc90b46f9a08c57dd77a0e6749"),
    "DeltaFlowCalibrationRecord": ("com.optionsedge.contracts.deltaflow.DeltaFlowCalibrationRecord", "9b6cd7d2c799e97970186b756595ef55010fe78295de78adce3588f8186748ca"),
    "DeltaFlowCorrectionEvent": ("com.optionsedge.contracts.deltaflow.DeltaFlowCorrectionEvent", "e36cb1446b3b3a062ed12aa7ce3863502b0b42c5f2376fe376d07813d494d1ff"),
    "DeltaFlowEvent": ("com.optionsedge.contracts.deltaflow.DeltaFlowEvent", "69ea6d69e3caff09ab352bc444ee7f221605164b95e42d8f3a61ae04466bf4f6"),
    "DeltaFlowGreekStoreValue": ("com.optionsedge.contracts.deltaflow.DeltaFlowGreekStoreValue", "95d4f24e71459b9b069cbe99d5bbd0d9035cf8f63b57ec7e7d0f51373ff18798"),
    "DeltaFlowIndexMarker": ("com.optionsedge.contracts.deltaflow.DeltaFlowIndexMarker", "de5ec0a4fd6956c0d77db82ddcd567a237d94b173264d9d9df7fddf96075706c"),
    "DeltaFlowIntradayState": ("com.optionsedge.contracts.deltaflow.DeltaFlowIntradayState", "7943da6da387ad03dc91195f4cb23fd65c76353d58c381701c04e3e57bef661e"),
    "DeltaFlowOiState": ("com.optionsedge.contracts.deltaflow.DeltaFlowOiState", "99c6bb787b3c920785804f9323c7047009a33ffa01453c953d5cf8c457a21a2a"),
    "DeltaFlowPositioningSnapshot": ("com.optionsedge.contracts.deltaflow.DeltaFlowPositioningSnapshot", "a90b512b5332744847d029d62e2ee5115f8f78d4f82c3fdea035d0ce89e11331"),
    "DeltaFlowPositioningState": ("com.optionsedge.contracts.deltaflow.DeltaFlowPositioningState", "cc53cedac9cf058652045c76941bfe2fb312c73137ba0cc6196dfa273b09eb35"),
    "DeltaFlowQuoteState": ("com.optionsedge.contracts.deltaflow.DeltaFlowQuoteState", "74d312690243c77dd8593306e8d8ced69125014f47172e0012d3150c78caf863"),
    "DeltaFlowReversibleContribution": ("com.optionsedge.contracts.deltaflow.DeltaFlowReversibleContribution", "ea5eb92c1d6ad1bfaa2a367a2ffeba5ac254c8fda8619850ba2008e4d9e3ace7"),
    "DeltaFlowRollingState": ("com.optionsedge.contracts.deltaflow.DeltaFlowRollingState", "82b1410f2c37d00f3f6ef45e980e4fd31f03b68b3b17ecdf68ce316ba6a738da"),
    "DeltaFlowSnapshot": ("com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot", "a058b5be5452108cf884cf05141d4a5bf4cfd1ddb1dbc5c27ca281339f19481d"),
    "DeltaFlowStrikeSnapshot": ("com.optionsedge.contracts.deltaflow.DeltaFlowStrikeSnapshot", "c1bbed4db8e9ef00c50e86855f72700a16b4b3aedcef1ffc8e98092e5bf449f1"),
    "DeltaFlowStrikeStoreValue": ("com.optionsedge.contracts.deltaflow.DeltaFlowStrikeStoreValue", "03299570558a7ebcd9b5326528189f4300aed534b0ffc21047378b2fb613b6c5"),
    "DeltaFlowTradeInput": ("com.optionsedge.contracts.deltaflow.DeltaFlowTradeInput", "b296c6657765f3d926f85513b4a5f5165c71bce9d152bbc27246c29e368e644e"),
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
