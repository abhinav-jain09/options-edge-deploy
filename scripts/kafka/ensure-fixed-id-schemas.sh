#!/usr/bin/env bash
# ensure-fixed-id-schemas.sh — a declared set of Avro schemas exists on ONE Schema Registry under FIXED ids, subject
# compatibility FULL, idempotently. The generic form of ensure-indicator-schemas.sh (same procedure, same registry calls, same
# mode capture/restore), driven by a map file so each TA-books service family (market-structure W0/B/C, es-trade-tools, wave-cycle)
# registers its own subjects without a copy of the script per family.
#
# WHY FIXED IDS: a Confluent-framed Avro value names its schema by REGISTRY ID, and registries assign ids independently, so any
# record that is mirrored or replayed across registries would resolve to a foreign schema. Subject-level IMPORT mode lets a schema
# be registered under the id of our choosing; the id is then identical everywhere (INDICATOR-AVRO-CONVERSION-PLAN.md section 4; the
# W0 runbook, docs/ta-books/W0-MARKET-STRUCTURE-AVRO-CUTOVER.md section 2, states the same rule for the market-structure subjects).
#
# Inputs (environment):
#   SCHEMA_REGISTRY_URL   the registry to reconcile (plain HTTP on the LAN; no credentials are configured)
#   SCHEMA_ID_MAP         the map file: one `subject|fixed id|file` per line (file relative to SCHEMAS_DIR); `#` lines and blanks ignored.
#                         The maps live in scripts/kafka/schema-id-maps/.
#   SCHEMAS_DIR           the contracts checkout's schemas directory the directory the map file column is relative to
#   DEPLOY_DRY_RUN        true = report only, never mutate
# Exit 0 only when every subject is registered under its fixed id at version 1 with compatibility FULL. Everything that exists and is
# not exactly that is a COLLISION and fails: a different schema under the subject, a later version, or the fixed id held by another
# schema. This script never repairs a collision and never deletes a subject.
#
# A read-only PREFLIGHT pass judges every map line (collision = fail) BEFORE the apply pass imports anything: a collision on a later subject never leaves
# the earlier ones imported. Per subject in the apply pass: (mode capture, disposable-subject IMPORT probe), idempotent verify, import, restore the mode in a trap on EVERY
# exit (an original override by PUT, an inherited mode by DELETE), then FULL read back. See ensure-indicator-schemas.sh for the long form.
set -euo pipefail
: "${SCHEMA_REGISTRY_URL:?SCHEMA_REGISTRY_URL unset — refusing to reconcile an unknown registry}"
: "${SCHEMAS_DIR:?SCHEMAS_DIR unset — the directory the map file column is relative to (the contracts checkout schemas-registry/ta)}"
: "${SCHEMA_ID_MAP:?SCHEMA_ID_MAP unset — the subject|id|file map file}"
[ -f "$SCHEMA_ID_MAP" ] || { echo "FAIL: SCHEMA_ID_MAP $SCHEMA_ID_MAP is not a file"; exit 1; }
DRY="${DEPLOY_DRY_RUN:-false}"
REG="${SCHEMA_REGISTRY_URL%/}"
CT='Content-Type: application/vnd.schemaregistry.v1+json'
PROBE_SUBJECT="${SCHEMA_PROBE_SUBJECT:-zz.ta.importmode.probe}"

# subject|fixed id|file — read from the map; RecordNameStrategy subjects. A map that is empty, has a malformed line, repeats a subject
# or repeats an id is refused before the registry is touched.
FIXED="$(grep -v '^[[:space:]]*#' "$SCHEMA_ID_MAP" | grep -v '^[[:space:]]*$' || true)"
[ -n "$FIXED" ] || { echo "FAIL: $SCHEMA_ID_MAP declares no subject"; exit 1; }
if ! awk -F'|' 'NF != 3 || $1 == "" || $2 !~ /^[0-9]+$/ || $3 == "" || seen_s[$1]++ || seen_i[$2]++ { bad = 1 } END { exit bad }' <<<"$FIXED"; then
  echo "FAIL: $SCHEMA_ID_MAP has a malformed line, a repeated subject or a repeated id"; exit 1
fi
COMPAT=FULL

# http <method> <path> [json body]  -> prints "<status> <body>"
http() {
  local method="$1" path="$2" body="${3:-}" out
  if [ -n "$body" ]; then
    out=$(curl -sS -m 20 -X "$method" -H "$CT" --data-binary "$body" -w $'\n%{http_code}' "$REG$path")
  else
    out=$(curl -sS -m 20 -X "$method" -H "$CT" -w $'\n%{http_code}' "$REG$path")
  fi
  printf '%s %s' "${out##*$'\n'}" "${out%$'\n'*}"
}
status() { printf '%s' "$1" | cut -d' ' -f1; }
body()   { printf '%s' "$1" | cut -d' ' -f2-; }
json_field() { python3 -c 'import json,sys; d=json.load(sys.stdin); v=d.get(sys.argv[1]); print("" if v is None else v)' "$2" <<<"$1"; }
# the registration body: {"schema": "<escaped schema text>"[, "id": N, "version": 1]}
reg_body() { python3 -c 'import json,sys; b={"schema": open(sys.argv[1]).read()}
if len(sys.argv) > 2: b["id"] = int(sys.argv[2]); b["version"] = 1
print(json.dumps(b))' "$@"; }

echo "=== ensure-fixed-id-schemas: $REG (dry_run=$DRY) map=$SCHEMA_ID_MAP schemas=$SCHEMAS_DIR ==="
GLOBAL=$(http GET /mode); [ "$(status "$GLOBAL")" = 200 ] || { echo "FAIL: GET /mode -> $GLOBAL"; exit 1; }
GLOBAL_MODE=$(json_field "$(body "$GLOBAL")" mode)
echo "global mode: $GLOBAL_MODE"

# ---- the mutability probe: subject-level IMPORT must be allowed (mode.mutability=true), proven on a
# disposable absent subject that is removed again, before any real subject is touched.
PR=$(http GET "/mode/$PROBE_SUBJECT")
[ "$(status "$PR")" = 404 ] || { echo "FAIL: probe subject $PROBE_SUBJECT already has a mode ($PR) — refusing to probe on a real subject"; exit 1; }
if [ "$DRY" = true ]; then
  echo "probe: DEPLOY_DRY_RUN=true — not setting IMPORT on $PROBE_SUBJECT"
else
  P1=$(http PUT "/mode/$PROBE_SUBJECT" '{"mode":"IMPORT"}')
  [ "$(status "$P1")" = 200 ] && [ "$(json_field "$(body "$P1")" mode)" = IMPORT ] \
    || { echo "FAIL: subject-level IMPORT is not allowed on this registry (mode.mutability=false?): PUT /mode/$PROBE_SUBJECT -> $P1"; exit 1; }
  P2=$(http DELETE "/mode/$PROBE_SUBJECT")
  [ "$(status "$P2")" = 200 ] || { echo "FAIL: could not remove the probe mode: DELETE /mode/$PROBE_SUBJECT -> $P2"; exit 1; }
  P3=$(http GET "/mode/$PROBE_SUBJECT")
  [ "$(status "$P3")" = 404 ] || { echo "FAIL: probe mode still present after DELETE: $P3"; exit 1; }
  echo "probe: subject-level IMPORT allowed (set and removed on $PROBE_SUBJECT)"
fi

# ---- mode capture/restore (one subject at a time; the trap restores whatever is pending)
CUR_SUBJECT=""; CUR_HAD_OVERRIDE=""; CUR_MODE=""
restore_mode() {
  [ -n "$CUR_SUBJECT" ] || return 0
  local s="$CUR_SUBJECT" r rr
  if [ "$CUR_HAD_OVERRIDE" = true ]; then
    r=$(http PUT "/mode/$s" "{\"mode\":\"$CUR_MODE\"}")
    [ "$(status "$r")" = 200 ] || { echo "FAIL: could not restore the override mode $CUR_MODE on $s: $r"; return 1; }
  else
    r=$(http DELETE "/mode/$s")
    case "$(status "$r")" in 200|404) ;; *) echo "FAIL: could not remove the import mode on $s: $r"; return 1 ;; esac
    rr=$(http GET "/mode/$s")
    [ "$(status "$rr")" = 404 ] || { echo "FAIL: $s still has a subject-level mode after restore: $rr"; return 1; }
  fi
  rr=$(http GET "/mode/$s?defaultToGlobal=true")
  [ "$(json_field "$(body "$rr")" mode)" = "$CUR_MODE" ] || { echo "FAIL: effective mode of $s is not the captured $CUR_MODE: $rr"; return 1; }
  CUR_SUBJECT=""
}
on_exit() { local rc=$?; restore_mode || rc=1; exit $rc; }
trap on_exit EXIT

ensure_full() { # <subject>
  local s="$1" c cm
  c=$(http GET "/config/$s"); cm=$(json_field "$(body "$c")" compatibilityLevel)
  if [ "$(status "$c")" = 200 ] && [ "$cm" = "$COMPAT" ]; then echo "    compatibility $COMPAT"; return 0; fi
  if [ "$DRY" = true ]; then echo "    compatibility is '${cm:-inherited}' — DEPLOY_DRY_RUN=true, not setting $COMPAT"; return 0; fi
  c=$(http PUT "/config/$s" "{\"compatibility\":\"$COMPAT\"}")
  [ "$(status "$c")" = 200 ] || { echo "FAIL: PUT /config/$s -> $c"; return 1; }
  c=$(http GET "/config/$s")
  [ "$(json_field "$(body "$c")" compatibilityLevel)" = "$COMPAT" ] || { echo "FAIL: compatibility of $s did not read back $COMPAT: $c"; return 1; }
  echo "    compatibility set to $COMPAT and read back"
}

fail=0
for PASS in preflight apply; do
while IFS='|' read -r subject id file; do
  [ -n "${subject:-}" ] || continue
  schema="$SCHEMAS_DIR/$file"
  [ -f "$schema" ] || { echo "FAIL: $schema missing"; fail=1; continue; }
  echo "--- [$PASS] $subject (id $id)"
  # the canonical text as the registry would normalize it: ask the registry whether THIS schema is already
  # registered under the subject (POST /subjects/<subject> returns the id/version it lives under, 404 if not)
  LOOKUP=$(http POST "/subjects/$subject" "$(reg_body "$schema")")
  case "$(status "$LOOKUP")" in
    200)
      have_id=$(json_field "$(body "$LOOKUP")" id); have_v=$(json_field "$(body "$LOOKUP")" version)
      if [ "$have_id" = "$id" ] && [ "$have_v" = 1 ]; then
        # version 1 is canonical under the fixed id; the subject must hold NOTHING else — producers run with
        # use.latest.version=true, so a later version would be what they resolve and frame under its own id
        V1=$(http GET "/subjects/$subject/versions")
        if [ "$(status "$V1")" != 200 ] || [ "$(body "$V1" | tr -d ' ')" != "[1]" ]; then
          echo "FAIL: $subject holds versions $(body "$V1") — only version 1 (the fixed id $id) is admissible; a later version would be the one producers resolve"
          fail=1; continue
        fi
        echo "    registered: version 1, id $id (the only version)"
        [ "$PASS" = apply ] && { ensure_full "$subject" || fail=1; }
        continue
      fi
      echo "FAIL: $subject holds the canonical schema under id $have_id version $have_v, not id $id version 1 — a collision this script never repairs"
      fail=1; continue ;;
    404) ;;
    *) echo "FAIL: lookup POST /subjects/$subject -> $LOOKUP"; fail=1; continue ;;
  esac
  # not registered under the subject: the subject must be absent (any other content is a collision) and
  # the fixed id must be free
  V=$(http GET "/subjects/$subject/versions")
  if [ "$(status "$V")" = 200 ]; then
    echo "FAIL: $subject exists with versions $(body "$V") but not the canonical schema — a collision this script never repairs"
    fail=1; continue
  fi
  I=$(http GET "/schemas/ids/$id")
  if [ "$(status "$I")" = 200 ]; then
    echo "FAIL: fixed id $id is already held by another schema on $REG — a collision this script never repairs"
    fail=1; continue
  fi
  if [ "$PASS" = preflight ]; then echo "    free: would import under id $id"; continue; fi
  if [ "$DRY" = true ]; then echo "    would import under id $id (DEPLOY_DRY_RUN=true)"; continue; fi
  # capture the mode, then import
  M=$(http GET "/mode/$subject")
  case "$(status "$M")" in
    200) CUR_HAD_OVERRIDE=true; CUR_MODE=$(json_field "$(body "$M")" mode) ;;
    404) CUR_HAD_OVERRIDE=false; CUR_MODE="$GLOBAL_MODE" ;;   # the registry's own "no subject mode" (40409): inherited
    *) echo "FAIL: cannot capture the mode of $subject (GET /mode/$subject -> $M) — a transient failure here would be restored as an inherited mode and delete a real override"; fail=1; continue ;;
  esac
  CUR_SUBJECT="$subject"
  R=$(http PUT "/mode/$subject" '{"mode":"IMPORT"}')
  [ "$(status "$R")" = 200 ] || { echo "FAIL: PUT /mode/$subject IMPORT -> $R"; fail=1; restore_mode || true; continue; }
  R=$(http POST "/subjects/$subject/versions" "$(reg_body "$schema" "$id")")
  [ "$(status "$R")" = 200 ] && [ "$(json_field "$(body "$R")" id)" = "$id" ] \
    || { echo "FAIL: import of $subject under id $id -> $R"; fail=1; restore_mode || true; continue; }
  R=$(http GET "/subjects/$subject/versions/1")
  [ "$(status "$R")" = 200 ] && [ "$(json_field "$(body "$R")" id)" = "$id" ] \
    || { echo "FAIL: $subject version 1 does not carry id $id: $R"; fail=1; restore_mode || true; continue; }
  restore_mode || { fail=1; continue; }
  echo "    imported: version 1, id $id"
  ensure_full "$subject" || fail=1
done <<< "$FIXED"
  # PREFLIGHT (read-only, every map line judged before any import): a collision on a LATER subject must not leave the earlier ones imported
  if [ "$PASS" = preflight ] && [ "$fail" -ne 0 ]; then echo "=== ensure-fixed-id-schemas: FAILED in preflight — nothing was changed ==="; exit 1; fi
done

if [ "$fail" -ne 0 ]; then echo "=== ensure-fixed-id-schemas: FAILED ==="; exit 1; fi
echo "=== ensure-fixed-id-schemas: OK ==="
