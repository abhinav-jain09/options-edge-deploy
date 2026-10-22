#!/usr/bin/env bash
# ensure-fixed-id-schemas.sh — a declared set of Avro schemas exists on ONE Schema Registry under FIXED ids, subject
# compatibility FULL, idempotently. The generic form of ensure-indicator-schemas.sh (same procedure, same registry calls, same
# mode capture/restore), driven by a map file so each TA-books service family (market-structure W0/B/C, es-trade-tools, wave-cycle)
# registers its own subjects without a copy of the script per family.
#
# WHY FIXED IDS: a Confluent-framed Avro value names its schema by REGISTRY ID, and registries assign ids independently, so any
# record that is mirrored or replayed across registries would resolve to a foreign schema. Subject-level IMPORT mode lets a schema
# be registered under the id of our choosing; the id is then identical everywhere (INDICATOR-AVRO-CONVERSION-PLAN.md section 4; the
# W0 runbook, docs/ta-books/MARKET-STRUCTURE-AVRO-CUTOVER.md section 2, states the same rule for the market-structure subjects).
#
# Inputs (environment):
#   SCHEMA_REGISTRY_URL   the registry to reconcile (plain HTTP on the LAN; no credentials are configured)
#   SCHEMA_ID_MAP         the map file: one `subject|fixed id|file[|version]` per line (file relative to SCHEMAS_DIR); `#` lines and blanks ignored.
#                         The optional fourth column is the registry VERSION of that row (default 1). A subject with several versions has one row per
#                         version, in order 1, 2, ... and EACH version has its OWN fixed id (a version of a subject cannot reuse the id of a different
#                         schema); version N is imported with `version`: N only after versions 1..N-1 are present and the schema is FULL-compatible with
#                         the latest version (read-only POST /compatibility). The maps live in scripts/kafka/schema-id-maps/.
#   SCHEMAS_DIR           the contracts checkout's schemas directory the directory the map file column is relative to
#   DEPLOY_DRY_RUN        true = report only, never mutate
# Exit 0 only when every subject holds exactly the versions its map rows declare, each under its fixed id, with compatibility FULL. Everything
# that exists and is not exactly that is a COLLISION and fails: a different schema under the subject or at a declared version, a version the
# map does not declare, or a fixed id held by another schema. This script never repairs a collision and never deletes a subject.
#
# A read-only PREFLIGHT pass judges every map line (collision = fail) BEFORE the apply pass imports anything: a collision ALREADY PRESENT on a later subject
# never leaves the earlier ones imported. (A writer racing the apply pass is outside that guarantee: registration is an operator-run
# maintenance step on a registry nobody else writes to, and any collision it meets is still refused, never repaired.) Per subject in the apply pass: (mode capture, disposable-subject IMPORT probe), idempotent verify, import, restore the mode in a trap on EVERY
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

# subject|fixed id|file[|version] — read from the map; RecordNameStrategy subjects. A map that is empty, has a malformed line, repeats a
# subject version, repeats an id, or does not number a subject's versions 1, 2, 3 ... in order is refused before the registry is touched.
FIXED="$(grep -v '^[[:space:]]*#' "$SCHEMA_ID_MAP" | grep -v '^[[:space:]]*$' || true)"
[ -n "$FIXED" ] || { echo "FAIL: $SCHEMA_ID_MAP declares no subject"; exit 1; }
if ! awk -F'|' '{ v = (NF == 4 ? $4 : 1) }
  (NF != 3 && NF != 4) || $1 == "" || $2 !~ /^[0-9]+$/ || $3 == "" || (NF == 4 && $4 !~ /^[1-9][0-9]*$/) || seen_i[$2]++ || v != last[$1] + 1 { bad = 1 }
  { last[$1] = v } END { exit bad }' <<<"$FIXED"; then
  echo "FAIL: $SCHEMA_ID_MAP has a malformed line, a repeated subject version, a repeated id or a subject whose versions are not 1, 2, 3 ... in order"; exit 1
fi
# the highest version each subject's rows declare: a subject may hold 1..MAXV and nothing else
maxv() { awk -F'|' -v s="$1" '$1 == s { v = (NF == 4 ? $4 : 1); if (v > m) m = v } END { print m + 0 }' <<<"$FIXED"; }
# [1,2,...,K] as the registry lists the versions of a subject
version_list() { local i out="1"; [ "$1" -ge 1 ] || { printf '[]'; return; }; for ((i = 2; i <= $1; i++)); do out="$out,$i"; done; printf '[%s]' "$out"; }
PLANNED=" "   # "subject@version " of every row of this pass judged importable or already present: a later version of the same subject may follow it on a registry that lacks both
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
if len(sys.argv) > 2: b["id"] = int(sys.argv[2]); b["version"] = int(sys.argv[3])
print(json.dumps(b))' "$@"; }

echo "=== ensure-fixed-id-schemas: $REG (dry_run=$DRY) map=$SCHEMA_ID_MAP schemas=$SCHEMAS_DIR ==="
GLOBAL=$(http GET /mode); [ "$(status "$GLOBAL")" = 200 ] || { echo "FAIL: GET /mode -> $GLOBAL"; exit 1; }
GLOBAL_MODE=$(json_field "$(body "$GLOBAL")" mode)
echo "global mode: $GLOBAL_MODE"

# ---- the mutability probe: subject-level IMPORT must be allowed (mode.mutability=true), proven on a
# disposable absent subject that is removed again, before any real subject is touched.
# the exit trap exists BEFORE the first mutation: it removes a probe mode that a failed DELETE left behind and restores the mode of the subject in flight
PROBE_SET=false; CUR_SUBJECT=""
on_exit() { local rc=$?; if [ "$PROBE_SET" = true ]; then http DELETE "/mode/$PROBE_SUBJECT" >/dev/null 2>&1 || true; fi; restore_mode || rc=1; exit $rc; }
trap on_exit EXIT

PR=$(http GET "/mode/$PROBE_SUBJECT")
[ "$(status "$PR")" = 404 ] || { echo "FAIL: probe subject $PROBE_SUBJECT already has a mode ($PR) — refusing to probe on a real subject"; exit 1; }
if [ "$DRY" = true ]; then
  echo "probe: DEPLOY_DRY_RUN=true — not setting IMPORT on $PROBE_SUBJECT"
else
  PROBE_SET=true
  P1=$(http PUT "/mode/$PROBE_SUBJECT" '{"mode":"IMPORT"}')
  [ "$(status "$P1")" = 200 ] && [ "$(json_field "$(body "$P1")" mode)" = IMPORT ] \
    || { echo "FAIL: subject-level IMPORT is not allowed on this registry (mode.mutability=false?): PUT /mode/$PROBE_SUBJECT -> $P1"; exit 1; }
  P2=$(http DELETE "/mode/$PROBE_SUBJECT")
  [ "$(status "$P2")" = 200 ] || { echo "FAIL: could not remove the probe mode: DELETE /mode/$PROBE_SUBJECT -> $P2"; exit 1; }
  P3=$(http GET "/mode/$PROBE_SUBJECT")
  [ "$(status "$P3")" = 404 ] && PROBE_SET=false || { echo "FAIL: probe mode still present after DELETE: $P3"; exit 1; }
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
PLANNED=" "
while IFS='|' read -r subject id file version; do
  [ -n "${subject:-}" ] || continue
  version="${version:-1}"
  schema="$SCHEMAS_DIR/$file"
  [ -f "$schema" ] || { echo "FAIL: $schema missing"; fail=1; continue; }
  echo "--- [$PASS] $subject version $version (id $id)"
  top=$(maxv "$subject")
  # the canonical text as the registry would normalize it: ask the registry whether THIS schema is already
  # registered under the subject (POST /subjects/<subject> returns the id/version it lives under, 404 if not)
  LOOKUP=$(http POST "/subjects/$subject" "$(reg_body "$schema")")
  case "$(status "$LOOKUP")" in
    200)
      have_id=$(json_field "$(body "$LOOKUP")" id); have_v=$(json_field "$(body "$LOOKUP")" version)
      if [ "$have_id" = "$id" ] && [ "$have_v" = "$version" ]; then
        # this version is canonical under its fixed id; the subject must hold the versions 1..K with version <= K <= the highest the map
        # declares and NOTHING else — producers resolve the exact schema they embed, but a stray later version would still be a version
        # this map never vetted
        V1=$(http GET "/subjects/$subject/versions")
        ok=false
        if [ "$(status "$V1")" = 200 ]; then
          have_list=$(body "$V1" | tr -d ' ')
          for ((k = version; k <= top; k++)); do [ "$have_list" = "$(version_list "$k")" ] && ok=true; done
        fi
        if [ "$ok" != true ]; then
          echo "FAIL: $subject holds versions $(body "$V1") — only $(version_list "$version") up to $(version_list "$top") is admissible (version $version is the fixed id $id); anything else was never vetted by this map"
          fail=1; continue
        fi
        echo "    registered: version $version, id $id (versions $(body "$V1" | tr -d ' '))"
        PLANNED="$PLANNED$subject@$version "
        [ "$PASS" = apply ] && { ensure_full "$subject" || fail=1; }
        continue
      fi
      echo "FAIL: $subject holds the canonical schema under id $have_id version $have_v, not id $id version $version — a collision this script never repairs"
      fail=1; continue ;;
    404) ;;
    *) echo "FAIL: lookup POST /subjects/$subject -> $LOOKUP"; fail=1; continue ;;
  esac
  # not registered under the subject: the subject must hold exactly the versions 1..version-1 (absent for version 1; on a registry that
  # lacks the earlier versions too, they are imported by the rows before this one) and the fixed id must be free
  V=$(http GET "/subjects/$subject/versions")
  if [ "$(status "$V")" = 200 ]; then
    if [ "$(body "$V" | tr -d ' ')" != "$(version_list $((version - 1)))" ]; then
      echo "FAIL: $subject exists with versions $(body "$V") but not the canonical schema as version $version — a collision this script never repairs"
      fail=1; continue
    fi
  elif [ "$version" -gt 1 ]; then
    case "$PLANNED" in
      *" $subject@$((version - 1)) "*) ;;
      *) echo "FAIL: $subject is absent and the map does not import version $((version - 1)) before version $version"; fail=1; continue ;;
    esac
  fi
  I=$(http GET "/schemas/ids/$id")
  if [ "$(status "$I")" = 200 ]; then
    echo "FAIL: fixed id $id is already held by another schema on $REG — a collision this script never repairs"
    fail=1; continue
  fi
  if [ "$version" -gt 1 ] && [ "$(status "$V")" = 200 ]; then
    # read-only: the registry judges compatibility under the SUBJECT'S configured level, and FULL is only enforced after an import, so a subject that is not
    # at FULL now must be refused here: its verdict would not be the FULL one this script promises
    CF=$(http GET "/config/$subject")
    if [ "$(status "$CF")" != 200 ] || [ "$(json_field "$(body "$CF")" compatibilityLevel)" != "$COMPAT" ]; then
      echo "FAIL: $subject is not at compatibility $COMPAT now ($CF): the registry's compatibility verdict for version $version would not be the $COMPAT one — set it deliberately first"
      fail=1; continue
    fi
    # the registry's own verdict on the new version against the latest one, under the subject's compatibility
    CK=$(http POST "/compatibility/subjects/$subject/versions/latest" "$(reg_body "$schema")")
    if [ "$(status "$CK")" != 200 ] || [ "$(json_field "$(body "$CK")" is_compatible)" != True ]; then
      echo "FAIL: version $version of $subject is not compatible with the latest registered version: $CK"
      fail=1; continue
    fi
    echo "    compatible with the latest version"
  fi
  PLANNED="$PLANNED$subject@$version "
  if [ "$PASS" = preflight ]; then echo "    free: would import as version $version under id $id"; continue; fi
  if [ "$DRY" = true ]; then echo "    would import as version $version under id $id (DEPLOY_DRY_RUN=true)"; continue; fi
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
  R=$(http POST "/subjects/$subject/versions" "$(reg_body "$schema" "$id" "$version")")
  [ "$(status "$R")" = 200 ] && [ "$(json_field "$(body "$R")" id)" = "$id" ] \
    || { echo "FAIL: import of $subject version $version under id $id -> $R"; fail=1; restore_mode || true; continue; }
  R=$(http GET "/subjects/$subject/versions/$version")
  [ "$(status "$R")" = 200 ] && [ "$(json_field "$(body "$R")" id)" = "$id" ] \
    || { echo "FAIL: $subject version $version does not carry id $id: $R"; fail=1; restore_mode || true; continue; }
  restore_mode || { fail=1; continue; }
  echo "    imported: version $version, id $id"
  ensure_full "$subject" || fail=1
done <<< "$FIXED"
  # PREFLIGHT (read-only, every map line judged before any import): a collision already present on a LATER subject must not leave the earlier ones imported
  if [ "$PASS" = preflight ] && [ "$fail" -ne 0 ]; then echo "=== ensure-fixed-id-schemas: FAILED in preflight — nothing was changed ==="; exit 1; fi
done

if [ "$fail" -ne 0 ]; then echo "=== ensure-fixed-id-schemas: FAILED ==="; exit 1; fi
echo "=== ensure-fixed-id-schemas: OK ==="
