#!/usr/bin/env bash
# SERVICE-REGISTRY VALIDATION (standalone-service-deployment design §13.9).
#
# Fails when the repo drifts from services.yaml — the single source of truth:
#   1. COMPLETENESS: every Deployment rendered by the monolithic env overlays must be
#      registered in services.yaml (a new service that skips registration fails here —
#      the gap that let delta-flow/maxpain/integration-test reach a deploy unbuilt).
#   2. STANDALONE SLICES: every managedBy=standalone service has k8s/services/<name>
#      with an overlay per declared env, rendering ONLY service-owned kinds (§13.5).
#   3. MIRROR RULE: the standalone render of each service must be EQUIVALENT to the
#      monolithic overlay's render of the same workload — env patches replicated in the
#      service overlay cannot drift from the top-level ones.
#   4. IMAGE NAME: the image basename in the standalone render matches services.yaml.
#
# Requires: kubectl (kustomize), yq. Read-only; no cluster access needed.
set -euo pipefail
cd "$(dirname "$0")/../.."

command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }
fail=0
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

ENVS=(dev production experiment)

echo "=== render monolithic overlays ==="
for e in "${ENVS[@]}"; do
  kubectl kustomize "k8s/overlays/$e" >"$TMP/mono-$e.yaml" || { echo "FATAL: k8s/overlays/$e does not render" >&2; exit 1; }
done

echo "=== 1) completeness: every rendered Deployment is registered ==="
yq -r '.services[].deployments[]' services.yaml | sort -u >"$TMP/registered.txt"
for e in "${ENVS[@]}"; do
  yq -r 'select(.kind=="Deployment") | .metadata.name' "$TMP/mono-$e.yaml"
done | grep -v '^---$' | sort -u >"$TMP/rendered.txt"
# Keycloak & friends are infra-owned, not service-registry entries. BOTH identity providers qualify:
# `oe-keycloak*` is the internal one and `bo-keycloak*` the public one for bleedingoptions.com, and a
# second identity provider is still an identity provider. The public WEB tier is deliberately NOT
# excluded — it is an application workload, it IS registered in services.yaml, and this check is what
# would notice if it ever stopped being.
unregistered="$(comm -23 "$TMP/rendered.txt" "$TMP/registered.txt" | { grep -vE '^(oe-keycloak|keycloak|bo-keycloak)' || true; })"
if [ -n "$unregistered" ]; then
  echo "FAIL: Deployments rendered by the overlays but NOT registered in services.yaml:" >&2
  printf '  %s\n' $unregistered >&2
  fail=1
else
  echo "ok: all $(wc -l <"$TMP/rendered.txt" | tr -d ' ') rendered Deployments are registered"
fi

echo "=== 2+3+4) standalone slices: structure, blast radius, mirror rule, image name ==="
while IFS= read -r name; do
  img="$(yq -r ".services[] | select(.name == \"$name\") | .image" services.yaml)"
  for e in $(yq -r ".services[] | select(.name == \"$name\") | .envs[]" services.yaml); do
    overlay="k8s/services/$name/overlays/$e"
    if [ ! -d "$overlay" ]; then
      echo "FAIL: $name declares env '$e' but $overlay does not exist" >&2
      fail=1; continue
    fi
    if ! kubectl kustomize "$overlay" >"$TMP/svc-$name-$e.yaml" 2>"$TMP/svc-$name-$e.err"; then
      echo "FAIL: $overlay does not render:" >&2; head -3 "$TMP/svc-$name-$e.err" >&2
      fail=1; continue
    fi
    # blast radius (§13.5)
    bad="$(yq -r '.kind' "$TMP/svc-$name-$e.yaml" | grep -v '^---$' | sort -u \
      | { grep -vE '^(Deployment|Service|HorizontalPodAutoscaler|ServiceMonitor|Ingress)$' || true; })"
    if [ -n "$bad" ]; then
      echo "FAIL: $overlay renders non-service-owned kinds: $bad" >&2
      fail=1
    fi
    # image basename (§13.9)
    # capture fully, then truncate — `yq | head -1` dies with SIGPIPE (rc 141) under
    # `set -o pipefail` when yq keeps writing after head exits (agent yq buffering).
    _imgs="$(yq -r 'select(.kind=="Deployment") | .spec.template.spec.containers[0].image' "$TMP/svc-$name-$e.yaml")"
    rendered_img="$(printf '%s\n' "$_imgs" | grep -v '^---$' | head -1)"
    base="${rendered_img##*/}"; base="${base%%[:@]*}"
    if [ "$base" != "$img" ]; then
      echo "FAIL: $overlay image basename '$base' != services.yaml image '$img'" >&2
      fail=1
    fi
    # mirror rule: standalone render == the same workload extracted from the monolith.
    #
    # It only applies to options-edge. The rule exists because a standalone slice and the
    # monolithic overlay describe the SAME workload and must not drift — but a tenant service
    # has no monolithic counterpart at all (the monolith renders one namespace, and it is not
    # theirs). Diffing against it compares a real render with an empty one and always fails.
    #
    # The exemption is deliberately keyed on the registry's namespace, not on a name pattern:
    # a service that forgets to declare its namespace stays under the rule, which is the safe
    # direction to fail in.
    svc_ns="$(yq -r ".services[] | select(.name == \"$name\") | .namespace // \"options-edge\"" services.yaml | head -1)"
    if [ "$svc_ns" != "options-edge" ]; then
      echo "  note: $name/$e mirror rule skipped (namespace '$svc_ns' has no monolithic overlay)"
    else
    for dep in $(yq -r ".services[] | select(.name == \"$name\") | .deployments[]" services.yaml); do
      yq "select(.metadata.name == \"$dep\")" "$TMP/mono-$e.yaml" \
        | yq ea '[.] | sort_by(.kind) | .[] | splitDoc' >"$TMP/mirror-mono.yaml"
      yq "select(.metadata.name == \"$dep\")" "$TMP/svc-$name-$e.yaml" \
        | yq ea '[.] | sort_by(.kind) | .[] | splitDoc' >"$TMP/mirror-svc.yaml"
      if ! diff -u "$TMP/mirror-mono.yaml" "$TMP/mirror-svc.yaml" >"$TMP/mirror-diff.txt"; then
        echo "FAIL: MIRROR DRIFT for $name/$e ($dep): the standalone overlay renders differently from the monolithic overlay." >&2
        echo "      Replicate the top-level env patch in $overlay (or vice versa). Diff:" >&2
        head -25 "$TMP/mirror-diff.txt" | sed 's/^/      /' >&2
        fail=1
      fi
    done
    fi
    # env image-tag CONSISTENCY guard (review): a service mapped in ANY image-tags env file
    # must be mapped in EVERY env it deploys to — a partial mapping means the production
    # fast path fails closed at deploy time (the render carries the base dev-registry ref
    # pre-pin, so the mapping is the only production-image source). Services with NO
    # mappings anywhere (own-job images: web, feed, ...) are out of this mechanism.
    if [ -f "image-tags/$e.yaml" ]; then
      _mapped_any=0
      for _f in image-tags/*.yaml; do
        # NOTE: plain grep >/dev/null, NOT grep -q — under pipefail, -q's early exit
        # SIGPIPEs yq and a successful match reads as pipeline failure.
        if yq -r ".images | to_entries[] | .value" "$_f" | grep "/$img:" >/dev/null; then _mapped_any=1; break; fi
      done
      if [ "$_mapped_any" = "1" ] && ! yq -r ".images | to_entries[] | .value" "image-tags/$e.yaml" | grep "/$img:" >/dev/null; then
        echo "FAIL: image '$img' (service $name) is mapped in some image-tags env but MISSING from image-tags/$e.yaml" >&2
        exit 1
      fi
    fi
    echo "ok: $name/$e (blast radius, image, mirror)"
  done
done < <(yq -r '.services[] | select(.managedBy == "standalone") | .name' services.yaml)

if [ "$fail" -ne 0 ]; then
  echo "=== validate-services: FAILED ===" >&2
  exit 1
fi

echo "=== 5) at-most-one VIX publisher (VIX feed separation design §7) ==="
# Runs here so BOTH the PR CI pass and the service-deploy validation stage
# (Jenkinsfile.service-deploy runs this script before any apply) enforce the
# assertion; the monolith path gets it via scripts/deploy/validate-platform.sh.
bash scripts/ci/validate-vix-single-publisher.sh

echo "=== 5b) exactly one pre-open GEX publisher, matching the declaration ==="
# Same shape as the VIX assertion and for the same reason: two publishers can own the
# pre-open gamma surface, BOTH selections render cleanly, and picking the wrong one is
# silent — pre-market GEX just stops appearing (incident 2026-08-24). The declaration in
# k8s/preopen-publisher.env makes any switch an explicit, reviewed edit.
bash scripts/ci/validate-preopen-single-publisher.sh

echo "=== 6) durable topics are preserved by the destructive resets ==="
# A topic declared retention=-1 in topics.env that the pre-market / clean-slate
# resets would still wipe is the 2026-07-28 basis-cold-start incident class —
# make that drift unmergeable rather than discoverable at 09:00 ET.
bash scripts/ci/validate-durable-topic-preservation.sh

# The OI anchor topic barrier: its parser, and the shipped script end to end against mocked CLIs.
# Wired here because a regression test nothing runs is not a test -- and this particular parser has
# already shipped one hole (it accepted "compact,delete", the exact policy it exists to reject).
for t in scripts/kafka/ensure-oi-anchor-topic-parse-test.sh scripts/kafka/ensure-oi-anchor-topic-e2e-test.sh \
         scripts/kafka/verify-topics-pure-compact-test.sh \
         scripts/kafka/ensure-gamma-ladder-path-topics-test.sh \
         scripts/kafka/reset-preserved-topics-test.sh \
         scripts/ci/validate-durable-topic-preservation-mutation-test.sh \
         scripts/ci/es-cvd-mirror-shape-test.sh \
         scripts/ci/es-auction-mirror-shape-test.sh; do
  if [ ! -x "$t" ]; then
    echo "FAIL: $t missing or not executable"
    exit 1
  fi
  if ! out=$(bash "$t" 2>&1); then
    echo "FAIL: $t"
    printf '%s\n' "$out" | sed 's/^/      /'
    exit 1
  fi
done
echo "topic contracts: oi-anchor barrier, pure-compact verification, and the durable-preservation mutation suite passed"

# --- continuous auto-hunt production acceptance (auto-arm req §3.1) ---
# Both flags must be EFFECTIVELY true in BOTH prod mirrors: asserted on the
# RENDERED manifests (not source text — a commented-out or duplicated env
# entry must fail here, not deploy the hunt inert). Exactly one entry per
# flag, value "true", on the reversal-confirmation container.
echo "=== 7) continuous auto-hunt: production flags effective in both rendered mirrors ==="
for r in "$TMP/mono-production.yaml" "$TMP/svc-reversal-confirmation-production.yaml"; do
  if [ ! -s "$r" ]; then
    echo "FAIL: rendered mirror $r missing — cannot assert auto-hunt flags"
    exit 1
  fi
  for flag in REVERSAL_HUNT_ENABLED REVERSAL_HUNT_AUTO_ARM; do
    # STRUCTURAL count on the named application container: project the env
    # entry's NAME (always present on a real entry), so a duplicate with an
    # empty value still counts as two (Codex r2 — Kubernetes deploys
    # duplicate env entries; value-projection would collapse them).
    q='select(.kind=="Deployment" and .metadata.name=="reversal-confirmation-service")
        | .spec.template.spec.containers[] | select(.name=="reversal-confirmation")
        | .env[] | select(.name=="'"$flag"'")'
    n="$(yq -r "$q | .name" "$r" | grep -c "^${flag}\$" || true)"
    if [ "$n" != "1" ]; then
      echo "FAIL: $flag appears $n times on the reversal-confirmation container in rendered $r (need exactly 1)"
      exit 1
    fi
    v="$(yq -r "$q | .value" "$r")"
    if [ "$v" != "true" ]; then
      echo "FAIL: $flag renders as '$v' in $r — the always-armed hunt would deploy inert"
      exit 1
    fi
  done
done
echo "auto-hunt flags: effective REVERSAL_HUNT_ENABLED + REVERSAL_HUNT_AUTO_ARM = true in both rendered mirrors"

echo "=== 8) U16 CVD-levels cross-service timing inequalities (ES-CVD-SPX-LEVELS-DESIGN.md CL-R10/G14) ==="
# Resolved STRUCTURALLY from each variable's OWNING deployment/container (grep-first-match would
# accept a hit on the wrong workload or a commented duplicate). Rules: the owning container must
# exist; the var may appear at most once on it (name-projection, duplicate-safe); a present entry
# must be a literal integer (valueFrom/indirect is undeterminable here → FAIL, never defaulted);
# an absent entry resolves to the code default. The inequalities keep staleness windows wider
# than the attestation heartbeats they watch (+2s margin; UI adds the 5s browser-skew allowance).
cvd_env() { # file deployment container var default -> value (FAILs the run on any ambiguity)
  local f="$1" dep="$2" ctr="$3" var="$4" def="$5" q n v
  q='select(.kind=="Deployment" and .metadata.name=="'"$dep"'")
      | .spec.template.spec.containers[] | select(.name=="'"$ctr"'")'
  n="$(yq -r "$q | .name" "$f" | grep -c "^${ctr}\$" || true)"
  if [ "$n" != "1" ]; then
    echo "FAIL: expected exactly one container '$ctr' in Deployment '$dep' of $f (found $n) — cannot resolve $var" >&2
    return 1
  fi
  n="$(yq -r "$q | .env[]? | select(.name==\"$var\") | .name" "$f" | grep -c "^${var}\$" || true)"
  case "$n" in
    0) echo "$def"; return 0 ;;
    1) ;;
    *) echo "FAIL: $var appears $n times on $dep/$ctr in $f (need at most 1)" >&2; return 1 ;;
  esac
  v="$(yq -r "$q | .env[]? | select(.name==\"$var\") | .value // \"\"" "$f")"
  if ! printf '%s' "$v" | grep -qE '^[0-9]+$'; then
    echo "FAIL: $var on $dep/$ctr in $f is not a literal integer (got '$v'; valueFrom/indirect values are undeterminable) " >&2
    return 1
  fi
  echo "$v"
}
# Misplacement guard: each var may live ONLY on its owning deployment. A patch that lands the
# env on the wrong workload must fail here, not silently satisfy (or dodge) the inequality.
cvd_only_on() { # file var allowed-deployment(or "-" for none)
  local f="$1" var="$2" allowed="$3" hits
  hits="$(yq -r 'select(.kind=="Deployment")
      | select([.spec.template.spec.containers[].env[]? | select(.name=="'"$var"'")] | length > 0)
      | .metadata.name' "$f" | { grep -v "^${allowed}\$" || true; })"
  if [ -n "$hits" ]; then
    echo "FAIL: $var set on non-owning deployment(s) in $f: $(printf '%s' "$hits" | tr '\n' ' ')" >&2
    return 1
  fi
}
ES4_CVD_MANIFEST="k8s/es4/services/es-cvd.yaml"
[ -s "$ES4_CVD_MANIFEST" ] || { echo "FAIL: $ES4_CVD_MANIFEST missing — cannot resolve CVD_LEVELS_HEARTBEAT_MS"; exit 1; }
[ -s "$TMP/mono-production.yaml" ] || { echo "FAIL: rendered mono-production.yaml missing — cannot resolve CVD_LEVELS_* vars"; exit 1; }
HB="$(cvd_env "$ES4_CVD_MANIFEST" es-cvd-service es-cvd CVD_LEVELS_HEARTBEAT_MS 5000)"
SRC_STALE="$(cvd_env "$TMP/mono-production.yaml" es-spx-align-service es-spx-align CVD_LEVELS_SOURCE_STALE_MS 15000)"
ALIGN_HB="$(cvd_env "$TMP/mono-production.yaml" es-spx-align-service es-spx-align CVD_LEVELS_ALIGN_HEARTBEAT_MS 5000)"
UI_STALE="$(cvd_env "$TMP/mono-production.yaml" options-edge-web web CVD_LEVELS_UI_STALE_MS 20000)"
cvd_only_on "$TMP/mono-production.yaml" CVD_LEVELS_HEARTBEAT_MS "-"
cvd_only_on "$TMP/mono-production.yaml" CVD_LEVELS_SOURCE_STALE_MS es-spx-align-service
cvd_only_on "$TMP/mono-production.yaml" CVD_LEVELS_ALIGN_HEARTBEAT_MS es-spx-align-service
cvd_only_on "$TMP/mono-production.yaml" CVD_LEVELS_UI_STALE_MS options-edge-web
[ "$SRC_STALE" -gt "$((HB + 2000))" ] || { echo "FAIL: CVD_LEVELS_SOURCE_STALE_MS ($SRC_STALE) must exceed CVD_LEVELS_HEARTBEAT_MS ($HB) + 2000"; exit 1; }
[ "$UI_STALE" -gt "$((ALIGN_HB + 7000))" ] || { echo "FAIL: CVD_LEVELS_UI_STALE_MS ($UI_STALE) must exceed CVD_LEVELS_ALIGN_HEARTBEAT_MS ($ALIGN_HB) + 7000 (5s skew + 2s margin)"; exit 1; }
echo "cvd-levels timing: SOURCE_STALE=$SRC_STALE > HB=$HB+2000; UI_STALE=$UI_STALE > ALIGN_HB=$ALIGN_HB+7000 (structural, owner-scoped)"

echo "=== 9) every registered service is SELECTABLE in service-deploy's SERVICE choice list ==="
# WHY THIS EXISTS. broker-execution-service and amt-order-bridge were registered in services.yaml
# with a full set of overlays, passed every check above, and still could not be deployed: neither
# appeared in Jenkinsfile.service-deploy's SERVICE choice parameter, so there was no way to select
# them. Sections 1-4 ask "is every rendered Deployment registered?" -- the opposite question, "is
# every registered service reachable by the job that deploys it?", was asked by nothing.
#
# An omission must therefore FAIL, and a deliberate hold must be DECLARED here rather than expressed
# by absence -- otherwise the two are indistinguishable, which is how the above happened. The
# exemption list is checked in BOTH directions: an exemption that is now selectable, or that names
# something that is not a registered slice, fails too, so it cannot quietly rot.
SERVICE_CHOICES_EXEMPT=(
  # 2026-07-26 operator hold: deliberately not deployable from this job. Also stated in the SERVICE
  # parameter's own description in Jenkinsfile.service-deploy.
  spread-skew
  spread-skew-postgres-writer
)
python3 - "${SERVICE_CHOICES_EXEMPT[@]}" <<'PYCHK' || fail=1
import re, sys, yaml

exempt = set(sys.argv[1:])
jf = 'Jenkinsfile.service-deploy'
text = open(jf).read()

# A parse miss must be FATAL, not an empty set. An empty set would make every service look missing
# (loud), but a block that matches while the NAMES do not would make a real omission look present
# (silent) -- so both the block and a plausible number of names are required.
m = re.search(r"choice\(name: 'SERVICE', choices: \[(.*?)\]\s*,", text, re.S)
if not m:
    sys.exit("FAIL: could not find the SERVICE choice block in %s -- this check cannot run, and a\n"
             "      check that cannot run must not pass. Fix the parser together with the parameter." % jf)
choices = set(re.findall(r"'([a-z0-9-]+)'", m.group(1)))
if len(choices) < 20:
    sys.exit("FAIL: parsed only %d SERVICE choices from %s; the block matched but the names did not.\n"
             "      Refusing to validate against a set this small." % (len(choices), jf))

reg = yaml.safe_load(open('services.yaml'))
def services(o):
    if isinstance(o, dict):
        for v in o.values():
            yield from services(v)
    elif isinstance(o, list):
        for v in o:
            if isinstance(v, dict) and 'name' in v:
                yield v
            else:
                yield from services(v)
registered = {s['name'] for s in services(reg) if s.get('sliceGenerated')}
if not registered:
    sys.exit("FAIL: no sliceGenerated services found in services.yaml -- the reader and the registry\n"
             "      have diverged; this check would otherwise pass vacuously.")

bad = []
missing = sorted(registered - choices - exempt)
if missing:
    bad.append("FAIL: registered in services.yaml but NOT selectable in service-deploy's SERVICE list:\n"
               + "".join("        %s\n" % n for n in missing)
               + "      Add each to the choice list, or to SERVICE_CHOICES_EXEMPT with the reason.")
stale = sorted(exempt & choices)
if stale:
    bad.append("FAIL: SERVICE_CHOICES_EXEMPT names service(s) that ARE selectable -- the hold was\n"
               "      lifted in the Jenkinsfile but not here:\n"
               + "".join("        %s\n" % n for n in stale))
unknown = sorted(exempt - registered)
if unknown:
    bad.append("FAIL: SERVICE_CHOICES_EXEMPT names service(s) that are not registered slices at all:\n"
               + "".join("        %s\n" % n for n in unknown))
if bad:
    sys.exit("\n".join(bad))

print("service selectability: %d registered slice(s); %d selectable, %d exempt by declaration (%s)"
      % (len(registered), len(registered & choices), len(exempt), ", ".join(sorted(exempt))))
PYCHK
[ "$fail" -eq 0 ] || { echo "validate-services: FAILED" >&2; exit 1; }

echo "=== validate-services: OK ==="
