#!/usr/bin/env bash
# assert-prod-images-fresh.sh <registry> <image-name>...
#
# Fail CLOSED unless every listed image's :prod tag in <registry> resolves to the SAME digest a
# SUCCESSFUL production build actually RECORDED pushing (its archived image-lock.env). Enforces the
# es4 rule: es4-deploy must never deploy code the registry does not PROVABLY have — a build reporting
# SUCCESS is not evidence the push landed (it can race the deploy, fail silently, be a no-op, or the
# tag can be moved/GC'd afterward). Runs BEFORE the first apply so a stale image never reaches es4.
#
# FAIL-CLOSED discipline (Codex): the ONLY non-error empty result is an explicit HTTP 404 — a producer
# job that does not exist, or a build with no image-lock artifact. EVERY other failure (auth, 5xx,
# transport, unparseable JSON) is FATAL. A wrong token, a controller hiccup, or a failed artifact
# fetch must never be silently read as "no receipt" and let a stale image through. Only a scan that
# PROVABLY succeeded and simply found no receipt for an image treats it as steady-state (not rebuilt
# -> nothing to prove -> skip); the trap case (a just-built image whose push did not land) is ALWAYS
# the newest receipt, so a freshly built image can never fall into the steady-state branch.
#
# Provenance: the producing jobs (PROD_BUILD_JOBS). A single BUILD_TARGET build records only its one
# image, so we scan back and pick each image's receipt from the build with the newest TIMESTAMP
# across ALL jobs (job-list order is NOT recency). prod-vs-dev is discriminated by the registry in the
# lock ref, so a dev-build lock can never satisfy a prod check.
#
# Side effect: writes the verified image@digest pairs to $VERIFIED_DIGESTS_FILE (if set) so the apply
# step pins the EXACT digest this gate proved, closing the check-then-resolve TOCTOU window.
#
# Env: JENKINS_URL (default http://localhost:8080), JENKINS_AUTH (user:token, required),
#      PROD_BUILD_JOBS (default: the known producers), PROD_BUILD_SCAN (default 60),
#      VERIFIED_DIGESTS_FILE (optional handoff path).
set -euo pipefail

REGISTRY="${1:?usage: assert-prod-images-fresh.sh <registry> <image-name>...}"; shift
[ "$#" -gt 0 ] || { echo "assert-prod-images-fresh: no images to verify" >&2; exit 1; }

JENKINS_URL="${JENKINS_URL:-http://localhost:8080}"
: "${JENKINS_AUTH:?JENKINS_AUTH (user:token) must be set to read image-lock artifacts}"
PROD_BUILD_JOBS="${PROD_BUILD_JOBS:-options-edge-processing option-edge-feed-gateway options-edge-databento-feed-deploy options-edge-web-deploy short-premium-agent}"
PROD_BUILD_SCAN="${PROD_BUILD_SCAN:-60}"
VERIFIED_DIGESTS_FILE="${VERIFIED_DIGESTS_FILE:-}"

# Credentials via a mode-0600 config file, never on the curl command line (a -u token is visible to
# `ps`). -g disables URL globbing — the tree queries contain [...]/{0,N} which curl would mangle.
CURLCFG="$(mktemp)"; MAP="$(mktemp)"; trap 'rm -f "$CURLCFG" "$MAP" "$MAP.builds" "$MAP.rcpt"' EXIT
umask 077; printf 'user = "%s"\n' "$JENKINS_AUTH" > "$CURLCFG"

case "$JENKINS_URL" in
  http://localhost*|http://127.0.0.1*|https://*) : ;;
  http://*) echo "WARN: plain-HTTP non-loopback JENKINS_URL '$JENKINS_URL' sends the token in clear text." >&2 ;;
esac

fatal() { echo "FAIL-CLOSED: $*" >&2; exit 1; }

# jfetch <path>  -> body in the global JFETCH_BODY; return 0 (HTTP 200), 44 (HTTP 404), or 2 (any
#                   other status / transport failure). It NEVER calls fatal itself: a `fatal` inside
#                   the `$(...)` of a caller would only exit the subshell (Codex Critical). The CALLER
#                   must `case` on the return and fatal in the MAIN shell for anything but 0/44.
JFETCH_BODY=""
jfetch() {
  local path="$1" resp code
  JFETCH_BODY=""
  resp="$(curl -sg --config "$CURLCFG" -w $'\n%{http_code}' "$JENKINS_URL/$path" 2>/dev/null)" || return 2
  code="${resp##*$'\n'}"; JFETCH_BODY="${resp%$'\n'*}"
  case "$code" in 200) return 0 ;; 404) return 44 ;; *) JFETCH_BODY="$code"; return 2 ;; esac
}

# --- 0. probe: prove the controller is reachable AND the token authenticates, else FATAL ----------
jfetch "api/json?tree=nodeName" && rc=0 || rc=$?
[ "$rc" -eq 0 ] || fatal "cannot reach Jenkins API at $JENKINS_URL with the given credentials (status/transport: ${JFETCH_BODY:-transport}) — refusing to deploy unverified."

# --- 1. refuse if ANY producing job has ANY build in progress (a push may be mid-flight) ----------
#     Every CONFIGURED producer job MUST exist: a 404 (typo, renamed job, or item-level authz hidden
#     as 404) would otherwise drop a whole producer and let its images look steady-state -> fail
#     closed on it. Only an ARTIFACT 404 (a build with no lock) is a legitimate skip, handled below.
for job in $PROD_BUILD_JOBS; do
  jfetch "job/$job/api/json?tree=builds[number,result,building,timestamp]{0,$PROD_BUILD_SCAN}" && rc=0 || rc=$?
  [ "$rc" -eq 0 ] || fatal "producer job '$job' is not reachable (status/transport: ${JFETCH_BODY:-transport}). Fix PROD_BUILD_JOBS or access; refusing to deploy on partial provenance."
  listing="$JFETCH_BODY"
  any_building="$(printf '%s' "$listing" | python3 -c 'import sys,json
try: d=json.load(sys.stdin)
except Exception: sys.exit(3)
print("yes" if any(b.get("building") for b in d.get("builds",[])) else "no")' 2>/dev/null)" \
    || fatal "could not parse the build list for '$job' — refusing to deploy."
  [ "$any_building" = "yes" ] && fatal "a '$job' build is IN PROGRESS — its image push may be mid-flight. Wait for it, then deploy."
  # stash the parsed SUCCESS builds (job number timestamp) for the receipt scan below (reuse the fetch)
  printf '%s' "$listing" | JOB="$job" python3 -c 'import sys,os,json
d=json.load(sys.stdin)
for b in d.get("builds",[]):
 if b.get("result")=="SUCCESS" and not b.get("building") and b.get("timestamp"):
   print(os.environ["JOB"], b["number"], b["timestamp"])' >> "$MAP.builds" \
    || fatal "could not parse the build list for '$job' — refusing to deploy."
done

# --- 2. build image-name -> (timestamp, digest) map; newest timestamp wins per image --------------
#     Artifact 404 = that build archived no lock (dev build / older format) -> skip that build.
#     Any OTHER artifact failure is FATAL (jfetch), so a failed fetch of the NEWEST receipt can never
#     silently fall through to an older one.
: > "$MAP.rcpt"
if [ -f "$MAP.builds" ]; then
  while read -r job n ts; do
    [ -n "$n" ] || continue
    jfetch "job/$job/$n/artifact/.jenkins-tmp/options-edge-image-lock.env" && arc=0 || arc=$?
    case "$arc" in
      0)  : ;;                                  # got the lock
      44) continue ;;                           # this build archived no lock (dev build / old format)
      *)  fatal "could not fetch build $job#$n's image-lock (status/transport: ${JFETCH_BODY:-transport}) — refusing to deploy on an incomplete receipt scan." ;;
    esac
    # grep-no-match (a lock without a prod-registry ref) is fine -> `|| true` on the GREP only, so a
    # python crash on malformed content still aborts (its exit propagates via pipefail).
    # grep passes any receipt CANDIDATE (`..@sha256:`); the parser then requires a FULL 64-hex digest.
    # A candidate whose digest is not exactly 64 hex (a truncated/corrupt push receipt) is FATAL —
    # never silently dropped, which would let the image fall back to an older receipt (Codex High).
    { printf '%s' "$JFETCH_BODY" | grep -E "_IMAGE=${REGISTRY}/[a-z0-9._-]+:[A-Za-z0-9._-]+@sha256:" || true; } \
      | REG="$REGISTRY" TS="$ts" python3 -c '
import sys, os, re
reg = re.escape(os.environ["REG"]); ts = os.environ["TS"]
pat = re.compile(r"=" + reg + r"/([a-z0-9._-]+):[A-Za-z0-9._-]+@(sha256:[0-9a-f]{64})(?![0-9a-f])")
bad = False
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    m = pat.search(line)
    if m:
        print(ts, m.group(1), m.group(2))
    else:
        sys.stderr.write("malformed image-lock receipt line: %s\n" % line[:160]); bad = True
sys.exit(1 if bad else 0)' >> "$MAP.rcpt" \
      || fatal "build $job#$n's image-lock has a malformed/corrupt receipt line — refusing to deploy."
  done < "$MAP.builds"
fi
# reduce: newest timestamp wins per image
sort -k1,1nr "$MAP.rcpt" | awk '!seen[$2]++ {print $2, $3}' > "$MAP"

recorded_digest() { awk -v n="$1" '$1==n{print $2; exit}' "$MAP"; }
live_prod_digest() {
  curl -sfIg \
    -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
    -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
    -H 'Accept: application/vnd.oci.image.index.v1+json' \
    -H 'Accept: application/vnd.docker.distribution.manifest.list.v2+json' \
    "http://${REGISTRY}/v2/${1}/manifests/prod" \
    | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest"{print $2}'
}

# --- 3. verify each requested image --------------------------------------------------------------
[ -n "$VERIFIED_DIGESTS_FILE" ] && : > "$VERIFIED_DIGESTS_FILE"
fail=0
for img in "$@"; do
  rec="$(recorded_digest "$img" || true)"
  if [ -z "$rec" ]; then
    # Provably scanned (probe + every reachable job/build) and found no receipt: this image was NOT
    # rebuilt in the window -> steady-state, nothing to prove. A just-built image is always the
    # newest receipt, so it can never reach this branch.
    echo "skip:  $img — not built in the last $PROD_BUILD_SCAN builds of any producing job (steady-state)"
    continue
  fi
  live="$(live_prod_digest "$img" || true)"
  [ -n "$live" ] || { echo "FAIL-CLOSED: '$img:prod' does not resolve in $REGISTRY (never pushed?)." >&2; fail=1; continue; }
  if [ "$live" != "$rec" ]; then
    echo "FAIL-CLOSED: '$img:prod' is STALE — the live tag is not what the build pushed." >&2
    echo "            registry :prod = $live" >&2
    echo "            build receipt  = $rec" >&2
    echo "            The environment does NOT have the built code. Await/re-run the build's push, then deploy." >&2
    fail=1; continue
  fi
  echo "fresh: $img:prod == build receipt ($live)"
  [ -n "$VERIFIED_DIGESTS_FILE" ] && printf '%s %s\n' "$img" "$live" >> "$VERIFIED_DIGESTS_FILE"
done

[ "$fail" -eq 0 ] || fatal "one or more images failed the freshness gate."
echo "assert-prod-images-fresh: all $# image(s) verified against a prod-build receipt."
