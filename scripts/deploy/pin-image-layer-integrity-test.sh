#!/usr/bin/env bash
# Proves resolve_repo_digest REFUSES an image whose manifest resolves but whose content the registry
# cannot serve -- the 2026-11-12 dev outage, where options-edge-web's manifest answered 200 while a
# 132 MB layer was a 404 and the deploy shipped it anyway. Runs against a fake registry on loopback,
# so it needs no dev host, no Docker and no network.
set -euo pipefail
cd "$(dirname "$0")/../.."

PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
STATE="$(mktemp -d)"; trap 'rm -rf "$STATE"; kill "${SRV:-0}" 2>/dev/null || true' EXIT

python3 scripts/deploy/pin-image-layer-integrity-fake-registry.py "$PORT" "$STATE" &
SRV=$!
for _ in $(seq 1 50); do
  curl -sf "http://127.0.0.1:$PORT/v2/" >/dev/null 2>&1 && break; sleep 0.1
done

# shellcheck source=/dev/null
source scripts/deploy/pin-image.sh
export DEPLOY_PLATFORM=linux/arm64 REGISTRY_SCHEME=http

mode() { printf '%s' "$1" > "$STATE/mode"; }
fails=0
expect_ok()   { if resolve_repo_digest "127.0.0.1:$PORT" "$2" dev >/dev/null 2>&1; then echo "  ok   $1"; else echo "  FAIL $1 (refused a healthy image)" >&2; fails=$((fails+1)); fi; }
expect_deny() { if resolve_repo_digest "127.0.0.1:$PORT" "$2" dev >/dev/null 2>&1; then echo "  FAIL $1 (ACCEPTED a broken image)" >&2; fails=$((fails+1)); else echo "  ok   $1"; fi; }

mode healthy;        expect_ok   "a complete manifest is accepted"                          app
mode missing_layer;  expect_deny "a layer the registry 404s is refused"                     app
mode short_layer;    expect_deny "a layer served SHORT (200, wrong length) is refused"      app
mode missing_config; expect_deny "a config blob the registry 404s is refused"               app
mode no_blobs;       expect_deny "a manifest naming no blobs at all is refused"             app
mode index_healthy;  expect_ok   "a multi-arch index with a complete platform manifest"     idx
mode index_broken;   expect_deny "an index whose PLATFORM manifest has a 404 layer"         idx
mode index_gone;     expect_deny "an index pointing at a sub-manifest the registry 404s"    idx
# Separates the status check from the length check: a 404 whose body happens to be exactly
# the declared size passes the length comparison, so only the HTTP status catches it.
mode not_found_exact_len; expect_deny "a 404 whose body is EXACTLY the declared size"      app
# The only route to the empty-manifest guard: the plain-manifest branch refuses a missing
# config digest first, so that guard is reachable only through an index's sub-manifest.
mode index_no_blobs; expect_deny "an index whose platform manifest names no blobs"         idx

# The message has to name the blob: the whole point is that the next person can go and look.
mode missing_layer
# NOT a pipeline: under `set -o pipefail` a pipe would inherit the refusal's intended non-zero exit
# and the `if` would read a correct refusal as a failed check. Capture, then match.
msg="$(resolve_repo_digest "127.0.0.1:$PORT" app dev 2>&1 >/dev/null || true)"
case "$msg" in
  *sha256:*) echo "  ok   the refusal names the offending blob" ;;
  *) echo "  FAIL the refusal does not name the blob: $msg" >&2; fails=$((fails+1)) ;;
esac

# A stranded sub-manifest is refused either way -- an unreadable one yields no blobs, which the
# empty-manifest guard catches. What the explicit check buys is the DIAGNOSIS: "the registry does
# not serve <digest>" sends someone to the right place, "names no blobs" does not. Pin the message,
# or the check is decoration the next refactor deletes.
mode index_gone
msg="$(resolve_repo_digest "127.0.0.1:$PORT" idx dev 2>&1 >/dev/null || true)"
case "$msg" in
  *"does not serve"*) echo "  ok   a stranded sub-manifest is named, not just counted" ;;
  *) echo "  FAIL the stranded sub-manifest is not diagnosed: $msg" >&2; fails=$((fails+1)) ;;
esac

[ "$fails" -eq 0 ] || { echo "pin-image layer integrity: $fails failure(s)" >&2; exit 1; }
echo "pin-image layer integrity: all checks passed"
