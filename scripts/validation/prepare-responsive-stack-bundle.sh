#!/usr/bin/env bash
# Source packaging only on the Jenkins Mac. No browser/load/performance execution here.
set -euo pipefail
verify_responsive_baseline(){
  [ "$#" = 5 ] || { echo 'BASELINE_ARGUMENTS_REQUIRED' >&2; return 1; }
  local repository="$1" baseline="$2" reference="$3" shared_main="$4" candidate="$5" revision parents expected actual stack asset
  for revision in "$baseline" "$reference" "$shared_main" "$candidate"; do
    [[ "$revision" =~ ^[0-9a-f]{40}$ ]] || { echo 'BASELINE_SHA_FORMAT' >&2; return 1; }
  done
  parents="$(git -C "$repository" show -s --format=%P "$baseline")" || return 1
  [ "$parents" = "$reference $shared_main" ] || { echo 'BASELINE_PARENT_MISMATCH' >&2; return 1; }
  git -C "$repository" merge-base --is-ancestor "$baseline" "$candidate" || { echo 'BASELINE_NOT_ANCESTOR' >&2; return 1; }
  for asset in context-tape-orders-model.js context-tape-orders-overlay.js context-tape-orders.css; do
    expected="$(git -C "$repository" rev-parse --verify "$reference:src/app/web/assets/$asset")" || { echo "REFERENCE_ASSET_REQUIRED: $asset" >&2; return 1; }
    actual="$(git -C "$repository" rev-parse --verify "$baseline:src/app/web/assets/$asset")" || { echo "BASELINE_ASSET_REQUIRED: $asset" >&2; return 1; }
    [ "$actual" = "$expected" ] || { echo "LEGACY_BASELINE_ASSET_MISMATCH: $asset" >&2; return 1; }
  done
  expected="$(git -C "$repository" rev-parse --verify "$shared_main:src/app/web/assets/context-tape-model.js")" || { echo 'SHARED_CANONICAL_MODEL_REQUIRED' >&2; return 1; }
  actual="$(git -C "$repository" rev-parse --verify "$baseline:src/app/web/assets/context-tape-model.js")" || { echo 'BASELINE_CANONICAL_MODEL_REQUIRED' >&2; return 1; }
  [ "$actual" = "$expected" ] || { echo 'SHARED_CANONICAL_MODEL_MISMATCH' >&2; return 1; }
  actual="$(git -C "$repository" rev-parse --verify "$candidate:src/app/web/assets/context-tape-model.js")" || { echo 'CANDIDATE_CANONICAL_MODEL_REQUIRED' >&2; return 1; }
  [ "$actual" = "$expected" ] || { echo 'CANONICAL_TAPE_MODEL_MISMATCH' >&2; return 1; }
  stack="$(git -C "$repository" ls-tree --name-only "$baseline" -- src/app/web/assets/context-tape-order-stack.js)" || return 1
  [ -z "$stack" ] || { echo 'BASELINE_CONTAINS_I3_STACK_MODULE' >&2; return 1; }
}
# Sourcing exposes only the same read-only verifier used by this packager.
if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then return 0; fi
[[ "${WEB_SHA:-}" =~ ^[0-9a-f]{40}$ ]] || exit 1
baseline=d508765049f459e1c5a75d92cf50c55ad5ab5caa
legacy_reference=81327c34034590b2f1be058ce2d0bfc1540d9967
shared_main=f7b44a70b5a3db9d5d68e5657cbd338e9802c6b9
infra_root="$(cd .. && pwd)"
mkdir bundle
git init -q source
git -C source remote add origin git@github.com:abhinav-jain09/options-edge.git
git -C source fetch --depth=512 origin "$WEB_SHA" "$baseline"
verify_responsive_baseline source "$baseline" "$legacy_reference" "$shared_main" "$WEB_SHA"
for variant in baseline candidate; do
  sha="$baseline"; [ "$variant" = baseline ] || sha="$WEB_SHA"
  mkdir -p "bundle/web/$variant"
  git -C source archive "$sha" src/app/web test/js/helpers | tar -x -C "bundle/web/$variant"
  if [ "$variant" = candidate ]; then
    git -C source archive "$sha" test/browser/helpers | tar -x -C "bundle/web/$variant"
  fi
done
cmp bundle/web/{baseline,candidate}/src/app/web/assets/context-tape-model.js
mkdir -p bundle/node_modules bundle/scripts/validation
mkdir -p bundle/tests
cp "$infra_root/tests/test_responsive_supervisor.py" bundle/tests/
runtime=/Users/abhinav/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules
[ -f "$runtime/playwright-core/package.json" ] || { echo 'Prepared Playwright core runtime required' >&2; exit 1; }
cp -R "$runtime/playwright-core" bundle/node_modules/
for file in run-responsive-stack-es4.sh responsive-stack-probe.cjs prepare-responsive-chart.cjs readiness-stimulus.cjs profile-responsive-dock.cjs measure-responsive-panel.cjs responsive-supervisor.py qualify-responsive-stack.py linked-exits-browser-probe.cjs headless-overlay-session.cjs; do
  cp "$infra_root/scripts/validation/$file" bundle/scripts/validation/
done
# The existing legacy overlay probe remains byte-identical, with its original workload/gates.
for variant in baseline candidate; do
  for asset in context-tape-orders-model.js context-tape-orders-overlay.js context-tape-orders.css; do
    cp "bundle/web/$variant/src/app/web/assets/$asset" "bundle/web/$variant/$asset"
  done
done
INFRA_SHA="$(git -C "$infra_root" rev-parse HEAD)" BASELINE_SHA="$baseline" LEGACY_REFERENCE_SHA="$legacy_reference" SHARED_MAIN_SHA="$shared_main" node - <<'JS'
const fs=require('node:fs');fs.writeFileSync('bundle/source-identities.json',JSON.stringify({classification:'test-only source qualification; no release/deployment authority',infrastructure:process.env.INFRA_SHA,baseline:process.env.BASELINE_SHA,candidate:process.env.WEB_SHA,legacyReference:process.env.LEGACY_REFERENCE_SHA,sharedMain:process.env.SHARED_MAIN_SHA},null,2));
JS
(cd bundle && find . -type f ! -name manifest.sha256 -print0 | sort -z | xargs -0 shasum -a 256) > bundle/manifest.sha256
