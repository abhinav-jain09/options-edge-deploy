#!/usr/bin/env bash
# Source packaging only on the Jenkins Mac. No browser/load/performance execution here.
set -euo pipefail
[[ "${WEB_SHA:-}" =~ ^[0-9a-f]{40}$ ]] || exit 1
baseline=81327c34034590b2f1be058ce2d0bfc1540d9967
infra_root="$(cd .. && pwd)"
mkdir bundle
git init -q source
git -C source remote add origin git@github.com:abhinav-jain09/options-edge.git
git -C source fetch --depth=512 origin "$WEB_SHA" "$baseline"
git -C source merge-base --is-ancestor "$baseline" "$WEB_SHA"
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
for file in run-responsive-stack-es4.sh responsive-stack-probe.cjs prepare-responsive-chart.cjs readiness-stimulus.cjs responsive-supervisor.py qualify-responsive-stack.py linked-exits-browser-probe.cjs headless-overlay-session.cjs; do
  cp "$infra_root/scripts/validation/$file" bundle/scripts/validation/
done
# The existing legacy overlay probe remains byte-identical, with its original workload/gates.
for variant in baseline candidate; do
  for asset in context-tape-orders-model.js context-tape-orders-overlay.js context-tape-orders.css; do
    cp "bundle/web/$variant/src/app/web/assets/$asset" "bundle/web/$variant/$asset"
  done
done
INFRA_SHA="$(git -C "$infra_root" rev-parse HEAD)" BASELINE_SHA="$baseline" node - <<'JS'
const fs=require('node:fs');fs.writeFileSync('bundle/source-identities.json',JSON.stringify({classification:'test-only source qualification; no release/deployment authority',infrastructure:process.env.INFRA_SHA,baseline:process.env.BASELINE_SHA,candidate:process.env.WEB_SHA},null,2));
JS
(cd bundle && find . -type f ! -name manifest.sha256 -print0 | sort -z | xargs -0 shasum -a 256) > bundle/manifest.sha256
