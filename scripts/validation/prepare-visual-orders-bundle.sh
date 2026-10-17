#!/usr/bin/env bash
# Compilation only on the Mac; measured work is guarded to run only on es4.
set -euo pipefail
for name in CANDIDATE_SHA BASELINE_SHA WEB_SHA; do
  [[ "${!name:-}" =~ ^[0-9a-f]{40}$ ]] || { echo "Invalid $name" >&2; exit 1; }
done
cache=/Users/abhinav/development/workspace/support/tmp/visual-orders-private-m2/repository
settings=/Users/abhinav/development/workspace/support/tmp/visual-orders-private-m2/settings.xml
[ -d "$cache" ] && [ -r "$settings" ] || { echo 'Prepared isolated Maven cache/settings are required; see validation runbook.' >&2; exit 1; }
root="$PWD"
# These two generated roots belong to this validation job. Never follow stale links.
reset_generated_dir(){
  local directory="$1"
  case "$directory" in .validation-m2|bundle) ;; *) echo 'Unexpected validation output root' >&2; exit 1;; esac
  [ ! -L "$directory" ] && { [ ! -e "$directory" ] || [ -d "$directory" ]; } || { echo 'Invalid validation output root' >&2; exit 1; }
  mkdir -p "$directory"
  find -P "$directory" -xdev -depth \( -type f -o -type l \) -delete
  find -P "$directory" -xdev -depth -mindepth 1 -type d -empty -delete
  [ -z "$(find -P "$directory" -mindepth 1 -print -quit)" ] || { echo 'Validation output root could not be emptied' >&2; exit 1; }
}
reset_generated_dir .validation-m2
reset_generated_dir bundle
mkdir -p bundle/{candidate,baseline,lib,web,scripts/validation}
rsync -a "$cache/" .validation-m2/
cp "$settings" .validation-settings.xml
checkout(){
  local directory="$1" repository="$2" revision="$3"
  mkdir -p "$directory"
  if [ ! -d "$directory/.git" ]; then git -C "$directory" init -q; git -C "$directory" remote add origin "git@github.com:abhinav-jain09/$repository.git"; fi
  [ "$(git -C "$directory" remote get-url origin)" = "git@github.com:abhinav-jain09/$repository.git" ] || { echo 'Unexpected validation source remote' >&2; exit 1; }
  git -C "$directory" fetch --depth=512 origin "$revision"
  git -C "$directory" checkout -q --detach "$revision"
  [ "$(git -C "$directory" rev-parse HEAD)" = "$revision" ]
}
checkout validation-candidate options-edge-processing "$CANDIDATE_SHA"
checkout validation-baseline options-edge-processing "$BASELINE_SHA"
checkout validation-web options-edge "$WEB_SHA"
git -C validation-candidate fetch --depth=512 origin main:refs/remotes/origin/main
[ "$(git -C validation-candidate merge-base HEAD origin/main)" = "$BASELINE_SHA" ] || { echo 'Baseline is not the exact candidate/main merge-base.' >&2; exit 1; }
(
 cd validation-candidate
 mvn -q -s "$root/.validation-settings.xml" -Dmaven.repo.local="$root/.validation-m2" -pl broker-execution-service -am -DskipTests clean package dependency:copy-dependencies -DincludeScope=test -DoutputDirectory="$root/bundle/lib"
)
(
 cd validation-baseline
 mvn -q -s "$root/.validation-settings.xml" -Dmaven.repo.local="$root/.validation-m2" -pl broker-execution-service -am -Dmaven.test.skip=true clean package
)
rsync -a --delete validation-candidate/broker-execution-service/target/classes/ bundle/candidate/classes/
rsync -a --delete validation-candidate/broker-execution-service/target/test-classes/ bundle/candidate/test-classes/
rsync -a --delete validation-baseline/broker-execution-service/target/classes/ bundle/baseline/classes/
cp validation-web/src/app/web/assets/context-tape-orders-model.js bundle/web/
git -C validation-web fetch --depth=512 origin main:refs/remotes/origin/main
web_baseline="$(git -C validation-web merge-base HEAD origin/main)"
mkdir -p bundle/web/{baseline,candidate}
for asset in context-tape-orders-model.js context-tape-orders-overlay.js context-tape-orders.css; do
  cp "validation-web/src/app/web/assets/$asset" "bundle/web/candidate/$asset"
  git -C validation-web show "$web_baseline:src/app/web/assets/$asset" > "bundle/web/baseline/$asset"
done
cp scripts/validation/run-visual-orders-es4.sh scripts/validation/visual-orders-model-probe.cjs scripts/validation/linked-exits-browser-probe.cjs scripts/validation/headless-overlay-session.cjs bundle/scripts/validation/
{
  printf 'infrastructure=%s\ncandidate=%s\nbaseline=%s\nweb=%s\n' "$(git rev-parse HEAD)" "$CANDIDATE_SHA" "$BASELINE_SHA" "$WEB_SHA"
  printf 'webBaseline=%s\n' "$web_baseline"
  shasum -a 256 bundle/lib/*.jar
} > bundle/source-identities.txt
