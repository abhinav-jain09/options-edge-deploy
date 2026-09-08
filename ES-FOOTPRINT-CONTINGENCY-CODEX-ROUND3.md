Reading additional input from stdin...
2026-09-08T19:05:03.077960Z ERROR codex_models_manager::manager: failed to load models cache: missing field `supports_parallel_tool_calls` at line 93 column 5
OpenAI Codex v0.147.0
--------
workdir: /private/tmp/oe-deploy-mirror
model: gpt-5.6-sol
provider: openai
approval: never
sandbox: read-only
reasoning effort: low
reasoning summaries: none
session id: 01a08268-d4ca-7821-b87e-8ed3b3382710
--------
user
Round 3 of the review of options-edge-deploy PR #1010 at /private/tmp/oe-deploy-mirror, branch
feat/footprint-gateway-contingency-guard. Your round-1 verdict was REQUEST_CHANGES with two HIGH
findings and several claim-by-claim notes.

Round-1 findings and what was done:
 1. HIGH — relay-off exemption failed open. FOLDED: the flag is now required to appear exactly once
    and hold exactly `true` or `false`; every other state is refused (env_count + a case statement).
    Three new mutation cases drive missing, non-canonical, and duplicated (false-then-true) flags.
 2. HIGH — the deployment path did not run the guard. FOLDED: Jenkinsfile.es4-deploy now runs it in
    the same place as validate-dealer-ledger-fire-quality.sh, before the first apply, unconditionally.
 3. MEDIUM (from your claim-by-claim audit) — expected-failure cases asserted only exit code 1, so an
    unrelated failure could "kill" a case for the wrong reason. FOLDED: expect() now also requires a
    message substring specific to the refusal under test, and prints the guard's output on mismatch.

Verify each of the three against the actual files — do not take this summary as evidence. Then
review the WHOLE change again line by line for anything still wrong, including:
  * whether env_count and env_value agree about what an "active" entry is (comments, indentation,
    a name appearing inside another value, list items in other blocks of the same file);
  * whether the new Jenkinsfile placement is inside the right stage and cannot be skipped by any
    ACTION or DEPLOY_DRY_RUN path that still applies the manifests;
  * whether the message substrings the test now asserts are stable against a reasonable future edit
    to the guard, or whether they make the suite brittle for no gain;
  * whether the guard should use yq like validate-services.sh instead of awk, and what concretely
    breaks if it does not.

End with exactly one line: VERDICT: APPROVE  or  VERDICT: REQUEST_CHANGES

ROUND-2 FINDING AND WHAT WAS DONE:
 HIGH — the guard parsed YAML globally with awk and could validate a sidecar in the gateway's place.
 FOLDED: the guard now selects the Deployment and container with yq (the
 validate-dealer-ledger-fire-quality.sh pattern), asserts the selection resolved exactly once, and
 reads env and resources.limits.memory from THAT container only. Refusals carry stable E_*
 identifiers and the mutation suite asserts those instead of prose (your maintainability note).
 Two new cases cover a sidecar declared before feed-gateway.

Verify against the files. Check in particular: the yq expressions are correct for BOTH manifests as
written; the "resolved exactly once" count cannot be fooled; env_count/env_value operate on the
rendered container and not the file; the sidecar fixture in the test really is inserted before
feed-gateway and really would have fooled the previous awk version; and whether yq v4.44.3 (the CI
pin) supports every expression used here.
codex
I’ll inspect the repository instructions and the complete PR diff, then trace the guard, Jenkins execution paths, fixtures, and pinned yq compatibility against the actual code.
exec
/bin/bash -lc "pwd && rg --files -g 'AGENTS.md' -g '"'!node_modules'"' -g '"'!vendor'"' .. | head -50 && git status --short && git branch --show-current && git diff --stat HEAD~1..HEAD" in /private/tmp/oe-deploy-mirror
 succeeded in 36264ms:
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-u8ahpeie' (errno=Operation not permitted)
2026-09-08 21:05:15.728 xcodebuild[76444:265488358]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:05:16.660 xcodebuild[76444:265488350] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-7dhHT8kA' (errno=Operation not permitted)
2026-09-08 21:05:18.423 xcodebuild[76602:265488735]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:05:19.060 xcodebuild[76602:265488717] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-BqPfW5Ky' (errno=Operation not permitted)
2026-09-08 21:05:24.860 xcodebuild[76894:265489522]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:05:26.138 xcodebuild[76894:265489518] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-50SgpPiD' (errno=Operation not permitted)
2026-09-08 21:05:29.770 xcodebuild[77032:265490092]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:05:31.024 xcodebuild[77032:265490086] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
feat/footprint-gateway-contingency-guard
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-hlDtxUv7' (errno=Operation not permitted)
2026-09-08 21:05:37.791 xcodebuild[77315:265490931]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:05:38.512 xcodebuild[77315:265490930] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-CVUHQmIt' (errno=Operation not permitted)
2026-09-08 21:05:41.435 xcodebuild[77384:265491147]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:05:43.227 xcodebuild[77384:265491143] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
 .../validate-footprint-gateway-contingency-test.sh |  50 +++++++--
 .../ci/validate-footprint-gateway-contingency.sh   | 119 ++++++++++++---------
 2 files changed, 108 insertions(+), 61 deletions(-)

codex
The environment’s macOS toolchain is trying to create cache files despite this being a read-only review. I’m working around that without modifying the checkout and will base the review on readable refs/files.
exec
/bin/bash -lc 'find .. -name AGENTS.md -print 2>/dev/null; /usr/bin/git --no-optional-locks status --short; /usr/bin/git --no-optional-locks branch --show-current; /usr/bin/git --no-optional-locks log -5 --oneline' in /private/tmp/oe-deploy-mirror
2026-09-08T19:06:09.785489Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
2026-09-08T19:06:28.839306Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
 succeeded in 33208ms:
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-Jzv2YBMx' (errno=Operation not permitted)
2026-09-08 21:06:11.078 xcodebuild[78538:265494119]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:06:12.113 xcodebuild[78538:265494099] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-kLghIZpg' (errno=Operation not permitted)
2026-09-08 21:06:14.221 xcodebuild[78711:265494522]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:06:14.890 xcodebuild[78711:265494513] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-pG4MbZMf' (errno=Operation not permitted)
2026-09-08 21:06:18.859 xcodebuild[78888:265494965]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:06:19.453 xcodebuild[78888:265494964] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-mGndEfi4' (errno=Operation not permitted)
2026-09-08 21:06:21.138 xcodebuild[78962:265495161]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:06:21.849 xcodebuild[78962:265495154] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
feat/footprint-gateway-contingency-guard
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-aYoX4kQ4' (errno=Operation not permitted)
2026-09-08 21:06:26.085 xcodebuild[79111:265495575]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:06:26.662 xcodebuild[79111:265495562] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-gFooCiAn' (errno=Operation not permitted)
2026-09-08 21:06:28.457 xcodebuild[79202:265495821]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:06:29.099 xcodebuild[79202:265495816] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
24926631 fix(footprint guard): read the SELECTED container with yq, not the first block in the file
ccd6374b fix(footprint guard): close the fail-open exemption and run it on the deploy path
52c60ebb ci(footprint): the G-R8 contingency is checked, not trusted
226a36fc Merge pull request #1009 from abhinav-jain09/feat/web-service-suppress-nested-autodeploy
335a6a73 fix(web-service-deploy): suppress the build job's dev auto-deploy (no double-rollout)

exec
/bin/bash -lc 'git diff --name-status origin/master...HEAD 2>/dev/null || git diff --name-status origin/main...HEAD 2>/dev/null; git log --oneline --decorate -8 2>/dev/null' in /private/tmp/oe-deploy-mirror
2026-09-08T19:07:14.608433Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
2026-09-08T19:07:33.883438Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
 succeeded in 35209ms:
M	.github/workflows/deploy-validation.yml
M	Jenkinsfile.es4-deploy
A	scripts/ci/validate-footprint-gateway-contingency-test.sh
A	scripts/ci/validate-footprint-gateway-contingency.sh
24926631 (HEAD -> feat/footprint-gateway-contingency-guard, origin/feat/footprint-gateway-contingency-guard) fix(footprint guard): read the SELECTED container with yq, not the first block in the file
ccd6374b fix(footprint guard): close the fail-open exemption and run it on the deploy path
52c60ebb ci(footprint): the G-R8 contingency is checked, not trusted
226a36fc (origin/main, origin/HEAD) Merge pull request #1009 from abhinav-jain09/feat/web-service-suppress-nested-autodeploy
335a6a73 (origin/feat/web-service-suppress-nested-autodeploy, feat/web-service-suppress-nested-autodeploy) fix(web-service-deploy): suppress the build job's dev auto-deploy (no double-rollout)
211e9e46 fix(gateway): dev — keep the pre-open STATUS plane, stand the VALUE plane down (#1008)
0c4a7aa4 fix(gateway): turn pre-open GEX arbitration off — it drops 100% of pre-open records (#1006)
9b48bbf7 fix(es-auction mirror): an unterminated brace group made the preflight die at EOF (#1004)

exec
/bin/bash -lc 'git diff --no-ext-diff --unified=100 origin/master...HEAD -- .github/workflows/deploy-validation.yml Jenkinsfile.es4-deploy scripts/ci/validate-footprint-gateway-contingency.sh scripts/ci/validate-footprint-gateway-contingency-test.sh 2>/dev/null || git diff --no-ext-diff --unified=100 origin/main...HEAD -- .github/workflows/deploy-validation.yml Jenkinsfile.es4-deploy scripts/ci/validate-footprint-gateway-contingency.sh scripts/ci/validate-footprint-gateway-contingency-test.sh 2>/dev/null' in /private/tmp/oe-deploy-mirror
2026-09-08T19:07:53.632474Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
 succeeded in 17617ms:
diff --git a/.github/workflows/deploy-validation.yml b/.github/workflows/deploy-validation.yml
index 9907b81e..762d207d 100644
--- a/.github/workflows/deploy-validation.yml
+++ b/.github/workflows/deploy-validation.yml
@@ -1,178 +1,189 @@
 name: Deploy Validation
 
 on:
   pull_request:
     branches:
       - main
   push:
     branches:
       - main
   workflow_dispatch:
 
 permissions:
   contents: read
 
 jobs:
   deploy-validation:
     runs-on: ubuntu-latest
     steps:
       - name: Check out repository
         uses: actions/checkout@v4
 
       - name: Install kubectl
         run: |
           set -euo pipefail
           curl -fsSLo kubectl "https://dl.k8s.io/release/v1.36.2/bin/linux/amd64/kubectl"
           chmod +x kubectl
           sudo mv kubectl /usr/local/bin/kubectl
 
       - name: Install yq
         run: |
           set -euo pipefail
           # PINNED, and the validators must be exercised against THIS version. A yq-version
           # difference (4.44 vs 4.53) silently turned validate-preopen-single-publisher into a
           # permanently red check that could not distinguish a real misconfiguration from the
           # tool: the `//` alternative operator is evaluated per document, so every non-matching
           # document in a rendered multi-doc stream produced its own default line.
           curl -fsSLo yq "https://github.com/mikefarah/yq/releases/download/v4.44.3/yq_linux_amd64"
           chmod +x yq
           sudo mv yq /usr/local/bin/yq
 
       - name: Service slices are regenerated (no drift)
         # Catches the recurring mirror-drift class: someone tunes a service's patch in the
         # monolith overlay (k8s/overlays/*) but forgets to re-run the slice generator, so the
         # generated k8s/services/*/overlays/* slices go stale and every deploy's Validate stage
         # fails later. Run the generator here and fail the PR if it would change any committed
         # slice — i.e. the author must commit the regenerated output.
         run: |
           set -euo pipefail
           bash scripts/deploy/generate-service-slices.sh
           if ! git diff --quiet -- k8s/services; then
             echo "::error::Service slices are out of date. Run scripts/deploy/generate-service-slices.sh and commit the regenerated k8s/services/**."
             git --no-pager diff --stat -- k8s/services
             echo "----- full diff -----"
             git --no-pager diff -- k8s/services
             exit 1
           fi
           echo "Service slices are up to date."
 
       # The same drift class one environment over, and a worse blast radius: k8s/es4/services/**
       # is generated from the PRODUCTION slices, so a prod env change silently queues itself up
       # for es4 — a LIVE environment — and lands whenever someone next re-renders for an
       # unrelated reason. Three such problems were sitting on main when this gate was added: an
       # inherited DATABENTO_GEX_DYNAMIC_OI_ENABLED flip, the IBKR_GEX_* pre-open wiring, and two
       # manifests that fell out of the renderer's SERVICES list yet are still deployed by
       # deploy-all. The gate therefore checks the file SET as well as file contents — deploy-all
       # globs the directory, so an un-rendered or uncommitted file there still reaches the box.
       # PyYAML is pinned to the version the committed manifests were rendered with: the check
       # compares emitted YAML byte-for-byte, so a formatting change between releases would read
       # as drift.
       - name: Install PyYAML
         run: python3 -m pip install --quiet 'pyyaml==6.0.3'
 
       - name: es4 manifests are regenerated and fully accounted for (no drift)
         run: bash scripts/ci/validate-es4-render.sh
 
       - name: Validate service registry (completeness + mirror + image name)
         run: scripts/ci/validate-services.sh
 
+      # ES Footprint G-R8: the record ceiling is ONE value across two manifests, and the relay's
+      # working set lives outside the heap. Neither fact is visible from either manifest alone,
+      # and both fail SILENTLY in production — a producer above the gateway's ceiling loses records
+      # at the relay with nothing logged upstream, and a container whose limit equals its -Xmx is
+      # OOM-killed on native growth the heap never sees. Checked only where the relay is enabled.
+      - name: ES Footprint deployment contingency (paired ceiling, G-R8 headroom)
+        run: scripts/ci/validate-footprint-gateway-contingency.sh
+
+      - name: ES Footprint contingency guard refuses each of its own violations
+        run: scripts/ci/validate-footprint-gateway-contingency-test.sh
+
       # VIX feed separation §7: validate-services.sh already runs the single-publisher
       # assertion against the REAL render; this additionally exercises the assertion's
       # full predicate matrix (five planned rollout states, both-owners violations,
       # runtime-equivalent spellings, fail-closed states) against crafted fixtures, so
       # a predicate regression cannot hide behind a currently-green render.
       - name: VIX single-publisher assertion test matrix
         run: python3 -m unittest -v tests.test_vix_single_publisher
 
       # One Service One Identity Rule: no service identity (Streams application.id / consumer group)
       # may carry a version suffix. FAIL-CLOSED — a red result here blocks the merge. This exists
       # because oi-shadow was born as "oi-shadow-v1" eleven days AFTER the platform-wide unversioning
       # migration and survived five weeks: a point-in-time sweep cannot see what is created after it.
       - name: Service identities are unversioned (One Service One Identity Rule)
         run: python3 -m unittest -v tests.test_service_identity_unversioned
 
       # Every tests/test_*.py must be COLLECTABLE by unittest, the only runner this repo has.
       # Three files were written as bare module-level test_ functions (pytest style) and so ran as
       # "0 tests ... OK" — including one carrying a genuinely failing assertion nobody could see.
       - name: Test files are collectable by unittest (no silently-empty suites)
         run: bash scripts/ci/validate-tests-are-collectable.sh
 
       # The es4 topic contract: topics.env is the single source of truth for the es4 broker, and
       # these two modules are what hold it to that. Both were among the uncollectable files above,
       # so neither had ever run here.
       # The close chain's session arithmetic, and the rule that no shell script may do date
       # arithmetic at all: `date -v` (BSD) vs `date -d` (GNU) is decided by the agent's PATH, and
       # that bet cost the 2026-09-01 morning catch-up its entire run.
       - name: Close-board session resolution + shell date-portability guard
         run: python3 -m unittest -v tests.test_close_board_session_resolution
 
       - name: es4 topic SSOT + partition contract
         run: python3 -m unittest -v tests.test_es4_topics_ssot tests.test_es4_topic_partition_contract tests.test_remove_retired_workloads
 
       - name: Validate dev GEX availability safeguards
         run: bash scripts/ci/validate-gex-dev-safety.sh
 
       # A mirrored es4 topic has two owners that both write its shape — the MM1 job that creates
       # and asserts it, and apply-topics.sh, which rewrites retention.ms + cleanup.policy on every
       # declared topic on every deploy. Undeclared, it was a deletion candidate for
       # cleanup-topics.sh; declared with the wrong shape, the deploy reconciles the mirror's own
       # contract away. This makes the two declarations unmergeable when they disagree.
       # (The retention=-1 preserved-or-rebuildable classification that es.tape-zones.board now
       # depends on is already gated: validate-services.sh runs
       # scripts/ci/validate-durable-topic-preservation.sh as its section 6.)
       - name: Validate mirrored es4 topic contracts (mirror jobs vs topics.env)
         run: bash scripts/ci/validate-mirrored-topic-contracts.sh
 
       # A Groovy pipeline is not shell-checked by anything. Jenkinsfile.es-auction-mirror shipped with
       # an unterminated brace group (`fi; { : }`) that Groovy and review both accepted and that failed
       # at RUNTIME on the job's first install, after the preflight had already talked to both brokers.
       - name: Validate the es-auction mirror's shell blocks parse
         run: bash scripts/ci/validate-jenkinsfile-shell-blocks.sh
 
       # topics.env is only the DEPLOY-TIME half of "the archive is never compacted". The other half
       # is the boot-time stamp: processing-common KafkaTopics.ensureTopic() runs
       # incrementalAlterConfigs on every service start, so a topic the deploy just set to delete is
       # re-compacted by the next restart unless OPTIONS_EDGE_UNCOMPACTED_SERVED_TOPICS="true" is in
       # the environment the services actually read. Compaction kept coming back on prod for exactly
       # this reason — each repair fixed one writer. This asserts the rendered environment carries
       # the flag on prod and es4, and that it never leaks into dev (which must stay compacted).
       - name: Validate archive uncompaction flag reaches prod + es4 and not dev
         run: bash scripts/ci/validate-archive-uncompaction-env.sh
 
       # "checked 5 mirrored topic(s)" proves only that five names were found. This drives the
       # validator against crafted fixtures — one broken dimension each, plus the mutations that
       # make its own parsing return nothing — so a checker that quietly starts approving
       # everything fails here instead of going green.
       - name: Mirrored-topic contract validator mutation suite
         run: bash scripts/ci/validate-mirrored-topic-contracts-test.sh
 
       # topics.env agreeing with the mirror job is only half the contract: --create --if-not-exists
       # binds nothing on an EXISTING target, so the job's own preflight assertions are what stand
       # between a drifted topic and a mirror that runs against it. This drives those assertions —
       # extracted from the Jenkinsfile, never copied — against mocked brokers, including a
       # pre-existing pure-compact target sitting at compact,delete.
       - name: es-cvd-mirror source/target shape assertions
         run: bash scripts/ci/es-cvd-mirror-shape-test.sh
 
       # Same contract for the auction-desk mirror (es.futures.auction), whose job additionally
       # RECONCILES a pre-existing target's config before asserting it -- so the suite also proves
       # a wrong-partition target is refused untouched and an ineffective alter still fails.
       - name: es-auction-mirror source/target shape assertions
         run: bash scripts/ci/es-auction-mirror-shape-test.sh
 
       - name: Validate image-pinning completeness (every rendered image wired into resolve-images + apply)
         run: scripts/ci/validate-image-pinning.sh
 
       # DBP-R4 drift gate. The 2026-07-24 Databento incident was caused by a feed deployment that
       # mounted only its API-key Secret and therefore inherited library code defaults
       # (DATABENTO_USE_LIVE_REPLAY=true, DATABENTO_PUBLISH_INTERVAL_MS=250). This resolves the FULL
       # rendered environment (env over envFrom, as Kubernetes does) and fails on any mandatory key
       # that is left to a default.
       - name: Validate feed configuration is fully explicit (no inherited library defaults)
         run: bash scripts/ci/validate-feed-config-explicit.sh
 
       # U16 (CL-R10 L4/O3): promtool check + unit tests over the CVD SPX levels paging rules.
       # The script downloads a sha256-pinned promtool when none is on PATH, so it is mandatory
       # here on the GitHub-hosted runner, not just runnable where prometheus is installed.
       - name: Validate CVD SPX levels alert rules (promtool check + test)
         run: bash scripts/ci/validate-cvd-levels-alerts.sh
diff --git a/Jenkinsfile.es4-deploy b/Jenkinsfile.es4-deploy
index d0fcf27c..352e5b8c 100644
--- a/Jenkinsfile.es4-deploy
+++ b/Jenkinsfile.es4-deploy
@@ -82,200 +82,208 @@ pipeline {
           ssh -o BatchMode=yes "$ES4_HOST" 'sudo install -d -o "$(id -un)" -g "$(id -gn)" /home/es4 /home/es4/repo /home/es4/repo/infra /home/es4/repo/scripts'
           rsync -az --delete infra/es4  "$ES4_HOST":/home/es4/repo/infra/
           # ES helpers call shared siblings under scripts/kafka. Sync the complete repo-owned
           # scripts tree so an on-box cleanup never runs with a partial dependency graph.
           rsync -az --delete scripts/ "$ES4_HOST":/home/es4/repo/scripts/
           ssh -o BatchMode=yes "$ES4_HOST" 'bash /home/es4/repo/scripts/es4/bootstrap-es4.sh'
         '''
       }
     }
     stage('Create topics') {
       when { expression { params.ACTION == 'create-topics' } }
       steps {
         sh '''
           set -euo pipefail
           rsync -az --delete scripts/ "$ES4_HOST":/home/es4/repo/scripts/
           ssh -o BatchMode=yes "$ES4_HOST" 'bash /home/es4/repo/scripts/es4/create-es-topics.sh'
           # STEADY-state audit: this action runs against a LIVE es4, so every owner has already
           # applied its own topic contract and the broker supplies the numbers. For the es4 set
           # topics.env is the EXACT desired shape — a clean-reset rebuilds the world from it, so a
           # width the file does not record does not survive. A live-vs-declared disagreement is
           # therefore something to ADJUDICATE (record the deliberate widening, or fix the topic),
           # and the script prints both remedies rather than assuming which applies. Running
           # create-topics WITHOUT this audit is exactly how topics.env drifted on 24 topics.
           ssh -o BatchMode=yes "$ES4_HOST" 'bash /home/es4/repo/scripts/es4/verify-topic-partition-contract.sh steady'
         '''
       }
     }
     stage('Clean reset (atomic Kafka + Schema Registry wipe)') {
       // Wipe Kafka data (all topics + _schemas) TOGETHER so no message can outlive its schema
       // id — the fix for the dead-schema-id poison ("Malformed data. Length is negative") that
       // wedged the scorer. DESTRUCTIVE: drops all es4 Kafka data; honor DEPLOY_DRY_RUN to preview.
       //
       // Historically the es-feed lived OUTSIDE the es4 cluster (on .252) and had to be fenced here
       // with the prod kubeconfig, or it would reconnect mid-wipe and re-emit cached schema ids into
       // the fresh cluster, recreating the poison. Since DBP-R33 the feed lives IN the es4 cluster,
       // so the on-box cleanup fences it along with every other es4 Deployment and no cross-cluster
       // fencing is needed. What remains below is a guard against that cleanup restoring it against
       // the declared intent.
       when { expression { params.ACTION == 'clean-reset' } }
       steps {
         sh '''
           set -euo pipefail
           rsync -az --delete scripts/ "$ES4_HOST":/home/es4/repo/scripts/
 
           # The prod-cluster es-feed is RETIRED (DBP-R33), so there is no prod feed to fence around
           # the wipe any more — the only es-feed lives in the es4 cluster and the on-box cleanup
           # already fences es4 Deployments itself. The prod kubeconfig is no longer needed here.
           #
           # ⚠️ What still matters: the on-box wipe (scripts/es4/cleanup-es4.sh) restores es4
           # Deployments to their CAPTURED replica counts, which is blind to ES_FEED_LOCATION. If es4
           # es-feed is live while the declared location is not es4, that restore brings it back
           # against intent. Fail closed BEFORE the wipe.
           # Prove BOTH kubeconfigs are the clusters we think they are before anything else. Without
           # this on the clean-reset path, swapped kubeconfigs would let the absence check inspect the
           # wrong cluster and pass.
           bash scripts/es4/assert-cluster-identity.sh "$PROD_KUBECONFIG" prod "$PROD_NS_UID"
           bash scripts/es4/assert-cluster-identity.sh "$ES4_KUBECONFIG"  es4  "$ES4_NS_UID"
 
           # (a) The on-box cleanup treats ES4_FEED_FENCED=1 as PROOF that the cross-cluster prod
           #     es-feed producer was quiesced. Since this stage no longer fences prod, that value
           #     would otherwise be an unchecked assertion — and a prod es-feed that still exists can
           #     publish straight through the wipe and recreate the dead-schema-id poison. Prove it.
           bash scripts/es4/assert-prod-es-feed-absent.sh "$PROD_KUBECONFIG"
 
           # (b) The cleanup no longer restores es-feed at all — it is always left at 0, whatever the
           #     snapshot captured, because DBP-R22 reserves starting it to the separate activate
           #     action. An earlier attempt passed a desired-replica count in instead; review showed
           #     that turned clean-reset INTO an activation path when the declared location was es4,
           #     and left an unset/arbitrary value unsafe. Not restoring it at all has neither hazard.
           #     After a reset, start the feed deliberately with ACTION=activate-es-feed.
 
           if [ "${DEPLOY_DRY_RUN}" = "true" ]; then
             echo "DRY: would run the on-box wipe (no prod es-feed to fence — retired per DBP-R33)"
             ssh -o BatchMode=yes "$ES4_HOST" "DEPLOY_DRY_RUN=true ES4_FEED_FENCED=1 ES4_EXPECTED_NS_UID=$ES4_NS_UID bash /home/es4/repo/scripts/es4/cleanup-es4.sh"
             exit 0
           fi
 
           echo "=== running on-box atomic wipe ==="
           ssh -o BatchMode=yes "$ES4_HOST" "DEPLOY_DRY_RUN=false ES4_FEED_FENCED=1 ES4_EXPECTED_NS_UID=$ES4_NS_UID bash /home/es4/repo/scripts/es4/cleanup-es4.sh"
         '''
       }
     }
     stage('Deploy to es4') {
       when { expression { params.ACTION == 'deploy-service' || params.ACTION == 'deploy-all' } }
       steps {
         sh '''
           set -euo pipefail
           export KUBECONFIG="$ES4_KUBECONFIG"
           DRY=""; [ "${DEPLOY_DRY_RUN:-false}" = "true" ] && DRY="--dry-run=server"
           # Identity FIRST: this stage mutates through $KUBECONFIG (es4). Proving identity after the
           # first apply would mean a mispointed kubeconfig had already written to the wrong cluster.
           bash scripts/es4/assert-cluster-identity.sh "$KUBECONFIG"      es4  "$ES4_NS_UID"
           bash scripts/es4/assert-cluster-identity.sh "$PROD_KUBECONFIG" prod "$PROD_NS_UID"
 
           # Dependency-scoped FIRE-input verdict (processing PR #594): es4 applies the COMMITTED
           # k8s/es4/services/*.yaml directly, so no overlay render exists for the other deploy paths'
           # guard to have caught. Assert here, BEFORE the first apply, and unconditionally — the
           # deploy-service path can target dealer-ledger, and deploy-all always includes it.
           bash scripts/ci/validate-dealer-ledger-fire-quality.sh
 
+          # Same reason, same place: es4 applies the COMMITTED k8s/es4/services/*.yaml directly, so
+          # the ES Footprint contingency has no overlay render for another path's guard to catch.
+          # Both facts it checks fail SILENTLY in production — a producer above the gateway's record
+          # ceiling loses records at the relay with nothing logged upstream, and a gateway whose
+          # memory limit equals its -Xmx is OOM-killed on native growth the heap never sees (G-R8).
+          # Unconditional: deploy-service can target either half of the pair, and deploy-all takes both.
+          bash scripts/ci/validate-footprint-gateway-contingency.sh
+
           kubectl apply $DRY -f k8s/es4/es4-base-env.yaml
           kubectl apply $DRY -f k8s/es4/es4-common-env.yaml
           # es-feed's OWN Databento configmap. Applied for every es4 deploy so the feed can
           # never run against inherited library defaults again (requirement DBP-R1/R2).
           kubectl apply $DRY -f k8s/es4/es4-feed-config.yaml
           mkdir -p .jenkins-tmp
 
           # ---- prod es-feed is RETIRED (DBP-R33): reconcile-delete it on every run ---------
           # `kubectl apply` never prunes, so removing the manifest from git does not remove the live
           # object. --ignore-not-found makes this idempotent, so it is not a one-time cleanup but a
           # standing guard: if anything ever recreates es-feed on prod, the next es4 deploy removes
           # it. Two copies cannot exist if only one location has a Deployment at all - which is why
           # this replaces the publisher-lease work (DBP-R28) rather than complementing it.
           if [ -z "$DRY" ]; then
             # Prove the kubeconfig really is the PROD cluster before deleting anything through it.
             # Without this, a PROD_KUBECONFIG that had been regenerated to point at .4 would delete
             # the LIVE es4 es-feed and the ConfigMap applied moments earlier in this same stage.
             KUBECONFIG="$PROD_KUBECONFIG" kubectl -n options-edge delete deployment es-feed --ignore-not-found
             KUBECONFIG="$PROD_KUBECONFIG" kubectl -n options-edge delete configmap es-feed-databento-config --ignore-not-found
           fi
 
           # ---- es-feed preflight: fail CLOSED before the DEPLOYMENT is applied -----------
           # (The three shared configmaps above are applied first; this is not "before all
           # mutation", it is before anything that could create or change the feed workload.)
           # Three independent hazards, each of which has already bitten us:
           #  (1) the shared Databento API key cannot carry two concurrent ES option-chain
           #      sessions, and both locations share the es-feed-spot consumer group;
           #  (2) the API-key secret does not exist in the es4 cluster yet — a missing or empty
           #      secret must be a loud failure, not a CrashLoop;
           #  (3) activation must never be a deploy side effect (requirement DBP-R22).
           if [ "$ACTION" = "deploy-service" ] && [ "$SERVICE" = "es-feed" ]; then
             bash scripts/es4/assert-es-feed-exclusive.sh "$ES4_KUBECONFIG" "$PROD_KUBECONFIG" es4
           fi
           # es-feed is EXCLUDED from deploy-all on purpose: it owns a Databento live session on a
           # shared API key, so it must only ever be applied as a deliberate, individually preflighted
           # action (SERVICE=es-feed) — never swept in by a bulk deploy that bypasses the preflight
           # above. `|| true` because grep exits 1 when it filters nothing, which under
           # `set -euo pipefail` would abort the whole deploy.
           if [ "$ACTION" = "deploy-all" ]; then
             # The spread-skew stack is under an operator hold (2026-07-26): bulk
             # deploys must not even apply it, in addition to its manifests being
             # pinned at replicas 0.
             FILES=$(ls k8s/es4/services/*.yaml | { grep -Ev '/(es-feed|spread-skew|spread-skew-postgres-writer)[.]yaml$' || true; })
             [ -n "$FILES" ] || { echo "deploy-all: no service manifests resolved" >&2; exit 1; }
             echo "deploy-all: es-feed and the held spread-skew stack intentionally EXCLUDED"
           else
             FILES="k8s/es4/services/${SERVICE}.yaml"
           fi
           for f in $FILES; do
             bash scripts/es4/pin-and-apply.sh "$f" "$REGISTRY" "$DRY"
           done
           if [ -z "$DRY" ] && { [ "$ACTION" = "deploy-all" ] || [ "$SERVICE" = "vix-option-inteligence" ]; }; then
             # Remove the legacy identity only after pin-and-apply has completed the replacement
             # rollout, avoiding duplicate consumers on the isolated ES broker.
             kubectl -n options-edge delete deployment zero-dte-intelligence-service --ignore-not-found
             kubectl -n options-edge delete service zero-dte-intelligence-service --ignore-not-found
           fi
           # es-feed is NEVER started by a deploy: the manifest always carries replicas 0 and this
           # path performs no scaling at all — activation is its own ACTION (requirement DBP-R22).
           if [ "$ACTION" = "deploy-service" ] && [ "$SERVICE" = "es-feed" ]; then
             echo "es-feed manifest applied at replicas 0. To start it: ACTION=activate-es-feed"
           fi
           [ -n "$DRY" ] || kubectl -n options-edge get pods -o wide
         '''
       }
     }
     stage('Activate es-feed (separate authorized action)') {
       when { expression { params.ACTION == 'activate-es-feed' } }
       steps {
         sh '''
           set -euo pipefail
           # Activation is deliberately NOT part of any deploy path (requirement DBP-R22).
           [ "${ES_FEED_LOCATION:-none}" = "es4" ] || {
             echo "activate-es-feed requires ES_FEED_LOCATION=es4 (got '${ES_FEED_LOCATION:-none}'); prod is retired per DBP-R33" >&2; exit 1; }
           # The two preconditions a deploy must never assume on the operator's behalf.
           [ "${ES_FEED_ACK_OI_AND_CAPACITY:-false}" = "true" ] || {
             echo "REFUSING TO ACTIVATE: ES_FEED_ACK_OI_AND_CAPACITY is false." >&2
             echo "  (1) DATABENTO_USE_LIVE_REPLAY=false means a start after the daily OI publication" >&2
             echo "      yields NO ES open interest, and es4 GEX is fail-closed on OI (ESM-Q4)." >&2
             echo "  (2) Real .4 headroom must be MEASURED with the full es4 fleet running (DBP-V10)." >&2
             exit 1; }
           # Activation does not pass through the deploy stage, so prove BOTH cluster identity and
           # prod retirement here too — otherwise a swapped kubeconfig or a surviving prod Deployment
           # would let es4 start alongside a second location.
           bash scripts/es4/assert-cluster-identity.sh "$PROD_KUBECONFIG" prod "$PROD_NS_UID"
           bash scripts/es4/assert-cluster-identity.sh "$ES4_KUBECONFIG"  es4  "$ES4_NS_UID"
           bash scripts/es4/assert-prod-es-feed-absent.sh "$PROD_KUBECONFIG"
           bash scripts/es4/assert-es-feed-exclusive.sh "$ES4_KUBECONFIG" "$PROD_KUBECONFIG" "$ES_FEED_LOCATION"
           KC="$ES4_KUBECONFIG"
           if [ "${DEPLOY_DRY_RUN:-false}" = "true" ]; then
             echo "DRY: would scale $ES_FEED_LOCATION es-feed to 1"; exit 0
           fi
           KUBECONFIG="$KC" kubectl -n options-edge scale deploy/es-feed --replicas=1
           KUBECONFIG="$KC" kubectl -n options-edge rollout status deploy/es-feed --timeout=600s
           echo "es-feed ACTIVE at $ES_FEED_LOCATION. Watch source age, not 'no restarts' —"
           echo "the widened startup grace can mask a client that is hours behind (DBP-R12)."
         '''
       }
     }
     stage('Deactivate es-feed') {
diff --git a/scripts/ci/validate-footprint-gateway-contingency-test.sh b/scripts/ci/validate-footprint-gateway-contingency-test.sh
new file mode 100755
index 00000000..6be1eb76
--- /dev/null
+++ b/scripts/ci/validate-footprint-gateway-contingency-test.sh
@@ -0,0 +1,138 @@
+#!/usr/bin/env bash
+# Mutation test for validate-footprint-gateway-contingency.sh: each guard must REFUSE its own
+# violation. A guard nobody has watched fail is a guard nobody knows is wired up.
+set -euo pipefail
+cd "$(dirname "$0")/../.."
+
+WORK=$(mktemp -d)
+trap 'rm -rf "$WORK"' EXIT
+GUARD=scripts/ci/validate-footprint-gateway-contingency.sh
+GW=k8s/es4/services/es-feed-gateway.yaml
+PROD=k8s/es4/services/es-cvd.yaml
+
+OUT="$WORK/out"
+
+run_against() {   # $1 = gateway manifest, $2 = producer manifest -> exit code; output in $OUT
+    local d="$WORK/case"; rm -rf "$d"; mkdir -p "$d/scripts/ci" "$d/k8s/es4/services"
+    cp "$GUARD" "$d/scripts/ci/"
+    cp "$1" "$d/k8s/es4/services/es-feed-gateway.yaml"
+    cp "$2" "$d/k8s/es4/services/es-cvd.yaml"
+    ( cd "$d" && ./scripts/ci/validate-footprint-gateway-contingency.sh ) > "$OUT" 2>&1 && echo 0 || echo $?
+}
+
+# A refusal is only a kill if it is THIS refusal. Asserting the exit code alone lets an unrelated
+# failure — a typo in the guard, a missing file, a parse error — count as every mutation being
+# caught, which is the failure mode a mutation suite exists to rule out. The assertion is on the
+# guard's STABLE identifier (E_*), not its prose, so rewording a message cannot break this suite.
+expect() {   # $1 = expected code, $2 = expected message substring ("" when passing), $3 = label, $4.. = manifests
+    local want="$1" msg="$2" label="$3"; shift 3
+    local got; got=$(run_against "$@")
+    if [ "$got" != "$want" ]; then
+        printf 'MUTATION SURVIVED: %s (guard exited %s, expected %s)\n' "$label" "$got" "$want" >&2
+        sed 's/^/    | /' "$OUT" >&2
+        exit 1
+    fi
+    if [ -n "$msg" ] && ! grep -qF -- "$msg" "$OUT"; then
+        printf 'WRONG REASON: %s exited %s but not for the reason under test\n' "$label" "$got" >&2
+        printf '    expected to contain: %s\n' "$msg" >&2
+        sed 's/^/    | /' "$OUT" >&2
+        exit 1
+    fi
+    printf '  killed: %s\n' "$label"
+}
+
+echo "baseline"
+expect 0 "footprint contingency OK" "the manifests as committed pass" "$GW" "$PROD"
+
+echo "mutations"
+# 1. the two ceilings diverge
+sed 's/^\( *\)value: "262144"/\1value: "524288"/' "$PROD" > "$WORK/prod-bigger.yaml"
+expect 1 "[E_CEILING_MISMATCH]" "producer ceiling above the gateway's" "$GW" "$WORK/prod-bigger.yaml"
+
+# 2. the producer stops declaring a ceiling at all
+grep -v 'FOOTPRINT_MAX_RECORD_BYTES' "$PROD" | grep -v '^ *value: "262144"' > "$WORK/prod-none.yaml"
+expect 1 "[E_ENV_COUNT]" "producer declares no ceiling" "$GW" "$WORK/prod-none.yaml"
+
+# 3. the limit is lowered back to the heap size (the incident this rule exists for)
+sed 's/^\( *\)memory: "2560Mi"/\1memory: "1536Mi"/' "$GW" > "$WORK/gw-tight.yaml"
+expect 1 "[E_HEADROOM]" "limit equal to -Xmx: no native headroom" "$WORK/gw-tight.yaml" "$PROD"
+
+# 4. the heap is raised into the headroom instead
+sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2176m/' "$GW" > "$WORK/gw-fat-heap.yaml"
+expect 1 "[E_HEADROOM]" "heap raised to leave under 512 MiB" "$WORK/gw-fat-heap.yaml" "$PROD"
+
+# 5. exactly at the boundary is ACCEPTED — the rule is >=, and a guard that refuses its own
+#    boundary would force every future budget to overshoot it
+sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2048m/' "$GW" > "$WORK/gw-boundary.yaml"
+expect 0 "headroom 512 MiB" "exactly 512 MiB of headroom is legal" "$WORK/gw-boundary.yaml" "$PROD"
+
+# 6. relay off: the guard has nothing to hold up and says so. Flip the flag's OWN value line —
+#    a blanket s/"true"/"false"/ hits the first unrelated flag in the file and passes for the
+#    wrong reason, which is exactly what the first version of this case did.
+awk '$1 == "-" && $2 == "name:" { name = $3 }
+     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/true/, "false") }
+     { print }' "$GW" > "$WORK/gw-off.yaml"
+grep -A1 GATEWAY_ES_FOOTPRINT_ENABLED "$WORK/gw-off.yaml" | grep -q 'value: "false"' \
+    || { echo "the disable mutation did not take: the case would pass for the wrong reason" >&2; exit 1; }
+expect 0 "footprint relay is disabled" "relay disabled: not checked" "$WORK/gw-off.yaml" "$PROD"
+
+# 6b. and with the relay off the guard is not merely quiet — it is quiet BECAUSE of the flag:
+#     the same manifest with a broken ceiling still passes while disabled, and fails once enabled
+awk '$1 == "-" && $2 == "name:" { name = $3 }
+     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES" { sub(/262144/, "999999") }
+     { print }' "$WORK/gw-off.yaml" > "$WORK/gw-off-broken.yaml"
+expect 0 "footprint relay is disabled" "relay disabled hides a ceiling mismatch (by design)" "$WORK/gw-off-broken.yaml" "$PROD"
+awk '$1 == "-" && $2 == "name:" { name = $3 }
+     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/false/, "true") }
+     { print }' "$WORK/gw-off-broken.yaml" > "$WORK/gw-on-broken.yaml"
+expect 1 "[E_CEILING_MISMATCH]" "and refuses the same manifest once the relay is enabled" "$WORK/gw-on-broken.yaml" "$PROD"
+
+# ---- the enablement flag itself must be a decision, never a parse failure -------------------
+# Each of these would, before the fix, have EXEMPTED the manifest from every check above.
+awk '$1 == "-" && $2 == "name:" && $3 == "GATEWAY_ES_FOOTPRINT_ENABLED" { skip = 2 }
+     skip > 0 { skip--; next } { print }' "$GW" > "$WORK/gw-noflag.yaml"
+expect 1 "[E_ENV_COUNT]" "the flag is missing entirely" "$WORK/gw-noflag.yaml" "$PROD"
+
+awk '$1 == "-" && $2 == "name:" { name = $3 }
+     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/true/, "TRUE") }
+     { print }' "$GW" > "$WORK/gw-badflag.yaml"
+expect 1 "[E_FLAG_VALUE]" "the flag is not a canonical boolean" "$WORK/gw-badflag.yaml" "$PROD"
+
+# false FIRST, then true: Kubernetes takes the last, the old guard read the first and stood down
+awk '$1 == "-" && $2 == "name:" && $3 == "GATEWAY_ES_FOOTPRINT_ENABLED" {
+         print "        - name: GATEWAY_ES_FOOTPRINT_ENABLED"; print "          value: \"false\"" }
+     { print }' "$GW" > "$WORK/gw-dupflag.yaml"
+expect 1 "[E_ENV_COUNT]" "the flag is declared twice" "$WORK/gw-dupflag.yaml" "$PROD"
+
+# ---- the guard must read the GATEWAY's container, not whichever block comes first -------------
+# A sidecar declared BEFORE feed-gateway, with its own small limit and its own env, is the case a
+# text scan cannot tell from the real thing: the file still holds every string the old parser
+# looked for, in the same shapes, just belonging to another container.
+awk '
+    $0 ~ /^ *containers: *$/ && !done {
+        print
+        print "      - name: log-shipper"
+        print "        image: 192.168.100.252:5000/log-shipper:prod"
+        print "        env:"
+        print "        - name: GATEWAY_ES_FOOTPRINT_ENABLED"
+        print "          value: \"false\""
+        print "        - name: GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES"
+        print "          value: \"1\""
+        print "        - name: JAVA_TOOL_OPTIONS"
+        print "          value: \"-Xms16m -Xmx32m\""
+        print "        resources:"
+        print "          limits:"
+        print "            memory: \"64Mi\""
+        done = 1
+        next
+    }
+    { print }
+' "$GW" > "$WORK/gw-sidecar.yaml"
+grep -q "log-shipper" "$WORK/gw-sidecar.yaml" || { echo "the sidecar fixture was not built" >&2; exit 1; }
+expect 0 "footprint contingency OK: ceiling 262144" "a sidecar before the gateway does not become the subject" "$WORK/gw-sidecar.yaml" "$PROD"
+
+# and past that sidecar the gateway's OWN violation is still caught
+sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2176m/' "$WORK/gw-sidecar.yaml" > "$WORK/gw-sidecar-bad.yaml"
+expect 1 "[E_HEADROOM]" "and the gateway own violation is still caught past the sidecar" "$WORK/gw-sidecar-bad.yaml" "$PROD"
+
+echo "every guard refused its own violation, for its own reason"
diff --git a/scripts/ci/validate-footprint-gateway-contingency.sh b/scripts/ci/validate-footprint-gateway-contingency.sh
new file mode 100755
index 00000000..148a7a02
--- /dev/null
+++ b/scripts/ci/validate-footprint-gateway-contingency.sh
@@ -0,0 +1,105 @@
+#!/usr/bin/env bash
+# ES Footprint deployment contingency (G-R8), enforced on the manifests rather than trusted.
+#
+# Two facts about this subsystem are only true if two SEPARATE manifests agree, and nothing else
+# in CI or on the deploy path noticed when they stopped agreeing:
+#
+#   1. The record ceiling is ONE value. The gateway drops any record above its own ceiling, so a
+#      producer allowed to emit larger records loses those records SILENTLY at the relay — the
+#      producer is happy, the topic has the bytes, and the page never sees them.
+#   2. G-R8's memory inequality. The relay's extra working set lives OUTSIDE the heap, so a
+#      container whose limit equals its -Xmx is OOM-killed on native growth the heap never sees.
+#      The rule is limit - Xmx >= 512 MiB, and it must hold wherever the relay is ENABLED.
+#
+# Both are read out of the SELECTED Deployment and container with yq, not grepped out of the file:
+# a text scan would also match an init container, a sidecar, a second document, or a commented
+# example, and the first version of this guard took whichever `limits:` block came first — which
+# would have validated a sidecar's memory while the gateway itself ran without headroom.
+# yq is already required on this same Jenkins path by validate-dealer-ledger-fire-quality.sh.
+#
+# Refusals carry a STABLE identifier (E_*). The mutation suite asserts those, so the prose can be
+# reworded without breaking the suite, and a case can never be "killed" by an unrelated failure.
+set -euo pipefail
+cd "$(dirname "$0")/../.."
+
+command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }
+
+fail() { printf 'FOOTPRINT CONTINGENCY [%s]: %s\n' "$1" "$2" >&2; exit 1; }
+
+GW=k8s/es4/services/es-feed-gateway.yaml
+GW_DEPLOYMENT=es-feed-gateway
+GW_CONTAINER=feed-gateway
+PROD=k8s/es4/services/es-cvd.yaml
+PROD_DEPLOYMENT=es-cvd-service
+PROD_CONTAINER=es-cvd
+
+tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
+
+# The one container, rendered. Everything below reads THIS, never the file.
+container() {   # $1 = manifest, $2 = deployment, $3 = container, $4 = output path
+    yq "select(.kind == \"Deployment\" and .metadata.name == \"$2\")
+        | .spec.template.spec.containers[] | select(.name == \"$3\")" "$1" > "$4"
+    [ -s "$4" ] || fail E_CONTAINER "no container '$3' in Deployment '$2' of $1"
+    # Two Deployments of the same name, or two containers of the same name, would make every
+    # question below ambiguous. yq concatenates them, so count the documents it produced.
+    local docs; docs=$(yq 'select(. != null) | .name' "$4" | grep -c . || true)
+    [ "$docs" = "1" ] || fail E_CONTAINER "container '$3' in Deployment '$2' of $1 resolved $docs times"
+}
+
+# Exactly one env entry of this name, and its value. Emptiness is asserted by COUNT, never by a
+# missing value: an entry present with an empty value and an entry absent are different states.
+env_count() { yq -r "[.env[] | select(.name == \"$2\")] | length" "$1"; }
+env_value() { yq -r "[.env[] | select(.name == \"$2\") | .value] | .[-1] // \"\"" "$1"; }
+
+require_one() {   # $1 = container file, $2 = var, $3 = which manifest (for the message)
+    local n; n=$(env_count "$1" "$2")
+    [ "$n" = "1" ] || fail E_ENV_COUNT "$3 declares $2 $n times in container $GW_CONTAINER; exactly one entry decides it"
+}
+
+mib() {   # a Kubernetes quantity in Mi, or empty when this guard cannot compare it
+    printf '%s' "$1" | awk '
+        /^[0-9]+Mi$/ { sub(/Mi$/, ""); print; next }
+        /^[0-9]+Gi$/ { sub(/Gi$/, ""); print $0 * 1024; next }
+        { print "" }'
+}
+
+container "$GW"   "$GW_DEPLOYMENT"   "$GW_CONTAINER"   "$tmp/gw"
+container "$PROD" "$PROD_DEPLOYMENT" "$PROD_CONTAINER" "$tmp/prod"
+
+# ---- the flag is a DECISION, never a parse failure -------------------------------------------
+# Anything other than exactly one entry holding exactly true or false is refused: a missing,
+# malformed or duplicated flag would otherwise exempt the manifest from every check below, and a
+# pair ordered false-then-true would deploy an ENABLED relay past a guard that stood down.
+require_one "$tmp/gw" GATEWAY_ES_FOOTPRINT_ENABLED "$GW"
+enabled=$(env_value "$tmp/gw" GATEWAY_ES_FOOTPRINT_ENABLED)
+case "$enabled" in
+    true) ;;
+    false) printf 'footprint relay is disabled in %s: nothing to check\n' "$GW"; exit 0 ;;
+    *) fail E_FLAG_VALUE "GATEWAY_ES_FOOTPRINT_ENABLED is '$enabled' in $GW; it must be exactly true or false" ;;
+esac
+
+# ---- (1) one ceiling, both sides --------------------------------------------------------------
+require_one "$tmp/gw"   GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES "$GW"
+require_one "$tmp/prod" FOOTPRINT_MAX_RECORD_BYTES            "$PROD"
+gw_bytes=$(env_value "$tmp/gw"   GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES)
+pr_bytes=$(env_value "$tmp/prod" FOOTPRINT_MAX_RECORD_BYTES)
+[ "$gw_bytes" = "$pr_bytes" ] \
+    || fail E_CEILING_MISMATCH "the record ceiling differs: gateway $gw_bytes vs producer $pr_bytes; a producer allowed to emit larger records loses them silently at the relay"
+
+# ---- (2) G-R8: limit - Xmx >= 512 MiB ---------------------------------------------------------
+limit_raw=$(yq -r '.resources.limits.memory // ""' "$tmp/gw")
+[ -n "$limit_raw" ] || fail E_NO_LIMIT "container $GW_CONTAINER in $GW declares no resources.limits.memory"
+limit=$(mib "$limit_raw")
+[ -n "$limit" ] || fail E_LIMIT_UNIT "memory limit '$limit_raw' is neither Mi nor Gi; this guard cannot compare it"
+
+require_one "$tmp/gw" JAVA_TOOL_OPTIONS "$GW"
+xmx_raw=$(env_value "$tmp/gw" JAVA_TOOL_OPTIONS)
+xmx=$(printf '%s' "$xmx_raw" | sed -n 's/.*-Xmx\([0-9]*\)m.*/\1/p')
+[ -n "$xmx" ] || fail E_NO_XMX "no -Xmx found in JAVA_TOOL_OPTIONS ('$xmx_raw')"
+
+headroom=$(( limit - xmx ))
+[ "$headroom" -ge 512 ] \
+    || fail E_HEADROOM "native headroom is ${headroom} MiB (limit ${limit} MiB - Xmx ${xmx} MiB); the relay's working set is OUTSIDE the heap and G-R8 requires at least 512 MiB"
+
+printf 'footprint contingency OK: ceiling %s on both sides; headroom %s MiB (limit %s, Xmx %s)\n' \
+    "$gw_bytes" "$headroom" "$limit" "$xmx"

2026-09-08T19:08:00.357439Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
exec
/bin/bash -lc "nl -ba scripts/ci/validate-footprint-gateway-contingency.sh; nl -ba scripts/ci/validate-footprint-gateway-contingency-test.sh; rg -n -C 8 'name: (feed-gateway|es-cvd)|GATEWAY_ES_FOOTPRINT|FOOTPRINT_MAX_RECORD|JAVA_TOOL_OPTIONS|limits:|memory:' k8s/es4/services/es-feed-gateway.yaml k8s/es4/services/es-cvd.yaml; rg -n 'ACTION|DEPLOY_DRY_RUN|kubectl apply|pin-and-apply|stage\\(' Jenkinsfile.es4-deploy" in /private/tmp/oe-deploy-mirror
 succeeded in 3ms:
8:// ACTIONS:
18://                  ACTION=activate-es-feed, so no reset can raise a Databento session).
19://                  DESTRUCTIVE (drops all es4 Kafka data); honors DEPLOY_DRY_RUN.
32:    choice(name: 'ACTION', choices: ['deploy-service', 'deploy-all', 'infra-sync', 'create-topics', 'clean-reset', 'activate-es-feed', 'deactivate-es-feed'],
47:    ], description: 'Service (deploy-service only). NOTE: es-feed is the RELOCATED ES options feed now running on the es4 cluster; it always deploys at replicas 0 — starting it is a separate ACTION=activate-es-feed.')
48:    booleanParam(name: 'DEPLOY_DRY_RUN', defaultValue: false,
49:      description: 'true = render + digest-pin + kubectl apply --dry-run=server only.')
76:    stage('Infra sync + bootstrap') {
77:      when { expression { params.ACTION == 'infra-sync' } }
91:    stage('Create topics') {
92:      when { expression { params.ACTION == 'create-topics' } }
109:    stage('Clean reset (atomic Kafka + Schema Registry wipe)') {
112:      // wedged the scorer. DESTRUCTIVE: drops all es4 Kafka data; honor DEPLOY_DRY_RUN to preview.
120:      when { expression { params.ACTION == 'clean-reset' } }
151:          #     After a reset, start the feed deliberately with ACTION=activate-es-feed.
153:          if [ "${DEPLOY_DRY_RUN}" = "true" ]; then
155:            ssh -o BatchMode=yes "$ES4_HOST" "DEPLOY_DRY_RUN=true ES4_FEED_FENCED=1 ES4_EXPECTED_NS_UID=$ES4_NS_UID bash /home/es4/repo/scripts/es4/cleanup-es4.sh"
160:          ssh -o BatchMode=yes "$ES4_HOST" "DEPLOY_DRY_RUN=false ES4_FEED_FENCED=1 ES4_EXPECTED_NS_UID=$ES4_NS_UID bash /home/es4/repo/scripts/es4/cleanup-es4.sh"
164:    stage('Deploy to es4') {
165:      when { expression { params.ACTION == 'deploy-service' || params.ACTION == 'deploy-all' } }
170:          DRY=""; [ "${DEPLOY_DRY_RUN:-false}" = "true" ] && DRY="--dry-run=server"
190:          kubectl apply $DRY -f k8s/es4/es4-base-env.yaml
191:          kubectl apply $DRY -f k8s/es4/es4-common-env.yaml
194:          kubectl apply $DRY -f k8s/es4/es4-feed-config.yaml
198:          # `kubectl apply` never prunes, so removing the manifest from git does not remove the live
220:          if [ "$ACTION" = "deploy-service" ] && [ "$SERVICE" = "es-feed" ]; then
228:          if [ "$ACTION" = "deploy-all" ]; then
239:            bash scripts/es4/pin-and-apply.sh "$f" "$REGISTRY" "$DRY"
241:          if [ -z "$DRY" ] && { [ "$ACTION" = "deploy-all" ] || [ "$SERVICE" = "vix-option-inteligence" ]; }; then
242:            # Remove the legacy identity only after pin-and-apply has completed the replacement
248:          # path performs no scaling at all — activation is its own ACTION (requirement DBP-R22).
249:          if [ "$ACTION" = "deploy-service" ] && [ "$SERVICE" = "es-feed" ]; then
250:            echo "es-feed manifest applied at replicas 0. To start it: ACTION=activate-es-feed"
256:    stage('Activate es-feed (separate authorized action)') {
257:      when { expression { params.ACTION == 'activate-es-feed' } }
279:          if [ "${DEPLOY_DRY_RUN:-false}" = "true" ]; then
289:    stage('Deactivate es-feed') {
290:      when { expression { params.ACTION == 'deactivate-es-feed' } }
314:            if [ "${DEPLOY_DRY_RUN:-false}" = "true" ]; then echo "DRY: would scale $loc es-feed to 0 and wait for pods"; continue; fi

codex
The structural checks look correctly scoped: both target tuples match the committed multi-document manifests, and the Jenkins invocation is before every apply in the only stage that applies service manifests. I’m now executing the guard/mutation suite and probing duplicate-selection behavior with the available yq version.
exec
/bin/bash -lc 'command -v yq && yq --version; bash -n scripts/ci/validate-footprint-gateway-contingency.sh scripts/ci/validate-footprint-gateway-contingency-test.sh; scripts/ci/validate-footprint-gateway-contingency.sh; scripts/ci/validate-footprint-gateway-contingency-test.sh' in /private/tmp/oe-deploy-mirror
 exited 1 in 158ms:
mktemp: mkdtemp failed on /var/folders/t3/_t_9ln7d49g_yvjqnyr_rdj40000gn/T/tmp.rXFZ6fRtRk: Operation not permitted
mktemp: mkdtemp failed on /var/folders/t3/_t_9ln7d49g_yvjqnyr_rdj40000gn/T/tmp.gPw78GxjKs: Operation not permitted

web search: 
web search: site:github.com/mikefarah/yq/tree/v4.44.3 expression operators select array length alternative operator yq ...
exec
/bin/bash -lc "yq 'select(.kind == \"Deployment\" and .metadata.name == \"es-feed-gateway\") | .spec.template.spec.containers[] | select(.name == \"feed-gateway\") | {\"name\": .name, \"enabled\": [.env[] | select(.name == \"GATEWAY_ES_FOOTPRINT_ENABLED\")], \"limit\": .resources.limits.memory}' k8s/es4/services/es-feed-gateway.yaml; yq 'select(.kind == \"Deployment\" and .metadata.name == \"es-cvd-service\") | .spec.template.spec.containers[] | select(.name == \"es-cvd\") | {\"name\": .name, \"ceiling\": [.env[] | select(.name == \"FOOTPRINT_MAX_RECORD_BYTES\")]}' k8s/es4/services/es-cvd.yaml; yq -r 'select(.kind == \"Deployment\" and .metadata.name == \"es-feed-gateway\") | .spec.template.spec.containers[] | select(.name == \"feed-gateway\") | [.env[] | select(.name == \"GATEWAY_ES_FOOTPRINT_ENABLED\")] | length' k8s/es4/services/es-feed-gateway.yaml" in /private/tmp/oe-deploy-mirror
 succeeded in 0ms:
name: feed-gateway
enabled:
  # ES Footprint relay (ES-FOOTPRINT-GATEWAY-DESIGN.md G-R1..G-R11). Same opt-in reason as the
  # CVD block above, and the same unprefixed names — TOPIC_PREFIX=es. resolves them to
  # es.futures.footprint*. These topics are NATIVE here: es-cvd-service publishes them on this
  # cluster, so unlike dev and prod there is no mirror in the path.
  #
  # G-R8a is fail-closed — a topic whose effective max.message.bytes exceeds the ceiling or
  # whose compression.type is not producer/uncompressed makes the gateway REFUSE TO START, as
  # do the three HotSpot layout flags the retained-heap arithmetic depends on. All verified on
  # es4 before enabling: every one of the four topics reads compression.type=producer and
  # max.message.bytes=1048588 (equal to the ceiling, and the check rejects only ABOVE it), and
  # UseCompressedOops / UseCompressedClassPointers / CompactStrings are all true on the
  # running JVM.
  - name: GATEWAY_ES_FOOTPRINT_ENABLED
    value: "true"
limit: "2560Mi"
ceiling: []
---
name: es-cvd
ceiling:
  # G-R8 (options-edge-feed-gateway/ES-FOOTPRINT-GATEWAY-DESIGN.md) requires this and the
  # gateway's GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES to be ONE value, set together: the gateway
  # drops any record above its own ceiling, so a producer allowed to emit larger records would
  # publish records the relay silently discards. Both defaults are 262144 and agreed only by
  # coincidence until now. See the matching entry in k8s/es4/services/es-feed-gateway.yaml.
  # MOVE THEM TOGETHER OR NOT AT ALL.
  - name: FOOTPRINT_MAX_RECORD_BYTES
    value: "262144"
0
---
1

exec
/bin/bash -lc "yq 'select(.kind == \"Deployment\" and .metadata.name == \"es-feed-gateway\") | .spec.template.spec.containers[] | select(.name == \"feed-gateway\")' k8s/es4/services/es-feed-gateway.yaml | yq 'select(. "'!= null) | .name'"' -; yq 'select(.kind == \"Deployment\" and .metadata.name == \"es-cvd-service\") | .spec.template.spec.containers[] | select(.name == \"es-cvd\")' k8s/es4/services/es-cvd.yaml | yq 'select(. "'!= null) | .name'"' -; yq 'select(.kind == \"Deployment\" and .metadata.name == \"es-feed-gateway\") | .spec.template.spec.containers[] | select(.name == \"feed-gateway\")' k8s/es4/services/es-feed-gateway.yaml | yq -r '[.env[] | select(.name == \"GATEWAY_ES_FOOTPRINT_ENABLED\")] | length' -" in /private/tmp/oe-deploy-mirror
 succeeded in 25ms:
1

exec
/bin/bash -lc "printf 'gw count='; yq 'select(.kind == \"Deployment\" and .metadata.name == \"es-feed-gateway\") | .spec.template.spec.containers[] | select(.name == \"feed-gateway\")' k8s/es4/services/es-feed-gateway.yaml | yq 'select(. "'!= null) | .name'"' - | tee /dev/stderr | grep -c .; printf 'prod count='; yq 'select(.kind == \"Deployment\" and .metadata.name == \"es-cvd-service\") | .spec.template.spec.containers[] | select(.name == \"es-cvd\")' k8s/es4/services/es-cvd.yaml | yq 'select(. "'!= null) | .name'"' - | tee /dev/stderr | grep -c ." in /private/tmp/oe-deploy-mirror
 succeeded in 140ms:
tee: /dev/stderr: Operation not permitted
1
prod count=tee: /dev/stderr: Operation not permitted
1

codex
No remaining blocking findings.

- Relay-off is fail-closed: the selected gateway container must contain exactly one flag whose scalar value is exactly `true` or `false`.
- Both manifest/container selections are correct. Counting selected `.name` outputs cannot be confused by comments, other YAML documents, sidecars, or names embedded in values.
- `env_count` and `env_value` operate exclusively on the rendered selected container. They use the same exact `.env[].name` predicate; comments, indentation, other lists, and substrings in values are irrelevant.
- The Jenkins guard is inside `Deploy to es4`, unconditional for both `deploy-service` and `deploy-all`, and precedes every `kubectl apply`/`pin-and-apply` call. Dry-run follows the same guarded route. Other actions do not apply these service manifests.
- Mutation failures now assert stable `E_*` identifiers, avoiding both wrong-reason passes and prose-coupled brittleness.
- The sidecar fixture is valid YAML, is inserted before `feed-gateway`, and contains the misleading flag, ceiling, heap, and memory fields that a global text scan could wrongly consume. The second sidecar case confirms the real gateway remains the subject.
- The YAML parsing is now done with yq; the remaining awk only converts already-selected Kubernetes memory quantities and therefore has no YAML-scoping problem.
- The expressions use established v4 operations—multi-document `select`, array traversal/construction, `length`, raw output, and `//`—available in the pinned v4.44.3 generation. The direct queries also resolve the committed manifests exactly once. The checkout’s read-only sandbox prevented the scripts’ `mktemp`-based end-to-end run, but shell parsing and the relevant yq expressions were independently checked. [Upstream v4.44.3 usage evidence](https://github.com/mikefarah/yq/discussions/2154)

VERDICT: APPROVE
tokens used
66.911
No remaining blocking findings.

- Relay-off is fail-closed: the selected gateway container must contain exactly one flag whose scalar value is exactly `true` or `false`.
- Both manifest/container selections are correct. Counting selected `.name` outputs cannot be confused by comments, other YAML documents, sidecars, or names embedded in values.
- `env_count` and `env_value` operate exclusively on the rendered selected container. They use the same exact `.env[].name` predicate; comments, indentation, other lists, and substrings in values are irrelevant.
- The Jenkins guard is inside `Deploy to es4`, unconditional for both `deploy-service` and `deploy-all`, and precedes every `kubectl apply`/`pin-and-apply` call. Dry-run follows the same guarded route. Other actions do not apply these service manifests.
- Mutation failures now assert stable `E_*` identifiers, avoiding both wrong-reason passes and prose-coupled brittleness.
- The sidecar fixture is valid YAML, is inserted before `feed-gateway`, and contains the misleading flag, ceiling, heap, and memory fields that a global text scan could wrongly consume. The second sidecar case confirms the real gateway remains the subject.
- The YAML parsing is now done with yq; the remaining awk only converts already-selected Kubernetes memory quantities and therefore has no YAML-scoping problem.
- The expressions use established v4 operations—multi-document `select`, array traversal/construction, `length`, raw output, and `//`—available in the pinned v4.44.3 generation. The direct queries also resolve the committed manifests exactly once. The checkout’s read-only sandbox prevented the scripts’ `mktemp`-based end-to-end run, but shell parsing and the relevant yq expressions were independently checked. [Upstream v4.44.3 usage evidence](https://github.com/mikefarah/yq/discussions/2154)

VERDICT: APPROVE
