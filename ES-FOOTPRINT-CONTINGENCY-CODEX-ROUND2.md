Reading additional input from stdin...
2026-09-08T18:59:08.516716Z ERROR codex_models_manager::manager: failed to load models cache: missing field `supports_parallel_tool_calls` at line 93 column 5
OpenAI Codex v0.147.0
--------
workdir: /private/tmp/oe-deploy-mirror
model: gpt-5.6-sol
provider: openai
approval: never
sandbox: read-only
reasoning effort: low
reasoning summaries: none
session id: 01a08263-6c2b-7dc0-a175-82bd2f397742
--------
user
Round 2 of the review of options-edge-deploy PR #1010 at /private/tmp/oe-deploy-mirror, branch
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
codex
I’ll inspect the repository instructions, the PR diff against its base, and the exact guard/test/Jenkins paths. Then I’ll trace all apply-capable branches and run the relevant tests if the workspace permits.
exec
/bin/bash -lc "pwd && rg --files -g 'AGENTS.md' -g '"'!node_modules'"' -g '"'!vendor'"' | head -50 && git status --short --branch && git branch --show-current && git remote -v && git log --oneline --decorate -8" in /private/tmp/oe-deploy-mirror
exec
/bin/bash -lc 'git branch -a --no-color; git log --oneline --decorate -12; git diff --stat origin/master...HEAD; git diff --name-status origin/master...HEAD' in /private/tmp/oe-deploy-mirror
 succeeded in 38730ms:
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-xoHiGyii' (errno=Operation not permitted)
2026-09-08 20:59:20.115 xcodebuild[60570:265446177]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:21.410 xcodebuild[60570:265446174] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-ynoJ7LI8' (errno=Operation not permitted)
2026-09-08 20:59:23.274 xcodebuild[60735:265446647]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:24.271 xcodebuild[60735:265446646] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
## feat/footprint-gateway-contingency-guard...origin/feat/footprint-gateway-contingency-guard
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-LO07FF8e' (errno=Operation not permitted)
2026-09-08 20:59:28.416 xcodebuild[60925:265447280]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:29.440 xcodebuild[60925:265447275] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-SrOGcrz4' (errno=Operation not permitted)
2026-09-08 20:59:31.473 xcodebuild[61044:265447617]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:32.332 xcodebuild[61044:265447612] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
feat/footprint-gateway-contingency-guard
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-DBqYzbOK' (errno=Operation not permitted)
2026-09-08 20:59:37.740 xcodebuild[61319:265448585]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:38.879 xcodebuild[61319:265448574] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-A0Ynpauz' (errno=Operation not permitted)
2026-09-08 20:59:41.979 xcodebuild[61445:265449071]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:42.911 xcodebuild[61445:265449070] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
gitlab	ssh://git@192.168.100.102:2224/options-edge/options-edge-deploy.git (fetch)
gitlab	ssh://git@192.168.100.102:2224/options-edge/options-edge-deploy.git (push)
origin	git@github.com:abhinav-jain09/options-edge-deploy.git (fetch)
origin	git@github.com:abhinav-jain09/options-edge-deploy.git (push)
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-XMyRJhYn' (errno=Operation not permitted)
2026-09-08 20:59:49.513 xcodebuild[61725:265449852]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:50.398 xcodebuild[61725:265449851] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-he6aFEiQ' (errno=Operation not permitted)
2026-09-08 20:59:52.739 xcodebuild[61837:265450155]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:53.624 xcodebuild[61837:265450146] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
ccd6374b (HEAD -> feat/footprint-gateway-contingency-guard, origin/feat/footprint-gateway-contingency-guard) fix(footprint guard): close the fail-open exemption and run it on the deploy path
52c60ebb ci(footprint): the G-R8 contingency is checked, not trusted
226a36fc (origin/main, origin/HEAD) Merge pull request #1009 from abhinav-jain09/feat/web-service-suppress-nested-autodeploy
335a6a73 (origin/feat/web-service-suppress-nested-autodeploy, feat/web-service-suppress-nested-autodeploy) fix(web-service-deploy): suppress the build job's dev auto-deploy (no double-rollout)
211e9e46 fix(gateway): dev — keep the pre-open STATUS plane, stand the VALUE plane down (#1008)
0c4a7aa4 fix(gateway): turn pre-open GEX arbitration off — it drops 100% of pre-open records (#1006)
9b48bbf7 fix(es-auction mirror): an unterminated brace group made the preflight die at EOF (#1004)
144ebf40 es4: the ES tape retains 7 days, so the Auction Desk can build its prior session (#1003)

exec
/bin/bash -lc 'git diff --stat origin/main...HEAD; git diff --name-status origin/main...HEAD; git diff --find-renames origin/main...HEAD' in /private/tmp/oe-deploy-mirror
 exited 128 in 37480ms:
2026-09-08 20:59:34.625 xcodebuild[61197:265448196]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:35.540 xcodebuild[61197:265448194] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-Fyuzsi7N' (errno=Operation not permitted)
2026-09-08 20:59:37.738 xcodebuild[61318:265448571]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:38.828 xcodebuild[61318:265448567] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
  _verify
+ agent/add-strike-invasion-prod-image-map
  agent/disable-es-predown-cron
+ agent/es4-cleanup-state-recovery
  agent/es4-gex-display-wiring
+ agent/fix-es-option-truth-model
+ agent/fix-es-option-truth-model-clean
+ agent/fix-es4-gex-recovery-config
+ agent/fix-option-truth-input
  agent/fix-prod-blank-expiry
  agent/internal-gateway-health
  agent/opb-dev-memory
+ agent/option-truth-dev-memory
  agent/prevent-dev-gex-outages
+ agent/prod-gex-static-carry-safety
  agent/prod-market-carry-restore-memory
  agent/prod-market-carry-restore-timeout
  agent/stabilize-kafka-transactions
+ agent/wire-sr3-rate-history
  check
+ chk
  chore/app-pg-secret
+ chore/delta-flow-drop-v1-appid
  chore/dev-gex-hot-partition-relief
  chore/dev-synthetic-standing
  chore/disable-dealer-ledger-accumulator
  chore/disable-dealer-ledger-and-spread-skew
  chore/disable-directional-pressure
  chore/disable-feed-gateway
  chore/disable-ibkr-feed-prod
  chore/disable-maxpain-sandwich-dp-ibkr
  chore/disable-maxpain-uw-gex
  chore/disable-option-price-behavior
  chore/disable-option-truth-engine
  chore/disable-volume-pace-databento
  chore/disable-volume-sandwich
  chore/drop-basis-mode-env
  chore/drop-es4-auto-sync
  chore/email-pin-build14
  chore/enable-dealer-ledger
  chore/es4-enable-strike-liquidity-heatmap
+ chore/es4-es-cvd-append-time-since
  chore/es4-keep-down-directional-pressure
  chore/es4-keep-down-heatmap
  chore/es4-keep-down-option-price-behavior
  chore/es4-render-drift
  chore/feed-dev-publish-interval-1s
  chore/feed-publish-interval-5s
  chore/feed-remove-spy-extra-instrument
  chore/gex-appid-canonical
  chore/hpsf-dlq-1day-retention
  chore/ib-expiry-auto-default
  chore/jenkins-only-kubernetes-deploy
  chore/kafka-1day-retention
  chore/keep-down-databento-vix-feed
  chore/keep-down-short-premium-agent
  chore/keep-down-vix-feed
  chore/local-dev-databento-topic-cleanup-script
+ chore/multileg-stop
  chore/offhours-manual-only
  chore/oi-nowcast-keep-down
  chore/pin-401-fix
  chore/pin-a7514eb7
  chore/pin-adf419f6
  chore/pin-boot-default
  chore/pin-watchlist-digest
  chore/regen-slices-progressdeadline
  chore/remove-cpu-limits
  chore/remove-detached-node-4
  chore/remove-disabled-services-from-deploy-path
+ chore/remove-fullfunding
  chore/remove-greek-move-auth-service
  chore/remove-ibkr-feed
  chore/remove-legacy-8090-marks
+ chore/remove-orchestrator-from-monolith-deploy
  chore/remove-staging
  chore/remove-step0-ack
  chore/remove-unusual-whales
  chore/remove-versioned-identities
  chore/req-portal-reconcile-req1-req2
+ chore/retire-hpsf-from-dev-and-prod
  chore/retire-oi-nowcast
  chore/retire-spread-skew
  chore/reversal-autopsy-retune
  chore/reversal-sensitivity-retune
  chore/short-premium-72b-timeout
  chore/short-premium-72b-timeout-v2
  chore/short-premium-cadence-60s
  chore/short-premium-final5-30s
  chore/short-premium-scale-to-0
  chore/smoke-drop-deep-verify
  chore/split-infra-and-fullfunding
  chore/stock-gex-image-first
  chore/vix-feed-replicas-zero
  ci/bring-up-all-canonical-env
  ci/bring-up-all-full-reactor-flag
  ci/bring-up-all-ui-only-gate
  ci/options-edge-deploy-derive-from-profile
  ci/options-edge-deploy-use-oeprofile
  ci/options-edge-deploy-zero-allowlist
  ci/validate-on-74-builders
  claude/databento-gex-history-deploy
  claude/deploy-gex-rollout
  claude/g1-deploy-orchestrator
  claude/g1-deploy-orchestrator-drop-pods-status
  claude/g1-deploy-orchestrator-image-pin
  claude/g1-orchestrator-allow-unauthenticated
  claude/gex-naive-sign
  claude/infra-overlay-remove-app-workloads
  claude/jenkins-deployer-rbac-for-orchestrator
  claude/kafka-changelog-cleanup
  claude/kafka-orphan-generation-purge
  claude/maxpain-deploy-review-fixes
  claude/maxpain-dev-deploy
  claude/maxpain-prod-promote
  claude/mission-sandwich-10s-interval
  claude/mission-sandwich-appid-v2
+ claude/mission-sandwich-volume-topic-fix
  claude/rocksdb-state-stores
  claude/suspicious-herschel-cbc69b
  codex/deploy-option-price-behavior-service
+ codex/dev-heatmap-replay-config
  codex/opb-latest-offset
  codex/remove-hpsf-smoke
+ codex/update-stage-guard
+ codex/wire-option-price-behavior-pipeline
  crossings-preserve
  crossings-topic-deploy
  deploy-render
  deploy/delta-flow-target
  docs/clarify-dev-prod-boundary
  docs/delta-flow-runbook
  docs/gate1-outage-accepted
  docs/gate1-rev8-consistency
  docs/single-service-deploy-runbook
  enable/tick-floor-dev
  es4/delta-flow-greek-freshness
  es4/gateway-initc-fix
  es4/gateway-wait-for-kafka
  es4/strike-intel-adaptive
  es4/strike-intelligence
+ feat/a5-ledger-durability
  feat/approach-monitor-dev-up
  feat/approach-monitor-manifests
  feat/approach-position-publisher
  feat/archive-es4-cvd
  feat/auto-resolve-databento-expiry
  feat/banknifty-pressure-route
  feat/basis-a1-evidence-prod
  feat/bleedingoptions-bootstrap
  feat/bleedingoptions-public-gamma-lab
  feat/bo-app-postgres
  feat/bring-up-all-experiment
  feat/bugzilla-keycloak-groups-extension
  feat/bugzilla-project-lifecycle
  feat/carry-5min-cadence
  feat/cascade-tier-order
+ feat/close-batch-pure-java
  feat/close-board-batch
+ feat/close-board-catch-up
  feat/close-board-db
  feat/close-board-job-dsn
  feat/close-db-inline-dsn
  feat/close-db-switch-on
  feat/close-direction-deploy
+ feat/context-tape-0700-window
  feat/context-tape-deploy
+ feat/context-tape-direction-a4
+ feat/context-tape-direction-dev-shadow
+ feat/context-tape-premarket-mode
  feat/continuous-kafka-archiver
  feat/corridor-gauge-deploy
+ feat/corridor-gauge-image-tags
  feat/cvd-levels-gateway-flag
  feat/cvd-mirror-dev-prod
  feat/cvd-spx-levels-contracts
  feat/dealer-ledger-advisory-on
  feat/dealer-ledger-deploy
  feat/dealer-ledger-shadow-visible-all-envs
  feat/dealer-ledger-v1-dev-config
  feat/delta-flow-r19-alert-rules
  feat/delta-flow-standalone-deploy
  feat/deploy-databento-expiry-auto
  feat/dev-basis-a1
  feat/dev-branch-deploy
  feat/dev-cascaded-spot
  feat/dev-databento-preopen-reenable
  feat/dev-delta-flow-greek-lookahead
  feat/dev-env-guard
+ feat/dev-feed-memory-watch
+ feat/dev-gex-overnight-oi-keepalive
  feat/dev-gex-rollout
  feat/dev-gex-topn
  feat/dev-kafka-retention-10h
  feat/dev-kafka-retention-cap-10h
  feat/dev-native-kafka-config
+ feat/dev-premarket-live-oi
  feat/dev-shadow-model-enable
+ feat/dev-shadow-model-enable-v2
  feat/dev-strike-intel-action-band-low
  feat/dev-unified-sr-wiring
  feat/dl-calibration-deploy
  feat/dl-fire-quality-dependency-scoped-register
  feat/dl-quote-stale-widen
  feat/dl-shadow-live-config
  feat/dl-v1-dev-config
  feat/dl-v1-dev-config-clean
+ feat/domain-bleadingoptions-p1
+ feat/domain-bleadingoptions-p2
  feat/domain-bleadingoptions-p3-retire
  feat/drop-classifier-deploy
+ feat/drop-nowcast-dev-topic
  feat/dynamic-oi-dev
  feat/email-ics
  feat/email-image-tag
  feat/email-nudge-template
  feat/email-service
  feat/enable-1dte-track
  feat/enable-offhours-ladder
+ feat/enable-oi-anchor-dev
  feat/enable-research-store-dev
  feat/ensure-indicator-topics-barrier
  feat/es-amt-deploy
  feat/es-cvd-manifest
  feat/es-feed-basis-reference
  feat/es-feed-liveness-probe
  feat/es-feed-relocate-es4-phase0-1
  feat/es-footprint-mirror-to-dev-prod
  feat/es-futures-flow-mirror-dev-prod
  feat/es-gex-enable-display
+ feat/es-gex-mirror-jenkins
  feat/es-gex-oi-persist
+ feat/es-gex-on-spx
  feat/es-open-direction-deploy
  feat/es-overnight-oi-carry-enable
+ feat/es-slope-prod
  feat/es-spx-align-deploy-cutover
  feat/es-spx-basis-sink
  feat/es-spx-basis-v2-env
  feat/es-strike-intel-enable
  feat/es-trades-from-es4
  feat/es-web-prior-close-symbol
  feat/es4-bridge-enable
  feat/es4-context-tape-gamma-migration
  feat/es4-context-tape-globex
  feat/es4-delta-flow-min-contracts
  feat/es4-delta-flow-ttl-4h
  feat/es4-delta-flow-use-native-side
  feat/es4-enable-gex-spxbridge
+ feat/es4-es-cvd-footprint
  feat/es4-es-price
  feat/es4-gamma-migration
  feat/es4-gateway-cvd-enable
  feat/es4-gateway-footprint-enable
  feat/es4-gateway-footprint-memory
  feat/es4-gateway-generous-heap
  feat/es4-gex-oi-parents
  feat/es4-globex-calendar
  feat/es4-heatmap-saturation
+ feat/es4-jenkins-only-admission
  feat/es4-liveness-globex-session
  feat/es4-mirror-service-deploy-tier1
+ feat/es4-mm2-config-reload
  feat/es4-oi-bootstrap-enable
  feat/es4-pace-stale-ms
  feat/es4-post-clean-verification
  feat/es4-spread-skew
  feat/es4-spxbridge-topic
  feat/es4-strike-intel-premium-floor
  feat/es4-two-sided-floor-off
  feat/experiment-build
  feat/experiment-deploy
  feat/experiment-native-deploy
  feat/experiment-unique-ports
+ feat/expiry-roll-1730-align
  feat/feed-restart-nonce-expiry-roll
  feat/feed-spy-extra-instrument
  feat/flip-spot-source-to-cascaded
* feat/footprint-gateway-contingency-guard
  feat/fullfunding-app
  feat/fullfunding-bootstrap-rbac
  feat/fullfunding-deploy-job
  feat/fullfunding-namespace
  feat/gamma-lab-pin-75316d3b
  feat/gamma-lab-pin-7c2c95b5
  feat/gamma-lab-pin-leaderboard
  feat/gamma-lab-pin-simple
  feat/gamma-lab-replicas-1
  feat/gamma-leadership-deploy-wiring
  feat/gamma-migration-deploy
  feat/gateway-nightly-restart-cron
  feat/gex-delta-redis-writer
+ feat/gex-deploy-wiring
  feat/gex-max-quote-age
+ feat/gex-preopen-topic
+ feat/gex-smile-dev-flag
  feat/gex-tick-floor-prod
  feat/gex-top3-es4
  feat/gex-top3-prod
  feat/gex-topics-provisioning
  feat/greek-move-auth-deploy
  feat/greek-move-auth-prod-enable
  feat/gw-restart-nonce-expiry-roll
  feat/heatmap-calibration-2026-07-30
  feat/heatmap-in-service-deploy
  feat/heatmap-saturation-configmap
  feat/heatmap-stall-alert
  feat/heatmap-topics
  feat/historical-replay-dot2
  feat/hot-strike-es4-topics
+ feat/hunt-auto-arm
  feat/hunt-buckets
  feat/hunt-deploy
  feat/hunt-dev-instance
  feat/hunt-mirror
+ feat/hunt-reenable
  feat/hunt-swing-dev
  feat/hunt-swing-topic-preservation
+ feat/hunt-threshold-28t62s
  feat/ibkr-feed-dev-vix-bringup
  feat/ibkr-feed-service-deploy
  feat/ibkr-preopen-dev-gate
  feat/ibkr-preopen-gex
  feat/ibkr-preopen-prod
+ feat/ibkr-preopen-prod-gate2
  feat/ibkr-spx-index-spot
+ feat/ibkr-trades-onboarding
  feat/ibkr-vix-priority-wiring
  feat/indicator-backfill-deploy
  feat/indicator-deploy
  feat/internal-web-leaderboard-db
  feat/jenkins-main-only-all-envs
  feat/jenkins-must-go-via-dev
  feat/jenkins-skip-kafka-topics
  feat/kafka-cleanup-discord
  feat/kafka-cleanup-nightly-jenkins-trigger
  feat/kafka-rf-single-source
  feat/keycloak-app-access-groups
  feat/keycloak-lan-admin
  feat/keycloak-trading-user-gate
  feat/local-dev-two-broker-rf2
  feat/main-schema-flag
  feat/multileg-deploy-wiring
  feat/multileg-sr-levels
  feat/multileg-topic-barrier
  feat/namespace-aware-service-deploy
  feat/new-service-jenkins-template
  feat/nifty-gex-public-route
  feat/nifty-gex-secret-writeback-carveout
  feat/nifty-gex-service-slice
  feat/offhours-allow-list
  feat/offhours-clean-slate
  feat/offhours-trigger-and-freed-gb
  feat/oi-anchor-enable-dev
  feat/oi-anchor-prod-enable
+ feat/oi-live-only-direct-fetch-off
  feat/oi-nowcast-topic-rename
  feat/oi-shadow-live-dev
  feat/oi-shadow-live-prod
  feat/oi-shadow-service
  feat/oi-snapshot-dev
  feat/opb-v2-calibrated-config
  feat/opb-v2-shadow-deploy
  feat/option-truth-engine-deploy
+ feat/option-truth-ui-wiring
  feat/pace-traj-session-window
  feat/persist-realtime-replay-config
  feat/pin-flow-explorer-deploy
  feat/pin-gamma-lab-401fix
  feat/pin-gamma-lab-csp-bars
  feat/pin-gamma-lab-pressure
  feat/pin-gamma-lab-topbar
  feat/pin-writer-dev-deploy
  feat/platform-error-watch
  feat/premarket-canary
  feat/premarket-readiness-agent
  feat/premarket-reset
  feat/preopen-gex-deploy
  feat/pressure-role-contract
  feat/processing-grafana-observability
  feat/prod-boot-gated-bringup
  feat/prod-gateway-auth
  feat/prod-morning-autostart
+ feat/prod-preopen-live-oi
  feat/prod-strike-intel-action-band-low
  feat/prod-strike-intel-lag-tolerant-gates2
  feat/public-closed-market-no-wait
  feat/public-gamma-lab-filling-board
  feat/public-gate-evidence
  feat/public-image-digest-pinning
+ feat/public-keep-view
+ feat/raw-writer-databento-persist
  feat/reenable-gateway-and-heatmap
  feat/reenable-volume-pace-code-only
  feat/reenable-volume-pace-databento
+ feat/register-stock-gex
  feat/replay-attestation-labels
  feat/req-portal-bugzilla-sso
  feat/req-portal-stack
  feat/req-realm-keycloak
  feat/req-tunnel-route
  feat/restore-greek-move-auth
+ feat/retention-derived-preserve
  feat/retire-pressure-public-routes
  feat/retire-prod-es-feed
  feat/reversal-confirmation-deploy
  feat/reversal-postgres-writer-deploy
+ feat/reversal-weakflow-calibration
  feat/service-deploy-approach
+ feat/service-deploy-intent-dedup
  feat/service-deploy-web
  feat/short-premium-15s
  feat/short-premium-72b
  feat/short-premium-agent-deploy
  feat/short-premium-enable-dev
  feat/short-premium-ollama
  feat/short-premium-prod
  feat/short-premium-prod-pinning
  feat/signal-follower-core-deploy
  feat/signal-follower-g1-deploy
  feat/signal-follower-g2-dev-flags
  feat/signal-follower-prod-flags
  feat/skip-hpsf-smoke
  feat/smoke-auth-wiring
+ feat/spot-vol-regime
  feat/spread-skew-deploy
  feat/spread-skew-prod-on
  feat/spy-feed-instance
  feat/standalone-all-processing-services
  feat/standalone-databento-feed
  feat/standalone-service-deployment
  feat/stock-gex-close-board
  feat/stock-gex-db
  feat/strike-flow-databento-canary-enable
  feat/strike-flow-delta-overlay-on
  feat/strike-flow-institutional-canary
  feat/strike-intelligence-deploy
  feat/strike-intelligence-prod-gex-ack
  feat/strike-invasion-deploy
  feat/strike-liquidity-deploy
  feat/strong-only-emission
+ feat/system-status-dev-env
  feat/tape-zones-es4-wiring
  feat/tape-zones-mirror
  feat/topics-4-partitions
  feat/track-prod-tunnel-config
  feat/trail-full-session
  feat/two-node-rf2-bootstrap
  feat/unified-sr-experiment-deploy
  feat/unified-sr-history-topic
  feat/verify-live-bugzilla-client
  feat/vix-cutover-both-envs
  feat/vix-dev-enable
  feat/vix-feed-separation
  feat/vix-parity-enable
  feat/vol-premium-deploy
  feat/web-app-k8s
  feat/web-service-suppress-nested-autodeploy
  feat/web-to-k8s
  feat/web-to-k8s-dev
  feat/wire-app-db-to-web
  feat/zero-dte-intelligence-deploy
+ feat/zero-dte-topic-reconcile
  feature/es-aggressor-expiry-roll
  feature/es-aggressor-flow-service
  feature/es-aggressor-flow-ui-deploy
  feature/es-seller-activity-deploy
  feature/hpsf-replay-stage-a-startup-gates
  feature/hpsf-topic-contract-deploy-fix
  feature/hpsf-v2.1-bugzilla-40-64
  feature/ibkr-strike-flow-deploy
  feature/ibkr-vix-price-topic
  feature/prod-keycloak
  feature/replay-backend-plane-ansible
  feature/replay-configurable-sampling
  feature/spx-mission-control-deploy
  fix/align-vix-option-topic
  fix/all-image-vars-dealer-ledger
  fix/allow-one-time-cleanup-stage
  fix/app-pg-secret-write
  fix/approach-dev-registry
  fix/approach-monitor-r7
  fix/approach-monitor-r9
  fix/archive-drift-newfile
  fix/archive-drop-cvd-snapshot
+ fix/archive-log-reset-detection
  fix/archive-offset-ahead-false-reset
  fix/auto-promote-no-wait
  fix/autostart-ibkr-feed-up
  fix/autostart-self-heal-stragglers
  fix/bo-postgres-runas
  fix/bo-realm-comment-keys
  fix/bring-up-all-deploy-all
  fix/bring-up-all-keep-down
  fix/bua-skip-selenium
  fix/bugzilla-category-scoped-to-issue-type
  fix/bugzilla-confirmed-entry-point
  fix/bugzilla-issue-type-required-marker
  fix/bugzilla-lifecycle-setters-and-probe
  fix/bugzilla-verifier-runs
  fix/bump-rollout-timeouts
  fix/calibration-dev-image-remap
  fix/changelog-cleanup-discovery-regex
+ fix/classifier-forward-interval-3000
  fix/classifier-heap-rightsize
  fix/classifier-mem-limit
  fix/cleanup-approves-prod-only-topics
  fix/cleanup-nonpublic-schema-guard
+ fix/cleanup-nonpublic-schema-guard2
  fix/close-db-dsn-binding
+ fix/compaction-dev-only
  fix/cvd-bars-reset-rebuildable
  fix/cvd-mirror-eff-anchor
  fix/cvd-mirror-eff-value
  fix/cvd-mirror-empty-lag
  fix/databento-vix-feed-enable
  fix/dealer-ledger-env-crosswire
  fix/dealer-ledger-heap-oom
  fix/dealer-ledger-prod-heap
  fix/dealer-ledger-verify-target
+ fix/declare-deploy-environment
  fix/declare-futures-aggressor-flow-shape
  fix/declare-mirrored-indicator-topics
  fix/declare-mirrored-tape-zones-board
  fix/declare-spx-basis-topics
  fix/dedupe-heatmap-choice
  fix/dedupe-mission-control-and-regen-slices
+ fix/delta-flow-gex-cadence
  fix/delta-flow-monitoring-review
  fix/delta-flow-prod-local-registry
  fix/delta-flow-prod-local-registry-2
  fix/deploy-gex-history-image-pin
  fix/deploy-jenkinsfile-extract-scripts
  fix/deploy-jenkinsfile-method-too-large
  fix/deploy-pin-digest-pin
  fix/deploy-pin-gex-delta-redis-writer
  fix/deploy-pin-postgres-writer-digest-pin
  fix/dev-always-pull
  fix/dev-broker-no-autocreate
+ fix/dev-browser-ws-8091
  fix/dev-build-partitions3
  fix/dev-carry-disable
  fix/dev-cleanup-kafka-shim
  fix/dev-cleanup-topic-contract
  fix/dev-cpu-headroom
  fix/dev-dealer-ledger-mem-web-probe
  fix/dev-declare-indicator-changelog-and-heatmap-retention
  fix/dev-delta-flow-lookahead-tolerance
+ fix/dev-disable-stock-gex
+ fix/dev-drop-replay-orchestrator
  fix/dev-es-mirror-partition-drift
+ fix/dev-feed-gateway-health-url
+ fix/dev-feed-gateway-history-partitions-32
+ fix/dev-feed-memory-3gi
  fix/dev-feed-revert-synthetic-spot
  fix/dev-gex-disable-dynamic-carry
  fix/dev-gex-duplicate-env-key
+ fix/dev-health-selfprobe
  fix/dev-kafka-topic-prefix
  fix/dev-oi-available-time
  fix/dev-preopen-back-to-ibkr-live
  fix/dev-prod-deploy-stages
  fix/dev-prod-environment-config
  fix/dev-push-images
  fix/dev-slice-dynamic-oi
  fix/dev-smoke-honor-weburl
  fix/dev-smoke-host-resolution
  fix/dev-strike-intel-bypass-agreement-2
+ fix/dev-topics-declare-delta-flow
+ fix/dev-web-api-base-8090
  fix/dev-web-apibase-relative
  fix/dev-web-port-8090-v2
+ fix/dev-web-ws-8093
  fix/disable-ibkr-classifier-volpace-integtest
  fix/disable-mission-svcs
  fix/disable-preopen-gex-arbitration
  fix/dl-dev-rearm-flow-derives
  fix/dl-fire-shadow-evidence-prod
  fix/dl-max-record-bytes
  fix/drop-broken-wal-sidecar
  fix/drop-dead-broker-4-bootstrap
  fix/drop-outputs-to-prod-broker
  fix/drop-topics-durability
  fix/durable-vix-lz4-schemas-keycloak-localip
  fix/email-amd64
  fix/email-date-wording
  fix/email-netpol
  fix/email-secrets-tmpdir
  fix/enable-vix-feed-both
  fix/es-align-clock-skew-es4
+ fix/es-auction-mirror-case
  fix/es-cvd-spx-feed-on-es4
  fix/es-cvd-spx-hostalias
  fix/es-web-delta-flow-stale-ms
  fix/es4-broker-num-partitions-32
  fix/es4-classifier-symbol-es
  fix/es4-clean-never-restore-feed-to-zero
  fix/es4-clean-reset-drift-guard-and-wedge
  fix/es4-clean-reset-rsync-and-state
  fix/es4-cleanup-atomic-reset
  fix/es4-cleanup-pv-check-sudo
  fix/es4-cleanup-topics-and-selfheal
  fix/es4-compacted-token-split
  fix/es4-compacted-token-split2
  fix/es4-contract-three-gaps
  fix/es4-declare-gateway-and-hunt-topics
  fix/es4-deploy-empty-if-block
  fix/es4-deploy-image-freshness-gate
  fix/es4-deploy-indicator-choice
  fix/es4-disable-auto-create-topics
  fix/es4-disable-oi-carry
  fix/es4-gateway-market-calendar
  fix/es4-gateway-required-topics
+ fix/es4-gex-dynamic-carry-pin
  fix/es4-guard-label-disarm
  fix/es4-indicator-topics-provisioning
  fix/es4-keep-down-spread-skew
  fix/es4-keepdown-dealer-ledger-calibration
  fix/es4-mm2-heartbeat-loop
  fix/es4-opb-v2r4
  fix/es4-pinflow-session-window
  fix/es4-pinflow-strike-band
  fix/es4-ssot-test-unsatisfiable
  fix/es4-streams-internal-partition-default
  fix/es4-tape-retention
  fix/es4-topic-partition-contract
  fix/es4-topics-from-ssot
  fix/es4-topics-glued-declaration
  fix/es4-under-declared-partitions
  fix/es4-underlying-spx-price-topic
  fix/es4-widen-seven-topics
  fix/experiment-build-keychain
  fix/experiment-deploy-reconcile-removed
  fix/experiment-deploy-secrets
  fix/experiment-exclude-ibkr-integration-test
  fix/experiment-localhost-pkce
  fix/feed-config-rollout-trigger
  fix/feed-env-mirror-order
  fix/feed-es-trades-source-for-basis
  fix/feed-gateway-slice-lost-container-name
  fix/feed-gateway-smoke-health
  fix/feed-liveness-probe
  fix/gamma-lab-api-base
  fix/gamma-lab-probes
  fix/gamma-lab-runasuser
  fix/gamma-leadership-dev-overlay-image
  fix/gamma-leadership-dev-registry
+ fix/gamma-migration-complete-fraction-055
  fix/gamma-migration-dev-registry
  fix/gamma-migration-image-tags
  fix/gamma-migration-session-persistence
+ fix/gateway-graceful-liveness
+ fix/gateway-probe-restore-name-ports
+ fix/gex-carry-failclosed-off
+ fix/gex-carry-topology-state
  fix/gex-disable-dynamic-carry
  fix/gex-flow-streams-threads
  fix/gex-maxpain-runtime-secrets
  fix/gex-oi-direct-fetch-durable
  fix/gex-oi-fetch-limit
  fix/gex-oi-regen-slice
  fix/gex-oi-session-identity
  fix/greek-move-auth-bootstrap-in-session
  fix/greek-move-auth-prod-monolith-mirror
  fix/greek-move-auth-regen-prod-slice
  fix/guard-add-resolve-profile-stage
  fix/heatmap-cpu-request
  fix/hpsf-blackbox-replay-jenkins-step
  fix/hpsf-opra-topic-live-shape
  fix/hpsf-opra-topic-partitions
  fix/hpsf-replay-agent-routing
  fix/hpsf-replay-agent-routing-local
  fix/hpsf-replay-generic-jenkins-job
  fix/hpsf-replay-remove-bugzilla-password-param
  fix/hpsf-replay-topic-metadata-retry
  fix/hpsf-replay-unique-image-tags
  fix/hpsf-stage-b-smoke-current-startup-logs
  fix/hunt-disable
  fix/ibkr-dev-port-4002
  fix/ibkr-feed-dev-required
  fix/ibkr-feed-dev-scale-zero
+ fix/ibkr-slice-regen
  fix/ignore-completed-pods-during-cleanup-wait
  fix/imagetools-http-registry
  fix/indicator-mono-mirror-genesis
  fix/infra-allow-app-postgres
  fix/internal-topic-config-skip-vanished
  fix/invasion-writer-image-tags
  fix/jenkins-build-platform-amd64
  fix/jenkins-built-in-node
  fix/jenkins-cps-extract-methods
  fix/jenkins-deploy-guard-kubeconfig-path
  fix/jenkins-dev-remote-registry
  fix/jenkins-dev-url-and-dry-run-safety
  fix/jenkins-kubectl-path
  fix/jenkins-methodtoolarge-extract2
  fix/jenkins-remote-kafka-default
  fix/jenkins-remote-kafka-default-clean
  fix/jenkins-workspace-tmp
  fix/kafka-managed-data-cleanup
  fix/kafka-node-56-to-4
  fix/kafka-sofr-current-cleanup
  fix/kafka-topic-rf-tolerant
  fix/keycloak-pin-control-plane
  fix/keycloak-postgres-clusterip
  fix/keycloak-secret-when
  fix/local-dev-cleanup-before-build
  fix/master-frontend-url
  fix/mirror-1dte-standalone-overlay
  fix/mirror-exemption-sigpipe
+ fix/mirror-targets-follow-es4-source
  fix/mirrored-es-topic-shapes
  fix/mission-control-monolithic-mirror
  fix/mission-pressure-bounded-state
  fix/mm2-heartbeat-loop
  fix/nifty-gex-image-pinning-wiring
  fix/nifty-gex-pipeline-timeout
+ fix/no-compaction-anywhere
  fix/nonce-dev-basis-resume
  fix/nonce-dev-basis-resume-2
  fix/normalize-jenkins-kubeconfig-params
  fix/normalize-kubeconfig-file-env
  fix/od-open10-mirrored-topic-names
  fix/od-open10-topic-names-standalone
  fix/oe-keycloak-realm-comment-keys
+ fix/offhours-exclude-calibration
  fix/offhours-preserve-pin-session-close
  fix/offhours-preserve-zerodte-research
  fix/oi-shadow-dev-raw-topic
+ fix/oi-shadow-unversioned-app-id
  fix/oi-topics-survive-the-wipe
  fix/one-identity-unversion-all
  fix/oom-tight-services-4gi
+ fix/opb-dev-mirror-final
+ fix/opb-dev-rolling-window-mirror
  fix/opb-disable-rolling-windows-prod
+ fix/opb-mirror-drift
  fix/opb-v1-removal-and-orphan-cleanup
  fix/opb-v2-bootstrap-calib
  fix/opb-v2-calib-values
  fix/opb-v2-candidate-v2
  fix/opb-v2-dedup-calib
  fix/opb-v2-ewma-180k
  fix/opb-v2-mark-live-calibrated
  fix/opb-v2-prod-calibrated
  fix/opb-v2-readiness-grace-for-slow-restore
  fix/opb-v2-surface-age-gate
  fix/opb-v2r-appid
  fix/opb-v2r2-appid
  fix/opb-v2r4-incremental
  fix/overnight-set-feed-name
  fix/permanent-seller-activity-deploy
  fix/pge-archive
  fix/pge-awk-quoting
  fix/pin-comment-yaml
  fix/pin-databento-gex-dev-image
  fix/pin-databento-gex-production-image
  fix/pin-gamma-lab-digest
  fix/pin-ibkr-feed-to-control-plane
  fix/pre-create-market-carry-topic
  fix/premarket-hours-label
  fix/premarket-readiness-drop-volpace
+ fix/preopen-status-on-values-on-chain
  fix/preserve-basis-state-in-resets
  fix/preserve-gamma-migration-crossings
  fix/preserve-indicator-warmup-state
  fix/preserve-reversal-calibration-topics
  fix/prod-1dte-slice
  fix/prod-allow-derived-spot
  fix/prod-drop-stale-broker-endpoints
+ fix/prod-es4-uncompacted-served-topics-env
  fix/prod-feed-liveness-deploy-env
  fix/prod-feed-liveness-env
  fix/prod-gateway-health-url
  fix/prod-integration-test-always
+ fix/prod-oi-direct-fetch-on
  fix/prod-opb-heap-3g
  fix/prod-partition-default-override
  fix/prod-preopen-live-oi
  fix/prod-schemas-retention-guard
  fix/prod-single-broker-rf1
  fix/prod-strike-intel-bypass-mirror
  fix/prod-strike-intel-conflict-bypass-agreement-2
  fix/prod-tunnel-ws-nodeport
  fix/prod-web-verify-remote
  fix/prune-zero-dte-legacy-identity
  fix/public-web-reach-pressure
  fix/pure-compact-verified-everywhere
  fix/pvc-storageclass-per-env
  fix/quarantine-mismatched-cells
  fix/raw-postgres-writer-single-pod-rollout
  fix/raw-to-display-mirror-drift
  fix/raw-writer-ensure-schema
+ fix/realm-scan-quoting
+ fix/reenable-opb
  fix/reenable-preopen-gex-dev
  fix/reenable-seed
  fix/regen-all-slices
  fix/regen-databento-feed-dev-slice
  fix/regen-slices
  fix/regenerate-mission-control-slices
+ fix/regenerate-vix-feed-slice
  fix/registry-drift
  fix/remote-postgres-dev-config
  fix/remote-runtime-config-defaults
  fix/rename-vix-option-inteligence
  fix/replay-moving-spot
  fix/restore-remote-jenkins-defaults
+ fix/retire-host-gateway-use-k8s
  fix/rollout-status-timeout-1200s
  fix/scorer-memory-bump
  fix/service-deploy-buildjob-routing
  fix/service-deploy-choice
+ fix/service-deploy-close-direction-choice
  fix/service-deploy-drop-classifier-choice
  fix/service-deploy-multicontainer-pin
  fix/service-deploy-pause-before-identity-prune
  fix/service-deploy-pause-clean
  fix/service-deploy-registry-reconcile
  fix/service-deploy-route-own-repo-buildjob
  fix/service-deploy-stderr
  fix/service-deploy-timeout
  fix/short-premium-cadence
  fix/short-premium-dev-synthetic
  fix/short-premium-down
  fix/short-premium-pause
  fix/signal-follower-mono-image-wiring
  fix/skip-web-smoke-dry-run
  fix/smoke-auth-deep-verify
  fix/smoke-authgated-mission-control
  fix/smoke-web-auth-posture
  fix/smoke-weburl-dev-scope
  fix/spot-band-flow-topic
  fix/spot-vol-regime-retention
  fix/spx-mission-control-auth-env
  fix/spx-mission-control-databento-deploy
  fix/sr3-quarterly-config
+ fix/stage-order-guard-after-hpsf-retirement
  fix/stock-gex-oi-schedule
  fix/stock-gex-symbol-budget
  fix/strike-flow-app-id-v5
  fix/strike-flow-appid-v3
  fix/strike-flow-cleanup-on-start
  fix/strike-flow-delta-overlay-off
+ fix/strike-flow-deploy-contract
  fix/strike-flow-distributed-live
  fix/strike-flow-unversioned-pin
  fix/strike-intel-oom-raise-memory
+ fix/strike-liquidity-review2
  fix/tape-zones-mirror-target-config
  fix/tick-floor-dev-mirror
  fix/timewarp-image-pinnable
  fix/timewarp-image-pinning
  fix/tunnel-watchdog-domain
+ fix/unify-ib-databento-expiry
  fix/validate-sigpipe
  fix/validate-stage-order-keycloak
  fix/vap-exempt-system-accounts-pvc-binding
  fix/verify-images-digest-compare
  fix/verify-pods-digest-norm
  fix/verify-rf-tolerant
+ fix/verify-topics-prod-subtraction
  fix/verify-topics-test-env-leak
  fix/verify-tunnel-req-client
+ fix/vix-feed-image-digest-pinnable
  fix/vix-index-fresh-seconds-90
+ fix/vix-intel-cpu-request
  fix/vix-price-stall-alerting
  fix/vix-unpause-script
  fix/volume-aggregator-appid-v2
  fix/volume-aggregator-cleanup-on-start
  fix/web-dev-auth-allow-disabled
  fix/web-dev-auth-back-on
  fix/web-dev-auth-off
  fix/web-dev-journal-mirror
  fix/web-dev-nodeselector
  fix/web-liquidity-history-rest-url-dev
  fix/web-multileg-service-base
  fix/web-node-pin
  fix/web-postgres-env
+ fix/web-rollingupdate-stable-8090
  fix/web-service-unbound-health-url
  fix/web-slice-dealer-ledger-flag
  fix/wire-es-gex-spx-align-image
  fix747
  follow-up-tmp
  gamma-now
  gexflow-throttle-interval
+ guard/preopen-publisher-selection
  guard/verify-compacted-contract
  hotfix/es4-groovy-escape
  hotfix/jenkins-deployer-lease-rbac
  hotfix/jenkins-deployer-pods-create
  hotfix/rewind-deploy-export-params
  hotfix/rewind-deploy-groovy-escape
  inv
  lifecycle
+ main
  mainlocal
  merge/rocksdb-state-stores-up-to-date
  mirrorfix
  oi-gate-now
  ops/clean-close-plus-30
  ops/dev-epoch-genesis-grant
  ops/dev-genesis-overlay-patch
+ ops/disable-spread-skew-all-envs
  ops/es-clean-topics-preserve-training
  ops/es4-cleanreset-keep-es-web
+ ops/gamma-lab-access-report
  ops/ibkr-feed-off-prod
  ops/offhours-keycloak-overnight
  ops/offhours-no-db-truncate-match-dev
  ops/revert-dev-genesis-grant
  ops/spx-mission-control-mono-mirror
  opt-exchange
+ perf/dealer-ledger-tier1
  promote/everything-to-main
  promote/everything-to-main-prod
  r13-mirror
  r13-topics
+ r14-fragility-topics
  refactor/market-data-expiry
+ revert/enable-live-replay
  revert/opb-v2r4-restore-stable
  revert/pin-flow-explorer-deploy
  revert/strong-only-emission
  spot-topic
  src
  svcwork
  tmp-clean
  tmp-main
  tmp/chk-drift
  tmp/mirror-multitarget
  tmp/render-check
  tmp/verify-drift
  verify-final
  verify-main
  verify-main2
  verify-main3
  verify-main4
  verify-run
  work/del
  work/gml
  work/hs
  remotes/gitlab/main
  remotes/origin/HEAD -> origin/main
  remotes/origin/chore/es4-es-cvd-append-time-since
  remotes/origin/chore/ib-expiry-auto-default
  remotes/origin/chore/offhours-manual-only
  remotes/origin/chore/prod-enable-directional-pressure
  remotes/origin/chore/remove-detached-node-4
  remotes/origin/chore/remove-fullfunding
  remotes/origin/chore/retire-hpsf-from-dev-and-prod
  remotes/origin/chore/revert-prod-oi-direct-fetch
  remotes/origin/chore/short-premium-72b-timeout
  remotes/origin/chore/split-infra-and-fullfunding
  remotes/origin/ci/bring-up-all-ui-only-gate
  remotes/origin/claude/rocksdb-state-stores
  remotes/origin/crossings-preserve
  remotes/origin/crossings-topic-deploy
  remotes/origin/docs/clarify-dev-prod-boundary
  remotes/origin/docs/delta-flow-runbook
  remotes/origin/feat/a5-ledger-durability
  remotes/origin/feat/approach-monitor-dev-up
  remotes/origin/feat/banknifty-pressure-route
  remotes/origin/feat/close-batch-pure-java
  remotes/origin/feat/close-board-catch-up
  remotes/origin/feat/context-tape-direction-a4
  remotes/origin/feat/context-tape-direction-dev-shadow
  remotes/origin/feat/dev-gex-overnight-oi-keepalive
  remotes/origin/feat/dev-shadow-model-enable
  remotes/origin/feat/dev-shadow-model-enable-v2
  remotes/origin/feat/es-amt-deploy
  remotes/origin/feat/es-footprint-mirror-to-dev-prod
  remotes/origin/feat/es-gex-mirror-jenkins
  remotes/origin/feat/es4-es-cvd-footprint
  remotes/origin/feat/es4-gateway-footprint-enable
  remotes/origin/feat/es4-gateway-footprint-memory
  remotes/origin/feat/es4-gex-oi-parents
  remotes/origin/feat/es4-mirror-service-deploy-tier1
  remotes/origin/feat/flip-spot-source-to-cascaded
  remotes/origin/feat/footprint-gateway-contingency-guard
  remotes/origin/feat/gamma-leadership-deploy-wiring
  remotes/origin/feat/gateway-nightly-restart-cron
  remotes/origin/feat/gex-deploy-wiring
  remotes/origin/feat/ibkr-trades-onboarding
  remotes/origin/feat/local-dev-two-broker-rf2
  remotes/origin/feat/nifty-gex-public-route
  remotes/origin/feat/nifty-gex-secret-writeback-carveout
  remotes/origin/feat/nifty-gex-service-slice
  remotes/origin/feat/oi-live-only-direct-fetch-off
  remotes/origin/feat/oi-snapshot-dev
  remotes/origin/feat/pin-gamma-lab-401fix
  remotes/origin/feat/pin-gamma-lab-csp-bars
  remotes/origin/feat/pin-gamma-lab-pressure
  remotes/origin/feat/pin-gamma-lab-topbar
  remotes/origin/feat/platform-error-watch
  remotes/origin/feat/premarket-canary
  remotes/origin/feat/pressure-role-contract
  remotes/origin/feat/prod-preopen-live-oi
  remotes/origin/feat/req-portal-stack
  remotes/origin/feat/retire-pressure-public-routes
  remotes/origin/feat/spy-feed-instance
  remotes/origin/feat/track-prod-tunnel-config
  remotes/origin/feat/vol-premium-deploy
  remotes/origin/feat/web-service-suppress-nested-autodeploy
  remotes/origin/feature/hpsf-v2.1-bugzilla-40-64
  remotes/origin/feature/ibkr-vix-price-topic
  remotes/origin/feature/replay-backend-plane-ansible
  remotes/origin/feature/replay-configurable-sampling
  remotes/origin/feature/spx-mission-control-deploy
  remotes/origin/fix/cleanup-nonpublic-schema-guard
  remotes/origin/fix/compaction-dev-only
  remotes/origin/fix/declare-deploy-environment
  remotes/origin/fix/delta-flow-greek-lookahead-tolerance
  remotes/origin/fix/deploy-pin-postgres-writer-digest-pin
  remotes/origin/fix/dev-cpu-headroom
  remotes/origin/fix/dev-delta-flow-lookahead-tolerance
  remotes/origin/fix/dev-es-mirror-partition-drift
  remotes/origin/fix/disable-mission-svcs
  remotes/origin/fix/disable-preopen-gex-arbitration
  remotes/origin/fix/es-auction-mirror-case
  remotes/origin/fix/es4-broker-num-partitions-32
  remotes/origin/fix/es4-clean-reset-drift-guard-and-wedge
  remotes/origin/fix/es4-compacted-token-split
  remotes/origin/fix/es4-deploy-image-freshness-gate
  remotes/origin/fix/es4-gateway-market-calendar
  remotes/origin/fix/es4-heatmap-topics-32
  remotes/origin/fix/es4-tape-retention
  remotes/origin/fix/feed-gateway-slice-lost-container-name
  remotes/origin/fix/gamma-leadership-dev-overlay-image
  remotes/origin/fix/gamma-leadership-dev-registry
  remotes/origin/fix/gateway-graceful-liveness
  remotes/origin/fix/gateway-probe-restore-name-ports
  remotes/origin/fix/gex-disable-dynamic-carry
  remotes/origin/fix/hpsf-blackbox-replay-jenkins-step
  remotes/origin/fix/hpsf-replay-agent-routing
  remotes/origin/fix/hpsf-replay-topic-metadata-retry
  remotes/origin/fix/hpsf-stage-b-smoke-current-startup-logs
  remotes/origin/fix/journal-nav-signin-url
  remotes/origin/fix/kafka-managed-data-cleanup
  remotes/origin/fix/kafka-sofr-current-cleanup
  remotes/origin/fix/mirror-targets-follow-es4-source
  remotes/origin/fix/nifty-gex-image-pinning-wiring
  remotes/origin/fix/nifty-gex-pipeline-timeout
  remotes/origin/fix/no-compaction-anywhere
  remotes/origin/fix/oi-shadow-unversioned-app-id
  remotes/origin/fix/opb-v1-removal-and-orphan-cleanup
  remotes/origin/fix/overnight-set-feed-name
  remotes/origin/fix/premarket-hours-label
  remotes/origin/fix/preopen-status-on-values-on-chain
  remotes/origin/fix/preserve-gamma-migration-crossings
  remotes/origin/fix/prod-es4-uncompacted-served-topics-env
  remotes/origin/fix/prod-oi-direct-fetch-on
  remotes/origin/fix/prod-preopen-live-oi
  remotes/origin/fix/public-web-reach-pressure
  remotes/origin/fix/pvc-storageclass-per-env
  remotes/origin/fix/reenable-preopen-gex-dev
  remotes/origin/fix/regenerate-vix-feed-slice
  remotes/origin/fix/service-deploy-buildjob-routing
  remotes/origin/fix/service-deploy-pause-before-identity-prune
  remotes/origin/fix/service-deploy-pause-clean
  remotes/origin/fix/service-deploy-route-own-repo-buildjob
  remotes/origin/fix/spot-band-flow-topic
  remotes/origin/fix/stage-order-guard-after-hpsf-retirement
  remotes/origin/fix/stock-gex-symbol-budget
  remotes/origin/fix/strike-flow-distributed-live
  remotes/origin/fix/timewarp-image-pinning
  remotes/origin/fix/verify-topics-prod-subtraction
  remotes/origin/fix/vix-feed-image-digest-pinnable
  remotes/origin/fix/vix-price-stall-alerting
  remotes/origin/main
  remotes/origin/r13-mirror
  remotes/origin/r13-topics
  remotes/origin/r14-fragility-topics
  remotes/origin/refactor/market-data-expiry
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-ntaLaTQn' (errno=Operation not permitted)
2026-09-08 20:59:47.006 xcodebuild[61648:265449630]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:47.845 xcodebuild[61648:265449626] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-MLhvQhbF' (errno=Operation not permitted)
2026-09-08 20:59:49.784 xcodebuild[61734:265449876]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:50.720 xcodebuild[61734:265449875] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
ccd6374b (HEAD -> feat/footprint-gateway-contingency-guard, origin/feat/footprint-gateway-contingency-guard) fix(footprint guard): close the fail-open exemption and run it on the deploy path
52c60ebb ci(footprint): the G-R8 contingency is checked, not trusted
226a36fc (origin/main, origin/HEAD) Merge pull request #1009 from abhinav-jain09/feat/web-service-suppress-nested-autodeploy
335a6a73 (origin/feat/web-service-suppress-nested-autodeploy, feat/web-service-suppress-nested-autodeploy) fix(web-service-deploy): suppress the build job's dev auto-deploy (no double-rollout)
211e9e46 fix(gateway): dev — keep the pre-open STATUS plane, stand the VALUE plane down (#1008)
0c4a7aa4 fix(gateway): turn pre-open GEX arbitration off — it drops 100% of pre-open records (#1006)
9b48bbf7 fix(es-auction mirror): an unterminated brace group made the preflight die at EOF (#1004)
144ebf40 es4: the ES tape retains 7 days, so the Auction Desk can build its prior session (#1003)
74e787e8 Merge pull request #1002 from abhinav-jain09/feat/es4-gateway-footprint-enable
36a3bde4 (origin/feat/es4-gateway-footprint-enable, feat/es4-gateway-footprint-enable) docs: archive the merge-gate round for the es4 footprint enablement
d5a91163 es4 gateway: enable the ES Footprint relay, and pin the record ceiling on both sides
3e1e7043 es4: SPX Auction Desk (es-amt) service, es.futures.auction topic + mirror, gateway opt-in (#994)
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-oUxKNfIA' (errno=Operation not permitted)
2026-09-08 20:59:56.140 xcodebuild[61994:265450600]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:56.977 xcodebuild[61994:265450593] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-TH8Gqa0m' (errno=Operation not permitted)
2026-09-08 20:59:58.092 xcodebuild[62098:265450941]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:59:58.520 xcodebuild[62098:265450940] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
fatal: ambiguous argument 'origin/master...HEAD': unknown revision or path not in the working tree.
Use '--' to separate paths from revisions, like this:
'git <command> [<revision>...] -- [<file>...]'
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-hVqDX49E' (errno=Operation not permitted)
2026-09-08 21:00:03.420 xcodebuild[62324:265451625]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:00:04.674 xcodebuild[62324:265451620] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-olByn80W' (errno=Operation not permitted)
2026-09-08 21:00:07.399 xcodebuild[62431:265451906]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:00:08.205 xcodebuild[62431:265451904] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
fatal: ambiguous argument 'origin/master...HEAD': unknown revision or path not in the working tree.
Use '--' to separate paths from revisions, like this:
'git <command> [<revision>...] -- [<file>...]'

 succeeded in 21521ms:
2026-09-08 21:00:07.741 xcodebuild[62438:265451941]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:00:08.401 xcodebuild[62438:265451937] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-rSKwE6pe' (errno=Operation not permitted)
2026-09-08 21:00:10.187 xcodebuild[62530:265452299]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:00:10.808 xcodebuild[62530:265452294] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
 .github/workflows/deploy-validation.yml            |  11 +++
 Jenkinsfile.es4-deploy                             |   8 ++
 .../validate-footprint-gateway-contingency-test.sh | 106 +++++++++++++++++++++
 .../ci/validate-footprint-gateway-contingency.sh   |  90 +++++++++++++++++
 4 files changed, 215 insertions(+)
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-rVvKridk' (errno=Operation not permitted)
2026-09-08 21:00:15.206 xcodebuild[62671:265452750]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:00:15.812 xcodebuild[62671:265452744] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-6guzWhLW' (errno=Operation not permitted)
2026-09-08 21:00:17.646 xcodebuild[62742:265452983]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:00:18.788 xcodebuild[62742:265452972] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
M	.github/workflows/deploy-validation.yml
M	Jenkinsfile.es4-deploy
A	scripts/ci/validate-footprint-gateway-contingency-test.sh
A	scripts/ci/validate-footprint-gateway-contingency.sh
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-L6hkSWe4' (errno=Operation not permitted)
2026-09-08 21:00:23.402 xcodebuild[62954:265453603]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:00:24.094 xcodebuild[62954:265453600] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-csvF7mvW' (errno=Operation not permitted)
2026-09-08 21:00:25.582 xcodebuild[63042:265453818]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:00:26.110 xcodebuild[63042:265453817] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
diff --git a/.github/workflows/deploy-validation.yml b/.github/workflows/deploy-validation.yml
index 9907b81e..762d207d 100644
--- a/.github/workflows/deploy-validation.yml
+++ b/.github/workflows/deploy-validation.yml
@@ -76,6 +76,17 @@ jobs:
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
diff --git a/Jenkinsfile.es4-deploy b/Jenkinsfile.es4-deploy
index d0fcf27c..352e5b8c 100644
--- a/Jenkinsfile.es4-deploy
+++ b/Jenkinsfile.es4-deploy
@@ -179,6 +179,14 @@ pipeline {
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
diff --git a/scripts/ci/validate-footprint-gateway-contingency-test.sh b/scripts/ci/validate-footprint-gateway-contingency-test.sh
new file mode 100755
index 00000000..e128b8b2
--- /dev/null
+++ b/scripts/ci/validate-footprint-gateway-contingency-test.sh
@@ -0,0 +1,106 @@
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
+# caught, which is the failure mode a mutation suite exists to rule out.
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
+expect 1 "the record ceiling differs" "producer ceiling above the gateway's" "$GW" "$WORK/prod-bigger.yaml"
+
+# 2. the producer stops declaring a ceiling at all
+grep -v 'FOOTPRINT_MAX_RECORD_BYTES' "$PROD" | grep -v '^ *value: "262144"' > "$WORK/prod-none.yaml"
+expect 1 "sets no FOOTPRINT_MAX_RECORD_BYTES" "producer declares no ceiling" "$GW" "$WORK/prod-none.yaml"
+
+# 3. the limit is lowered back to the heap size (the incident this rule exists for)
+sed 's/^\( *\)memory: "2560Mi"/\1memory: "1536Mi"/' "$GW" > "$WORK/gw-tight.yaml"
+expect 1 "G-R8 native headroom is 0 MiB" "limit equal to -Xmx: no native headroom" "$WORK/gw-tight.yaml" "$PROD"
+
+# 4. the heap is raised into the headroom instead
+sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2176m/' "$GW" > "$WORK/gw-fat-heap.yaml"
+expect 1 "G-R8 native headroom is 384 MiB" "heap raised to leave under 512 MiB" "$WORK/gw-fat-heap.yaml" "$PROD"
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
+expect 1 "the record ceiling differs" "and refuses the same manifest once the relay is enabled" "$WORK/gw-on-broken.yaml" "$PROD"
+
+# ---- the enablement flag itself must be a decision, never a parse failure -------------------
+# Each of these would, before the fix, have EXEMPTED the manifest from every check above.
+awk '$1 == "-" && $2 == "name:" && $3 == "GATEWAY_ES_FOOTPRINT_ENABLED" { skip = 2 }
+     skip > 0 { skip--; next } { print }' "$GW" > "$WORK/gw-noflag.yaml"
+expect 1 "declares GATEWAY_ES_FOOTPRINT_ENABLED 0 times" "the flag is missing entirely" "$WORK/gw-noflag.yaml" "$PROD"
+
+awk '$1 == "-" && $2 == "name:" { name = $3 }
+     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/true/, "TRUE") }
+     { print }' "$GW" > "$WORK/gw-badflag.yaml"
+expect 1 "it must be exactly true or false" "the flag is not a canonical boolean" "$WORK/gw-badflag.yaml" "$PROD"
+
+# false FIRST, then true: Kubernetes takes the last, the old guard read the first and stood down
+awk '$1 == "-" && $2 == "name:" && $3 == "GATEWAY_ES_FOOTPRINT_ENABLED" {
+         print "        - name: GATEWAY_ES_FOOTPRINT_ENABLED"; print "          value: \"false\"" }
+     { print }' "$GW" > "$WORK/gw-dupflag.yaml"
+expect 1 "declares GATEWAY_ES_FOOTPRINT_ENABLED 2 times" "the flag is declared twice" "$WORK/gw-dupflag.yaml" "$PROD"
+
+echo "every guard refused its own violation, for its own reason"
diff --git a/scripts/ci/validate-footprint-gateway-contingency.sh b/scripts/ci/validate-footprint-gateway-contingency.sh
new file mode 100755
index 00000000..55d331b7
--- /dev/null
+++ b/scripts/ci/validate-footprint-gateway-contingency.sh
@@ -0,0 +1,90 @@
+#!/usr/bin/env bash
+# ES Footprint deployment contingency (G-R8), enforced on the manifests rather than trusted.
+#
+# Two facts about this subsystem are only true if two SEPARATE manifests agree, and nothing else
+# in CI notices when they stop agreeing:
+#
+#   1. The record ceiling is ONE value. The gateway drops any record above its own ceiling, so a
+#      producer allowed to emit larger records loses those records SILENTLY at the relay — the
+#      producer is happy, the topic has the bytes, and the page never sees them.
+#   2. G-R8's memory inequality. The relay's extra working set lives OUTSIDE the heap, so a
+#      container whose limit equals its -Xmx is OOM-killed on native growth the heap never sees.
+#      The rule is limit - Xmx >= 512 MiB, and it must hold wherever the relay is ENABLED.
+#
+# Both are checked only where the relay is actually on: a manifest with the flag off is not
+# holding anything up, and demanding the headroom there would be noise.
+set -euo pipefail
+cd "$(dirname "$0")/../.."
+
+fail() { printf 'FOOTPRINT CONTINGENCY: %s\n' "$*" >&2; exit 1; }
+
+# The value of an env var in a manifest: the `value:` line that FOLLOWS its `- name:` line.
+# The value of an env var: the `value:` line that FOLLOWS its `- name:` line. Taking $2 alone
+# truncates a value with spaces to its first word — which is how "-Xms256m -Xmx1536m" read as
+# "-Xms256m" and this check reported a missing -Xmx that was right there.
+env_value() {
+    awk -v want="$2" '
+        $1 == "-" && $2 == "name:" { name = $3; next }
+        $1 == "value:" && name == want {
+            sub(/^ *value: */, ""); gsub(/^"|"$/, ""); print; exit
+        }
+    ' "$1"
+}
+
+# How many ACTIVE entries declare this name. Two entries make the manifest ambiguous, and which
+# one Kubernetes honours is not something this guard should be guessing at.
+env_count() {
+    awk -v want="$2" '
+        $1 == "-" && $2 == "name:" && $3 == want { n++ }
+        END { print n + 0 }
+    ' "$1"
+}
+
+mib() { printf '%s' "$1" | awk '{ if ($0 ~ /Mi$/) { sub(/Mi$/, ""); print $0 } else if ($0 ~ /Gi$/) { sub(/Gi$/, ""); print $0 * 1024 } else { print "" } }'; }
+
+GW=k8s/es4/services/es-feed-gateway.yaml
+PROD=k8s/es4/services/es-cvd.yaml
+[ -f "$GW" ] || fail "$GW is missing"
+[ -f "$PROD" ] || fail "$PROD is missing"
+
+# The exemption must be a DECISION, not a parse failure. Anything other than exactly one entry
+# holding exactly `true` or `false` is refused: a missing, malformed or duplicated flag would
+# otherwise exempt the manifest from every check below, and a pair ordered false-then-true would
+# deploy an ENABLED relay past a guard that read the first value and stood down.
+count=$(env_count "$GW" GATEWAY_ES_FOOTPRINT_ENABLED)
+[ "$count" = "1" ] \
+    || fail "$GW declares GATEWAY_ES_FOOTPRINT_ENABLED $count times; exactly one entry decides whether the relay is on"
+enabled=$(env_value "$GW" GATEWAY_ES_FOOTPRINT_ENABLED)
+case "$enabled" in
+    true) ;;
+    false)
+        printf 'footprint relay is disabled in %s: nothing to check\n' "$GW"
+        exit 0
+        ;;
+    *) fail "GATEWAY_ES_FOOTPRINT_ENABLED is '$enabled' in $GW; it must be exactly true or false" ;;
+esac
+
+# ---- (1) one ceiling, both sides -------------------------------------------------------------
+gw_bytes=$(env_value "$GW" GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES)
+pr_bytes=$(env_value "$PROD" FOOTPRINT_MAX_RECORD_BYTES)
+[ -n "$gw_bytes" ] || fail "the relay is enabled but $GW sets no GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES"
+[ -n "$pr_bytes" ] || fail "the relay is enabled but $PROD sets no FOOTPRINT_MAX_RECORD_BYTES"
+[ "$gw_bytes" = "$pr_bytes" ] \
+    || fail "the record ceiling differs: gateway $gw_bytes vs producer $pr_bytes. A producer allowed to emit larger records loses them silently at the relay"
+
+# ---- (2) G-R8: limit - Xmx >= 512 MiB ---------------------------------------------------------
+limit_raw=$(awk '/^ *limits:/ { inlim = 1; next } inlim && /memory:/ { v = $2; gsub(/"/, "", v); print v; exit }' "$GW")
+[ -n "$limit_raw" ] || fail "no memory limit found in $GW"
+limit=$(mib "$limit_raw")
+[ -n "$limit" ] || fail "memory limit '$limit_raw' is neither Mi nor Gi; this check cannot compare it"
+
+xmx_raw=$(env_value "$GW" JAVA_TOOL_OPTIONS)
+xmx=$(printf '%s' "$xmx_raw" | sed -n 's/.*-Xmx\([0-9]*\)m.*/\1/p')
+[ -n "$xmx" ] || fail "no -Xmx found in JAVA_TOOL_OPTIONS ('$xmx_raw')"
+
+headroom=$(( limit - xmx ))
+[ "$headroom" -ge 512 ] \
+    || fail "G-R8 native headroom is ${headroom} MiB (limit ${limit} MiB - Xmx ${xmx} MiB); the relay's working set is OUTSIDE the heap and needs >= 512 MiB"
+
+printf 'footprint contingency OK: ceiling %s on both sides; headroom %s MiB (limit %s, Xmx %s)\n' \
+    "$gw_bytes" "$headroom" "$limit" "$xmx"

codex
The three stated folds are present in the diff. I’m now checking whether their implementation matches Kubernetes/YAML semantics, especially the AWK parser’s document scope and the Jenkins action/dry-run control flow—not just whether the added lines exist.
exec
/bin/bash -lc "nl -ba scripts/ci/validate-footprint-gateway-contingency.sh; nl -ba scripts/ci/validate-footprint-gateway-contingency-test.sh; sed -n '1,280p' Jenkinsfile.es4-deploy | nl -ba; rg -n \"kubectl (apply|replace|create)|ACTION|DEPLOY_DRY_RUN|validate-footprint|validate-dealer\" Jenkinsfile.es4-deploy; rg -n -C 5 \"GATEWAY_ES_FOOTPRINT_ENABLED|GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES|JAVA_TOOL_OPTIONS|FOOTPRINT_MAX_RECORD_BYTES|limits:\" k8s/es4/services/es-feed-gateway.yaml k8s/es4/services/es-cvd.yaml; nl -ba .github/workflows/deploy-validation.yml | sed -n '45,115p'; nl -ba scripts/ci/validate-services.sh | sed -n '1,180p'" in /private/tmp/oe-deploy-mirror
 succeeded in 7ms:
     1	#!/usr/bin/env bash
     2	# SERVICE-REGISTRY VALIDATION (standalone-service-deployment design §13.9).
     3	#
     4	# Fails when the repo drifts from services.yaml — the single source of truth:
     5	#   1. COMPLETENESS: every Deployment rendered by the monolithic env overlays must be
     6	#      registered in services.yaml (a new service that skips registration fails here —
     7	#      the gap that let delta-flow/maxpain/integration-test reach a deploy unbuilt).
     8	#   2. STANDALONE SLICES: every managedBy=standalone service has k8s/services/<name>
     9	#      with an overlay per declared env, rendering ONLY service-owned kinds (§13.5).
    10	#   3. MIRROR RULE: the standalone render of each service must be EQUIVALENT to the
    11	#      monolithic overlay's render of the same workload — env patches replicated in the
    12	#      service overlay cannot drift from the top-level ones.
    13	#   4. IMAGE NAME: the image basename in the standalone render matches services.yaml.
    14	#
    15	# Requires: kubectl (kustomize), yq. Read-only; no cluster access needed.
    16	set -euo pipefail
    17	cd "$(dirname "$0")/../.."
    18	
    19	command -v yq >/dev/null 2>&1 || { echo "FATAL: yq is required" >&2; exit 1; }
    20	fail=0
    21	TMP="$(mktemp -d)"
    22	trap 'rm -rf "$TMP"' EXIT
    23	
    24	ENVS=(dev production experiment)
    25	
    26	echo "=== render monolithic overlays ==="
    27	for e in "${ENVS[@]}"; do
    28	  kubectl kustomize "k8s/overlays/$e" >"$TMP/mono-$e.yaml" || { echo "FATAL: k8s/overlays/$e does not render" >&2; exit 1; }
    29	done
    30	
    31	echo "=== 1) completeness: every rendered Deployment is registered ==="
    32	yq -r '.services[].deployments[]' services.yaml | sort -u >"$TMP/registered.txt"
    33	for e in "${ENVS[@]}"; do
    34	  yq -r 'select(.kind=="Deployment") | .metadata.name' "$TMP/mono-$e.yaml"
    35	done | grep -v '^---$' | sort -u >"$TMP/rendered.txt"
    36	# Keycloak & friends are infra-owned, not service-registry entries. BOTH identity providers qualify:
    37	# `oe-keycloak*` is the internal one and `bo-keycloak*` the public one for bleedingoptions.com, and a
    38	# second identity provider is still an identity provider. The public WEB tier is deliberately NOT
    39	# excluded — it is an application workload, it IS registered in services.yaml, and this check is what
    40	# would notice if it ever stopped being.
    41	unregistered="$(comm -23 "$TMP/rendered.txt" "$TMP/registered.txt" | { grep -vE '^(oe-keycloak|keycloak|bo-keycloak)' || true; })"
    42	if [ -n "$unregistered" ]; then
    43	  echo "FAIL: Deployments rendered by the overlays but NOT registered in services.yaml:" >&2
    44	  printf '  %s\n' $unregistered >&2
    45	  fail=1
    46	else
    47	  echo "ok: all $(wc -l <"$TMP/rendered.txt" | tr -d ' ') rendered Deployments are registered"
    48	fi
    49	
    50	echo "=== 2+3+4) standalone slices: structure, blast radius, mirror rule, image name ==="
    51	while IFS= read -r name; do
    52	  img="$(yq -r ".services[] | select(.name == \"$name\") | .image" services.yaml)"
    53	  for e in $(yq -r ".services[] | select(.name == \"$name\") | .envs[]" services.yaml); do
    54	    overlay="k8s/services/$name/overlays/$e"
    55	    if [ ! -d "$overlay" ]; then
    56	      echo "FAIL: $name declares env '$e' but $overlay does not exist" >&2
    57	      fail=1; continue
    58	    fi
    59	    if ! kubectl kustomize "$overlay" >"$TMP/svc-$name-$e.yaml" 2>"$TMP/svc-$name-$e.err"; then
    60	      echo "FAIL: $overlay does not render:" >&2; head -3 "$TMP/svc-$name-$e.err" >&2
    61	      fail=1; continue
    62	    fi
    63	    # blast radius (§13.5)
    64	    bad="$(yq -r '.kind' "$TMP/svc-$name-$e.yaml" | grep -v '^---$' | sort -u \
    65	      | { grep -vE '^(Deployment|Service|HorizontalPodAutoscaler|ServiceMonitor|Ingress)$' || true; })"
    66	    if [ -n "$bad" ]; then
    67	      echo "FAIL: $overlay renders non-service-owned kinds: $bad" >&2
    68	      fail=1
    69	    fi
    70	    # image basename (§13.9)
    71	    # capture fully, then truncate — `yq | head -1` dies with SIGPIPE (rc 141) under
    72	    # `set -o pipefail` when yq keeps writing after head exits (agent yq buffering).
    73	    _imgs="$(yq -r 'select(.kind=="Deployment") | .spec.template.spec.containers[0].image' "$TMP/svc-$name-$e.yaml")"
    74	    rendered_img="$(printf '%s\n' "$_imgs" | grep -v '^---$' | head -1)"
    75	    base="${rendered_img##*/}"; base="${base%%[:@]*}"
    76	    if [ "$base" != "$img" ]; then
    77	      echo "FAIL: $overlay image basename '$base' != services.yaml image '$img'" >&2
    78	      fail=1
    79	    fi
    80	    # mirror rule: standalone render == the same workload extracted from the monolith.
    81	    #
    82	    # It only applies to options-edge. The rule exists because a standalone slice and the
    83	    # monolithic overlay describe the SAME workload and must not drift — but a tenant service
    84	    # has no monolithic counterpart at all (the monolith renders one namespace, and it is not
    85	    # theirs). Diffing against it compares a real render with an empty one and always fails.
    86	    #
    87	    # The exemption is deliberately keyed on the registry's namespace, not on a name pattern:
    88	    # a service that forgets to declare its namespace stays under the rule, which is the safe
    89	    # direction to fail in.
    90	    svc_ns="$(yq -r ".services[] | select(.name == \"$name\") | .namespace // \"options-edge\"" services.yaml | head -1)"
    91	    if [ "$svc_ns" != "options-edge" ]; then
    92	      echo "  note: $name/$e mirror rule skipped (namespace '$svc_ns' has no monolithic overlay)"
    93	    else
    94	    for dep in $(yq -r ".services[] | select(.name == \"$name\") | .deployments[]" services.yaml); do
    95	      yq "select(.metadata.name == \"$dep\")" "$TMP/mono-$e.yaml" \
    96	        | yq ea '[.] | sort_by(.kind) | .[] | splitDoc' >"$TMP/mirror-mono.yaml"
    97	      yq "select(.metadata.name == \"$dep\")" "$TMP/svc-$name-$e.yaml" \
    98	        | yq ea '[.] | sort_by(.kind) | .[] | splitDoc' >"$TMP/mirror-svc.yaml"
    99	      if ! diff -u "$TMP/mirror-mono.yaml" "$TMP/mirror-svc.yaml" >"$TMP/mirror-diff.txt"; then
   100	        echo "FAIL: MIRROR DRIFT for $name/$e ($dep): the standalone overlay renders differently from the monolithic overlay." >&2
   101	        echo "      Replicate the top-level env patch in $overlay (or vice versa). Diff:" >&2
   102	        head -25 "$TMP/mirror-diff.txt" | sed 's/^/      /' >&2
   103	        fail=1
   104	      fi
   105	    done
   106	    fi
   107	    # env image-tag CONSISTENCY guard (review): a service mapped in ANY image-tags env file
   108	    # must be mapped in EVERY env it deploys to — a partial mapping means the production
   109	    # fast path fails closed at deploy time (the render carries the base dev-registry ref
   110	    # pre-pin, so the mapping is the only production-image source). Services with NO
   111	    # mappings anywhere (own-job images: web, feed, ...) are out of this mechanism.
   112	    if [ -f "image-tags/$e.yaml" ]; then
   113	      _mapped_any=0
   114	      for _f in image-tags/*.yaml; do
   115	        # NOTE: plain grep >/dev/null, NOT grep -q — under pipefail, -q's early exit
   116	        # SIGPIPEs yq and a successful match reads as pipeline failure.
   117	        if yq -r ".images | to_entries[] | .value" "$_f" | grep "/$img:" >/dev/null; then _mapped_any=1; break; fi
   118	      done
   119	      if [ "$_mapped_any" = "1" ] && ! yq -r ".images | to_entries[] | .value" "image-tags/$e.yaml" | grep "/$img:" >/dev/null; then
   120	        echo "FAIL: image '$img' (service $name) is mapped in some image-tags env but MISSING from image-tags/$e.yaml" >&2
   121	        exit 1
   122	      fi
   123	    fi
   124	    echo "ok: $name/$e (blast radius, image, mirror)"
   125	  done
   126	done < <(yq -r '.services[] | select(.managedBy == "standalone") | .name' services.yaml)
   127	
   128	if [ "$fail" -ne 0 ]; then
   129	  echo "=== validate-services: FAILED ===" >&2
   130	  exit 1
   131	fi
   132	
   133	echo "=== 5) at-most-one VIX publisher (VIX feed separation design §7) ==="
   134	# Runs here so BOTH the PR CI pass and the service-deploy validation stage
   135	# (Jenkinsfile.service-deploy runs this script before any apply) enforce the
   136	# assertion; the monolith path gets it via scripts/deploy/validate-platform.sh.
   137	bash scripts/ci/validate-vix-single-publisher.sh
   138	
   139	echo "=== 5b) exactly one pre-open GEX publisher, matching the declaration ==="
   140	# Same shape as the VIX assertion and for the same reason: two publishers can own the
   141	# pre-open gamma surface, BOTH selections render cleanly, and picking the wrong one is
   142	# silent — pre-market GEX just stops appearing (incident 2026-08-24). The declaration in
   143	# k8s/preopen-publisher.env makes any switch an explicit, reviewed edit.
   144	bash scripts/ci/validate-preopen-single-publisher.sh
   145	
   146	echo "=== 6) durable topics are preserved by the destructive resets ==="
   147	# A topic declared retention=-1 in topics.env that the pre-market / clean-slate
   148	# resets would still wipe is the 2026-07-28 basis-cold-start incident class —
   149	# make that drift unmergeable rather than discoverable at 09:00 ET.
   150	bash scripts/ci/validate-durable-topic-preservation.sh
   151	
   152	# The OI anchor topic barrier: its parser, and the shipped script end to end against mocked CLIs.
   153	# Wired here because a regression test nothing runs is not a test -- and this particular parser has
   154	# already shipped one hole (it accepted "compact,delete", the exact policy it exists to reject).
   155	for t in scripts/kafka/ensure-oi-anchor-topic-parse-test.sh scripts/kafka/ensure-oi-anchor-topic-e2e-test.sh \
   156	         scripts/kafka/verify-topics-pure-compact-test.sh \
   157	         scripts/kafka/reset-preserved-topics-test.sh \
   158	         scripts/ci/validate-durable-topic-preservation-mutation-test.sh \
   159	         scripts/ci/es-cvd-mirror-shape-test.sh \
   160	         scripts/ci/es-auction-mirror-shape-test.sh; do
   161	  if [ ! -x "$t" ]; then
   162	    echo "FAIL: $t missing or not executable"
   163	    exit 1
   164	  fi
   165	  if ! out=$(bash "$t" 2>&1); then
   166	    echo "FAIL: $t"
   167	    printf '%s\n' "$out" | sed 's/^/      /'
   168	    exit 1
   169	  fi
   170	done
   171	echo "topic contracts: oi-anchor barrier, pure-compact verification, and the durable-preservation mutation suite passed"
   172	
   173	# --- continuous auto-hunt production acceptance (auto-arm req §3.1) ---
   174	# Both flags must be EFFECTIVELY true in BOTH prod mirrors: asserted on the
   175	# RENDERED manifests (not source text — a commented-out or duplicated env
   176	# entry must fail here, not deploy the hunt inert). Exactly one entry per
   177	# flag, value "true", on the reversal-confirmation container.
   178	echo "=== 7) continuous auto-hunt: production flags effective in both rendered mirrors ==="
   179	for r in "$TMP/mono-production.yaml" "$TMP/svc-reversal-confirmation-production.yaml"; do
   180	  if [ ! -s "$r" ]; then

exec
/bin/bash -lc "nl -ba Jenkinsfile.es4-deploy | sed -n '120,260p'; rg -n \"kubectl .*apply|kubectl apply|apply \\"'$DRY|ACTION ==|params.ACTION|DEPLOY_DRY_RUN" Jenkinsfile.es4-deploy' in /private/tmp/oe-deploy-mirror
 succeeded in 0ms:
   120	      when { expression { params.ACTION == 'clean-reset' } }
   121	      steps {
   122	        sh '''
   123	          set -euo pipefail
   124	          rsync -az --delete scripts/ "$ES4_HOST":/home/es4/repo/scripts/
   125	
   126	          # The prod-cluster es-feed is RETIRED (DBP-R33), so there is no prod feed to fence around
   127	          # the wipe any more — the only es-feed lives in the es4 cluster and the on-box cleanup
   128	          # already fences es4 Deployments itself. The prod kubeconfig is no longer needed here.
   129	          #
   130	          # ⚠️ What still matters: the on-box wipe (scripts/es4/cleanup-es4.sh) restores es4
   131	          # Deployments to their CAPTURED replica counts, which is blind to ES_FEED_LOCATION. If es4
   132	          # es-feed is live while the declared location is not es4, that restore brings it back
   133	          # against intent. Fail closed BEFORE the wipe.
   134	          # Prove BOTH kubeconfigs are the clusters we think they are before anything else. Without
   135	          # this on the clean-reset path, swapped kubeconfigs would let the absence check inspect the
   136	          # wrong cluster and pass.
   137	          bash scripts/es4/assert-cluster-identity.sh "$PROD_KUBECONFIG" prod "$PROD_NS_UID"
   138	          bash scripts/es4/assert-cluster-identity.sh "$ES4_KUBECONFIG"  es4  "$ES4_NS_UID"
   139	
   140	          # (a) The on-box cleanup treats ES4_FEED_FENCED=1 as PROOF that the cross-cluster prod
   141	          #     es-feed producer was quiesced. Since this stage no longer fences prod, that value
   142	          #     would otherwise be an unchecked assertion — and a prod es-feed that still exists can
   143	          #     publish straight through the wipe and recreate the dead-schema-id poison. Prove it.
   144	          bash scripts/es4/assert-prod-es-feed-absent.sh "$PROD_KUBECONFIG"
   145	
   146	          # (b) The cleanup no longer restores es-feed at all — it is always left at 0, whatever the
   147	          #     snapshot captured, because DBP-R22 reserves starting it to the separate activate
   148	          #     action. An earlier attempt passed a desired-replica count in instead; review showed
   149	          #     that turned clean-reset INTO an activation path when the declared location was es4,
   150	          #     and left an unset/arbitrary value unsafe. Not restoring it at all has neither hazard.
   151	          #     After a reset, start the feed deliberately with ACTION=activate-es-feed.
   152	
   153	          if [ "${DEPLOY_DRY_RUN}" = "true" ]; then
   154	            echo "DRY: would run the on-box wipe (no prod es-feed to fence — retired per DBP-R33)"
   155	            ssh -o BatchMode=yes "$ES4_HOST" "DEPLOY_DRY_RUN=true ES4_FEED_FENCED=1 ES4_EXPECTED_NS_UID=$ES4_NS_UID bash /home/es4/repo/scripts/es4/cleanup-es4.sh"
   156	            exit 0
   157	          fi
   158	
   159	          echo "=== running on-box atomic wipe ==="
   160	          ssh -o BatchMode=yes "$ES4_HOST" "DEPLOY_DRY_RUN=false ES4_FEED_FENCED=1 ES4_EXPECTED_NS_UID=$ES4_NS_UID bash /home/es4/repo/scripts/es4/cleanup-es4.sh"
   161	        '''
   162	      }
   163	    }
   164	    stage('Deploy to es4') {
   165	      when { expression { params.ACTION == 'deploy-service' || params.ACTION == 'deploy-all' } }
   166	      steps {
   167	        sh '''
   168	          set -euo pipefail
   169	          export KUBECONFIG="$ES4_KUBECONFIG"
   170	          DRY=""; [ "${DEPLOY_DRY_RUN:-false}" = "true" ] && DRY="--dry-run=server"
   171	          # Identity FIRST: this stage mutates through $KUBECONFIG (es4). Proving identity after the
   172	          # first apply would mean a mispointed kubeconfig had already written to the wrong cluster.
   173	          bash scripts/es4/assert-cluster-identity.sh "$KUBECONFIG"      es4  "$ES4_NS_UID"
   174	          bash scripts/es4/assert-cluster-identity.sh "$PROD_KUBECONFIG" prod "$PROD_NS_UID"
   175	
   176	          # Dependency-scoped FIRE-input verdict (processing PR #594): es4 applies the COMMITTED
   177	          # k8s/es4/services/*.yaml directly, so no overlay render exists for the other deploy paths'
   178	          # guard to have caught. Assert here, BEFORE the first apply, and unconditionally — the
   179	          # deploy-service path can target dealer-ledger, and deploy-all always includes it.
   180	          bash scripts/ci/validate-dealer-ledger-fire-quality.sh
   181	
   182	          # Same reason, same place: es4 applies the COMMITTED k8s/es4/services/*.yaml directly, so
   183	          # the ES Footprint contingency has no overlay render for another path's guard to catch.
   184	          # Both facts it checks fail SILENTLY in production — a producer above the gateway's record
   185	          # ceiling loses records at the relay with nothing logged upstream, and a gateway whose
   186	          # memory limit equals its -Xmx is OOM-killed on native growth the heap never sees (G-R8).
   187	          # Unconditional: deploy-service can target either half of the pair, and deploy-all takes both.
   188	          bash scripts/ci/validate-footprint-gateway-contingency.sh
   189	
   190	          kubectl apply $DRY -f k8s/es4/es4-base-env.yaml
   191	          kubectl apply $DRY -f k8s/es4/es4-common-env.yaml
   192	          # es-feed's OWN Databento configmap. Applied for every es4 deploy so the feed can
   193	          # never run against inherited library defaults again (requirement DBP-R1/R2).
   194	          kubectl apply $DRY -f k8s/es4/es4-feed-config.yaml
   195	          mkdir -p .jenkins-tmp
   196	
   197	          # ---- prod es-feed is RETIRED (DBP-R33): reconcile-delete it on every run ---------
   198	          # `kubectl apply` never prunes, so removing the manifest from git does not remove the live
   199	          # object. --ignore-not-found makes this idempotent, so it is not a one-time cleanup but a
   200	          # standing guard: if anything ever recreates es-feed on prod, the next es4 deploy removes
   201	          # it. Two copies cannot exist if only one location has a Deployment at all - which is why
   202	          # this replaces the publisher-lease work (DBP-R28) rather than complementing it.
   203	          if [ -z "$DRY" ]; then
   204	            # Prove the kubeconfig really is the PROD cluster before deleting anything through it.
   205	            # Without this, a PROD_KUBECONFIG that had been regenerated to point at .4 would delete
   206	            # the LIVE es4 es-feed and the ConfigMap applied moments earlier in this same stage.
   207	            KUBECONFIG="$PROD_KUBECONFIG" kubectl -n options-edge delete deployment es-feed --ignore-not-found
   208	            KUBECONFIG="$PROD_KUBECONFIG" kubectl -n options-edge delete configmap es-feed-databento-config --ignore-not-found
   209	          fi
   210	
   211	          # ---- es-feed preflight: fail CLOSED before the DEPLOYMENT is applied -----------
   212	          # (The three shared configmaps above are applied first; this is not "before all
   213	          # mutation", it is before anything that could create or change the feed workload.)
   214	          # Three independent hazards, each of which has already bitten us:
   215	          #  (1) the shared Databento API key cannot carry two concurrent ES option-chain
   216	          #      sessions, and both locations share the es-feed-spot consumer group;
   217	          #  (2) the API-key secret does not exist in the es4 cluster yet — a missing or empty
   218	          #      secret must be a loud failure, not a CrashLoop;
   219	          #  (3) activation must never be a deploy side effect (requirement DBP-R22).
   220	          if [ "$ACTION" = "deploy-service" ] && [ "$SERVICE" = "es-feed" ]; then
   221	            bash scripts/es4/assert-es-feed-exclusive.sh "$ES4_KUBECONFIG" "$PROD_KUBECONFIG" es4
   222	          fi
   223	          # es-feed is EXCLUDED from deploy-all on purpose: it owns a Databento live session on a
   224	          # shared API key, so it must only ever be applied as a deliberate, individually preflighted
   225	          # action (SERVICE=es-feed) — never swept in by a bulk deploy that bypasses the preflight
   226	          # above. `|| true` because grep exits 1 when it filters nothing, which under
   227	          # `set -euo pipefail` would abort the whole deploy.
   228	          if [ "$ACTION" = "deploy-all" ]; then
   229	            # The spread-skew stack is under an operator hold (2026-07-26): bulk
   230	            # deploys must not even apply it, in addition to its manifests being
   231	            # pinned at replicas 0.
   232	            FILES=$(ls k8s/es4/services/*.yaml | { grep -Ev '/(es-feed|spread-skew|spread-skew-postgres-writer)[.]yaml$' || true; })
   233	            [ -n "$FILES" ] || { echo "deploy-all: no service manifests resolved" >&2; exit 1; }
   234	            echo "deploy-all: es-feed and the held spread-skew stack intentionally EXCLUDED"
   235	          else
   236	            FILES="k8s/es4/services/${SERVICE}.yaml"
   237	          fi
   238	          for f in $FILES; do
   239	            bash scripts/es4/pin-and-apply.sh "$f" "$REGISTRY" "$DRY"
   240	          done
   241	          if [ -z "$DRY" ] && { [ "$ACTION" = "deploy-all" ] || [ "$SERVICE" = "vix-option-inteligence" ]; }; then
   242	            # Remove the legacy identity only after pin-and-apply has completed the replacement
   243	            # rollout, avoiding duplicate consumers on the isolated ES broker.
   244	            kubectl -n options-edge delete deployment zero-dte-intelligence-service --ignore-not-found
   245	            kubectl -n options-edge delete service zero-dte-intelligence-service --ignore-not-found
   246	          fi
   247	          # es-feed is NEVER started by a deploy: the manifest always carries replicas 0 and this
   248	          # path performs no scaling at all — activation is its own ACTION (requirement DBP-R22).
   249	          if [ "$ACTION" = "deploy-service" ] && [ "$SERVICE" = "es-feed" ]; then
   250	            echo "es-feed manifest applied at replicas 0. To start it: ACTION=activate-es-feed"
   251	          fi
   252	          [ -n "$DRY" ] || kubectl -n options-edge get pods -o wide
   253	        '''
   254	      }
   255	    }
   256	    stage('Activate es-feed (separate authorized action)') {
   257	      when { expression { params.ACTION == 'activate-es-feed' } }
   258	      steps {
   259	        sh '''
   260	          set -euo pipefail
19://                  DESTRUCTIVE (drops all es4 Kafka data); honors DEPLOY_DRY_RUN.
48:    booleanParam(name: 'DEPLOY_DRY_RUN', defaultValue: false,
49:      description: 'true = render + digest-pin + kubectl apply --dry-run=server only.')
77:      when { expression { params.ACTION == 'infra-sync' } }
92:      when { expression { params.ACTION == 'create-topics' } }
112:      // wedged the scorer. DESTRUCTIVE: drops all es4 Kafka data; honor DEPLOY_DRY_RUN to preview.
120:      when { expression { params.ACTION == 'clean-reset' } }
153:          if [ "${DEPLOY_DRY_RUN}" = "true" ]; then
155:            ssh -o BatchMode=yes "$ES4_HOST" "DEPLOY_DRY_RUN=true ES4_FEED_FENCED=1 ES4_EXPECTED_NS_UID=$ES4_NS_UID bash /home/es4/repo/scripts/es4/cleanup-es4.sh"
160:          ssh -o BatchMode=yes "$ES4_HOST" "DEPLOY_DRY_RUN=false ES4_FEED_FENCED=1 ES4_EXPECTED_NS_UID=$ES4_NS_UID bash /home/es4/repo/scripts/es4/cleanup-es4.sh"
165:      when { expression { params.ACTION == 'deploy-service' || params.ACTION == 'deploy-all' } }
170:          DRY=""; [ "${DEPLOY_DRY_RUN:-false}" = "true" ] && DRY="--dry-run=server"
190:          kubectl apply $DRY -f k8s/es4/es4-base-env.yaml
191:          kubectl apply $DRY -f k8s/es4/es4-common-env.yaml
194:          kubectl apply $DRY -f k8s/es4/es4-feed-config.yaml
198:          # `kubectl apply` never prunes, so removing the manifest from git does not remove the live
257:      when { expression { params.ACTION == 'activate-es-feed' } }
279:          if [ "${DEPLOY_DRY_RUN:-false}" = "true" ]; then
290:      when { expression { params.ACTION == 'deactivate-es-feed' } }
314:            if [ "${DEPLOY_DRY_RUN:-false}" = "true" ]; then echo "DRY: would scale $loc es-feed to 0 and wait for pods"; continue; fi

exec
/bin/bash -lc 'command -v yq || true; command -v shellcheck || true; bash scripts/ci/validate-footprint-gateway-contingency.sh; bash scripts/ci/validate-footprint-gateway-contingency-test.sh; bash -n scripts/ci/validate-footprint-gateway-contingency.sh scripts/ci/validate-footprint-gateway-contingency-test.sh' in /private/tmp/oe-deploy-mirror
 succeeded in 71ms:
footprint contingency OK: ceiling 262144 on both sides; headroom 1024 MiB (limit 2560, Xmx 1536)
mktemp: mkdtemp failed on /var/folders/t3/_t_9ln7d49g_yvjqnyr_rdj40000gn/T/tmp.CQzY7HJvvt: Operation not permitted

exec
/bin/bash -lc 'git show --stat --oneline ccd6374b; git show --format=fuller --find-renames ccd6374b --; git diff --check origin/main...HEAD; git show origin/main:Jenkinsfile.es4-deploy | rg -n "kubectl .*apply|pin-and-apply|ACTION ==|params.ACTION"' in /private/tmp/oe-deploy-mirror
2026-09-08T19:01:21.405915Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
 succeeded in 31855ms:
2026-09-08 21:00:51.504 xcodebuild[64187:265456674]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:00:52.057 xcodebuild[64187:265456672] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-mdR1VKam' (errno=Operation not permitted)
2026-09-08 21:00:53.280 xcodebuild[64274:265456909]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:00:53.785 xcodebuild[64274:265456908] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
ccd6374b fix(footprint guard): close the fail-open exemption and run it on the deploy path
 Jenkinsfile.es4-deploy                             |  8 +++
 .../validate-footprint-gateway-contingency-test.sh | 57 ++++++++++++++++------
 .../ci/validate-footprint-gateway-contingency.sh   | 28 +++++++++--
 3 files changed, 75 insertions(+), 18 deletions(-)
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-CHjkxQQW' (errno=Operation not permitted)
2026-09-08 21:00:56.926 xcodebuild[64473:265457449]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:00:57.564 xcodebuild[64473:265457436] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-brm6hJxQ' (errno=Operation not permitted)
2026-09-08 21:00:59.336 xcodebuild[64605:265457749]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:01:00.266 xcodebuild[64605:265457746] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
commit ccd6374b64fbf8d96fa23812592ffd0120e1b8e6
Author:     Abhinav Jain <abhinav.jain09@gmail.com>
AuthorDate: Tue Sep 8 20:58:47 2026 +0200
Commit:     Abhinav Jain <abhinav.jain09@gmail.com>
CommitDate: Tue Sep 8 20:58:47 2026 +0200

    fix(footprint guard): close the fail-open exemption and run it on the deploy path
    
    Codex round 1, two HIGH findings, both correct:
    
    1. The relay-off exemption failed OPEN. Anything that was not the literal
       string `true` — a missing flag, a malformed one, a duplicate pair
       ordered false-then-true (Kubernetes takes the LAST, the guard read the
       first) — exempted the manifest from every check. The flag is now a
       decision: exactly one entry, exactly `true` or `false`, and anything
       else is refused. Three mutation cases drive those states.
    
    2. The path that actually applies these manifests did not run the guard.
       Jenkinsfile.es4-deploy applies the committed k8s/es4/services/*.yaml
       directly, and repository precedent (validate-dealer-ledger-fire-quality,
       processing #594) puts dependency-scoped checks there, before the first
       apply. The guard now runs in that same place, unconditionally.
    
    Also folded a MEDIUM: an expected-failure case asserted only the exit
    code, so an unrelated guard failure returning 1 would have counted as
    every mutation being caught — the failure mode a mutation suite exists to
    rule out. Each case now asserts the refusal it is actually testing for.
    
    Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>

diff --git a/Jenkinsfile.es4-deploy b/Jenkinsfile.es4-deploy
index d0fcf27c..352e5b8c 100644
--- a/Jenkinsfile.es4-deploy
+++ b/Jenkinsfile.es4-deploy
@@ -179,6 +179,14 @@ pipeline {
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
diff --git a/scripts/ci/validate-footprint-gateway-contingency-test.sh b/scripts/ci/validate-footprint-gateway-contingency-test.sh
index 69b98ee9..e128b8b2 100755
--- a/scripts/ci/validate-footprint-gateway-contingency-test.sh
+++ b/scripts/ci/validate-footprint-gateway-contingency-test.sh
@@ -10,48 +10,60 @@ GUARD=scripts/ci/validate-footprint-gateway-contingency.sh
 GW=k8s/es4/services/es-feed-gateway.yaml
 PROD=k8s/es4/services/es-cvd.yaml
 
-run_against() {   # $1 = gateway manifest, $2 = producer manifest -> exit code of the guard
+OUT="$WORK/out"
+
+run_against() {   # $1 = gateway manifest, $2 = producer manifest -> exit code; output in $OUT
     local d="$WORK/case"; rm -rf "$d"; mkdir -p "$d/scripts/ci" "$d/k8s/es4/services"
     cp "$GUARD" "$d/scripts/ci/"
     cp "$1" "$d/k8s/es4/services/es-feed-gateway.yaml"
     cp "$2" "$d/k8s/es4/services/es-cvd.yaml"
-    ( cd "$d" && ./scripts/ci/validate-footprint-gateway-contingency.sh >/dev/null 2>&1 ) && echo 0 || echo $?
+    ( cd "$d" && ./scripts/ci/validate-footprint-gateway-contingency.sh ) > "$OUT" 2>&1 && echo 0 || echo $?
 }
 
-expect() {   # $1 = expected code, $2 = label, $3.. = manifests
-    local want="$1" label="$2"; shift 2
+# A refusal is only a kill if it is THIS refusal. Asserting the exit code alone lets an unrelated
+# failure — a typo in the guard, a missing file, a parse error — count as every mutation being
+# caught, which is the failure mode a mutation suite exists to rule out.
+expect() {   # $1 = expected code, $2 = expected message substring ("" when passing), $3 = label, $4.. = manifests
+    local want="$1" msg="$2" label="$3"; shift 3
     local got; got=$(run_against "$@")
     if [ "$got" != "$want" ]; then
         printf 'MUTATION SURVIVED: %s (guard exited %s, expected %s)\n' "$label" "$got" "$want" >&2
+        sed 's/^/    | /' "$OUT" >&2
+        exit 1
+    fi
+    if [ -n "$msg" ] && ! grep -qF -- "$msg" "$OUT"; then
+        printf 'WRONG REASON: %s exited %s but not for the reason under test\n' "$label" "$got" >&2
+        printf '    expected to contain: %s\n' "$msg" >&2
+        sed 's/^/    | /' "$OUT" >&2
         exit 1
     fi
     printf '  killed: %s\n' "$label"
 }
 
 echo "baseline"
-expect 0 "the manifests as committed pass" "$GW" "$PROD"
+expect 0 "footprint contingency OK" "the manifests as committed pass" "$GW" "$PROD"
 
 echo "mutations"
 # 1. the two ceilings diverge
 sed 's/^\( *\)value: "262144"/\1value: "524288"/' "$PROD" > "$WORK/prod-bigger.yaml"
-expect 1 "producer ceiling above the gateway's" "$GW" "$WORK/prod-bigger.yaml"
+expect 1 "the record ceiling differs" "producer ceiling above the gateway's" "$GW" "$WORK/prod-bigger.yaml"
 
 # 2. the producer stops declaring a ceiling at all
 grep -v 'FOOTPRINT_MAX_RECORD_BYTES' "$PROD" | grep -v '^ *value: "262144"' > "$WORK/prod-none.yaml"
-expect 1 "producer declares no ceiling" "$GW" "$WORK/prod-none.yaml"
+expect 1 "sets no FOOTPRINT_MAX_RECORD_BYTES" "producer declares no ceiling" "$GW" "$WORK/prod-none.yaml"
 
 # 3. the limit is lowered back to the heap size (the incident this rule exists for)
 sed 's/^\( *\)memory: "2560Mi"/\1memory: "1536Mi"/' "$GW" > "$WORK/gw-tight.yaml"
-expect 1 "limit equal to -Xmx: no native headroom" "$WORK/gw-tight.yaml" "$PROD"
+expect 1 "G-R8 native headroom is 0 MiB" "limit equal to -Xmx: no native headroom" "$WORK/gw-tight.yaml" "$PROD"
 
 # 4. the heap is raised into the headroom instead
 sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2176m/' "$GW" > "$WORK/gw-fat-heap.yaml"
-expect 1 "heap raised to leave under 512 MiB" "$WORK/gw-fat-heap.yaml" "$PROD"
+expect 1 "G-R8 native headroom is 384 MiB" "heap raised to leave under 512 MiB" "$WORK/gw-fat-heap.yaml" "$PROD"
 
 # 5. exactly at the boundary is ACCEPTED — the rule is >=, and a guard that refuses its own
 #    boundary would force every future budget to overshoot it
 sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2048m/' "$GW" > "$WORK/gw-boundary.yaml"
-expect 0 "exactly 512 MiB of headroom is legal" "$WORK/gw-boundary.yaml" "$PROD"
+expect 0 "headroom 512 MiB" "exactly 512 MiB of headroom is legal" "$WORK/gw-boundary.yaml" "$PROD"
 
 # 6. relay off: the guard has nothing to hold up and says so. Flip the flag's OWN value line —
 #    a blanket s/"true"/"false"/ hits the first unrelated flag in the file and passes for the
@@ -61,17 +73,34 @@ awk '$1 == "-" && $2 == "name:" { name = $3 }
      { print }' "$GW" > "$WORK/gw-off.yaml"
 grep -A1 GATEWAY_ES_FOOTPRINT_ENABLED "$WORK/gw-off.yaml" | grep -q 'value: "false"' \
     || { echo "the disable mutation did not take: the case would pass for the wrong reason" >&2; exit 1; }
-expect 0 "relay disabled: not checked" "$WORK/gw-off.yaml" "$PROD"
+expect 0 "footprint relay is disabled" "relay disabled: not checked" "$WORK/gw-off.yaml" "$PROD"
 
 # 6b. and with the relay off the guard is not merely quiet — it is quiet BECAUSE of the flag:
 #     the same manifest with a broken ceiling still passes while disabled, and fails once enabled
 awk '$1 == "-" && $2 == "name:" { name = $3 }
      $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES" { sub(/262144/, "999999") }
      { print }' "$WORK/gw-off.yaml" > "$WORK/gw-off-broken.yaml"
-expect 0 "relay disabled hides a ceiling mismatch (by design)" "$WORK/gw-off-broken.yaml" "$PROD"
+expect 0 "footprint relay is disabled" "relay disabled hides a ceiling mismatch (by design)" "$WORK/gw-off-broken.yaml" "$PROD"
 awk '$1 == "-" && $2 == "name:" { name = $3 }
      $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/false/, "true") }
      { print }' "$WORK/gw-off-broken.yaml" > "$WORK/gw-on-broken.yaml"
-expect 1 "and refuses the same manifest once the relay is enabled" "$WORK/gw-on-broken.yaml" "$PROD"
+expect 1 "the record ceiling differs" "and refuses the same manifest once the relay is enabled" "$WORK/gw-on-broken.yaml" "$PROD"
+
+# ---- the enablement flag itself must be a decision, never a parse failure -------------------
+# Each of these would, before the fix, have EXEMPTED the manifest from every check above.
+awk '$1 == "-" && $2 == "name:" && $3 == "GATEWAY_ES_FOOTPRINT_ENABLED" { skip = 2 }
+     skip > 0 { skip--; next } { print }' "$GW" > "$WORK/gw-noflag.yaml"
+expect 1 "declares GATEWAY_ES_FOOTPRINT_ENABLED 0 times" "the flag is missing entirely" "$WORK/gw-noflag.yaml" "$PROD"
+
+awk '$1 == "-" && $2 == "name:" { name = $3 }
+     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/true/, "TRUE") }
+     { print }' "$GW" > "$WORK/gw-badflag.yaml"
+expect 1 "it must be exactly true or false" "the flag is not a canonical boolean" "$WORK/gw-badflag.yaml" "$PROD"
+
+# false FIRST, then true: Kubernetes takes the last, the old guard read the first and stood down
+awk '$1 == "-" && $2 == "name:" && $3 == "GATEWAY_ES_FOOTPRINT_ENABLED" {
+         print "        - name: GATEWAY_ES_FOOTPRINT_ENABLED"; print "          value: \"false\"" }
+     { print }' "$GW" > "$WORK/gw-dupflag.yaml"
+expect 1 "declares GATEWAY_ES_FOOTPRINT_ENABLED 2 times" "the flag is declared twice" "$WORK/gw-dupflag.yaml" "$PROD"
 
-echo "every guard refused its own violation"
+echo "every guard refused its own violation, for its own reason"
diff --git a/scripts/ci/validate-footprint-gateway-contingency.sh b/scripts/ci/validate-footprint-gateway-contingency.sh
index 903bb557..55d331b7 100755
--- a/scripts/ci/validate-footprint-gateway-contingency.sh
+++ b/scripts/ci/validate-footprint-gateway-contingency.sh
@@ -31,6 +31,15 @@ env_value() {
     ' "$1"
 }
 
+# How many ACTIVE entries declare this name. Two entries make the manifest ambiguous, and which
+# one Kubernetes honours is not something this guard should be guessing at.
+env_count() {
+    awk -v want="$2" '
+        $1 == "-" && $2 == "name:" && $3 == want { n++ }
+        END { print n + 0 }
+    ' "$1"
+}
+
 mib() { printf '%s' "$1" | awk '{ if ($0 ~ /Mi$/) { sub(/Mi$/, ""); print $0 } else if ($0 ~ /Gi$/) { sub(/Gi$/, ""); print $0 * 1024 } else { print "" } }'; }
 
 GW=k8s/es4/services/es-feed-gateway.yaml
@@ -38,11 +47,22 @@ PROD=k8s/es4/services/es-cvd.yaml
 [ -f "$GW" ] || fail "$GW is missing"
 [ -f "$PROD" ] || fail "$PROD is missing"
 
+# The exemption must be a DECISION, not a parse failure. Anything other than exactly one entry
+# holding exactly `true` or `false` is refused: a missing, malformed or duplicated flag would
+# otherwise exempt the manifest from every check below, and a pair ordered false-then-true would
+# deploy an ENABLED relay past a guard that read the first value and stood down.
+count=$(env_count "$GW" GATEWAY_ES_FOOTPRINT_ENABLED)
+[ "$count" = "1" ] \
+    || fail "$GW declares GATEWAY_ES_FOOTPRINT_ENABLED $count times; exactly one entry decides whether the relay is on"
 enabled=$(env_value "$GW" GATEWAY_ES_FOOTPRINT_ENABLED)
-if [ "$enabled" != "true" ]; then
-    printf 'footprint relay is not enabled in %s: nothing to check\n' "$GW"
-    exit 0
-fi
+case "$enabled" in
+    true) ;;
+    false)
+        printf 'footprint relay is disabled in %s: nothing to check\n' "$GW"
+        exit 0
+        ;;
+    *) fail "GATEWAY_ES_FOOTPRINT_ENABLED is '$enabled' in $GW; it must be exactly true or false" ;;
+esac
 
 # ---- (1) one ceiling, both sides -------------------------------------------------------------
 gw_bytes=$(env_value "$GW" GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES)
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-cEUMZu5Q' (errno=Operation not permitted)
2026-09-08 21:01:06.230 xcodebuild[64809:265458372]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:01:07.000 xcodebuild[64809:265458366] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-Ka1hH1Pg' (errno=Operation not permitted)
2026-09-08 21:01:09.420 xcodebuild[64884:265458629]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:01:10.855 xcodebuild[64884:265458627] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-lbBLLDo5' (errno=Operation not permitted)
2026-09-08 21:01:14.913 xcodebuild[65126:265459370]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:01:16.393 xcodebuild[65126:265459369] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-BLR23qLi' (errno=Operation not permitted)
2026-09-08 21:01:18.509 xcodebuild[65219:265459664]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:01:19.359 xcodebuild[65219:265459662] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
49:      description: 'true = render + digest-pin + kubectl apply --dry-run=server only.')
77:      when { expression { params.ACTION == 'infra-sync' } }
92:      when { expression { params.ACTION == 'create-topics' } }
120:      when { expression { params.ACTION == 'clean-reset' } }
165:      when { expression { params.ACTION == 'deploy-service' || params.ACTION == 'deploy-all' } }
182:          kubectl apply $DRY -f k8s/es4/es4-base-env.yaml
183:          kubectl apply $DRY -f k8s/es4/es4-common-env.yaml
186:          kubectl apply $DRY -f k8s/es4/es4-feed-config.yaml
190:          # `kubectl apply` never prunes, so removing the manifest from git does not remove the live
231:            bash scripts/es4/pin-and-apply.sh "$f" "$REGISTRY" "$DRY"
234:            # Remove the legacy identity only after pin-and-apply has completed the replacement
249:      when { expression { params.ACTION == 'activate-es-feed' } }
282:      when { expression { params.ACTION == 'deactivate-es-feed' } }

exec
/bin/bash -lc "yq -r '.kind, .metadata.name, (.spec.template.spec.containers | length), (.spec.template.spec.containers[] | [.name, (.env | length), .resources.limits.memory] | @tsv)' k8s/es4/services/es-feed-gateway.yaml; yq -r '.spec.template.spec.containers[] | select(.name == \"es-feed-gateway\") | .env[] | select(.name == \"GATEWAY_ES_FOOTPRINT_ENABLED\" or .name == \"GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES\" or .name == \"JAVA_TOOL_OPTIONS\") | [.name, .value] | @tsv' k8s/es4/services/es-feed-gateway.yaml; yq -r '.spec.template.spec.containers[] | [.name, (.env[]? | select(.name == \"FOOTPRINT_MAX_RECORD_BYTES\") | .value)] | @tsv' k8s/es4/services/es-cvd.yaml; yq --version; rg -n --glob '*.sh' \"env_count\\(\\)|env_value\\(\\)|yq -r.*containers.*env\" scripts/ci | head -80" in /private/tmp/oe-deploy-mirror
 succeeded in 76ms:



---
es-cvd	262144
yq (https://github.com/mikefarah/yq/) version v4.53.3
scripts/ci/validate-preopen-single-publisher.sh:56:  vals="$(yq -r "select(.kind==\"Deployment\" and .metadata.name==\"$DEPLOY_NAME\") | .spec.template.spec.containers[] | select(.name==\"databento-gex\") | .env[]? | select(.name==\"$name\") | .value" "$render" 2>/dev/null || true)"
scripts/ci/validate-gex-dev-safety.sh:15:env_value() {
scripts/ci/validate-footprint-gateway-contingency.sh:25:env_value() {
scripts/ci/validate-footprint-gateway-contingency.sh:36:env_count() {

2026-09-08T19:01:27.602315Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
exec
/bin/bash -lc "sed -n '1,180p' scripts/ci/validate-dealer-ledger-fire-quality.sh | nl -ba; rg -n \"yq\" Jenkinsfile.es4-deploy scripts/es4 | head -40; git diff --check origin/main...HEAD 2>&1 | tail -20" in /private/tmp/oe-deploy-mirror
 succeeded in 6681ms:
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-7snEA6Z0' (errno=Operation not permitted)
2026-09-08 21:01:47.292 xcodebuild[66545:265462886]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:01:48.035 xcodebuild[66545:265462883] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-mXCZ1s05' (errno=Operation not permitted)
2026-09-08 21:01:49.347 xcodebuild[66703:265463225]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 21:01:50.014 xcodebuild[66703:265463224] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.

2026-09-08T19:01:53.640422Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
codex
Found one remaining blocking issue.

- HIGH — The guard still parses YAML globally with AWK instead of selecting the intended Deployment/container. In [validate-footprint-gateway-contingency.sh](/private/tmp/oe-deploy-mirror/scripts/ci/validate-footprint-gateway-contingency.sh:25), both `env_count` and `env_value` accept matching `- name:` entries anywhere in the multi-document file. They ignore comments and ordinary indentation and do not confuse the name inside another value, but they also cannot distinguish another container or YAML block. More importantly, the memory-limit parser at line 76 takes the first `limits:` followed by any `memory:`. Adding an init container or sidecar before `feed-gateway` could therefore make the guard validate that container’s memory limit while the gateway itself has insufficient headroom. This is a concrete fail-open case for the invariant the guard exists to enforce. The repository already requires `yq` on this exact Jenkins path through `validate-dealer-ledger-fire-quality.sh`; that script demonstrates the appropriate pattern: select Deployment `es-feed-gateway`, then container `feed-gateway`, and inspect its `env` and `resources.limits.memory`. Thus using AWK provides no dependency advantage.

Verification of the round-1 folds:

- Relay-off handling is substantially fixed: the flag must count exactly once and its extracted value must be exactly `true` or `false`; missing, noncanonical, and false-then-true duplicate mutations are present. However, “exactly once” currently means anywhere in the file rather than exactly once in the target container, which is part of the blocking parser issue above.
- The Jenkins guard is correctly inside `Deploy to es4`, unconditional for both `deploy-service` and `deploy-all`, after cluster identity checks and before the first `kubectl apply`. `DEPLOY_DRY_RUN` changes apply mode but does not skip the guard. No other `ACTION` branch applies these committed service manifests.
- `expect()` now verifies both exit status and a reason-specific fixed-string fragment and prints captured output on mismatch. This closes the unrelated-exit-code hole.

The asserted message fragments are somewhat brittle, particularly computed prose such as `G-R8 native headroom is 0 MiB` and `384 MiB`. A reasonable wording change would break tests without changing behavior. Stable refusal identifiers such as `E_CEILING_MISMATCH`, `E_HEADROOM`, and `E_FLAG_COUNT` would preserve the “right reason” assertion without treating human-readable prose as an API. I consider this maintainability debt, not independently blocking.

The committed manifests pass the guard and both scripts pass `bash -n`. The mutation suite could not execute in this read-only review environment because `mktemp -d` was denied.

VERDICT: REQUEST_CHANGES
tokens used
58.496
Found one remaining blocking issue.

- HIGH — The guard still parses YAML globally with AWK instead of selecting the intended Deployment/container. In [validate-footprint-gateway-contingency.sh](/private/tmp/oe-deploy-mirror/scripts/ci/validate-footprint-gateway-contingency.sh:25), both `env_count` and `env_value` accept matching `- name:` entries anywhere in the multi-document file. They ignore comments and ordinary indentation and do not confuse the name inside another value, but they also cannot distinguish another container or YAML block. More importantly, the memory-limit parser at line 76 takes the first `limits:` followed by any `memory:`. Adding an init container or sidecar before `feed-gateway` could therefore make the guard validate that container’s memory limit while the gateway itself has insufficient headroom. This is a concrete fail-open case for the invariant the guard exists to enforce. The repository already requires `yq` on this exact Jenkins path through `validate-dealer-ledger-fire-quality.sh`; that script demonstrates the appropriate pattern: select Deployment `es-feed-gateway`, then container `feed-gateway`, and inspect its `env` and `resources.limits.memory`. Thus using AWK provides no dependency advantage.

Verification of the round-1 folds:

- Relay-off handling is substantially fixed: the flag must count exactly once and its extracted value must be exactly `true` or `false`; missing, noncanonical, and false-then-true duplicate mutations are present. However, “exactly once” currently means anywhere in the file rather than exactly once in the target container, which is part of the blocking parser issue above.
- The Jenkins guard is correctly inside `Deploy to es4`, unconditional for both `deploy-service` and `deploy-all`, after cluster identity checks and before the first `kubectl apply`. `DEPLOY_DRY_RUN` changes apply mode but does not skip the guard. No other `ACTION` branch applies these committed service manifests.
- `expect()` now verifies both exit status and a reason-specific fixed-string fragment and prints captured output on mismatch. This closes the unrelated-exit-code hole.

The asserted message fragments are somewhat brittle, particularly computed prose such as `G-R8 native headroom is 0 MiB` and `384 MiB`. A reasonable wording change would break tests without changing behavior. Stable refusal identifiers such as `E_CEILING_MISMATCH`, `E_HEADROOM`, and `E_FLAG_COUNT` would preserve the “right reason” assertion without treating human-readable prose as an API. I consider this maintainability debt, not independently blocking.

The committed manifests pass the guard and both scripts pass `bash -n`. The mutation suite could not execute in this read-only review environment because `mktemp -d` was denied.

VERDICT: REQUEST_CHANGES
