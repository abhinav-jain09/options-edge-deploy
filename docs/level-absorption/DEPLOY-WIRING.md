# Level Absorption — deploy wiring (options-edge-deploy)

Service: `level-absorption-service` (options-edge-processing, module `level-absorption-service`, image `options-edge-level-absorption`,
Jenkins `BUILD_TARGET=options-edge-level-absorption`). Wired for **dev and production**, phase **EYEBALL** (status events, NO verdict),
**held at `replicas: 0` everywhere**. **It cannot start yet**, on any environment: two required input files do not exist and cannot
be invented (below). Deploy is done when the image is built, pushed and digest-pinned.

Sources read: `runtime/RuntimeSettings.java`, `StartupChecks.java`, `FrozenConfigurationFile.java`, `LevelAbsorptionServiceMain.java`,
`level-absorption-input-contract-manifest.json`, `scripts/kafka/apply-level-absorption-topics.sh`,
`docs/level-absorption/as-built/runtime-gaps.md` section 6.2.

## What was wired (the es-compression-expansion / hedging-pressure touch points)

| Touch point | Change |
|---|---|
| `k8s/services/level-absorption/base/` | Deployment (`replicas: 0`, `Recreate`, one container), kustomization |
| `k8s/services/level-absorption/overlays/{dev,production}/` | generated slices (`scripts/deploy/generate-service-slices.sh`) |
| `k8s/overlays/{dev,production}/kustomization.yaml` | service base referenced; dev image remap `host.docker.internal:5001/options-edge-level-absorption:dev` |
| `services.yaml` | `level-absorption`, standalone, `sliceGenerated`, envs dev + production |
| `image-tags/{dev,production}.yaml` | `level-absorption-service` mapping (`:dev` / `:prod`) |
| `Jenkinsfile` | `LEVEL_ABSORPTION_IMAGE` parameter, image-default map, promote-to-production list |
| `Jenkinsfile.service-deploy` | `level-absorption` in the `SERVICE` choice list; stage "Verify level-absorption output topics" |
| `scripts/deploy/{resolve-images,image-lock,image-preflight,apply}.sh` | `LEVEL_ABSORPTION_IMAGE` resolved, lock-checked, preflighted and digest-pinned (dev + production loop) |
| `scripts/kafka/topics.env` | voice, closures, manual-line: 1 partition, 7 days, exact one partition. **Events is deliberately not declared** (below) |
| `scripts/kafka/ensure-level-absorption-topics.sh` (+ `-test.sh`) | first-deploy barrier for all four topics; test wired into `scripts/ci/validate-services.sh` |
| `scripts/ops/{morning-autostart,oe-boot-bringup,dev-cleanup}.sh` | `level-absorption-service` added to `KEEP_DOWN` (both lists, kept equal) and `DISABLED_DEV` |

## Environment

Set in the manifest (`k8s/services/level-absorption/base/level-absorption-deployment.yaml`).

| Variable | Value in the manifest | Code default | Note |
|---|---|---|---|
| `LEVEL_ABSORPTION_PHASE` | `EYEBALL` | none (required; `EYEBALL` or `LIVE`) | decided: EYEBALL on dev and production, the publication table does not exist yet |
| `LEVEL_ABSORPTION_PUBLICATION_TABLE` | **not set** | none | with `EYEBALL` a set value is REFUSED at start; it is required only for `LIVE` |
| `LEVEL_ABSORPTION_FROZEN_CONFIGURATION` | `/home/options-edge/config/level-absorption/frozen-configuration.json` | none (required) | owner-supplied file (the file name is this wiring's choice) |
| `LEVEL_ABSORPTION_RETENTION_ARTIFACT` | `/home/options-edge/config/level-absorption/retention-artifact.json` | none (required) | owner-supplied file (the file name is this wiring's choice) |
| `LEVEL_ABSORPTION_LATENCY_TRACE_DIRECTORY` | `/home/options-edge/data/level-absorption/latency` | none (required) | hostPath, read-write |
| `KAFKA_LEVEL_ABSORPTION_EVENTS_TOPIC` | `futures.level-absorption.events` | same | |
| `KAFKA_LEVEL_ABSORPTION_VOICE_TOPIC` | `futures.level-absorption.voice` | same | |
| `KAFKA_LEVEL_ABSORPTION_CLOSURES_TOPIC` | `futures.level-absorption.closures` | same | |
| `KAFKA_LEVEL_ABSORPTION_HOST_LEVELS_TOPIC` | `options.spx.market-structure.ui` | manifest base name | version 2 (geometry sites, section 6.2): the market-structure host's `UiProjection` record; created by `scripts/kafka/ensure-market-structure-topics.sh`; required at startup only when a HOST site kind is enabled in the Phase 0 freeze |
| `KAFKA_LEVEL_ABSORPTION_FOOTPRINT_TOPIC` | `es.futures.footprint.bars` | manifest base `futures.footprint.bars` | dev and production carry the es4 output mirrored under `es.` while `TOPIC_PREFIX` is empty, so the full name is given (an override must end with the manifest's name) |
| `KAFKA_LEVEL_ABSORPTION_AUCTION_TOPIC` | `es.futures.auction` | manifest base `futures.auction` | same reason |
| `KAFKA_LEVEL_ABSORPTION_{TRADES,MANUAL_LINE}_TOPIC` | not set | manifest base names | `underlying.es.trades.linearized` (the linearizer's name on dev and production) and `futures.level-absorption.manual-line` |
| `LEVEL_ABSORPTION_CONSUMER_GROUP` | `level-absorption-service` | same | |
| `LEVEL_ABSORPTION_SYMBOL` | `ES.v.0` | same | |
| `HEALTH_PORT` | `8080` | `8080` | |
| `LEVEL_ABSORPTION_TAPE_SILENCE_MILLIS` | `30000` | `30000` (1000..3600000) | |
| `LEVEL_ABSORPTION_MAX_HELD_PRINTS` | `500000` | `500000` (1000..50000000) | |
| `LEVEL_ABSORPTION_SILENT_TAPE_GRACE_MILLIS` | `5000` | `5000` (0..600000) | |
| `LEVEL_ABSORPTION_LIVENESS_BOUND_MILLIS` | `120000` | `120000` (5000..3600000) | |
| `LEVEL_ABSORPTION_LEVEL_SOURCE_MAX_AGE_MILLIS` | `3600000` | `3600000` (60000..172800000) | |
| `LEVEL_ABSORPTION_CONTAINER_PATH_PREFIX` | not set | none | not needed: container paths equal host paths |
| `JAVA_TOOL_OPTIONS` | `-Xms256m -Xmx1024m` | | unmeasured; requests 1Gi / limit 2Gi |
| `KAFKA_BOOTSTRAP_SERVERS`, `TOPIC_PREFIX`, runtime profile | from `options-edge-config` | | |

**Storage.** hostPaths `/home/options-edge/config/level-absorption` (read-only mount) and `/home/options-edge/data/level-absorption`
(read-write), mounted at the same paths in the container.

## Why it cannot start yet (inputs that cannot be invented, so no ConfigMap was made)

1. **Phase 0 freeze file** `frozen-configuration.json`: `formatVersion 1`, `kind level-absorption.frozen-configuration`, a `phase0` block
   (freeze commit, candidate percentile, bootstrap generator and seed) and every parameter of section 9. `minimumEffort`,
   `minimumBarImbalanceBps` and `flatRatioCutoff` have **no code default anywhere**: the calibration tools write them once at the Phase 0
   commit. A documented minimal valid content does not exist, so none was made.
2. **Retention artifact** `retention-artifact.json`: built from one measured session's events with
   `RetentionMeasurement` (`java -cp <jar> ...runtime.RetentionMeasurement events.tsv 1 30 2 <events topic> retention.json`); it holds
   `retentionBytes = ceil(measuredBytesPerDay x retentionDays x safetyFactor)`, and the service refuses to start unless the broker reports
   exactly that value on the events topic.
3. **The events topic** `futures.level-absorption.events` (1 partition, delete, `retention.ms=-1`, `retention.bytes` = the artifact's
   integer). It is therefore **not declared in `topics.env`**, and the barrier creates it only when `LEVEL_ABSORPTION_EVENTS_RETENTION_BYTES`
   is supplied; otherwise it WARNS and passes (the service is held at 0; set `LEVEL_ABSORPTION_REQUIRE_EVENTS_TOPIC=1` on the scale-up
   deploy to make it fatal). When the artifact exists: declare the topic in `topics.env` with that cap and classify it in
   `OPTIONS_EDGE_RESET_PRESERVED_TOPICS` in the same change.
4. **Input topics** (`es.futures.footprint.bars`, `es.futures.auction`, `underlying.es.trades.linearized`, one partition each, each keeping at
   least 7 days or forever; `topics.env` declares `-1`, 7 days and 7 days) must exist and be fed where the service runs. They are declared
   in `topics.env`, but whether the es4 mirrors feed them on dev was not verified here; the service refuses to start naming the missing topic.

No secret is needed.

## Choices recorded

- **Held at 0 in the base** (as `hedging-pressure`, `early-auction-verdict`); no production hold patch (that exists for
  `es-compression-expansion` only because its base runs at 1).
- **Readiness probe = `/health/live`** (the es-compression-expansion rule, owner 2026-10-07): `/health/ready` is 503 until caught up and
  while the level-source record is stale, which would time out rollouts outside the session. `/health/ready` stays for operators.
- **Topic barrier reuses the semantics of `apply-level-absorption-topics.sh`** (same shapes and retentions) but never types the events
  cap; the apply script (in options-edge-processing, needs the artifact file) remains the way to create the events topic.
- Voice, closures and manual-line are declared in `topics.env` at 7 days exactly (the apply script's value; the service accepts >= 7 days).

## Still needs the owner

- The Phase 0 freeze file and the retention artifact on the host under `/home/options-edge/config/level-absorption/`.
- The events topic with the artifact's cap, plus its `topics.env` declaration and reset classification.
- The enable: `replicas: 1`, regenerate slices, remove the name from both `KEEP_DOWN` lists and `DISABLED_DEV` (steps at the top of the
  base Deployment). `LIVE` additionally needs the publication table and a deploy change.

## Jenkins: deploying to dev later (the owner's act under the Deployment Permission Rule; nothing here deploys)

Job `service-deploy` (`Jenkinsfile.service-deploy`):

| Parameter | Value |
|---|---|
| `SERVICE` | `level-absorption` |
| `ENVIRONMENT` | `dev` |
| `BUILD_IMAGES` | `true` (builds `options-edge-level-absorption` with `BUILD_TARGET=options-edge-level-absorption`) |
| `PROCESSING_PERMITTED_SHA` | the 40-character commit of options-edge-processing to build |
| `CONTRACTS_PERMITTED_SHA` | the 40-character commit of options-edge-contracts the build installs |
| `PERMITTED_SHA` | the 40-character commit of this repository (options-edge-deploy) containing this wiring, after it is merged |
| `DEPLOY_DRY_RUN` | `false` (set `true` first for a rehearsal) |

Result: image built, pushed, digest-pinned; Deployment applied at `replicas: 0`; voice, closures and manual-line created/verified; events
verified if it exists. Production uses `ENVIRONMENT=production`.
