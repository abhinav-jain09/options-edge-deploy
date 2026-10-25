# Level Absorption — deploy wiring (options-edge-deploy)

Service: `level-absorption-service` (options-edge-processing, module `level-absorption-service`, image `options-edge-level-absorption`,
Jenkins `BUILD_TARGET=options-edge-level-absorption`). Wired for **dev and production**, phase **EYEBALL** (status events, NO verdict).
**Dev runs it (replicas 1) since 2026-10-25; production is held at `replicas: 0`.** See the section "State since 2026-10-25" at the end for the
configuration, the events topic cap and the validator; the older table below describes the original wiring.

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

## State since 2026-10-25: RUNS ON DEV (EYEBALL), held at replicas 0 on production

Decided by Claude under the owner's standing instruction (the owner may overrule any of it):

1. **The configuration** is the EYEBALL-ONLY PROVISIONAL file of the Phase 0 run of 2026-10-25 (options-edge-processing
   `docs/level-absorption/phase0/`, PR 1344, README there). The comparison fold selected NO candidate (lower bound 4.09 points against
   the 15-point bar), so it is not a Phase 0 freeze, no verdict can ship, and the service refuses any phase but EYEBALL on it. It and the
   retention artifact are copied verbatim into an IMMUTABLE ConfigMap named by its content digest, `level-absorption-config-<first 12 hex of sha256(data)>`
   (`k8s/services/level-absorption/base/level-absorption-config.yaml`; a changed configuration is a new object and a changed pod template, so the pod rolls), mounted read-only at `/home/options-edge/config/level-absorption` (no host path). The standalone slices carry Deployment/Service only,
   so `Jenkinsfile.service-deploy` applies that one file in the stage `Apply level-absorption configuration` (DEV ONLY) and reads the live object back (immutable, exact keys, canonical digest).
2. **The events topic** `futures.level-absorption.events` is declared in `topics.env`: 1 partition, delete, `retention.ms=-1`,
   `retention.bytes=21765840` = ceil(362,764 bytes/day x 30 days x 2) (362,764 = the largest of 158 measured calibration sessions with
   every verdict class publishing; mean 61,793; 30 and 2 are the document's starting values). It is RESET-PRESERVED
   (`OPTIONS_EDGE_RESET_PRESERVED_TOPICS` and `premarket-reset.sh`). The barrier (dev) reads the integer out of the ConfigMap file, requires the topic and RAISES an existing lower cap to it (never lowers: a lower target is refused).
   `scripts/ci/validate-level-absorption-config.sh` (wired into `validate-services.sh`) fails when the artifact, its arithmetic, the topics.env
   cap, the Deployment's paths or the LA-050 input topic names (footprint `es.futures.footprint.bars`, tape `underlying.es.trades.linearized`) disagree.
3. **Dev runs it** (`k8s/overlays/dev/level-absorption-dev-enable-patch.yaml`, replicas 1; removed from `DISABLED_DEV`). The shared base and
   **production stay at replicas 0** and in the production KEEP_DOWN lists. The production slice now names the ConfigMap volume but nothing is deployed there.
4. **Inputs on dev**: the footprint and auction topics are the es4 mirrors; while es4 is off they are silent and the service waits (live
   stays 200). The input topics must exist, otherwise the service refuses to start naming the missing topic.
5. **A new Phase 0** (a longer corpus) replaces the ConfigMap content and the cap in ONE change; the validator keeps them together.

