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
   (`k8s/overlays/dev/level-absorption-config.yaml`; a changed configuration is a new object and a changed pod template, so the pod rolls), mounted read-only at `/home/options-edge/config/level-absorption` (no host path). The standalone slices carry Deployment/Service only,
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


## Friday timing and readiness repair, 2026-10-25

The dev-only enable patch now probes `/health/deployment-ready` from the companion processing fix: it accepts expected closed-market idleness after catch-up, but rejects an in-session stale level source or silent tape, and never masks a halted engine or wedged loop. Liveness stays `/health/live`; production remains at zero replicas with its existing probe. The generated dev slice mirrors the patch. Deploy this change together with the processing image implementing the endpoint; an older image correctly fails the new probe instead of silently claiming readiness. The processing manifest gives finalized mirrored footprints 15000 ms and cold restarts include the last regular session. EYEBALL and the publication gate stay in place. Rollout must use the Jenkins service-deploy path, after the companion image is available. The linearizer source already requests one dev replica; restore the observed zero-replica runtime through its existing Jenkins service job, rather than adding a competing scale mechanism.

Indicator support/resistance enablement (2026-10-25): the immutable dev ConfigMap now enables Bollinger, envelope, Keltner, STARC, Donchian and Parabolic SAR kinds from `options.indicators.studies.current`. The retained dev lineage is SPX; deploy together with processing PR #1414's selected-SPX reader, bounded native study Avro decoding, exact-band named observations and valid moving/narrow-level withdrawals. No market-structure SR-zone records were present on Friday, so that topic is not enabled. The new configuration and retention files are copied verbatim from processing `docs/level-absorption/phase0/indicator-{eyeball-configuration,retention-artifact}.json`; the historical Phase 0 artifacts remain unchanged. The 343684500-byte cap comes from the full one-Friday diagnostic session (5728075 bytes/day x 30 days x 2), including the post-session missing-bar tail, and is explicitly a provisional single-session sample. Production remains at zero replicas; its generated slice follows the shared volume-name reference without enabling a workload. The service must remain EYEBALL; this change supplies no selection or publication authorization. Merge approval and Jenkins rollout remain required.

Current owner-requested mode (2026-10-25) supersedes the indicator-mode enablement above: the dev ConfigMap now copies `phase0/market-structure-{eyeball-configuration,retention-artifact}.json` from processing PR #1414. It enables HOST_SWING/HOST_RANGE_EDGE/OPENING_RANGE_EDGE and 1m/5m/15m structure timeframes, disables indicator study kinds, and uses the existing persistent classifier. The paired image must decode native host Avro enums/decimal bytes and suppress free-price OBSERVATION output whenever host kinds are enabled. Only confirmed multi-close states count as persistent absorption. Friday's host replay confirms none; no threshold was lowered to manufacture the 11:34 signal. The single-Friday retention sample, including its diagnostic tail, is 1344189 bytes/day x 30 x 2 = 80651340 bytes. ConfigMap name, artifact pins, shared volume references and topic cap change atomically; production stays at zero replicas. This remains EYEBALL with no trading verdict/voice publication; merge approval and Jenkins deployment remain pending.

Two-sided owner-requested extension (2026-10-25), superseding the preceding net-effort host diagnostic: dev only sets `LEVEL_ABSORPTION_TWO_SIDED_HOST=true`; the paired processing image refuses this flag with a non-EYEBALL table. It compares raw gross buy/sell volume separately with signed price reward and maintains independent persistence. TWO_SIDED_READING carries no legacy confirmation or trading/voice verdict; its statusLine header and structured decision logs retain per-side evidence and a combined TWO_SIDED_LOW_REWARD assessment when both sides have low reward. Detailed per-side metrics are not new Avro value fields. Existing gross-side threshold reuse is explicitly uncalibrated and does not authorize LIVE. The unchanged host configuration is pinned with `phase0/two-sided-host-retention-artifact.json` from processing PR #1414. The final packaged Friday replay round-trips 2484 events through the released Avro schema: 1361259 bytes/day × 30 × 2 = 81675540 bytes (one provisional session including diagnostic tail). Both ConfigMap digest/name and shared mount references follow these exact bytes; production remains replicas zero and flag off. Config validation explicitly checks the dev-only flag. Friday produced 96 full-window readings, 33 persistent two-sided low-reward updates across 15 host identities; 7659.50 SPX first reaches persistence at 11:36, not 11:34. See processing `docs/level-absorption/FRIDAY-TWO-SIDED-EFFORT-20261023.md` for all levels and limitations. Merge and Jenkins rollout remain pending; this change did not modify the running dev pod.

SPX directional path increment (2026-10-26): coordinated contracts PR #138 (additive 0.45.0), processing PR #1414 and web PR #1243. Canonical reconciliation PATH-1–PATH-8 is in processing LEVEL-ABSORPTION-SERVICE-REQUIREMENT.md; every row remains IMPLEMENTING pending coordinated merge/live verification. Dev's immutable ConfigMap gains the exact pinned `path-configuration.json` from processing `phase0/path-difficulty-eyeball-configuration.json`; the content name and shared mount references change with all three files. Dev alone sets `LEVEL_ABSORPTION_PATH_CONFIGURATION` and `LEVEL_ABSORPTION_PATH_UI_ENABLED`; production stays off and the service remains replicas zero there. The footprint producer and existing events retention artifact/cap are unchanged.

The separate `futures.level-absorption.path` is one partition, key SPX, pure compact, retention.ms=-1/retention.bytes=-1, RESET-REBUILDABLE from retained tape; it is not an outcome-history topic. The service-deploy dev barrier calls `ensure-absorption-path-topic.sh` before rollout. It creates a missing topic then verifies exact partition/cleanup/retention shape, and never alters an existing one. Seven CLI-double tests cover creation, no mutation of a valid existing topic, metadata/config failure and wrong shape. The normal schema inventory gains AbsorptionPathSnapshot, whose resource is present in contracts 0.45.0.

Rollout order matters: merge approved contracts first, build the pinned processing/web artifacts, create/verify the dev path topic through the guarded Jenkins topic workflow, then run the existing guarded schema-registration action with CVD_SCHEMA_ENVIRONMENTS=dev and the permitted contracts coordinate/SHAs, before the service/web rollout. Registration skips absent topics, so do not run it before topic creation and assume it supplied this subject. No registry auto-registration is enabled. All broker/registry/deployment operations remain Jenkins-only. After rollout verify service health, private API fresh/unknown transitions and both directional boxes on the actual current SPX gamma ladder. Existing unreconciled historical calibration/sign-off obligations stay open; this is provisional, uncalibrated, ES-basis-derived SPX evidence.

Local validation: generated slices; exact config/lineage/cap/dev-only flags; durable-topic preservation validation; seven new barrier tests; shell/Python syntax and packaged schema inventory. Processing passes 1807 reactor tests; contracts 1073 listed (one optional golden writer skipped); web 42 focused Java + 229 JS tests and a real React/Chromium fixture at 360px with bearer/expiry checks. The packaged Friday replay round-trips 1029 path snapshots through released Avro, maximum 673 bytes. RTH has 565 readable bars and 215 feed-gap withdrawals: missing evidence must stay unknown. Historical gamma snapshots were unavailable, and no live latency, rollout, Bugzilla publication or market-hours sign-off is claimed.
