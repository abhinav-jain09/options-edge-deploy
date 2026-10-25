> **RETIRED 2026-10-25.** Break Score was retired as a predictor (options-edge-processing `docs/break-score/`, decided by Claude under the owner's standing instruction; the owner may overrule). `break-score-service` was never started: the render holds its Deployment at `replicas: 0` on dev and production, and as of 2026-10-25 the dev cluster has no such Deployment object (production was not inspected, it is untouched). It stays held at `replicas: 0`; there is no SHADOW mode, no shadow period and no LIVE switch pending, and `BREAK_SCORE_MODE` is retired with the service. The wiring below is kept so the already-validated render, image lock and topic scripts keep validating; do not scale it up. Removing the wiring entirely is a separate cleanup that touches the image lock, topics.env preserved-topic checks and the keep-down lists.

# Break Score — deploy wiring (options-edge-deploy)

Service: `break-score-service` (options-edge-processing, module `break-score-service`, image `options-edge-break-score`, Jenkins
`BUILD_TARGET=options-edge-break-score`). Version 1 is **SHADOW only**: no voice, no screen. Wired for **dev and production**,
**held at `replicas: 0` everywhere**: deploy is done when the image is built, pushed and digest-pinned; scaling is the owner's.

Sources read for this wiring: `RuntimeSettings.java`, `BreakScoreConfig.java`, `ManualLevels.java`, `EventTopicContract.java`,
`BreakScoreTopicApplyMain.java`, `BreakScoreServiceMain.java`, `docs/break-score/as-built/runtime-gaps.md` section 3.

## What was wired (the es-compression-expansion / hedging-pressure touch points)

| Touch point | Change |
|---|---|
| `k8s/services/break-score/base/` | Deployment (`replicas: 0`, `Recreate`, one container), ConfigMap `break-score-config`, kustomization |
| `k8s/services/break-score/overlays/{dev,production}/` | generated slices (`scripts/deploy/generate-service-slices.sh`), Deployment only |
| `k8s/overlays/{dev,production}/kustomization.yaml` | service base referenced; dev image remap `host.docker.internal:5001/options-edge-break-score:dev` |
| `services.yaml` | `break-score`, standalone, `sliceGenerated`, envs dev + production |
| `image-tags/{dev,production}.yaml` | `break-score-service` mapping (`:dev` / `:prod`) |
| `Jenkinsfile` | `BREAK_SCORE_IMAGE` parameter, image-default map, promote-to-production list |
| `Jenkinsfile.service-deploy` | `break-score` in the `SERVICE` choice list; stage "Verify break-score output topic" |
| `scripts/deploy/{resolve-images,image-lock,image-preflight,apply}.sh` | `BREAK_SCORE_IMAGE` resolved, lock-checked, preflighted and digest-pinned (dev + production loop) |
| `scripts/kafka/topics.env` | `futures.break-score.events:1`, retention `-1`, retention.bytes `16777216`, exact one partition, **RESET-PRESERVED** |
| `scripts/ops/premarket-reset.sh` | `futures.break-score.events` added to `PRESERVE_TOPICS_REGEX` (required by `validate-durable-topic-preservation.sh` for a RESET-PRESERVED topic) |
| `scripts/kafka/ensure-break-score-topics.sh` (+ `-test.sh`) | first-deploy barrier; test wired into `scripts/ci/validate-services.sh` |
| `scripts/ops/{morning-autostart,oe-boot-bringup,dev-cleanup}.sh` | `break-score-service` added to `KEEP_DOWN` (both lists, kept equal) and `DISABLED_DEV` |

## Environment

Set in the manifest (`k8s/services/break-score/base/break-score-deployment.yaml`). "Default" is the code default in `RuntimeSettings`.

| Variable | Value in the manifest | Code default | Note |
|---|---|---|---|
| `BREAK_SCORE_MODE` | `SHADOW` | none (required; `SHADOW` or `LIVE`) | decided: shadow only |
| `KAFKA_BREAK_SCORE_TRADES_TOPIC` | `underlying.es.trades.linearized` | same | the linearized tape; `TOPIC_PREFIX` is empty on dev and production |
| `KAFKA_BREAK_SCORE_EVENTS_TOPIC` | `futures.break-score.events` | same | |
| `BREAK_SCORE_CONSUMER_GROUP` | `break-score-service` | same | |
| `BREAK_SCORE_SYMBOL` | `ES.v.0` | same | |
| `BREAK_SCORE_BAR_TYPE` | `TIME` | `TIME` | `BREAK_SCORE_VOLUME_BAR_SIZE` stays UNSET (a size with `TIME` is refused) |
| `BREAK_SCORE_CONFIG` | `/home/options-edge/config/break-score/break-score.properties` | none (required) | ConfigMap key |
| `BREAK_SCORE_MANUAL_LEVELS` | `/home/options-edge/config/break-score/manual-levels.tsv` | none (required) | ConfigMap key, shipped empty |
| `BREAK_SCORE_BASELINE_DIR` | `/home/options-edge/data/break-score/baselines` | none (required) | hostPath |
| `HEALTH_PORT` | `8080` | `8080` | |
| `JAVA_TOOL_OPTIONS` | `-Xms256m -Xmx640m` | | sibling sizing, unmeasured |
| `BREAK_SCORE_TAPE_SILENCE_MILLIS` | not set | `30000` (1000..3600000) | |
| `BREAK_SCORE_MANUAL_POLL_MILLIS` | not set | `2000` (100..600000) | |
| `BREAK_SCORE_TIMER_IDLE_MILLIS` | not set | `5000` (1000..3600000) | |
| `BREAK_SCORE_CONTAINER_STORAGE_PREFIXES` | not set | none | not needed: container paths equal host paths |
| `KAFKA_BOOTSTRAP_SERVERS`, `TOPIC_PREFIX`, runtime profile | from `options-edge-config` | | |

**Storage.** The container sees the same paths as the host, so `/home/options-edge` (always allowed by the Home Directory Storage
Rule check) admits every path with no container-prefix override. `/home/options-edge/data/break-score` is a hostPath
(`DirectoryOrCreate`, read-write; the service writes `baselines/`); `/home/options-edge/config/break-score` is the ConfigMap, read-only.

**Calibratable file** (`break-score.properties`, ConfigMap `break-score-config`): the seven keys `BreakScoreConfig.load` reads, with
`BreakScoreConfig.startingValues()`: `breakTicks=2`, `absorptionK=4.0`, `retestProximityTicks=2`, `failHoldMillis=120000`,
`confirmDepartTicks=4`, `w1=0.5`, `w2=0.5`. The other constants are FIXED in code and have no key. **Manual levels**
(`manual-levels.tsv`): an empty file, which `ManualLevels` treats as a valid empty set.

**Topic.** `futures.break-score.events`: 1 partition, `cleanup.policy=delete`, `retention.ms=-1`, `retention.bytes=16777216`
(decided 2026-10-16), `message.timestamp.type=CreateTime`. The deploy barrier creates it when absent and verifies it (the service itself
only verifies and refuses to start on a wrong shape).

## Choices recorded (where the code or template left one)

- **Held at 0 in the base**, as `hedging-pressure` and `early-auction-verdict` (a base at 0 holds both environments; a production hold
  *patch*, as `es-compression-expansion` has, exists only because that base runs at 1).
- **Readiness probe = `/health/live`**, as `es-compression-expansion` (owner 2026-10-07): `/health/ready` is 503 on a dark tape and would
  time out every overnight or weekend rollout. `/health/ready` stays available to operators.
- **Reset class PRESERVED** for the events topic (the shadow period's evidence). The owner may flip it to rebuildable.
- **The ConfigMap is part of the monolithic render only.** The standalone slices and their validator are Deployment/Service only, so
  `service-deploy` does not create `break-score-config`; the full (monolithic) deploy does. The pod mounts it with `optional: false`, so
  the owner's scale-up needs the ConfigMap to exist first.
- Container sizing (requests 768Mi / limit 1536Mi) copies the sibling and is unmeasured.

## Still needs the owner

1. **Real manual levels**: replace the empty `manual-levels.tsv` value (format `priceTicks<TAB>side<TAB>MANUAL<TAB>yyyy-mm-dd`, UP or
   DOWN, a non-empty file ends with a newline, every record must be for the current trade date).
2. **Baseline seeding** before the first start (otherwise the first ten sessions score nothing, by design): on the host, with the
   trailing 20 usable replay trade files,
   `java -cp app.jar com.optionsedge.processing.breakscore.runtime.BaselineBootstrapMain /home/options-edge/data/break-score/baselines <trade files>`.
3. **The enable**: set `replicas: 1`, regenerate the slices, remove the name from both `KEEP_DOWN` lists and `DISABLED_DEV`, deploy
   (full steps in the comment at the top of the base Deployment).
4. No secret is needed. The open owner items in the requirement (acknowledgement boundary, busiest-30-minutes algorithm, lag metric,
   hosting environment) are not deploy inputs.

## Jenkins: deploying to dev later (the owner's act under the Deployment Permission Rule; nothing here deploys)

Job `service-deploy` (`Jenkinsfile.service-deploy`):

| Parameter | Value |
|---|---|
| `SERVICE` | `break-score` |
| `ENVIRONMENT` | `dev` |
| `BUILD_IMAGES` | `true` (builds `options-edge-break-score` from options-edge-processing with `BUILD_TARGET=options-edge-break-score`) |
| `PROCESSING_PERMITTED_SHA` | the 40-character commit of options-edge-processing to build |
| `CONTRACTS_PERMITTED_SHA` | the 40-character commit of options-edge-contracts the build installs |
| `PERMITTED_SHA` | the 40-character commit of this repository (options-edge-deploy) containing this wiring, after it is merged |
| `DEPLOY_DRY_RUN` | `false` (set `true` first for a render + pin + validate rehearsal) |

Result: image built, pushed, digest-pinned; Deployment applied at `replicas: 0`; the topic barrier has created/verified
`futures.break-score.events`. Production uses the same parameters with `ENVIRONMENT=production`.
