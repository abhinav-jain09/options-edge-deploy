# break-classifier-service (Phase F): deploy wiring, live on dev and production

Producer: `options-edge-processing` `break-classifier-service` (processing `153dd3ce`, #1291). Owner standard (2026-10-22): a service is calibrated and tested on historical
data, then runs live on dev and production, never in shadow. Summary section: `DEPLOY-ROLLOUT.md` 5b.

## Honest state

* The producer's pre-registered validation FAILED (p=1681/2001, development fold). Every classification it publishes is `UNDECIDED(NO_MEASURED_EDGE)` carrying the measured numbers; there is no trap/real verdict to relay.
* The two parent inputs have NO producer on main yet: `BreakParentRecord` (X1, `options.spx.market-structure.break-events`) and `SwingPivotRecord` (X2, `options.spx.market-structure.swing-pivots`). market-structure-service does not publish them. The topics are created EMPTY by the barrier so the boot verification passes. The live service therefore starts, publishes its `BreakStatus` and NO `BreakMark` until a Phase B/C producer exists. **Blocker for marks, not for the service.**
* X6 is a release SELF-attestation: `/app/release/x6-attestation.txt` is staged in the image by the processing build and must name the embedded Phase B parameter-set hash, or the service refuses to start.
* The broker's `message.max.bytes` is 1048588 on both brokers (checked 2026-10-22) and STAYS there: **no broker-wide change is made or needed** (revision 2026-10-30). The service needs a checkpoint record of up to several MiB, so the checkpoint topic carries a TOPIC-LEVEL `max.message.bytes` of 9 437 184 and the service checks that topic's own effective limit (>= 8 MiB). Kafka accepts a topic-level limit above the broker default. The earlier design RAISED the cluster-wide default to 9 437 184; on dev that pushed five `es.futures.footprint*` topics (which inherit the default) above the feed gateway's 1 MiB ceiling check (`ES_FOOTPRINT_TOPIC_INVALID`) and both gateway images crash-looped. That raise was removed.

## Inputs and outputs per environment (identical on dev and production; `TOPIC_PREFIX` is empty)

| topic | role | partitions | shape | where it comes from |
|---|---|---|---|---|
| `options.indicators.bars` | input, SPX bars | 8 (`BREAK_SPX_BARS_PARTITIONS=8`) | delete, 45 days | indicator-service, already on dev and production |
| `es.futures.footprint.bars` | input, ES footprint bars | 1 | -1 | the es4 output mirrored under `es.` on dev and production (es4 hosts the futures side only and has no `options.indicators.bars`, so the service runs in dev and production, never es4) |
| `options.spx.market-structure.break-events` | input X1 | 1 | delete, 172 800 000 ms | created EMPTY by the barrier; no producer yet |
| `options.spx.market-structure.swing-pivots` | input X2 | 1 | delete, 172 800 000 ms | created EMPTY by the barrier; no producer yet |
| `options.spx.break-classification` | output (BreakMark, BreakOutcome, BreakStatus) | 1 | delete, `retention.ms` EXACTLY 7 776 000 000 | the barrier |
| `options.spx.break-classification.checkpoint` | transactional checkpoint | 1 | compact, topic-level `max.message.bytes` 9 437 184 (service needs its own effective limit >= 8 MiB), small-segment compaction cadence | the barrier |

The service never creates a topic and verifies all six at every start. Avro subjects (contracts 0.37.0, options-edge-contracts #128, `87511c9acb7a897850c2fa436d59c45eaef24481`, `schemas-registry/ta`): `BreakMark` 2601, `BreakOutcome` 2602, `BreakStatus` 2603, `BreakClassifierCheckpoint` 2604, `BreakParentRecord` 2605, `SwingPivotRecord` 2606 (all version 1, FULL), listed in `scripts/kafka/schema-id-maps/break-classifier.map` (named after the service because `Jenkinsfile.service-deploy` picks the map by `SERVICE`).

## What the barrier does (`scripts/kafka/ensure-break-classification-topics.sh`, service-deploy stage "Verify break-classification topics")

1. Makes NO broker change and reads no broker config (no `--entity-type brokers`, no `--entity-default`, no `kafka-broker-api-versions`).
2. Creates each absent topic at the shape above (checkpoint with topic-level `max.message.bytes=9437184`) and verifies all five; an EXISTING checkpoint whose own topic-level `max.message.bytes` is absent (it only inherits the broker default) or below 8 388 608 is raised to 9 437 184 on that topic alone (raise only, never lowered; a failed alter fails the barrier). Otherwise an existing topic is never recreated and its verified shape is never altered, any difference fails closed naming the topic (the only changes to an existing topic are that checkpoint limit pin and the checkpoint's compaction cadence, next step).
3. Gives the checkpoint the recovery-topic compaction cadence (16 MiB segments, 5 minute max compaction lag) after verification.

`scripts/kafka/topics.env` declares the same five topics, with `OPTIONS_EDGE_TOPIC_MAX_MESSAGE_BYTES_OVERRIDES` as the per-topic `max.message.bytes` contract honoured by `topic-shape.lib.sh` (create and alter), `scripts/ops/dev-cleanup.sh` (create and reconcile) and `verify-topics.sh` (floor check), so a nightly dev wipe recreates the checkpoint at the bound. Reset class of the checkpoint: RESET_REBUILDABLE, reset together with its inputs and the output (justification in `topics.env`).

## Exact rollout (the owner runs these; nothing here was run by the change)

Jenkins job `options-edge-service-deploy` (`Jenkinsfile.service-deploy`). `PERMITTED_SHA` is the full 40-character deploy-repo `origin/main` commit at run time (`git rev-parse origin/main`).

0. Dry run per environment (reports only; the registration part prints `would import` x6, ids free on dev and production as read 2026-10-22):
   `SERVICE=break-classifier ENVIRONMENT=dev REGISTER_TA_SCHEMAS=true CONTRACTS_PERMITTED_SHA=87511c9acb7a897850c2fa436d59c45eaef24481 DEPLOY_DRY_RUN=true PERMITTED_SHA=<deploy main>`, then the same with `ENVIRONMENT=production`.
1. Image: processing `BUILD_TARGET=options-edge-break-classifier` (Jenkins image parameter `BREAK_CLASSIFIER_IMAGE`), which stages `/app/release/x6-attestation.txt` (the separate processing PR). Either build it there, or let step 2 build it with `BUILD_IMAGES=true PROCESSING_PERMITTED_SHA=<processing main> CONTRACTS_PERMITTED_SHA=87511c9acb7a897850c2fa436d59c45eaef24481` (then drop `BUILD_IMAGES=false`).
2. Register, barrier, deploy in one run, dev then production:
   `SERVICE=break-classifier ENVIRONMENT=<dev|production> REGISTER_TA_SCHEMAS=true CONTRACTS_PERMITTED_SHA=87511c9acb7a897850c2fa436d59c45eaef24481 BUILD_IMAGES=false DEPLOY_DRY_RUN=false PERMITTED_SHA=<deploy main>`.
   Stage order is fixed: Register TA-books schemas, Verify break-classification topics (topics only), rollout (digest pin, Recreate, replicas 1). Image digests are pinned by service-deploy at deploy time; none is recorded in the repository.
3. Gateway: `GATEWAY_BREAK_CLASSIFICATION_ENABLED` is `"true"` in both feed-gateway env patches and their generated slices; deploy the gateway image that understands it (gateway PR) with `SERVICE=feed-gateway`.
4. Web: `web-service-deploy` once the gateway frames are live.

Verify after step 2 (pod UID and `restartCount` captured at first Running): `restartCount` 0 for 10 minutes, no ERROR/`FAILED_CONTRACT` line, `/health/ready` 200 (state LIVE), the log line `break-classifier starting: enabled=true`, the live `ENVIRONMENT` and `BREAK_CLASSIFIER_ENABLED` readings, and that `options.spx.break-classification` carries Avro schema id 2603 status records. Abort criteria and the two durable rollback levels are in each enable patch header (flag `"false"`; replicas 0 together with `KEEP_DOWN` in `morning-autostart.sh` and `oe-boot-bringup.sh`, and `DISABLED_DEV` on dev). The registered subjects are additive and stay; there is no broker change to undo.

## Not done by this change

No Jenkins job was triggered, no schema registered, no topic created, no cluster touched. The read-only registry probe (dry run of `ensure-fixed-id-schemas.sh` against `192.168.100.102:8082` and `192.168.100.252:8082`) reported `would import` for all six subjects with ids 2601-2606 free on both.
