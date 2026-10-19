# spot-vol-regime-service Avro cutover (options.spx.spot-vol-regime.current / .events and the two state stores)

Rule: Kafka Serialization Rule + owner ruling of 2026-10-18 (no versioned topic names; delete and recreate under the own name; retained
JSON is deleted, never read). Contracts 0.31.0 (PR options-edge-contracts#119), processing #1179, feed-gateway #241. Generic wipe tool:
`scripts/kafka/roll-forward-topics.sh` (deploy PR 1320, docs/KAFKA-AVRO-TOPIC-ROLL-FORWARD.md). Registration tool: this PR.

## HARD PREREQUISITE (blocks step 5; nothing below may start without it)
**Merge options-edge-deploy#1320 (`scripts/kafka/roll-forward-topics.sh`) FIRST.** The destructive wipe in step 5 is performed ONLY by that merged script;
this runbook neither contains nor copies it. At cutover record here, in the cutover change, the merge commit sha of #1320 that the operator ran:
`roll-forward-topics.sh merge commit sha: ____________` and run the script from a checkout at (or after) that sha. Steps 1-4 and the registration script
of this PR do not need it; step 5 and everything after it do. If #1320 is not merged, STOP at the end of step 4.

## What is converted
| Item | Shape | Subject (RecordNameStrategy, FULL) |
|---|---|---|
| `options.spx.spot-vol-regime.current` | 1 partition, `compact,delete`, retention 24h | `com.optionsedge.contracts.spotvolregime.SpotVolRegimeSnapshot` |
| `options.spx.spot-vol-regime.events` | 1 partition, `delete`, retention 24h | same subject (one record type) |
| changelog `options-edge-spot-vol-regime-svr-window-store-changelog` | Streams-internal, compacted | `...SpotVolRegimeWindowState` |
| changelog `options-edge-spot-vol-regime-svr-last-frame-store-changelog` | Streams-internal, compacted | `...SpotVolRegimeLastFrameState` |

(Application id and so the changelog names follow `SPOT_VOL_REGIME_STREAMS_APP_ID` / the session-scoped id; read the real names off the broker.)
Not converted, not ours: `underlying.vix.price` is a JSON input written by another component.

## Reader-completeness evidence (origin/main, run 2026-10-19)
```text
for r in options-edge-processing options-edge-feed-gateway options-edge-approach-monitor options-edge-contracts options-edge-deploy \
         options-edge-trading-journal short-premium-agent options-edge options-edge-databento-feed; do
  git -C $r grep -l -E 'spot-vol-regime\.(current|events)|SPOT_VOL_REGIME_(CURRENT|EVENTS)|SpotVolRegimeSnapshot|SpotVolRegimeTopics' origin/main
done
```
* processing: the producer itself (`spot-vol-regime-service`), `docs/market-read/MARKET-READ-REQUIREMENT.md` (prose), and
  `trade-evidence-api` (`registry.py` freshness subscription, `inventory.json` env listing). `trade-evidence-api` IS in the repo set (options-edge-processing#1179):
  its collector decodes the topic through the registry schema id with fastavro, and `collector.AVRO_ONLY_TOPICS` now refuses any non-Avro (JSON) value on it;
  `trade-evidence-api/tests/test_spot_vol_regime_avro_decode.py` pins an Avro frame written by the Java serializer (decodes, Decimal preserved) and that JSON is refused.
* feed-gateway: the only data reader; converted in #241 (Avro consumers, identical browser JSON).
* contracts: the types. deploy: k8s env values, `topics.env`, topic-preservation scripts (shape only). options-edge web: `option-chain.js` reads the gateway websocket JSON, unchanged.
* approach-monitor, trading-journal, short-premium-agent, databento-feed: no match. No NAS archive entry, no mirror (envs dev and production only), no Postgres writer.
  `.events` has no reader anywhere.

## Cutover order (everything that can fail non-destructively runs BEFORE any destruction)
1. Merge contracts 0.31.0, then processing and gateway; build both images against 0.31.0 and confirm the resolved digests. Build no prod image of either until this
   environment's cutover (a prod image of the gateway would read Avro against the still-JSON prod topic).
2. **Register (before the wipe)** on the target registry; a refusal here leaves production untouched:
   `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas/spotvolregime scripts/kafka/ensure-spot-vol-regime-schemas.sh` (sha256-pinned bytes, FULL by
   read-back, read-everything-then-mutate; `DEPLOY_DRY_RUN=true` only reports). Dev registry `http://host.docker.internal:8082`, production `http://192.168.100.252:8082`.
3. Stop the JSON producer and reader: scale the service and the gateway through Jenkins (no `kubectl`). From here the topics are wiped.
4. Streams state of the service: reset group `options-edge-spot-vol-regime` offsets to 0 on the retained input partitions and read them back; DELETE both changelog topics
   (never merely purge: a compacted JSON tail stays restorable, and the Avro serde cannot restore it) and the local `KAFKA_STREAMS_STATE_DIR` (RocksDB) of the pod.
5. Read the TopicIds of both output topics, freeze topic administration, then wipe and recreate empty (shape is copied from the live topics):
   `scripts/kafka/roll-forward-topics.sh --recreate --cluster dev|production --topics options.spx.spot-vol-regime.current,options.spx.spot-vol-regime.events --pre-cutover-id <t>=<id> ... --registry <url> --subject <t>=com.optionsedge.contracts.spotvolregime.SpotVolRegimeSnapshot ... --writers-and-readers-stopped <bootstrap>`
   Verify partitions=1, cleanup policies, retention 24h and end offset 0.
6. Deploy the Avro gateway; verify it Ready with zero records consumed (it must never meet retained JSON).
7. Enable the Avro service (`SPOT_VOL_REGIME_ENABLED`); verify the first records on both topics start with magic byte 0 and resolve (`/schemas/ids/<id>/versions`) to
   `com.optionsedge.contracts.spotvolregime.SpotVolRegimeSnapshot`; verify the changelogs hold only Avro; verify the browser `spot-vol-regime` events are unchanged.

## Rollback
There is none by decision (roll forward only). Before step 3 nothing has changed (registration is additive under FULL). After step 5 the topics are empty by design; a failure is
fixed forward by `--verify --new-topic-id ...` or, if JSON appears, by stopping the JSON writer and wiping again as the generic tool describes.
