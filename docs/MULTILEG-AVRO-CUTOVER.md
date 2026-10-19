# multileg-structure-service Avro cutover (options.multileg.observations / .window.status / .sr.levels)

Topics keep their own names; they are wiped and recreated carrying Avro. Contracts 0.34.0.

**Dependency: this runbook REQUIRES options-edge-deploy PR #1320 (`scripts/kafka/roll-forward-topics.sh`,
`scripts/kafka/avro-rollforward.lib.sh`, `docs/KAFKA-AVRO-TOPIC-ROLL-FORWARD.md`) to be merged first. Do not run the cutover from a
tree that lacks it.** Wire types: `avgPrice`, `strike` and `spot` are decimal(38,18) (the shortest round-trip text of the double; a value needing more than 18 places, non-finite, -0.0 or beyond 38 digits is REFUSED, never rounded - the service counts and logs such a record as a dropped encode, `multileg_encode_refused_total`); scores, surprisal, effort and level rates/distances
are derived statistics and stay double.

## Reader-completeness evidence (origin/main, run 2026-10-19)

```text
for r in options-edge-processing options-edge-feed-gateway options-edge-approach-monitor options-edge-contracts \
         options-edge-deploy options-edge-trading-journal short-premium-agent options-edge options-edge-databento-feed; do
  git -C $r grep -l -E 'multileg\.(observations|window\.status|sr\.levels)' origin/main
done
```

Result: only `options-edge-processing` multileg-structure-service `MultilegSettings.java` (the producer's own topic defaults) and
`trade-evidence-api/inventory.json` (an env-var inventory listing); `options-edge-deploy` k8s manifests (env values),
`scripts/kafka/ensure-multileg-topics.sh` and `topics.env` (topic shape). No consumer, gateway, Postgres writer, archive, mirror or
script reads these topics. `options-edge-feed-gateway`, `approach-monitor`, `contracts`, `trading-journal`, `short-premium-agent`,
`options-edge` (web) and `databento-feed`: no match. The web page reads the service over HTTP/SSE (`/api/multileg/stream`,
`/api/multileg/levels`), whose JSON is unchanged. `options.opra.tcbbo` (JSON, written by the Databento feed) is an INPUT of this
service, not one of its outputs, and is out of scope.

## Statefulness inventory (what survives a wipe)

* The service is a plain manual-assignment consumer (group `multileg-structure-service`); it commits offsets ONLY on its INPUT topics
  (`options.opra.tcbbo`, `underlying.spx.price`), which are not touched. No Kafka Streams: there are no changelog, repartition or
  global-store topics and no state directory to rotate.
* No consumer group holds an offset on any of the three output topics (no reader exists, see the evidence above), so there is no group
  offset to reset. The roll-forward script still verifies this after the recreate (strict mode fails on any committed offset; in live
  mode it warns on one beyond the new log end). If a group ever shows up, delete its offsets with
  `kafka-consumer-groups --bootstrap-server <bootstrap> --delete-offsets --group <g> --topic <t>` before any reader starts.
* The status topic's partition count is read at boot; the script recreates each topic with the live partition count (4), replication
  factor and every topic-level config (`cleanup.policy=delete`), captured from the broker before the delete.

## Cutover order (everything that can fail non-destructively runs BEFORE any destruction)

Per environment; `<bootstrap>` is `192.168.100.102:19092` (dev; or `localhost:19092`) or `192.168.100.252:9092` (production) and the script
refuses a bootstrap that is not the named `--cluster`'s broker. Prerequisite: PR #1320 merged.

1. Merge contracts (0.34.0), then processing; build the multileg image against 0.34.0 and confirm its resolved digest (not the deploy's
   SUCCESS). Build no prod image of this service for an environment before its cutover step 6 completes.
2. REGISTRY PREPARATION - register and verify all three subjects on the TARGET registry:
   `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas/multileg scripts/kafka/ensure-multileg-schemas.sh`
   Phase 1 reads and validates EVERY subject before any write (latest version byte-equal to the canonical schema, readable config,
   subject mode READWRITE) and refuses with the registry untouched on any of those. The script hash-pins the exact bytes of the three
   canonical files, so the contracts 0.34.0 tree must be the one passed in `SCHEMAS_DIR`. What holds exactly: after phase 1, the writes
   run in a safe order (register every absent subject at version 1, THEN set FULL on every subject) and a failure part-way (registry
   outage or a refused write) leaves only canonical version-1 registrations and/or FULL configs - idempotent state, never a wrong
   schema - which a re-run completes. The sequence is NOT atomic across subjects. `DEPLOY_DRY_RUN=true` runs phase 1 only and reports
   what would be done. This step is non-destructive and runs before anything is wiped.
3. Stop non-Kubernetes JSON writers/copiers of these topics: none exist (see the evidence above); nothing to stop.
4. Avro writer first: roll the new multileg image (workloads are Jenkins-only; scaling to zero is not available, so the wipe happens
   with the Avro writer live). Prove the OLD JSON writer pod is gone: exactly one `multileg-structure-service` pod, on the new digest.
5. Read the TopicIds off the live broker and freeze topic administration:
   `kafka-topics --bootstrap-server <bootstrap> --describe --topic <t>` for each of `options.multileg.observations`,
   `options.multileg.window.status`, `options.multileg.sr.levels` (record the `TopicId` of each).
6. Wipe and recreate (run on a host with the Kafka CLI; the last positional is the optional Kafka bin dir):
   ```
   scripts/kafka/roll-forward-topics.sh --recreate --cluster <dev|production> \
     --topics options.multileg.observations,options.multileg.window.status,options.multileg.sr.levels \
     --pre-cutover-id options.multileg.observations=<id1> \
     --pre-cutover-id options.multileg.window.status=<id2> \
     --pre-cutover-id options.multileg.sr.levels=<id3> \
     --registry <registry> \
     --subject options.multileg.observations=com.optionsedge.contracts.multileg.MultilegObservation \
     --subject options.multileg.window.status=com.optionsedge.contracts.multileg.MultilegWindowStatus \
     --subject options.multileg.sr.levels=com.optionsedge.contracts.multileg.MultilegSrLevels \
     --avro-writer-live --json-writers-stopped <bootstrap>
   ```
   The operator asserts `--avro-writer-live --json-writers-stopped` after step 4. The script checks every topic (id equals the supplied
   pre-cutover id, shape captured) before deleting any, verifies each subject exists with FULL compatibility, deletes and recreates in order,
   and stops at the first failure naming the untouched topics (re-run with `--topics` set to those). It prints `NEW TOPIC ID: <topic>=<id>`
   per topic. A topic that was recreated but failed its post-check is finished with `--verify --new-topic-id <topic>=<id>` (same arguments
   otherwise, no mutation). If JSON is found on a recreated topic it is contaminated: stop the JSON writer and `--recreate` again with
   `--pre-cutover-id <topic>=<printed id>`.
7. Verify the wire (the script's post-checks cover new ids, exact partitions/RF/configs, offsets, and that the first and last record per
   non-empty partition are Avro whose schema id is registered under that topic's subject). Additionally, once the service has produced:
   a decoded record per topic (`kafka-avro-console-consumer ... --max-messages 1`), the three registry subjects at version 1, and
   `/api/multileg/levels` and `/api/multileg/stream` unchanged in shape.
8. There are no Avro readers to roll (none exist); the web surface reads the service over HTTP/SSE, whose JSON is unchanged.

Rollback: before step 6 it is scaling/rolling the previous image back. After step 6 the topics are empty by design (no history is carried)
and the previous (JSON) image must not be started against them.
