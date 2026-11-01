# greek-move-authenticity-service Avro cutover

Topics keep their own names and are wiped and recreated carrying Avro (no versioned sibling, roll forward only).
Contracts >= 0.55.0 (`schemas/greekmoveauth/`; contracts PR 118, released as 0.55.0), processing PR 1175, feed-gateway PR 239.
Merge train (all on contracts main): delta-flow (PR 120) 0.54.0, greek-move (PR 118) 0.55.0, spot-vol-regime 0.56.0, strike-invasion 0.57.0, multileg 0.58.0; the artifacts are
cumulative, so 0.55.0 and later contain both `GreekMoveAuthAvro` and `DeltaFlowAvro`: the service reads `delta-flow-by-strike` as Avro AND writes greek-move-auth as Avro
from ONE coordinate (the processing `contracts.version` for PR 1175 is 0.55.0).

## Topics and subjects

| Topic | Shape | Value |
| --- | --- | --- |
| `options.spx.greek-move-auth.current` | 32 partitions, compacted, 24h | `MoveAuthenticityVerdict` |
| `options.spx.greek-move-auth.events` | 32 partitions, delete, 24h | `MoveAuthenticityVerdict` |
| `<appId>-frame-rekey` (internal repartition) | DEFAULT_PARTITIONS, delete, 24h | `SpotFrameEvent` / `DeltaFrameEvent` / `GexFrameEvent` |
| `<appId>-gma-{spot-window,delta-window,gex-latest,baseline,engine-state,last-verdict}-store-changelog` | compacted | `SampleWindow`, `DeltaSampleWindow`, `GexFrameEvent`, `BaselineState`, `EngineCarryState`, `LastVerdict` |

`<appId>` defaults to `options-edge-greek-move-authenticity`. Nine subjects, each the record full name
`com.optionsedge.contracts.greekmoveauth.<Record>` (RecordNameStrategy), FULL compatibility.

## Readers (origin/main)

Only `options-edge-feed-gateway` reads `current` (now Avro, same browser JSON; the web page is unchanged). Nothing reads `events` or archives either topic.

`trade-evidence-api` (processing `trade-evidence-api/registry.py:29` subscribes to `current`; no canonical binding) reads it through `Collector.decode` in
`trade-evidence-api/collector.py`: the generic Confluent Avro path (schema fetched by id, cached, `fastavro`, processing commit `a08fab4a6`). `current` is listed in
`AVRO_ONLY_TOPICS` (processing PR 1175): a value that is not Confluent-framed Avro (JSON, or any non-magic-byte payload) is REFUSED before any registry lookup and
counted as a decode error, with no JSON fallback. `tests/test_greek_move_auth_avro_reader.py` pins both the decode of a real `MoveAuthenticityVerdict` frame and the JSON refusal.

Price fidelity: the writer refuses (never rounds) any price needing more than 18 fractional digits, a non-finite price or negative zero; the service drops such an
input and counts it in `gma_malformed_records_total` (`GreekMoveAuthTopologyTest`), it does not crash-loop.

### Input: `delta-flow-by-strike` (Avro `DeltaFlowStrikeSnapshot`)

The service reads `delta-flow-by-strike` through `AvroDeltaFlowReader` (behind the `DeltaFlowReader` seam; `DeltaFlowAvro.RecordDeserializer`, subject
`com.optionsedge.contracts.deltaflow.DeltaFlowStrikeSnapshot`). There is no JSON reader any more: JSON on that topic fails the deserializer (counted as a
deserialization failure) and is never decoded as data (`GreekMoveAuthTopologyTest.jsonBytesOnTheDeltaFlowTopicAreRefusedNotDecoded`).
`net = strikeNetCallDeltaFlow + strikeNetPutDeltaFlow`, `eventMs = asOfEventTimeMs`.

## Ordering with delta-flow-by-strike (HARD)

This cutover is ordered with, and after the registration step of, the delta-flow Avro cutover: `docs/DELTA-FLOW-AVRO-CUTOVER.md` (deploy PR 1324).
* **Who registers the delta-flow subject.** `scripts/kafka/ensure-delta-flow-schemas.sh` from deploy PR 1324 registers the 17 delta-flow subjects, including
  `DeltaFlowStrikeSnapshot`. `ensure-greek-move-auth-schemas.sh` (this PR) registers ONLY the nine greek-move subjects and never the delta-flow one. The delta-flow
  script must have run on the environment's registry BEFORE this service starts, or its delta source cannot decode.
* **Recreate `delta-flow-by-strike` only AFTER this reader is deployed.** The delta-flow runbook recreates that topic empty under its own name; an image of this
  service that still reads JSON would read Avro bytes as text and silently drop every record. This reader (processing PR 1175, `AvroDeltaFlowReader`) is merged and its
  image built BEFORE the by-strike topic is recreated, and the service stays at 0 replicas across the recreate (the delta-flow runbook step 3 already stops it). Never run
  an image built before PR 1175 against an Avro by-strike topic, nor one built from PR 1175 against a JSON by-strike topic.
* Contracts >= 0.55.0 is required (0.54.0 is delta-flow, 0.55.0 adds greek-move; the released artifacts are cumulative).

## Order (one environment at a time, off-hours, topic administration frozen)

1. Contracts PR 120 (0.54.0) and PR 118 (0.55.0) are already merged; merge processing PR 1175, feed-gateway PR 239, then this change; build the service and gateway images against that release.
2. Register the subjects (read-all-then-mutate, sha256-pinned; add `DEPLOY_DRY_RUN=true` first). Registries: dev `http://host.docker.internal:8082`, production `http://192.168.100.252:8082`:
   `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas/greekmoveauth scripts/kafka/ensure-greek-move-auth-schemas.sh`
   and, from deploy PR 1324, `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas/deltaflow scripts/kafka/ensure-delta-flow-schemas.sh` (before step 6).
3. **Stop every reader and writer**, and verify each is at 0 replicas (service-deploy paths; the workloads are Jenkins-only, so verify the path you use really holds replicas at 0):
   `greek-move-authenticity-service`, the gateway's greek-move-auth consumption (`options-edge-feed-gateway`), and `trade-evidence-api` (a documented reader of `current`,
   `registry.py:29`). Confirm with `kubectl get deploy <name> -o jsonpath='{.status.replicas}'` empty/0 and no active member in the groups below.
4. **Record, then DELETE the internal topics** by exact name (never purge) with the service at 0, and delete its local state (`emptyDir`). Internal topics of application id `<appId>`
   (default `options-edge-greek-move-authenticity`, plus the environment suffix; read it off the broker with `kafka-consumer-groups --list | grep greek-move`):
   `<appId>-frame-rekey` and the six changelogs `<appId>-gma-{spot-window,delta-window,gex-latest,baseline,engine-state,last-verdict}-store-changelog`.
   First `kafka-topics --describe --topic <t>` each and note its TopicId (`id_before`), partitions and configs; then `kafka-topics --delete --topic <t>`; then confirm each is absent
   (`--describe` fails with UnknownTopicOrPartition). Never rely on broker auto-create (off on dev).
5. **Delete every consumer group that holds an offset on these topics, THEN wipe** (the harness's strict preflight REFUSES any committed offset on a topic it is about to delete; offsets survive a topic delete, keyed by topic name):
   ```
   # runbook-lint:delete-groups
   kafka-consumer-groups --bootstrap-server <bootstrap> --describe --all-groups | awk '$2 ~ /^options\.spx\.greek-move-auth\.(current|events)$/ || $2 ~ /-frame-rekey$/ || $2 == "delta-flow-by-strike" {print $1}' | sort -u
   kafka-consumer-groups --bootstrap-server <bootstrap> --delete --group <g>      # each one (non-Empty refuses: stop its reader first)
   kafka-consumer-groups --bootstrap-server <bootstrap> --describe --all-groups | awk '$2 ~ /^options\.spx\.greek-move-auth\.(current|events)$/' | wc -l      # must print 0
   ```
   Expect: the service's Streams group (`<appId>`), the gateway's greek-move groups, and the `trade-evidence-api` group (names as printed by the first command). The delta-flow-by-strike
   group offsets are deleted by the delta-flow runbook step 5 (same rule); the service group deleted here covers its by-strike offsets.
6. Wipe and recreate the two output topics empty with the merged generic harness `scripts/kafka/roll-forward-topics.sh` (deploy main, commit 8d7f4c65 or later; run from a checkout at or after it, everything stopped, strict mode).
   Read each TopicId first (`kafka-topics --describe --topic <t>`), then:
   `scripts/kafka/roll-forward-topics.sh --recreate --cluster dev --topics options.spx.greek-move-auth.current,options.spx.greek-move-auth.events --pre-cutover-id options.spx.greek-move-auth.current=<id> --pre-cutover-id options.spx.greek-move-auth.events=<id> --registry http://host.docker.internal:8082 --subject options.spx.greek-move-auth.current=com.optionsedge.contracts.greekmoveauth.MoveAuthenticityVerdict --subject options.spx.greek-move-auth.events=com.optionsedge.contracts.greekmoveauth.MoveAuthenticityVerdict --writers-and-readers-stopped localhost:19092`
   then the same with `--verify` instead of `--recreate`, adding `--new-topic-id <topic>=<id>` for each id `--recreate` printed. Production: `--cluster production --registry http://192.168.100.252:8082 ... 192.168.100.252:9092`.
   `delta-flow-by-strike` is recreated by the delta-flow runbook, not here, and only after this reader is deployed.
7. **Start the service first and verify the internal topics before anything else is started.** What the code does (processing PR 1175, `GreekMoveAuthStreams.start()`):
   `ensureTopics()` runs BEFORE `new KafkaStreams(...)`/`streams.start()`. It calls `KafkaTopics.ensureTopic` for `<appId>-frame-rekey` (cleanup.policy=delete,
   retention.ms=`GREEK_MOVE_AUTH_CHANGELOG_RETENTION_MS` default 86400000, segment.bytes=`GREEK_MOVE_AUTH_CHANGELOG_SEGMENT_BYTES` default 67108864, partitions=`KafkaSettings.DEFAULT_PARTITIONS` =
   env `KAFKA_TOPIC_PARTITIONS_DEFAULT`, code default 4: read the deployed value off the render, not the code) and create-only for `current` (`ensureServedTopic`) and `events`; it does NOT rely on broker auto-create.
   `ensureTopic` creates the topic when absent, and when it exists with fewer partitions it EXPANDS it in place, so verify the count below. The six changelogs are created by Kafka Streams'
   own internal-topic manager (admin client) when `streams.start()` runs, compacted, with the partition count of the repartition topic they follow; this is also independent of broker auto-create.
   After the service reports its first start, and BEFORE starting the gateway or `trade-evidence-api`, verify with `kafka-topics --describe --topic <t>` for `<appId>-frame-rekey` and each of the six changelogs:
   the TopicId differs from `id_before` (a new topic), partitions equal the deployed `KAFKA_TOPIC_PARTITIONS_DEFAULT` for frame-rekey and equal frame-rekey's count for every changelog, `cleanup.policy` is `delete` for
   frame-rekey and `compact` for the changelogs, and frame-rekey `retention.ms`/`segment.bytes` match the values above. Any mismatch: stop the service and repeat steps 4-6.
   Also verify `delta-flow-by-strike` is Avro or empty, and that the service's deserialization-failure metric stays 0.
8. Bring the readers back in order: the gateway (verify Ready with zero records consumed), then `trade-evidence-api` (verify its `current` cache fills from Avro and its decode-error counter for `current` stays 0). Verify the first
   records on both output topics start with magic byte 0 and decode under the canonical subject id.
