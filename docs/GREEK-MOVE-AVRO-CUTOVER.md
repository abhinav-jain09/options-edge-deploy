# greek-move-authenticity-service Avro cutover

Topics keep their own names and are wiped and recreated carrying Avro (no versioned sibling, roll forward only).
Contracts >= 0.36.0 (`schemas/greekmoveauth/`; contracts PR 118), processing PR 1175, feed-gateway PR 239.
Contracts PR 118 was renumbered 0.32.0 -> 0.36.0 because it MERGES the delta-flow content of contracts PR 120 (0.35.0, `DeltaFlowAvro`): the service
reads `delta-flow-by-strike` as Avro AND writes greek-move-auth as Avro from ONE coordinate. Merge PR 120 first; if main has moved, renumber 118 at merge
(the processing `contracts.version` must name a release that contains both `GreekMoveAuthAvro` and `DeltaFlowAvro`).

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
* Contracts >= 0.36.0 is required (PR 118 contains PR 120's content: merge 120 first or renumber).

## Order (one environment at a time, off-hours, topic administration frozen)

1. Merge contracts PR 120, then contracts PR 118 (>= 0.36.0), processing PR 1175, feed-gateway PR 239, then this change; build the service and gateway images against that release.
2. Register the subjects (read-all-then-mutate, sha256-pinned; add `DEPLOY_DRY_RUN=true` first). Registries: dev `http://host.docker.internal:8082`, production `http://192.168.100.252:8082`:
   `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas/greekmoveauth scripts/kafka/ensure-greek-move-auth-schemas.sh`
   and, from deploy PR 1324, `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas/deltaflow scripts/kafka/ensure-delta-flow-schemas.sh` (before step 6).
3. Scale the service and the gateway's greek-move-auth consumption to zero. Delete the service's consumer group (the effective Streams application id) so no committed
   offset survives the recreate of the two output topics and of `delta-flow-by-strike`, and read it back.
4. DELETE the repartition and the six changelogs by exact name (never purge), and the service's local state (`emptyDir`).
5. Wipe and recreate the two output topics empty with the merged generic harness `scripts/kafka/roll-forward-topics.sh` (deploy main, commit 8d7f4c65 or later; run from a checkout at or after it).
   Read each TopicId first (`kafka-topics --describe --topic <t>`), then:
   `scripts/kafka/roll-forward-topics.sh --recreate --cluster dev --topics options.spx.greek-move-auth.current,options.spx.greek-move-auth.events --pre-cutover-id options.spx.greek-move-auth.current=<id> --pre-cutover-id options.spx.greek-move-auth.events=<id> --registry http://host.docker.internal:8082 --subject options.spx.greek-move-auth.current=com.optionsedge.contracts.greekmoveauth.MoveAuthenticityVerdict --subject options.spx.greek-move-auth.events=com.optionsedge.contracts.greekmoveauth.MoveAuthenticityVerdict --writers-and-readers-stopped localhost:19092`
   then the same with `--verify` instead of `--recreate`, adding `--new-topic-id <topic>=<id>` for each id `--recreate` printed. Production: `--cluster production --registry http://192.168.100.252:8082 ... 192.168.100.252:9092`.
   `delta-flow-by-strike` is recreated by the delta-flow runbook, not here, and only after this reader is deployed.
6. Deploy the gateway, verify it Ready with zero records consumed, then the service (delta-flow subject registered). Verify the first records on both outputs start with
   magic byte 0 and decode under the canonical subject id, and that the service's deserialization-failure metric stays 0.
