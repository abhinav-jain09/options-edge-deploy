# greek-move-authenticity-service Avro cutover

Topics keep their own names and are wiped and recreated carrying Avro (no versioned sibling, roll forward only).
Contracts 0.32.0 (`schemas/greekmoveauth/`), processing PR 1175, feed-gateway PR 239.

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

Only `options-edge-feed-gateway` reads `current` (now Avro, same browser JSON; the web page is unchanged). `trade-evidence-api` (processing `trade-evidence-api/registry.py:29` subscribes to `current`; no canonical binding) reads it through
`Collector.decode` in `trade-evidence-api/collector.py`: the generic Confluent Avro path (schema fetched by id, cached, `fastavro`, processing
commit `a08fab4a6`). `current` is listed in `AVRO_ONLY_TOPICS` (processing PR 1175): a value that is not Confluent-framed Avro (JSON,
or any non-magic-byte payload) is REFUSED before any registry lookup and counted as a decode error, with no JSON fallback.
`tests/test_greek_move_auth_avro_reader.py` pins both the decode of a real `MoveAuthenticityVerdict` frame and the JSON refusal.
Price fidelity: the writer refuses (never rounds) any price needing more than 18 fractional digits, a non-finite price or negative
zero; the service drops such an input and counts it in `gma_malformed_records_total` (`GreekMoveAuthTopologyTest`), it does not crash-loop. Nothing reads `events` or archives either topic. `delta-flow-by-strike` is a JSON INPUT written by delta-flow-service;
its conversion is scheduled in the next wave and `LegacyJsonDeltaFlowReader` (the only JSON read) must switch to Avro in lockstep.

## Order

1. Merge contracts, processing, gateway; build the service and gateway images against contracts 0.32.0.
2. Register the subjects (read-all-then-mutate, sha256-pinned; add `DEPLOY_DRY_RUN=true` first):
   `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas/greekmoveauth scripts/kafka/ensure-greek-move-auth-schemas.sh`
3. Scale the service and the gateway's greek-move-auth consumption to zero; reset the Streams group to offset 0 on the retained inputs and read it back.
4. DELETE the repartition and the six changelogs (never purge), and the service's local state.
5. Wipe and recreate the two output topics empty (generic script, deploy PR 1320; one `--pre-cutover-id` per topic read off the live broker):
   `scripts/kafka/roll-forward-topics.sh --recreate --cluster dev --topics options.spx.greek-move-auth.current,options.spx.greek-move-auth.events --pre-cutover-id options.spx.greek-move-auth.current=<id> --pre-cutover-id options.spx.greek-move-auth.events=<id> --writers-and-readers-stopped localhost:19092`
   then the same with `--verify --new-topic-id ...`; production uses `--cluster production ... 192.168.100.252:9092`.
6. Deploy the gateway, verify it Ready with zero records consumed, then the service; verify the first records start with magic byte 0 and decode under the canonical subject id.

## Ordering with delta-flow-by-strike

`LegacyJsonDeltaFlowReader` (behind the single `DeltaFlowReader` seam in the service) still reads the JSON `delta-flow-by-strike` topic.
JSON on Kafka is prohibited, so this cutover is ordered AFTER the delta-flow-by-strike Avro cutover; the reader swap to the Avro decoder is
the final commit on the processing PR before merge. Do not roll this change set before that.
