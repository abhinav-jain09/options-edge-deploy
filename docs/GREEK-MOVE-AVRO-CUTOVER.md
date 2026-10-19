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

Only `options-edge-feed-gateway` reads `current` (now Avro, same browser JSON; the web page is unchanged). `trade-evidence-api` decodes
generic Avro. Nothing reads `events` or archives either topic. `delta-flow-by-strike` is a JSON INPUT written by delta-flow-service;
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
