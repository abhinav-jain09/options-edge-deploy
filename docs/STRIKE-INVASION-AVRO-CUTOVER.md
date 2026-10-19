# strike-invasion JSON -> Avro: cutover runbook

Kafka Serialization Rule (rule.md): JSON on Kafka is prohibited; a topic that changes serialization is **wiped and recreated under its own
name** (no `.vN`/`.avro` sibling, no dual publish, no rollback). Contracts 0.33.0 (`schemas/strikeinvasion/*.avsc`, `StrikeInvasionAvro`,
`StrikeInvasionJson`): Confluent framing, RecordNameStrategy (subject = record full name, one subject per environment), FULL, `auto.register=false`.

## Subjects (23, `com.optionsedge.contracts.strikeinvasion.` + name)
* public: `StrikeInvasionSnapshot` (`options.spx.strike-invasion.current`); events topic = 12 types `StrikeInvasionEvent` + `NonFiniteSpotDropped`,
  `OutOfOrderSpotDropped`, `NonFiniteStrikeDropped`, `SessionReset`, `CandidateInvalidated`, `Stage1Retired`, `State`, `Stage1`, `Invasion`, `Stage2Timeout`,
  `DefenseBroken`, `Stage2Verdict` (`options.spx.strike-invasion.events`).
* internal (the service's repartition topics and store changelogs): `StrikeInvasion` + `SpotEvidence`, `SrEvidence`, `GexEvidence`, `FlowEvidence`,
  `BehaviorEvidence`, `PaceEvidence`, `PaceFrame`, `WallFrame`, `WallRow`, `WallMeta`.
Register all of them: `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas/strikeinvasion scripts/kafka/ensure-strike-invasion-schemas.sh`
(exactly these 23 files, FULL by read-back, canonical = latest; `DEPLOY_DRY_RUN=true` mutates nothing). Registries: dev/prod `:8082`, es4 `:8081`.

## Topics and shapes (scripts/kafka/topics.env; `<app>` = `options-edge-strike-invasion-service-dev` | `-prod`)
| topic | shape | recreated by |
|---|---|---|
| `options.spx.strike-invasion.current` | 32 partitions, compacted, `retention.ms=-1`, RESET-REBUILDABLE (Postgres holds its content) | `apply-topics.sh` |
| `options.spx.strike-invasion.events` | 1 partition, delete-retention, config stamped by the service; NAS-archived (floor 200) | `ensure-partition-only-topics.sh` / service `ensureTopic` |
| `<app>-repart-{spot,sr,gex,flow,behavior,pace,wall}` | 1 partition each | dev: partition-only list; prod and dev: the service |
| `<app>-{sr,gex,flow,behavior,pace}-store-changelog` | Streams-managed | Streams on first start |
| `es.options.spx.strike-invasion.current` (es4) | 4 partitions, declared only so the es4 gateway can read metadata | verify EMPTY; if it holds records, delete+recreate |
State dir `kafka-streams-state` is an `emptyDir` (lost with the pod). Consumer groups: `<app>` (the Streams app), `invasion-postgres-writer`,
the feed-gateway groups `options-edge-feed-gateway-*`, `early-auction-verdict` (EAV; its local store is truth: check it holds nothing derived from JSON events),
spx-mission-control (`kafka-consumer-groups --list | grep mission`), approach-monitor reads neither topic.

## Order (rule step 4; one environment at a time, off-hours, topic administration frozen)
1. Merge contracts 0.33.0, the processing PRs (service, postgres writer, mission control, EAV), feed-gateway, trading-journal/python readers, then this change.
   From the first merge on, an image built from main for any converted service reads Avro: build no prod image before the prod cutover.
2. **Register** the 23 subjects (above) on the environment's registry.
3. **Stop** (service-deploy paths; no `kubectl scale`): feed-gateway strike-invasion consumer path, `invasion-postgres-writer` (first confirm its lag on `current`
   is 0, else those records exist only in Kafka), spx-mission-control, early-auction-verdict, `strike-invasion`; suspend the NAS events archive cron line.
4. **Reset the Streams state** with the service at 0: `KAFKA_BOOTSTRAP_SERVERS=<bootstrap> STRIKE_INVASION_APP_ID=<app> scripts/kafka/reset-strike-invasion-state.sh`
   (group absent/Empty, group deleted, the 7 repartition + 5 changelog topics deleted by exact name, none left). Offsets are deliberately NOT reset to 0 (the
   manifest runs `auto.offset.reset=latest` and evidence older than 120 s is excluded; a replay from 0 would re-emit days of events into the new topic).
4b. **Pin the start offsets** (service still at 0): `KAFKA_BOOTSTRAP_SERVERS=<bootstrap> STRIKE_INVASION_APP_ID=<app> STRIKE_INVASION_SOURCE_TOPICS="<the seven
   source topics of this environment, TOPIC_PREFIX and *_INPUT_*_TOPIC overrides applied>" scripts/kafka/strike-invasion-start-offsets.sh pin`. It captures the end offset of every
   source partition, commits exactly those offsets for the Streams group and records them, so the restart position is a fact and not `auto.offset.reset=latest`.
5. **Delete and recreate `current` and `events`** with the generic roll-forward tool. **HARD PREREQUISITE: options-edge-deploy#1320 (`scripts/kafka/roll-forward-topics.sh`,
   `docs/KAFKA-AVRO-TOPIC-ROLL-FORWARD.md`) is merged first; record its merge sha in the cutover log at cutover time and run it from that sha.** Read each TopicId first
   (`kafka-topics --describe --topic <t>`) and freeze topic administration, then, everything stopped:
   `scripts/kafka/roll-forward-topics.sh --recreate --cluster dev|production --topics options.spx.strike-invasion.current,options.spx.strike-invasion.events
   --pre-cutover-id <current>=<id> --pre-cutover-id <events>=<id> --registry <url> --subject options.spx.strike-invasion.current=com.optionsedge.contracts.strikeinvasion.StrikeInvasionSnapshot
   --subject options.spx.strike-invasion.events=com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventStage1 --writers-and-readers-stopped <bootstrap> [kafka-bin-dir]`
   (strict mode: both topics must come back EMPTY, so no record is sampled and the single events subject is only the existence/FULL check; the other eleven event subjects are
   covered by `ensure-strike-invasion-schemas.sh`). It copies the live shape (current: 32 partitions, compacted, retention -1; events: 1 partition). Then delete the writer group:
   `kafka-consumer-groups --delete --group invasion-postgres-writer`. Do NOT use `apply-topics.sh` to recreate `current` ad hoc.
6. **Deploy the Avro readers** (writer, mission control, EAV, feed-gateway) and verify Ready with zero records consumed.
6b. **START-OFFSET GATE (mandatory, before the producer is enabled)**: `scripts/kafka/strike-invasion-start-offsets.sh verify` with the same environment as step 4b.
   It requires the Streams group to be Empty and EVERY pinned source partition to be committed at exactly its pinned end offset. Any `MISMATCH`/`ABORT` (missing partition,
   non-numeric or different offset, group absent or running) means: do NOT deploy `strike-invasion`; re-run `pin` (step 4b) and verify again. Only a printed
   `start-offset gate OK` line allows step 7.
7. **Deploy the Avro producer** (`strike-invasion`); verify its first records: `kafka-avro-console-consumer --topic options.spx.strike-invasion.events --property schema.registry.url=<registry>`
   and the first byte `00` + 4-byte id resolving to a `...strikeinvasion.StrikeInvasion*` subject; browser `strike-invasion` payload unchanged.
8. **NAS archive switch** (prod): set `OE_INVASION_AVRO_TOPICS="options.spx.strike-invasion.events"` on the events archive cron line and resume it. The reader
   `StrikeInvasionEventsArchiveReader.java` writes the JSON-era line (`StrikeInvasionJson.eventJson`); the archive unit ships it with the contracts jar
   (Jenkinsfile.archive-scripts-deploy builds from guarded latest main, so contracts 0.33.0 must be merged first). Old archived files stay JSON-era lines.
9. Full clean-slate flows (`dev-cleanup.sh`, `prod-clean-slate.sh`) already delete every non-preserved topic, including all of the above, and recreate the declared set.
