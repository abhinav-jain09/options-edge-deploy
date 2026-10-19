# Converting an existing JSON Kafka topic to Avro: roll forward under the same name

Rules: Kafka Serialization Rule (JSON on Kafka is prohibited) and the owner ruling of 2026-10-18: no versioned topic names. A topic
that changes serialization is **deleted and recreated empty under its own name**; retained JSON is deleted, never read by an Avro reader.
Tool: `scripts/kafka/roll-forward-topics.sh` (tests: `scripts/kafka/roll-forward-topics-test.sh`). The single-topic precursor with the
full worked example is `docs/UNDERLYING-SPX-PRICE-AVRO-CUTOVER.md`.

## Runbook order (one environment)
1. Merge the contract, then the reader and writer changes. A prod image built from main for a converted service reads Avro against a
   still-JSON topic: build no prod images of converted services until this environment's cutover.
2. **Register** every subject on the environment's registry before any producer (sha256-pinned schema, compatibility FULL, read back). Subject strategy is RecordNameStrategy: subject = the record's full name (one subject for every environment). Producers run with `auto.register.schemas=false` (verify the writer's config; it must refuse an unregistered shape, not register one).
3. Stop non-Kubernetes JSON writers and byte copiers (archive crons, console-consumer|producer bridges, launchd mirrors) and keep them stopped.
4. **Avro writer first**: roll it, then prove the OLD JSON writer pod is gone (one pod, new image digest).
5. **Read the TopicIds** off the live broker (`kafka-topics --describe --topic <t>`), same `<bootstrap>` as step 6, and freeze topic
   administration (Kafka's delete takes a name, not an id).
6. **Wipe and recreate**:
   `roll-forward-topics.sh --recreate --cluster dev|production --topics t1,t2 --pre-cutover-id t1=<id> --pre-cutover-id t2=<id> --avro-writer-live --json-writers-stopped <bootstrap> [bin-dir]`
   (everything stopped: `--writers-and-readers-stopped` instead; then the topic must be empty and no group may hold an offset).
   Also required: `--registry <url> --subject <topic>=<record.full.Name>` per topic. Before any change the script verifies each subject exists with FULL compatibility; in live mode each sampled record's schema id must be registered under that topic's subject.
   Bootstrap is bound to the cluster (dev `localhost:19092`, `host.docker.internal:19092`, `192.168.100.102:19092`; production `192.168.100.252:9092`).
7. **Roll the Avro readers**, then **verify the wire** (registry subjects, a decoded record per topic, reader group lag).

## What the script does
* Preflight over ALL topics before any delete: id equals the supplied pre-cutover id (else the run is refused and nothing is deleted),
  and the live partition count, replication factor and every topic-level config are captured to `$RF_STATE_DIR` (default
  `~/.roll-forward-topics/<cluster>/<topic>.shape`, `--state-dir` overrides). No per-topic table exists; the recreate copies the live shape.
  Configs are captured machine-exact from `kafka-configs --describe` (only `DYNAMIC_TOPIC_CONFIG` overrides, values verbatim). Only LIST-typed keys (`cleanup.policy`, `leader.replication.throttled.replicas`, `follower.replication.throttled.replicas`) with a comma value are created in bracket form (`[compact,delete]`); every other value, commas included, is passed verbatim.
* The shape file also records the cluster and the pre-cutover TopicId; absent-topic recovery and every post-check refuse a shape not bound to the supplied id.
* Config verification is two-way: the broker's overrides (read back with `kafka-configs`) must equal the captured set exactly; a mismatch fails the run with `kafka-configs --alter` recovery instructions. A topic present at preflight but absent at its turn is refused unless `--topic-absent-after-failed-create` is given. A tombstone is recognised ONLY as the unique sentinel `__OE_TOMBSTONE__` that the consumer is told to print for a null value (`--property null.literal=...`; a consumer that ignores the property prints `null`, which is refused, never accepted). Zero bytes from a consumer is NEVER evidence of anything and fails the gate; the registry subject is matched by exact JSON equality (`jq` is required); a consumer-group row with a non-integer offset fails closed. Live-mode (`--avro-writer-live`) evidence is exactly: the first and last record of each partition are positively read as Confluent-framed Avro (or the sentinel) AND their schema id is registered under the topic's subject. It is a two-point sample, not a scan; it does not prove that no JSON sits between them. Strict mode's evidence is broker end offset 0 on every partition. Before the first delete, preflight proves for every topic that its id, shape, offsets and the consumer groups are readable; the post-checks themselves can only run after a topic is recreated, so the run stops at the first failing topic and names those not touched. Offsets must name each partition 0..N-1 exactly once.
* Unknown options and extra positional arguments are refused.
* Last-look id re-check immediately before each delete; absent topic refused unless `--topic-absent-after-failed-create`
  (the shape captured by the failed run is reused).
* Post-checks per topic, from the broker: new id (printed first as `NEW TOPIC ID: <topic>=<id>`), exact partitions/RF/configs, offsets
  readable for every partition, empty (strict) or first and last record per non-empty partition verifiably Avro or a tombstone
  (live mode, fail closed; a sample, not a scan), stale consumer-group offsets (strict: fail; live: WARN only for offsets beyond the log end).
* Topics run in order and the run **stops at the first failure**, printing the untouched topics. Re-run with `--topics <remaining>`
  (a finished topic has a new id and is refused by design). Created-but-post-check-failed with no JSON: `--verify --new-topic-id t=<id>`
  (no mutation). JSON found: the topic is contaminated; stop the JSON writer and `--recreate` again with `--pre-cutover-id t=<printed id>`.

## Learned on 2026-10-18/19
* **Workloads are Jenkins-only** (an admission policy denies `kubectl scale/apply`) and no job scales to zero, so readers cannot be
  stopped first: hence Avro writer first, wipe, then readers. Only OLD JSON readers meet an unreadable format, briefly.
* **Committed consumer offsets survive a topic delete** (keyed by topic name). A group that kept one resumes beyond the new log end.
  Strict mode fails on any; live mode reports offsets beyond the log end and the per-reader gate decides. Delete with
  `kafka-consumer-groups --delete-offsets` (a reset to 0 is still a committed offset).
* **Kafka Streams global-store checkpoints** record the end offset of the source topic on the volume; a recreated topic makes it stale.
  Discard it in the reader at start, or empty that PVC. Any reader with persistent state keyed by source offsets needs the same.
* **Persistent state** (changelogs, snapshots, PVC stores) written from JSON records outlives the wipe; inventory it per reader and
  rotate or rebuild it. A topic recreated while a stale JSON image still runs gets JSON again within seconds: a new TopicId does not prove no JSON since.

The test suite uses CLI mocks (no real broker or registry); run a dry rehearsal on a scratch topic on dev before first production use.
