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
   `roll-forward-topics.sh --recreate --cluster dev|production|es4 --topics t1,t2 --pre-cutover-id t1=<id> --pre-cutover-id t2=<id> --avro-writer-live --json-writers-stopped <bootstrap> [bin-dir]`
   (everything stopped: `--writers-and-readers-stopped` instead; then the topic must be empty and no group may hold an offset).
   Also required: `--registry <url> --subject <topic>=<record.full.Name>` per topic. Before any change the script verifies each subject exists with FULL compatibility; in live mode each sampled record's schema id must be registered under that topic's subject.
   Bootstrap is bound to the cluster (dev `localhost:19092`, `host.docker.internal:19092`, `192.168.100.102:19092`; production `192.168.100.252:9092`; es4 `192.168.100.4:9092`).
7. **Roll the Avro readers**, then **verify the wire** (registry subjects, a decoded record per topic, reader group lag).

## What the script does
* Preflight over ALL topics before any delete: id equals the supplied pre-cutover id (else the run is refused and nothing is deleted),
  and the live partition count, replication factor and every topic-level config are captured to `$RF_STATE_DIR` (default
  `~/.roll-forward-topics/<cluster>/<topic>.shape`, `--state-dir` overrides). No per-topic table exists; the recreate copies the live shape.
  Configs are captured machine-exact from `kafka-configs --describe` (only `DYNAMIC_TOPIC_CONFIG` overrides, values verbatim). The value/`sensitive=... synonyms={` boundary is chosen delimiter-safely: the one candidate whose first synonym repeats `key=value` AND whose entire synonym list parses (every entry `SRC:key=value`, same key, separated by `, `, no stray `}`, nothing after the final `}`); a line off-grammar, or with zero or several consistent candidates, refuses the run before any delete. Before EVERY create (normal path included) the consumer groups are re-read and both group gates re-run; a failure after the delete leaves the topic absent (recover with `--topic-absent-after-failed-create` once the offsets are cleared). Only LIST-typed keys (`cleanup.policy`, `leader.replication.throttled.replicas`, `follower.replication.throttled.replicas`) with a comma value are created in bracket form (`[compact,delete]`); every other value, commas included, is passed verbatim.
* The shape file also records the cluster and the pre-cutover TopicId; absent-topic recovery and every post-check refuse a shape not bound to the supplied id.
* Config verification is two-way: the broker's overrides (read back with `kafka-configs`) must equal the captured set exactly; a mismatch fails the run with `kafka-configs --alter` recovery instructions. A topic present at preflight but absent at its turn is refused unless `--topic-absent-after-failed-create` is given. A tombstone is recognised ONLY as the unique sentinel `__OE_TOMBSTONE__` that the consumer is told to print for a null value (`--property null.literal=...`; a consumer that ignores the property prints `null`, which is refused, never accepted). Zero bytes from a consumer is NEVER evidence of anything and fails the gate; the registry subject is matched by exact JSON equality (`jq` is required); a consumer-group row with a non-integer offset fails closed. The WHOLE `kafka-consumer-groups --describe --all-groups` answer must be recognisably complete (header lines, data rows of at least 5 fields after a header, no error lines); an empty answer is accepted only if `--list` also succeeds and is empty. A `}` inside a config value that is also its first synonym is refused as ambiguous. Live-mode (`--avro-writer-live`) evidence is exactly: the first and last record of each partition are positively read as Confluent-framed Avro (or the sentinel) AND their schema id is registered under the topic's subject. It is a two-point sample, not a scan; it does not prove that no JSON sits between them. Strict mode's evidence is broker end offset 0 on every partition. Before the first delete, preflight proves for every topic that its id and shape are captured, that BOTH offset snapshots (earliest and latest) are well-formed (integer fields, each partition exactly once, count equal to the described count) that every consumer-group row naming it is parseable and, in strict mode, that no consumer group holds a committed offset for it (the same predicate the post-check applies), refusing with zero deletes otherwise (the same two group gates also apply to a `--topic-absent-after-failed-create` topic before its create). Immediately before each delete ONE `kafka-topics --describe` supplies the TopicId, partition count, RF and the raw `Configs:` text, all compared with the preflight capture (no second read of topic state, so an id and a config set from different topic incarnations cannot be paired); the residual window between that describe and the delete call is inherent to Kafka without an admin lock and is not closed here and the group gates are re-run; any change refuses that topic, earlier topics stay done, later ones are untouched; the post-checks themselves can only run after a topic is recreated, so the run stops at the first failing topic and names those not touched. Offsets must name each partition 0..N-1 exactly once.
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

## Pending PRODUCTION pre-start steps after the 2026-11-02 wave-1 cutover (prod was left shut down)
Prod topics are Avro, the prod deployments pin the Avro digests at replicas 0, and the prod schema registry was stopped. Before the registry/services are started:
1. **Register `ReversalTopCandidate` under fixed id 2901** (a reversal-confirmation image built before processing b447a71c8 crash-loops without it; the current one needs the subject):
   `SCHEMA_REGISTRY_URL=http://192.168.100.252:8082 SCHEMAS_DIR=<contracts>/schemas/reversal-candidate SCHEMA_ID_MAP=scripts/kafka/schema-id-maps/reversal-top-candidate.map bash scripts/kafka/ensure-fixed-id-schemas.sh`
   Enforced: `scripts/ops/prod-start-prereqs.sh` (called by `morning-autostart.sh`) keeps reversal-confirmation-service at 0 while the subject is absent, under another id, or the registry is unreachable. `oe-boot-bringup.sh` is installed as a single root-owned file and does NOT call it: after a reboot, scale reversal-confirmation back to 0 until step 1 is done.
2. **short-premium-agent**: roll to the Avro image (dev `27-59602c227bbc`, prod `28-59602c227bbc`) once the prod registry is up for the qualification; move native Agent A off avro-releases/build-15 with `ENVIRONMENT=production QUALIFICATION_ONLY=true DEPLOY_NATIVE_A=true`. The prod deployment still pins the pre-Avro digest `sha256:184e03c77a7c...`.
3. **trade-evidence-api**: the prod deployment now pins `prod-15-512d8881d0bb` (`sha256:77599700...`); confirm it is the Avro build (its collector must refuse JSON on the converted topics) before it is started.
4. **underlying.vix.price (cut over on dev on 2026-11-06, and on es4 the same day but the es4 wipe did NOT hold, `docs/VIX-UNDERLYING-PRICE-AVRO-CUTOVER.md`)**: production still carries JSON and its registry does not hold `com.optionsedge.contracts.hpsf.VixUnderlyingPrice`. es4's `es.underlying.vix.price` was refilled with production's JSON by MM2 within hours of its wipe (4681 records, measured 2026-11-08), so es4 needs a second wipe inside production's own cutover — es4 is not finished. Before the registry/services start: register the subject under the FIXED id 3061 (`scripts/kafka/schema-id-maps/vix-underlying-price.map`; the MM2 prod -> es4 copy needs the same id on both sides), then feed -> wipe `underlying.vix.price` (`--cluster production`) -> readers. Until then no production reader of that topic (spot-vol-regime, reversal-confirmation, vix-option-inteligence, drop-classifier, feed-gateway) may be started on a post-change image, and no production image of the feed may be built from main.
   Because this is an OUTSTANDING step rather than a regression, `prod underlying.vix.price` is listed in `schema-manifest/mirror-evidence-pending.txt`
   so the nightly `options-edge-schema-code-verify` reports PENDING instead of FAIL (2026-11-08; `scripts/ci/schema-mirror-evidence.py`). The entry is
   not a mute: it FAILS the run the moment production starts framing — removing it is part of the cutover, and the declared id is validated again from
   that moment. While it stands, the es4 (destination) registry must already map 3061 to the subject, which is checked on every run.
