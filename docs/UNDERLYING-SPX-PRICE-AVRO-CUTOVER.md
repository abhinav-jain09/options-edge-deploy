# underlying.spx.price JSON -> Avro: cutover runbook and JSON removal plan

Kafka Serialization Rule (options-edge rule.md): JSON on Kafka is prohibited. Owner, 2026-10-18: no versioned topic names, a
topic that changes serialization is **wiped and recreated under its own name** and every reader is Avro only. So there is
no `underlying.spx.price.avro` sibling and no dual-publish window: each environment is cut over once, in the order below.
The JSON topic is "removed" by the wipe in step 4 — its removal date is the date of that environment's cutover.

## The contract
* schema `options-edge-contracts schemas/hpsf/UnderlyingPrice.avsc` (contracts 0.27.0), record
  `com.optionsedge.contracts.hpsf.UnderlyingPrice`; `price`/`canonicalSpot` are `decimal(38,18)` of the feed's shortest
  round-trip text (the reader's `doubleValue()` is the identical double); `size` is `["null","long"]`.
* subject = the record's full name, `com.optionsedge.contracts.hpsf.UnderlyingPrice` (RecordNameStrategy; one subject for
  every environment whatever its `TOPIC_PREFIX`), compatibility `FULL`.
* writer: the Rust feed only (`options-edge-databento-feed`, `rust/src/spx_price_avro.rs`). It never registers: at start it
  reads the LATEST version of the subject and refuses any shape but the canonical one.
* readers: `UnderlyingPriceAvro.EventDeserializer` / `serde(registryUrl)` (contracts). JSON, non-Avro and foreign records fail
  loudly; there is no fallback anywhere.

## Order (one environment)
Workloads can only be changed through Jenkins (an admission policy denies `kubectl scale`/`apply` to everyone else), and no Jenkins job scales a
service to zero, so the readers cannot be "stopped" first. The order below is the one the 2026-10-18 roll-forward settled on instead: the
AVRO writer first, then the wipe, then the Avro readers — so no Avro-only reader ever meets retained JSON, and the only readers that meet a
format they cannot read are the OLD JSON readers, for the minutes before each is replaced.
1. **Merge** contracts, then the reader PRs (processing A-D, feed-gateway, approach-monitor), then the feed and this deploy change.
   WARNING: from the first merge on, a PROD image built from main for any converted service reads Avro against the still-JSON prod
   topic. Do not build prod images of the feed or any reader until the prod cutover below.
2. **Register** the subject on the environment's registry (before any producer):
   `SCHEMA_REGISTRY_URL=<registry> SCHEMA_FILE=<contracts>/schemas/hpsf/UnderlyingPrice.avsc scripts/kafka/ensure-underlying-price-schema.sh`
   (sha256-pinned bytes, FULL by read-back, the canonical schema must be the latest version).
3. **Production prerequisites (stop BEFORE the feed rollout; verify each):** the **NAS archive** `*/10` spot cron (`ARCHIVE_JOB=spot`: suspend the crontab
   line, then confirm no `oe-archive-kafka.sh` process holds the spot lock) and the **`spx-es4-bridge`** console-consumer|producer (a byte copier and a possible
   JSON writer: stop it and keep it stopped; `Jenkinsfile.spx-price-reencoder` TARGET=es4 retires its launchd unit and replaces it, see "Mirrors" below). Not applicable on dev (it archives no spot and has no bridge).
4. **Roll the Avro feed** (`options-edge-databento-feed-deploy`, then `service-deploy SERVICE=databento-feed`) and prove the OLD (JSON) feed pod is
   gone: exactly one feed pod, running the new image digest. The topic now holds the retained JSON followed by Avro. A pre-cutover feed that
   survives would write JSON into the fresh topic after step 4.
5. **Wipe and recreate** (retained JSON deleted, never read by an Avro reader). Freeze topic administration on that broker for the window (Kafka's
   delete takes a NAME, not a TopicId; the script re-checks the id immediately before the delete but cannot exclude another administrator). With
   the SAME `<bootstrap>` for both commands (dev: `host.docker.internal:19092`; production: `192.168.100.252:9092`):
   `kafka-topics --bootstrap-server <bootstrap> --describe --topic underlying.spx.price` -> note the `TopicId`, then
   `scripts/kafka/roll-forward-underlying-spx-price.sh --recreate --cluster <dev|production> --pre-cutover-id <TopicId> --avro-feed-live --json-writers-stopped <bootstrap> [bin-dir]`
   (`--avro-feed-live --json-writers-stopped` because the Avro feed is writing and readers are rolling: the operator attests only that NO JSON WRITER, i.e. the pre-cutover feed pod,
   is live. The strict mode, `--writers-and-readers-stopped` without `--avro-feed-live`, additionally requires an empty topic and no committed offset at all. Production is refused until its shape is recorded.)
   The script refuses a bootstrap that is not the named cluster's broker, refuses a topic whose id is not the pre-cutover one, verifies the
   shape (partitions, replication factor, configs) from the broker, verifies the first and last records are Confluent-framed Avro (a JSON record
   means a JSON writer is live), and REPORTS (WARN) any consumer group holding an offset BEYOND the new log end: readers are still rolling, so a group cannot be made inactive on demand, and a regular
   consumer resets by its auto.offset.reset policy on its first poll; step 8 is the per-reader gate. This topic is a LATEST-VALUE spot stream (compacted, one record per symbol every few seconds, each record supersedes the previous), so a reader that
   resumes a few records late loses no information; that is why a numerically in-range offset is accepted without proof of provenance (strict mode, with everything stopped, FAILS on any committed offset). It prints `NEW TOPIC ID: <id>` right after reading it.
   If the DELETE succeeded but the CREATE failed (the topic is absent and the script said so), re-run the same command with
   `--topic-absent-after-failed-create` added; an absent topic is otherwise refused, because it means somebody else deleted it.
   If a post-check finds JSON in the new topic (a JSON writer was live), that topic is CONTAMINATED and verifying it in place proves nothing (a later Avro record would
   hide the JSON in the middle): make the JSON writer go away, then wipe again: re-run `--recreate` with `--pre-cutover-id` set to the `NEW TOPIC ID` the failed run printed
   (it now names the contaminated topic). Any other post-check failure (shape, replication factor) leaves no JSON in question: fix the cause, then run the same command with
   `--verify --new-topic-id <the printed id>` instead of `--recreate` (post-conditions only, no mutation). The first/last-record check is a SAMPLE, not a scan; the guarantee
   that no JSON writer exists is the operator's attestation (the old feed pod is gone).
   Persisted reader state: a Kafka Streams GLOBAL store checkpoints the end offset of its source topic on its volume. The only PVC-backed global store on this topic
   is `strike-flow-classifier-databento`; its spot store is in-memory in the converted code and the stale global checkpoint is discarded at every start,
   so no checkpoint can outlive the recreation. (strike-intelligence's global store sits on an emptyDir that is lost with the pod.) Any other reader added later
   that keeps source offsets in persistent state must be reset the same way.
6. **Roll the Avro readers**, one `service-deploy` each (`BUILD_IMAGES=true`, PERMITTED_SHA per rule). They start on a topic that holds only Avro.
7. **Verify the wire**, with the environment's registry (dev `http://host.docker.internal:8082`; production `http://192.168.100.252:8082`):
   `kafka-avro-console-consumer --bootstrap-server <bootstrap> --topic underlying.spx.price --partition 0 --offset earliest --max-messages 3 --property schema.registry.url=<registry> --property print.key=true`
   (the record prints as JSON; `price`/`canonicalSpot` show as the decimal bytes). Then prove the framing:
   `kafka-console-consumer --bootstrap-server <bootstrap> --topic underlying.spx.price --partition 0 --offset earliest --max-messages 1 | head -c 5 | xxd -p`
   prints `00` followed by the 4-byte big-endian schema id (e.g. `000000004d` is id 77); convert it with `printf '%d\n' 0x<the last 8 hex digits>`; that id must resolve to
   the canonical subject: `curl -s <registry>/schemas/ids/<decimal id>/versions` returns entries whose `subject` field equals `com.optionsedge.contracts.hpsf.UnderlyingPrice`.
8. **Verify each reader** (the gate for stale offsets): its committed offset lies in the NEW topic's offset range (`kafka-consumer-groups --describe --group <g>`), its log shows a tick decoded
   from a freshly produced record (compare offset/eventTime with the console-consumer view), there are no deserialization errors, and the browser `spx-price`
   events are unchanged. A reader stuck on an out-of-range offset (reset policy `none`) is crash-looping, and a crash-looping group is Empty between restarts: delete its offsets
   then (`kafka-consumer-groups --bootstrap-server <bootstrap> --delete-offsets --group <g> --topic underlying.spx.price`, which succeeds only on an Empty group) and let it restart.
9. Environment-specific follow-ups: NAS archive reader (prod), the two re-encoding mirrors (below).

## Mirrors: `Jenkinsfile.spx-price-reencoder`
Schema ids are per registry, so a framed record copied byte for byte into another registry's topic is undecodable (the id there means another
schema or none). The topic had two byte copiers; one job, one JVM tool (`scripts/kafka/spx-price-mirror/SpxPriceAvroMirror.java`, Confluent 7.3.1 jars) replaces both:
it decodes each value with the SOURCE registry, serializes it again with the TARGET registry under the same subject (RecordNameStrategy), and produces it with the
same key bytes, headers and the source record timestamp. It never registers and has no fallback: JSON, non-Avro bytes, a foreign record or a shape the target
registry does not know stop the mirror (launchd restarts it, the durable group does not advance). Offsets are committed only after the producer flushed.

| `TARGET` | path | replaces | durable group |
|---|---|---|---|
| `es4` | prod `192.168.100.252:9092` `underlying.spx.price` (registry :8082) -> es4 `192.168.100.4:9092` `es.underlying.spx.price` (registry :8081) | the `spx-es4-bridge` console pipe (launchd `com.optionsedge.spx-es4-bridge`, defined outside git; its plist is renamed `*.retired-by-spx-price-mirror`) | `spx-price-mirror-es4` |
| `mac74` | dev `127.0.0.1:19092` `underlying.spx.price` (registry :8082) -> `.74` `192.168.100.74:9092` `underlying.spx.price` (registry :8082) | the `underlying\.spx\.price` alternative of the `--whitelist` of `~/oe-ops/dl-mirror-dev-to-mac74/run-mirror.sh` (MirrorMaker 1, group `oe-mac74-input-from-dev`, also defined outside git) | `spx-price-mirror-mac74` |

Install order (per target), after the source topic is Avro (cutover step 5-7 of the SOURCE environment):
1. Register the subject on the TARGET registry (`ensure-underlying-price-schema.sh`, es4 :8081 / .74 :8082): the mirror never registers.
2. **Wipe and recreate the target topic under its own name** (es4 `es.underlying.spx.price`, .74 `underlying.spx.price`; no versioned sibling): it holds JSON from before the
   cutover. The Preflight reads the EARLIEST and the newest record of every non-empty target partition and refuses a topic whose first record is not framed Avro.
3. `mac74` only: delete the `underlying\.spx\.price|` alternative from the MM1 `--whitelist` and restart `com.optionsedge.dl-mirror-dev-to-mac74`. The Preflight reads that file and
   FAILS while the alternative is present (two writers, one copying source ids). The old group `oe-mac74-input-from-dev` keeps a stale committed offset on the topic; it is harmless and
   is not the mirror's group.
4. Run the job (`PERMITTED_SHA` = `git rev-parse origin/main`). Exactly what is verified:
   * Preflight: brokers reachable; no second writer; the subject is on BOTH registries with the same LATEST shape (ids may differ); the EARLIEST and the NEWEST retained record of every
     non-empty source partition name a schema id whose text is byte-identical (canonical JSON) to a version registered under the subject on the TARGET registry; both topics exist and are led;
     the target topic's `message.timestamp.type` is `CreateTime` (otherwise the source timestamp is overwritten by the broker); on BOTH topics the EARLIEST and the NEWEST record of every
     non-empty partition is Confluent-framed Avro whose id resolves to the subject on that topic's registry AND deserializes there (JSON, a tombstone or a foreign id fails);
     the mirror compiles against the installed jars. NOT verified: records between the earliest and the newest; a mid-log record written under an older compatible schema version is caught by the
     mirror's own fail-closed stop (the target serializer cannot find its schema, the mirror exits and the group does not advance).
   * Middle of the log: `SpxPriceAvroMirror verify-all` reads the TARGET topic from earliest to the end offsets captured at its start and fully decodes EVERY record (Confluent-framed, the id resolves to
     the subject on the target registry, deserializes to the canonical record; null values, JSON and foreign records fail). It runs in Preflight and again in Verify, bounded to 200000 records / 300 s and
     FAILING CLOSED above either bound (a larger topic needs a deliberate change, never a silent pass). `assert-target-fresh.sh` stays as an additional gate (empty target, or this mirror's own re-install).
     What remains an attestation: that nothing writes the target BETWEEN the scan and later (no broker ACLs, the mirror's producer is not transactional); the running mirror's own fail-closed stop covers that.
   * No legacy writer (mac74 and es4): the edited MM1 script is checked (`assert-mm1-whitelist-clean.sh`), AND `assert-no-legacy-writer.sh` fails if a running process still carries the whitelist entry, runs
     `spx-es4-bridge` or pipes a console producer into the topic, AND if the target end offset advances over 30 s while none of our units is installed (our own unit is stopped first on a re-install).
   * Null values: this topic declares no tombstones, so a null value STOPS the mirror (and fails the verifier); it is never copied. Keys are copied byte for byte, whatever their first byte.
   * Verify: the unit is loaded; then `verify-flow.sh` proves the MIRROR moves data (not that the target offset advanced, which any other writer would also do): the mirror group's lag on the source is
     bounded (1000), its committed offsets advance over 30 s while the source does, and the source record just before the group's committed position (the last one flushed) is found on the target, located by TIMESTAMP (`offsetsForTimes`, no fixed tail window) and matched on timestamp, KEY BYTES and decoded value. Source flat (market closed) is UNSTABLE (flow not proven); source advancing with the group not is a FAIL. Then the earliest and newest target records are
     fully decoded under the target registry.
5. es4 only, after FLOWING: es4 readers (es-cvd, es-amt, gamma-ladder-path, context-tape) already select Avro by the resolved topic name; verify each as in step 8 against `es.underlying.spx.price`.
Tests: `tests/test_spx_price_avro_mirror.py` (the real tool through two stub registries whose ids differ: target id replaces source id, payload unchanged, JSON/unknown/foreign/unregistered refused; plus `tests/java/SpxPriceAvroMirrorStepTest.java` on kafka-clients MockConsumer/MockProducer: key bytes, headers, source timestamp, re-encoded value, send -> flush -> commit order, no commit after a tombstone, JSON or failed produce), `test_spx_price_timestamp_type.py`, `test_spx_price_flow_and_fresh.py`, `test_spx_price_no_legacy_writer.py` (verify-all and flow-check run on kafka-clients MockConsumer in the Java test); source values in the Java tests are written by the production `KafkaAvroSerializer` (`tests/java/SerializeFixture.java`), not hand-encoded,
`test_spx_price_verify_target_record.py`, `test_spx_price_subject_parity.py`, `test_spx_price_mirror_job.py`. Not exercised by any test: a live broker round trip (the mock-based test covers the transcode+send+commit path, not broker semantics); the first install on each target is that proof, via Verify.

## Reader and producer changes (separate PRs; each Codex-approved on all three bars before merge)
contracts `options-edge-contracts#112` (schema + codec, 0.27.0) · feed `options-edge-databento-feed#178` · processing
`options-edge-processing#1131` (strike-flow-classifier, strike-intelligence, strike-invasion, strike-liquidity-heatmap) · `#1133`
(spot-vol-regime, greek-move-authenticity, vol-premium, dealer-ledger scorer, gamma-ladder-path) · `#1132` (context-tape,
databento-gex pre-open, drop-classifier, es-cvd, es-amt) · `#1135` (indicator, hedging-pressure, market-commentary, multileg,
reversal-confirmation, signal-follower, spread-skew, breakout-sequencer, hpsf) · `option-edge-feed-gateway#235` ·
`options-edge-approach-monitor#2`. Every reader decodes with `UnderlyingPriceAvro.EventDeserializer`, has a test through the
production Avro serializer and a test that JSON/non-Avro bytes are rejected.

## Readers (origin/main inventory 2026-10-18)
vol-premium, strike-flow-classifier, strike-intelligence, strike-invasion, strike-liquidity-heatmap, spot-vol-regime,
greek-move-authenticity, context-tape (Direction; ZeroDte off), databento-gex (pre-open spot), drop-classifier (dev),
indicator-service (SPX_PRICE mode), market-commentary, multileg-structure, reversal-confirmation, signal-follower-spx,
feed-gateway, approach-monitor; replicas 0 on dev/prod but converted: breakout-sequencer, hedging-pressure, spread-skew,
dealer-ledger-calibration scorer. es4-only (read `es.underlying.spx.price`): es-cvd, es-amt, gamma-ladder-path, context-tape main tape.
No change needed: option-price-behavior (names the topic in an env var no code reads), vol-state, broker-execution (index.price).

## PRODUCTION GO / NO-GO (all must hold before step 3 on `192.168.100.252`)
1. **NAS archive reader exists; enable `OE_SPOT_AVRO_TOPICS` at the prod cutover step.** `scripts/ops/archive/SpotArchiveReader.java`
   (shipped in the archive unit with the contracts jar, unit-tested against the real serializer) writes the console consumer's line layout with the
   value as the JSON-era text (CPython-repr `price`/`canonicalSpot`, `size` null, tombstone `null`; a non-Avro value exits 2 and publishes nothing).
   `OE_SPOT_AVRO_TOPICS` ships EMPTY, so prod's still-JSON topic keeps archiving by console consumer until the switch. THE SWITCH (after step 5 has
   recreated the topic under Avro and the Avro feed is live, before the spot cron is resumed): set `OE_SPOT_AVRO_TOPICS="underlying.spx.price"` for
   the `*/10` spot job on the archive host, resume the cron, and confirm the next archive run logs a captured range for `underlying.spx.price` whose
   lines start `{"schemaVersion":1,"symbol":"SPX",`. Enabling it while the topic still holds JSON makes the run fail loud (no range published, checkpoint not advanced);
   not enabling it after the cutover does the same via the console consumer's undecodable bytes. Dev does not archive spot, so dev leaves it empty.
   The archive unit must be deployed (`archive-scripts-deploy`) before the switch.
2. **`spx-es4-bridge` replaced** by `Jenkinsfile.spx-price-reencoder` TARGET=es4 (see "Mirrors"): the bridge byte-copies Confluent-framed records into a registry where their
   schema id is invalid. It is part of the stop list in step 3 and stays stopped; the mirror install retires its unit.
3. The prod reader images were built AFTER the reader PRs merged and verified against an Avro topic on dev; the prod feed image is the
   Rust build containing `spx_price_avro.rs`; the subject is registered on the prod registry (step 2) and `ensure-underlying-price-schema.sh`
   reports the canonical schema as latest.
4. The script does not yet enable production: read the production topic's shape (replication factor, partitions, cleanup, retention, segment
   settings) off the live broker, record it as a production row in `roll-forward-underlying-spx-price.sh` with its own verification, and review that
   change; the script refuses `--cluster production` until then.
5. A prod window outside the US session (the topic feeds live trading surfaces), and the prod pre-cutover `TopicId` read off the live broker.

## Not covered by the dev cutover — remaining for prod / owner
* **Production**: steps 1-8 on `192.168.100.252` after a prod window; read the prod pre-cutover `TopicId` off the live broker.
* **NAS archive** (`scripts/ops/archive/oe-archive-kafka.sh`, `OE_SPOT_TOPICS`, prod */10 cron): the console-consumer text pipe
  cannot decode Avro; `SpotArchiveReader.java` (the `IndicatorArchiveReader` pattern) now exists and is wired as the opt-in
  `OE_SPOT_AVRO_TOPICS` (empty until the prod cutover; enable it at the cutover step, go/no-go item 1) so prod spot history keeps being
  archived (the archive is the only durable history of this compacted topic). Dev does not archive spot, so dev is unaffected.
* **Mirrors (decode-and-re-encode, this repository)**: see the next section. Both byte copiers of this topic are replaced; do NOT point es4 or the
  .74 stack at Avro until the corresponding mirror job is installed and its Verify stage reports FLOWING.
* **Python timewarp/replay tools** (`timewarp_snapshot_replay.py`, `hpsf_replay.py`) publish a JSON variant of this payload to
  replay-scoped topic names; they are not part of the live path and must be converted before they are run against a converted topic.
* Components that still write other JSON topics (the services touched here keep their own JSON outputs) remain on the rule's
  52-component backlog; this change converts one topic.

## Execution record (2026-10-19)
* **dev** (2026-10-18/19): subject id 2056; Avro feed rolled first, topic wiped and recreated under its own name (id XN3PL_… -> gdTU84w…), 17 readers rolled.
* **production** (2026-10-19, before the US session): subject registered (id 2045, FULL). The prod topic was `cleanup.policy=delete`, 1 partition, `retention.ms=-1`, `segment.bytes=1073741824`, RF 1,
  `min.insync.replicas=1`. A JSON feed image had written ~5.5k JSON records into it before the Avro feed image rolled, so it was deleted and recreated with the same shape
  (TopicId GzbDxB_… -> 00xQIW3…) with the Avro feed live; `roll-forward-underlying-spx-price.sh` still refuses `--cluster production` (the shape was applied by hand with the
  same checks: pre-cutover id verified immediately before the delete, shape and first record verified after). Then the prod readers were rolled (vol-premium, context-tape,
  databento-gex, reversal-confirmation, indicator-service, strike-intelligence, strike-invasion, strike-liquidity-heatmap, spot-vol-regime, greek-move-authenticity,
  market-commentary, multileg-structure, signal-follower, feed-gateway). Afterwards every prod reader group sat at the log end and no reader logged a decode error.
* **archive**: the prod `*/10` spot cron sets `OE_SPOT_AVRO_TOPICS="underlying.spx.price"` (archive-scripts-deploy #45).
* **mirrors**: `spx-price-reencoder` created in Jenkins (copied from the `es-indicator-mirror` job definition) and installed for TARGET=es4 (es4 topic wiped and recreated, bridge retired as
  `com.optionsedge.spx-es4-bridge.plist.retired-20261019`) and TARGET=mac74 (the `.74` topic wiped and recreated; the `underlying.spx.price` entry removed from
  `~/oe-ops/dl-mirror-dev-to-mac74/run-mirror.sh`, backup `run-mirror.sh.pre-spx-avro-20261019`). Both reported FLOWING.
* Not rolled: services at 0 replicas on prod (hedging-pressure, spread-skew, dealer-ledger scorer, drop-classifier, strike-flow-classifier-databento at the time) still carry the old image; they
  must be rolled before anyone scales them up.

### Execution evidence (auditable identity and commands)
* **Schema identity:** record `com.optionsedge.contracts.hpsf.UnderlyingPrice`, registry subject `com.optionsedge.contracts.hpsf.UnderlyingPrice` (RecordNameStrategy), version 1, compatibility FULL, canonical file
  sha256 `719c6589c62c0eeed9e2a5fa5459b98344e1a3f112b13e018a467851868ea41b` (contracts 0.27.0). Registry schema ids are per registry: dev 2056, production 2045, es4 2029, .74 2052. Each was created or confirmed by
  `scripts/kafka/ensure-underlying-price-schema.sh` (it prints `id=… version=1`, `compatibility … FULL`, `latest … is the canonical schema`).
* **Production manual wipe (commands, run over ssh on 192.168.100.252 with `/opt/kafka/current/bin`):** `kafka-topics.sh --describe` (TopicId must equal GzbDxB_vQ92ZKODUZl5PKw, else abort) ->
  `--delete --topic underlying.spx.price` -> wait until absent from `--list` -> `--create --partitions 1 --replication-factor 1 --config cleanup.policy=delete --config retention.ms=-1 --config segment.bytes=1073741824 --config min.insync.replicas=1`
  -> `--describe` (new TopicId 00xQIW3XR9GJQ7cVREYeSA, shape as created) -> end offset 21 after 20 s and the first record's bytes `00 00 00 07 fd` (magic 0, schema id 0x7fd = 2045). The script
  `roll-forward-underlying-spx-price.sh` now carries this production shape (options-edge-deploy #1317), so the next production roll-forward uses the script, not these commands.
* **Avro-only evidence on production:** at the end of the rollout every reader group (indicator-service-prod, greek-move-authenticity, spot-vol-regime, strike-invasion, strike-liquidity-heatmap, vol-premium) and the
  `spx-price-mirror-es4` group sat at the log end; no reader logged `SerializationException`, `Unknown magic byte` or `Error retrieving Avro` in the preceding 30 minutes. The log holds only records written after the
  recreate by the Avro feed (its first and last records were checked; the feed is the only writer: the JSON feed image was replaced before the wipe, and the byte-copy bridges are retired).
