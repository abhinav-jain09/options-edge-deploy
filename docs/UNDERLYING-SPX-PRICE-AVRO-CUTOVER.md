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
1. **Merge** contracts, then the reader PRs (processing A-D, feed-gateway, approach-monitor), then the feed and this deploy change.
   WARNING: from the first merge on, a PROD image built from main for any converted service reads Avro against the still-JSON prod
   topic. Do not build prod images of the feed or any reader until the prod cutover below.
2. **Register** the subject on the environment's registry (before any producer):
   `SCHEMA_REGISTRY_URL=<registry> SCHEMA_FILE=<contracts>/schemas/hpsf/UnderlyingPrice.avsc scripts/kafka/ensure-underlying-price-schema.sh`
   (sha256-pinned bytes, FULL by read-back, the canonical schema must be the latest version).
3. **Stop** the old (JSON) feed and EVERY reader of the topic (the list is in "Readers" below). Production adds two readers that are not
   Kubernetes workloads: the **NAS archive** `*/10` spot cron (`ARCHIVE_JOB=spot`: suspend the crontab line, then confirm no `oe-archive-kafka.sh`
   process holds the spot lock, so the post-recreate group check cannot see its offsets and its checkpoint is not advanced over the wipe) and
   the **`spx-es4-bridge`** console-consumer|producer (stop it and keep it stopped until the decode-and-re-encode mirror replaces it). The
   operator asserts all of this; Kafka cannot prove a writer is absent. Do not leave a pre-cutover feed running: it writes JSON into the fresh topic.
4. **Wipe and recreate empty**. Freeze topic administration on that broker for the window (Kafka's delete takes a NAME, not a
   TopicId; the script re-checks the id immediately before the delete but cannot exclude another administrator). With the SAME
   `<bootstrap>` for both commands (dev: `host.docker.internal:19092`; production: `192.168.100.252:9092`):
   `kafka-topics --bootstrap-server <bootstrap> --describe --topic underlying.spx.price` -> note the `TopicId`, then
   `scripts/kafka/roll-forward-underlying-spx-price.sh --recreate --cluster dev|production --pre-cutover-id <TopicId> --writers-and-readers-stopped <bootstrap> [bin-dir]`.
   The script refuses a bootstrap that is not the named cluster's broker, refuses a topic whose id is not the pre-cutover one,
   verifies the shape from the broker, and FAILS if any consumer group still holds a committed offset for the topic (committed
   offsets are keyed by topic name; a surviving one would resume beyond the new log end and skip the first Avro records).
   If only the consumer-group check fails (the topic was already recreated, so a second `--recreate` correctly refuses the new id):
   DELETE those stopped groups' offsets (`kafka-consumer-groups --bootstrap-server <bootstrap> --delete-offsets --group <g> --topic underlying.spx.price`, or `--delete --group <g>`; a reset to 0 still leaves a committed offset and is refused by the verifier), then run the SAME
   command with `--verify` instead of `--recreate` (post-conditions only, no mutation) and continue at step 5.
   Persisted reader state: a Kafka Streams GLOBAL store checkpoints the end offset of its source topic on its volume. The only
   PVC-backed global store on this topic is `strike-flow-classifier-databento`; its spot store is in-memory in the converted
   code, so no checkpoint can outlive the recreation. (strike-intelligence's global store sits on an emptyDir that is lost with
   the pod.) Any other reader added later that keeps source offsets in persistent state must be reset the same way.
5. **Deploy the Avro readers** (each `service-deploy` with `BUILD_IMAGES=true`, PERMITTED_SHA per rule) — they start on an empty topic.
6. **Enable the Avro feed** (`options-edge-databento-feed-deploy` then `service-deploy SERVICE=databento-feed`). Verify its first
   records decode, with the environment's registry (dev `http://host.docker.internal:8082`; production `http://192.168.100.252:8082`):
   `kafka-avro-console-consumer --bootstrap-server <bootstrap> --topic underlying.spx.price --partition 0 --offset earliest --max-messages 3 --property schema.registry.url=<registry> --property print.key=true`
   (the record prints as JSON; `price`/`canonicalSpot` show as the decimal bytes). Then prove the wire framing: 
   `kafka-console-consumer --bootstrap-server <bootstrap> --topic underlying.spx.price --partition 0 --offset earliest --max-messages 1 | head -c 5 | xxd -p`
   prints `00` followed by the 4-byte big-endian schema id (e.g. `000000004d` is id 77); that id must resolve to the canonical subject:
   `curl -s <registry>/schemas/ids/<decimal id>/versions` returns entries whose `subject` field equals `com.optionsedge.contracts.hpsf.UnderlyingPrice`. Every reader's log then
   shows a tick.
7. Verify each reader: its committed offset now lies in the NEW topic's offset range (`kafka-consumer-groups --describe --group <g>`), its
   log shows a tick decoded from a known, freshly produced record (compare the offset/eventTime with the console-consumer view),
   there are no deserialization errors, and the browser `spx-price` events are unchanged.
8. Environment-specific follow-ups: NAS archive reader (prod), es4 bridge (below).

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
1. **NAS archive converted AND deployed** (`oe-archive-kafka.sh`, `OE_SPOT_TOPICS`, prod */10 cron): an Avro reader writing the JSON-era line
   (the `IndicatorArchiveReader` pattern) is live; otherwise prod spot history stops being archived at step 6 (the archive is the only durable
   history of this compacted topic). NO-GO until proven on a staged range.
2. **`spx-es4-bridge` stopped or replaced** (see below): it byte-copies Confluent-framed records into a registry where their schema id is
   invalid. It is part of the stop list in step 3 and stays stopped until the decode-and-re-encode mirror is installed.
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
  cannot decode Avro; it needs an Avro reader writing the JSON-era line (the `IndicatorArchiveReader` pattern) BEFORE the prod
  topic converts, or prod spot history stops being archived (the archive is the only durable history of this compacted topic).
  Dev does not archive spot, so dev is unaffected.
* **es4 byte-copy bridge** `spx-es4-bridge` (console-consumer | console-producer from prod `underlying.spx.price` to es4
  `es.underlying.spx.price`, defined outside git): schema ids are per-registry, so a byte copy of Avro is undecodable on es4. It
  needs a decode-and-re-encode mirror (the `es-indicator-mirror` pattern). es4 readers (es-cvd, es-amt, gamma-ladder-path,
  context-tape) already select Avro by the resolved topic name; do NOT point es4 at Avro until the bridge is converted.
* **Python timewarp/replay tools** (`timewarp_snapshot_replay.py`, `hpsf_replay.py`) publish a JSON variant of this payload to
  replay-scoped topic names; they are not part of the live path and must be converted before they are run against a converted topic.
* Components that still write other JSON topics (the services touched here keep their own JSON outputs) remain on the rule's
  52-component backlog; this change converts one topic.
