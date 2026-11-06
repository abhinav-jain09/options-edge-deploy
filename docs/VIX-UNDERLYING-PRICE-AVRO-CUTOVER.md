# underlying.vix.price JSON -> Avro: cutover runbook

Kafka Serialization Rule (options-edge rule.md): JSON on Kafka is prohibited. Owner, 2026-10-18: no versioned topic names; a topic that
changes serialization is **wiped and recreated under its own name** and every reader is Avro only. Siblings: `docs/UNDERLYING-SPX-PRICE-AVRO-CUTOVER.md`,
`docs/ES-UNDERLYING-PRICE-AVRO-CUTOVER.md`, and the harness `docs/KAFKA-AVRO-TOPIC-ROLL-FORWARD.md`.

## The topic and the contract
* `underlying.vix.price` on dev (1 partition, delete cleanup) and production; `es.underlying.vix.price` on es4 (4 partitions) is its **byte copy** from production by
  MirrorMaker 2 (`infra/es4/mm2/mm2.properties`, prod -> es4). Writer: the `databento-vix-feed` role of `options-edge-databento-feed` (Rust, `rust/src/vix_price_avro.rs`);
  it publishes the OPRA monthly-parity estimate (`SYNTHETIC_OPTION_SPOT`) and, when the IBKR hop topic `underlying.vix.index.price` is fresh, the cash-index print (`IBKR_INDEX`).
* Schema `options-edge-contracts schemas/hpsf/VixUnderlyingPrice.avsc` (contracts 0.74.0), record `com.optionsedge.contracts.hpsf.VixUnderlyingPrice`: the JSON field for field.
  `price`, `atmStrike`, the four ATM quotes and the three spreads are `decimal(38,18)` (exact shortest round-trip text of the feed's double); `dispersion` and
  `parityDiscountErrorEstVolPts` stay doubles; the parity block and the eight ATM evidence fields are optional (null on an `IBKR_INDEX` print). Price policy: a tick whose price is
  non-finite, `-0.0`, needs more than 18 fractional digits, is out of range, or is not positive after the feed's own 2-decimal publication rounding is REFUSED by the writer (counted drop
  `vix_price_dropped_total{reason="avro_unencodable"|"invalid_price"}`), never rounded to fit.
* subject = the record full name (RecordNameStrategy), compatibility `FULL`, `auto.register.schemas=false`: the feed reads the LATEST registered version at start and refuses to start on any other shape.
* **One FIXED id, 3061, in every registry** (`schema-manifest`, family `hpsf`, `scripts/kafka/schema-id-maps/vix-underlying-price.map`). Unlike the SPX/ES price topics (each registry assigns its own id),
  this topic is byte-mirrored production -> es4, and a Confluent-framed value names its schema by registry id, so the id has to mean the same schema on both sides (schema governance R3f; the mirror header
  now declares the subject and the manifest entry has no `idByEnvironment`).
* Readers (all Avro only, no JSON fallback): spot-vol-regime, reversal-confirmation, vix-option-inteligence (live V1 runtime; the dark Stage-B parser decodes the frame as a pure function and requires id 3061),
  drop-classifier (and its copy inside es-liquidity-engine), hpsf-processing Stage B, the feed-gateway (browsers still receive the identical JSON), the trade-evidence-api collector, short-premium-agent.

## Order (one environment)
Workloads change only through Jenkins on es4 and prod; on dev a hand `kubectl set image` of a digest-pinned image is accepted. Readers cannot be stopped first, so: Avro writer first, wipe, Avro readers.
1. Merge contracts (#166), feed (#193), processing, gateway, short-premium-agent and this change. A PRODUCTION image of the feed built from main now writes Avro: build none before production's cutover.
2. **Register** on the environment's registry before any producer (fixed id, FULL, read back):
   `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas/hpsf SCHEMA_ID_MAP=scripts/kafka/schema-id-maps/vix-underlying-price.map scripts/kafka/ensure-fixed-id-schemas.sh`
   (dev `http://192.168.100.102:8082`, or the Tailscale address `100.117.21.102` when the LAN is unreachable; es4 `http://192.168.100.4:8081`; **never** production's registry from this change).
3. **Take every non-Kubernetes JSON writer and byte copier away and keep them away**: the prod -> dev `com.optionsedge.vixbridge` launchd bridge (`scripts/vix-bridge`, deleted here; the installed plist is renamed `.disabled-*`),
   and `underlying.vix.price` in the dev -> mac74 MirrorMaker 1 whitelist (`~/oe-ops/dl-mirror-dev-to-mac74/run-mirror.sh`). On es4 the only writer is the MM2 copy from production, which is down.
4. **Roll the Avro writer** (dev: the digest-pinned feed image with the Avro writer, `databento-vix-feed`); prove ONE pod on the new digest and, because the IBKR hop topic may be silent off-hours, that it started (subject found) and has 0 restarts.
5. Read the TopicId off the broker (`kafka-topics --describe`), delete the consumer groups that hold offsets on the topic (`kafka-consumer-groups --delete-offsets`; the rolled readers keep no committed offset).
6. **Wipe and recreate** with the harness:
   `scripts/kafka/roll-forward-topics.sh --recreate --cluster dev|es4 --topics <t> --pre-cutover-id <t>=<TopicId> --registry <url> --subject <t>=com.optionsedge.contracts.hpsf.VixUnderlyingPrice --avro-writer-live --json-writers-stopped <bootstrap> <kafka-bin-dir>`
   (the writer is idle off-hours, so the topic is empty after the recreate: the post-check passes on an empty topic).
7. **Roll the Avro readers**, then verify the wire: first byte `00` + id `3061` on the newest record (when the feed ticks), restarts 0, no `UNDECODABLE`/`DESERIALIZATION` lines.

## Production (NOT done by this change: prod stays shut down, its registry stopped)
Pending steps, in order, before production's `databento-vix-feed`, spot-vol-regime, reversal-confirmation, vix-option-inteligence, drop-classifier or the gateway are started:
1. Start production's registry, then register the subject with the same map (`SCHEMA_REGISTRY_URL=http://192.168.100.252:8082 ... ensure-fixed-id-schemas.sh`); id 3061 must be free there (`schema-sync.sh SERVICE=@all ENVIRONMENT=production` plans it from the manifest entry, which already lists `prod`).
2. Convert the leftovers that still read the JSON topic on production: `~/oe-ops/market-sentinel.py` (topic format `avro`), `oe-ops/freshness-topics.conf` is timestamp-only (no change).
3. Then the same register -> feed (production image built only now) -> wipe (`--cluster production`, `192.168.100.252:9092`) -> readers sequence. MM2 resumes copying Avro bytes whose id 3061 resolves in the es4 registry; es4's retained `es.underlying.vix.price` was wiped and recreated empty on 2026-11-06.
