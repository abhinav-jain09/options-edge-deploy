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

* The production render of `databento-vix-feed` is DELIBERATELY unchanged: `KAFKA_SCHEMA_REGISTRY_URL` is set only by the dev overlay. Without it the role exits at start, so a production start before
  its own cutover fails closed instead of reaching for production's registry.
* The IBKR VIX source never writes this topic: the shared config still names `underlying.vix.price` for the CONSUMERS of `KAFKA_IBKR_VIX_PRICE_TOPIC` (the gateway), but the
  ibkr-feed producer is pointed at the hop topic `underlying.vix.index.price` on dev AND (explicit override, at zero replicas) on production, does not exist on es4, and `FeedConfig` refuses the canonical name at boot.
  `tests/test_vix_underlying_price_manifest.py` pins that.

## Order (one environment)
Workloads change only through Jenkins on es4 and prod; on dev a hand `kubectl set image` of a digest-pinned image is accepted. Readers cannot be stopped first, so: Avro writer first, wipe, Avro readers.
1. Merge contracts (#166), feed (#193), processing, gateway, short-premium-agent and this change. A PRODUCTION image of the feed built from main now writes Avro: build none before production's cutover.
2. **Register** on the environment's registry before any producer (fixed id, FULL, read back):
   `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas/hpsf SCHEMA_ID_MAP=scripts/kafka/schema-id-maps/vix-underlying-price.map scripts/kafka/ensure-fixed-id-schemas.sh`
   (dev `http://192.168.100.102:8082`, or the Tailscale address `100.117.21.102` when the LAN is unreachable; es4 `http://192.168.100.4:8081`; **never** production's registry from this change).
3. **Take every non-Kubernetes JSON writer and byte copier away and prove it** (this is what `--json-writers-stopped` attests):
   * the prod -> dev `com.optionsedge.vixbridge` launchd bridge (`scripts/vix-bridge`, deleted by this change): `launchctl bootout gui/$(id -u)/com.optionsedge.vixbridge 2>/dev/null; mv ~/Library/LaunchAgents/com.optionsedge.vixbridge.plist{,.disabled-<date>}`;
     verify `launchctl list | grep -c vixbridge` = 0 and `pgrep -fl 'vix_bridge.py|run-vix-bridge.sh'` prints nothing;
   * the dev -> mac74 MirrorMaker 1 whitelist: `underlying.vix.price` removed from `~/oe-ops/dl-mirror-dev-to-mac74/run-mirror.sh` (the IBKR hop topic `underlying.vix.index.price` stays); verify `launchctl list | grep -c dl-mirror` = 0
     and `sed -n 22p ~/oe-ops/dl-mirror-dev-to-mac74/run-mirror.sh | tr '|' '\n' | grep -c 'underlying\\.vix\\.price$'` = 0;
   * the es4 end offsets of `es.underlying.vix.price` do not move over a minute (its only writer is the MM2 copy from production, which is down). The es4 -> mac74 MM1 consumer group `oe-mac74-input-from-es4` (on 192.168.100.74) READS the es4 topic and keeps working across the wipe; it is not a writer to either topic here.
4. **Roll the Avro writer** (dev: the digest-pinned feed image with the Avro writer, `databento-vix-feed`) TOGETHER WITH its `KAFKA_SCHEMA_REGISTRY_URL` (the dev overlay value; a `service-deploy SERVICE=databento-vix-feed` applies both, and a hand rollout must patch image and env in the one change, e.g. `kubectl patch` as the Jenkins deployer account; the role exits at boot without the env). Verify the pod env and digest before moving on; prove ONE pod on the new digest and, because the IBKR hop topic may be silent off-hours, that it started (subject found) and has 0 restarts.
5. Read the TopicId off the broker (`kafka-topics --describe`). Consumer groups that are INACTIVE and hold offsets on the topic are cleared with `kafka-consumer-groups --bootstrap-server <bootstrap> --delete-offsets --execute --group <g> --topic <t>`; a group with a live member (the running old readers, the es4 -> mac74 MM1) cannot be cleared and is left: its offset becomes beyond-the-log-end after the recreate, the harness (live mode) only WARNs, and the rolled reader resets on its first poll (`auto.offset.reset`).
6. **Wipe and recreate** with the harness:
   `scripts/kafka/roll-forward-topics.sh --recreate --cluster dev|es4 --topics <t> --pre-cutover-id <t>=<TopicId> --registry <url> --subject <t>=com.optionsedge.contracts.hpsf.VixUnderlyingPrice --avro-writer-live --json-writers-stopped <bootstrap> <kafka-bin-dir>`
   (the writer is idle off-hours, so the topic is empty after the recreate: the post-check passes on an empty topic).
   **es4 rationale:** there the topic has NO live writer at all (its only writer is the MM2 copy from the shut-down production). `--avro-writer-live` is used only because strict `--writers-and-readers-stopped` is impossible
   (the MM1 consumer group on 192.168.100.74 is a live reader holding offsets); it is the operator attestation the harness defines, backed here by the unchanged end offsets and the empty post-check. The first Avro record
   on es4 will be a production-written one carrying id 3061.
7. **Roll the Avro readers**, then verify the wire: first byte `00` + id `3061` on the newest record (when the feed ticks), restarts 0, no `UNDECODABLE`/`DESERIALIZATION` lines.

The manifest entry already lists `prod` (one fixed id everywhere is what makes the MM2 copy sound). That does NOT register anything on production by itself: a production write by `schema-sync.sh` /
`schema-sync-all.sh` requires `PLAN_SHA256` from a dry run of the same commit (R13a), i.e. an explicit, authorized production cutover; until then a production deploy of spot-vol-regime also refuses an image whose label names a
subject production's registry lacks. Nothing in this change runs against production.

The production RENDER of `databento-vix-feed` still says `replicas: 1` (unchanged by this change; live production holds it at 0 through `morning-autostart` KEEP_DOWN, checked 2026-11-06: 0/0). No production apply is part of this change, and a production start of the feed without the registry env exits at boot.

## Production (NOT done by this change: prod stays shut down, its registry stopped)
Pending steps, in order, before production's `databento-vix-feed`, spot-vol-regime, reversal-confirmation, vix-option-inteligence, drop-classifier or the gateway are started:
0. In the SAME change that starts the production feed, add `KAFKA_SCHEMA_REGISTRY_URL=http://192.168.100.252:8082` to `k8s/services/databento-vix-feed/base/databento-vix-feed-deployment.yaml` (and regenerate the slices).
1. Start production's registry, then register the subject with the same map (`SCHEMA_REGISTRY_URL=http://192.168.100.252:8082 ... ensure-fixed-id-schemas.sh`); id 3061 must be free there (`schema-sync.sh SERVICE=@all ENVIRONMENT=production` plans it from the manifest entry, which already lists `prod`).
2. Convert the leftovers that still read the JSON topic on production: `~/oe-ops/market-sentinel.py` (topic format `avro`), `oe-ops/freshness-topics.conf` is timestamp-only (no change).
3. Then the same register -> feed (production image built only now) -> wipe (`--cluster production`, `192.168.100.252:9092`) -> readers sequence. MM2 resumes copying Avro bytes whose id 3061 resolves in the es4 registry; es4's retained `es.underlying.vix.price` was wiped and recreated empty on 2026-11-06.
