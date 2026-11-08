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
* Governance bindings: only `spot-vol-regime` ships a `schema-requirements.json` that can name the subject (dev and production). reversal-confirmation and vix-option-inteligence are on the R6 backlog (`schema-manifest/r6-backlog.txt`, no label yet);
  drop-classifier (and its es-liquidity-engine copy) read the topic only where `DROP_VIX_BOOTSTRAP` is set, which PRODUCTION does (`DROP_VIX_BOOTSTRAP=192.168.100.252:9092` on `drop-classifier-service`, read from the live deployment 2026-11-08), so production's drop-classifier is a real reader of this topic even though by the module's own PARTIAL rule the subject is not declared in its label; the gateway, agent, trade-evidence-api and hpsf are unlabelled readers.
* Readers (all Avro only, no JSON fallback): spot-vol-regime, reversal-confirmation, vix-option-inteligence (live V1 runtime; the dark Stage-B parser decodes the frame as a pure function and requires id 3061),
  drop-classifier (and its copy inside es-liquidity-engine), hpsf-processing Stage B, the feed-gateway (browsers still receive the identical JSON), the trade-evidence-api collector, short-premium-agent.

* The production render of `databento-vix-feed` **now carries** `KAFKA_SCHEMA_REGISTRY_URL=http://192.168.100.252:8082` (production's cutover step 0, done 2026-11-08; it comes from the role's base and
  the dev overlay overrides it with the dev registry). It used to be set only by the dev overlay, and the role's exit-at-start was what made a production start fail closed before the cutover. **That guard is
  gone**: what now keeps a production start honest is the registry itself — the role reads the subject at start and exits when it is ABSENT, so until id 3061 is registered on production's registry the feed
  still refuses to run, and the prod Deployment also remains digest-pinned to the pre-Avro image until a `service-deploy` run moves it.
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
     the retired bridge also shipped a boot-level LaunchDaemon variant (`com.optionsedge.vixbridge.daemon.plist`): `sudo launchctl bootout system/com.optionsedge.vixbridge` and move `/Library/LaunchDaemons/com.optionsedge.vixbridge.plist` aside if it exists (checked 2026-11-06 on the dev Mac: absent, `launchctl print system/com.optionsedge.vixbridge` not found);
     verify `launchctl list | grep -c vixbridge` = 0 and `pgrep -fl 'vix_bridge.py|run-vix-bridge.sh'` prints nothing;
   * the dev -> mac74 MirrorMaker 1 whitelist: `underlying.vix.price` removed from `~/oe-ops/dl-mirror-dev-to-mac74/run-mirror.sh` (the IBKR hop topic `underlying.vix.index.price` stays); verify `launchctl list | grep -c dl-mirror` = 0
     and `sed -n 22p ~/oe-ops/dl-mirror-dev-to-mac74/run-mirror.sh | tr '|' '\n' | grep -c 'underlying\\.vix\\.price$'` = 0;
   * the es4 end offsets of `es.underlying.vix.price` do not move over a minute. Its only writer is the MM2 copy from production — and **do not assume that copy is down**: the 2026-11-06 run attested it was, and it was not. MM2 refilled the freshly recreated es4 topic with production's JSON the same afternoon (measured 2026-11-08: 4681 records on partition 0, `2026-11-06T14:30:13Z` .. `2026-11-06T21:00:28Z`, production's own payloads), so the es4 wipe did not hold and es4 must be wiped again as part of production's cutover. Stop or pause the MM2 unit for the `underlying.vix.price` path and re-measure the end offsets before attesting this. The es4 -> mac74 MM1 consumer group `oe-mac74-input-from-es4` (on 192.168.100.74) READS the es4 topic and keeps working across the wipe; it is not a writer to either topic here.
4. **Roll the Avro writer** (dev: the digest-pinned feed image with the Avro writer, `databento-vix-feed`) TOGETHER WITH its `KAFKA_SCHEMA_REGISTRY_URL` (the dev overlay value; a `service-deploy SERVICE=databento-vix-feed` applies both, and a hand rollout must patch image and env in the one change, e.g. `kubectl patch` as the Jenkins deployer account; the role exits at boot without the env). Verify the pod env and digest before moving on; prove ONE pod on the new digest and, because the IBKR hop topic may be silent off-hours, that it started (subject found) and has 0 restarts.
5. Read the TopicId off the broker (`kafka-topics --describe`). Consumer groups that are INACTIVE and hold offsets on the topic are cleared with `kafka-consumer-groups --bootstrap-server <bootstrap> --delete-offsets --execute --group <g> --topic <t>`; a group with a live member (the running old readers, the es4 -> mac74 MM1) cannot be cleared and is left: its offset becomes beyond-the-log-end after the recreate, the harness (live mode) only WARNs, and the rolled reader resets on its first poll (`auto.offset.reset`).
6. **Wipe and recreate** with the harness:
   `scripts/kafka/roll-forward-topics.sh --recreate --cluster dev|es4 --topics <t> --pre-cutover-id <t>=<TopicId> --registry <url> --subject <t>=com.optionsedge.contracts.hpsf.VixUnderlyingPrice --avro-writer-live --json-writers-stopped <bootstrap> <kafka-bin-dir>`
   (the writer is idle off-hours, so the topic is empty after the recreate: the post-check passes on an empty topic).
   **es4 rationale, and why the 2026-11-06 es4 run did not hold:** the only writer of the es4 topic is the MM2 copy from production, so the run treated
   it as having no live writer. That was WRONG — MM2 was copying, production was writing JSON on trading days, and the recreated es4 topic was full of
   production's JSON within hours. `--avro-writer-live` was used because strict `--writers-and-readers-stopped` is impossible there (the MM1 consumer
   group on 192.168.100.74 is a live reader holding offsets); it is the operator attestation the harness defines, and on es4 that attestation is only
   true once the MM2 path for this topic is actually stopped and the end offsets are re-measured as unchanged. The first Avro record on es4 will be a
   production-written one carrying id 3061, so the es4 wipe belongs AFTER production writes Avro, not before.
7. **Roll the Avro readers**, then verify the wire: first byte `00` + id `3061` on the newest record (when the feed ticks), restarts 0, no `UNDECODABLE`/`DESERIALIZATION` lines.

The manifest entry already lists `prod` (one fixed id everywhere is what makes the MM2 copy sound). That does NOT register anything on production by itself: a production write by `schema-sync.sh` /
`schema-sync-all.sh` requires `PLAN_SHA256` from a dry run of the same commit (R13a), i.e. an explicit, authorized production cutover; until then a production deploy of spot-vol-regime also refuses an image whose label names a
subject production's registry lacks. Nothing in this change runs against production.

The production RENDER of `databento-vix-feed` still says `replicas: 1` (unchanged by this change). It is **not** held down by `morning-autostart` any more — `scripts/ops/morning-autostart.sh`'s `KEEP_DOWN` no longer lists it (checked 2026-11-08), so autostart scales it to 1 on every trading morning and the JSON it writes is production's own. What keeps a production start honest is no longer a missing registry env (step 0 added it) and never was a KEEP_DOWN entry: it is that the role exits at start while id 3061 is ABSENT from the registry the env now names, and that the prod Deployment is still digest-pinned to the pre-Avro image. No production apply is part of this change.

## Production (NOT done by this change: production's registry is stopped and these steps are outstanding)
Pending steps, in order, before production's `databento-vix-feed`, spot-vol-regime, reversal-confirmation, vix-option-inteligence, drop-classifier or the gateway are started:
0. ~~Add `KAFKA_SCHEMA_REGISTRY_URL=http://192.168.100.252:8082` to the feed's base deployment and regenerate the slices.~~ **DONE** (2026-11-08): the base `k8s/services/databento-vix-feed/base/databento-vix-feed-deployment.yaml` now carries it and the production slice `overlays/production/manifest.yaml` was regenerated with it; the dev overlay keeps overriding it with the dev registry by the same env name, so the dev slice is unchanged. `tests/test_vix_underlying_price_manifest.py` now pins that EVERY render names its own registry instead of pinning its absence. This is inert until a `service-deploy SERVICE=databento-vix-feed ENVIRONMENT=production` run applies it: the prod Deployment is still digest-pinned to the pre-Avro image.
1. **Start production's registry — this needs root on `192.168.100.252` and is the step a Claude session cannot do**: `systemctl start schema-registry.service` returns `Interactive authentication required` and `sudo -n` requires a password. Measured 2026-11-08: the unit is `inactive` but `UnitFileState=enabled`, and the journal shows it ran until `2026-11-07 21:43:32` and was stopped cleanly (SIGTERM, `ExecMainStatus=143`, `Result=success`) — so it runs on trading days and is stopped out of hours, and no `oe-schema-registry-healthcheck.timer` is installed to bring it back. Then register the subject with the same map (`SCHEMA_REGISTRY_URL=http://192.168.100.252:8082 ... ensure-fixed-id-schemas.sh`); id 3061 must be free there (`schema-sync.sh SERVICE=@all ENVIRONMENT=production` plans it from the manifest entry, which already lists `prod`).
2. ~~Convert the leftovers that still read the JSON topic on production.~~ **DONE** (verified 2026-11-08): `~/oe-ops/market-sentinel.py` already lists `underlying.vix.price` with topic format `avro`, and `oe-ops/freshness-topics.conf` is timestamp-only (no change was ever needed). Nothing remains in this step; re-verify both files at the cutover rather than re-doing it.
3. Then the same register -> feed (production image built only now) -> wipe (`--cluster production`, `192.168.100.252:9092`) -> readers sequence. MM2 copies Avro bytes whose id 3061 resolves in the es4 registry.
   * **es4's `es.underlying.vix.price` is NOT empty and must be wiped again as part of that same sequence.** Measured 2026-11-08 on `192.168.100.4:9092`: 4681 records on partition 0, earliest `2026-11-06T14:30:13Z`, newest `2026-11-06T21:00:28Z` with the same payload and timestamp as production's newest record — i.e. MM2 was NOT down, and it refilled the freshly recreated es4 topic with production's JSON within hours of the 2026-11-06 wipe. Any statement in this runbook that the es4 topic "was wiped and recreated empty on 2026-11-06" is true only of the moment of the wipe. `scripts/ci/schema-mirror-evidence.py` samples the mirror SOURCE only, so this does not show up there; it was found by reading the es4 side by hand.

### State measured on 2026-11-08 (Sunday, market closed, production scaled to 0)
These are the facts that have to be re-measured at the real cutover, not assumed from this runbook:
* `schema-registry.service` on `192.168.100.252` is **inactive** (enabled, so it comes back on boot); `kafka.service` is active and the broker answers with 381 topics. Step 1 therefore starts with starting that unit.
* Every Deployment in the prod `options-edge` namespace is at 0 replicas with no pod running — but that is the OFF-HOURS state, not a continuous
  shutdown. Production is still brought up on trading days: its own `underlying.vix.price` carries JSON written on 2026-11-04, 11-05 and 11-06
  (retained offsets 305611..315636, earliest `2026-11-04T15:02:37Z`, newest `2026-11-06T21:00:28Z` — Friday's close). The JSON writer is that
  Deployment, and `morning-autostart.sh` scales it back to 1 on the next trading morning, so a wipe before the Avro production image is live and
  deployed is refilled with JSON within minutes of the next open. What wave-1 left shut down is the registry and the held services, not the fleet.
* `databento-vix-feed` is **no longer** in `morning-autostart.sh`'s `KEEP_DOWN` list, so autostart does start it on a trading morning.
* `drop-classifier-service` on production carries `DROP_VIX_BOOTSTRAP=192.168.100.252:9092`, so it is a live reader of this topic on production.
* `reversal-confirmation-service` is additionally held at 0 by `scripts/ops/prod-start-prereqs.sh` until `ReversalTopCandidate` is registered under id 2901 (`docs/KAFKA-AVRO-TOPIC-ROLL-FORWARD.md`, pending production step 1). One of the five readers of this topic cannot be started until that unrelated step is done.

Until the sequence above runs, `prod underlying.vix.price` is listed in `schema-manifest/mirror-evidence-pending.txt` so the nightly `options-edge-schema-code-verify` reports PENDING rather than FAIL. **Removing that line is part of this cutover**: the checker FAILS while the line is present and the newest production record is framed, which is how the declared id gets validated again the moment production starts writing Avro.
