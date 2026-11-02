# ES price topics JSON -> Avro: cutover runbook

Kafka Serialization Rule (options-edge rule.md): JSON on Kafka is prohibited. Owner, 2026-10-18: no versioned topic names; a topic that
changes serialization is **wiped and recreated under its own name** and every reader is Avro only. Sibling runbooks:
`docs/UNDERLYING-SPX-PRICE-AVRO-CUTOVER.md` (the proven sequence this follows) and `docs/KAFKA-AVRO-TOPIC-ROLL-FORWARD.md` (the harness).

## The topics
* `es.underlying.es.price` on es4 (compacted, key `ES`, written by the es4 `es-feed`); `underlying.es.price` on production (delete cleanup,
  written by the production `databento-feed`'s Kafka-source path). Same Rust writer, same record; the topic name is the only difference.

## The contract
* schema `options-edge-contracts schemas/hpsf/EsUnderlyingPrice.avsc` (contracts 0.59.0), record
  `com.optionsedge.contracts.hpsf.EsUnderlyingPrice`: the twelve `UnderlyingPrice` fields (same types; `price`/`canonicalSpot` are
  `decimal(38,18)` of the feed's shortest round-trip text) plus seven optional `spxBasis*` fields (the ES->SPX basis mapping, default null).
* subject = the record's full name (RecordNameStrategy; one subject for every environment), compatibility `FULL`.
* writer: the Rust feed only (`rust/src/es_price_avro.rs`, `EsPriceAvro`). It reads the LATEST registered version of the subject at start
  (only when `ES_PRICE_ENABLED=true`) and refuses to start on any other shape. It never registers.
* readers:
  * price-only Streams readers (`greek-move-authenticity`, `strike-intelligence`, `gamma-ladder-path`) decode with
    `UnderlyingPriceAvro.serde(registryUrl)`, which accepts the ES record by its RECORD name (never by topic name) and ignores the basis fields;
  * the es4 `es-feed` follows production's enriched `underlying.es.price` for the basis (`ES_BASIS_REFERENCE_*`); its Avro decoder
    resolves the writer schema by id at `ES_BASIS_REFERENCE_SCHEMA_REGISTRY_URL` (production's registry; ids are per registry).
  JSON, non-Avro bytes and foreign records fail loudly. There is no fallback anywhere.

## Order (es4)
Workloads change only through Jenkins (admission policy), so readers cannot be stopped first: Avro writer first, wipe, Avro readers.
1. Merge contracts, feed, processing (readers' tests) and this change. From the first merge on, a PRODUCTION image of the feed built from
   main writes Avro to `underlying.es.price`, and `es-spx-align` / the web VWAP reader / the archive still read JSON there: build no
   production image of the feed until the production cutover (see "Production" below).
2. **Register** on es4's registry before any producer:
   `SCHEMA_REGISTRY_URL=http://192.168.100.4:8081 SCHEMA_FILE=<contracts>/schemas/hpsf/EsUnderlyingPrice.avsc scripts/kafka/ensure-es-underlying-price-schema.sh`
   (sha256-pinned bytes, FULL by read-back, the canonical schema must be the latest version). After ANY es4 `clean-reset` (which wipes the
   registry) run it again BEFORE `activate-es-feed`, or the feed fails closed at start.
3. **Take the JSON writer away without starting an Avro one.** `es-feed` deploys at replicas 0 by design, so
   `es4-deploy ACTION=deploy-service SERVICE=es-feed` (the new, digest-pinned image) removes the JSON pod and starts nothing. Prove it:
   zero `es-feed` pods. From here until step 5 NO process writes `es.underlying.es.price`, so Avro and retained JSON can never meet in
   the topic: the order is writer absent -> wipe -> Avro writer, not Avro writer -> wipe.
4. **Wipe and recreate** with the harness (same `<bootstrap>` for describe and recreate; freeze topic administration for the window):
   `kafka-topics --bootstrap-server 192.168.100.4:9092 --describe --topic es.underlying.es.price` -> note the TopicId, then
   `scripts/kafka/roll-forward-topics.sh --recreate --cluster es4 --topics es.underlying.es.price --pre-cutover-id es.underlying.es.price=<TopicId> --registry http://192.168.100.4:8081 --subject es.underlying.es.price=com.optionsedge.contracts.hpsf.EsUnderlyingPrice --avro-writer-live --json-writers-stopped 192.168.100.4:9092 <kafka-bin-dir>`
   (`--json-writers-stopped` is the attestation that no JSON writer exists; the Avro writer is not up yet, so the post-check finds an empty topic and
   passes). Strict mode (`--writers-and-readers-stopped`) is impossible: the readers cannot be stopped (Jenkins-only), and their committed
   offsets survive the delete (the harness reports any beyond the new log end; a regular consumer resets on its first poll).
5. **Start the Avro writer:** `es4-deploy ACTION=activate-es-feed` (`ES_FEED_LOCATION=es4`, with its acknowledgements). The topic holds
   only Avro from its first record. Check the pod is the new image digest.
6. **Roll the readers** (`greek-move-authenticity`, `strike-intelligence`, `gamma-ladder-path`: `es4-deploy ACTION=deploy-service`). They
   start on a topic that holds only Avro (the old gamma-ladder-path image, which still parses JSON text, reads Avro bytes as junk until it rolls). A Streams global store checkpoint that names the old end offset is discarded by the reader or
   its emptyDir is lost with the pod (strike-intelligence's spot store is an emptyDir).
7. **Verify the wire:** magic byte `0` and the registered id on the first and last record; `greek-move-authenticity` and
   `strike-intelligence` no longer log skipped/undecodable ES price records; restarts 0; the registry subject has exactly one version.

## The dev source and the basis-reference mirror
While production is down, the es4 `es-feed` takes its ES->SPX basis from the DEV feed's enriched `underlying.es.price` (dev
`ES_PRICE_ENABLED=true`, same Rust writer), carried onto es4 as `es.underlying.es.price.basis-reference` (es4 broker, es4 schema ids).
The 2026-10-27 text bridge that did this (`com.optionsedge.es-price-basisref-bridge-192-168-100-4-9092` on the dev Mac) cannot carry
Avro and is replaced by `Jenkinsfile.es-price-basisref-mirror` (decode with the dev registry, re-encode with es4's, same subject, durable
group `es-price-mirror-es4`; its install retires the old bridge). Avro end to end therefore also needs, in this order:
1. Register the subject on the DEV registry (`SCHEMA_REGISTRY_URL=http://192.168.100.102:8082 ... ensure-es-underlying-price-schema.sh`).
2. Roll the DEV feed to an image carrying the Avro writer (the dev cluster is arm64; the remote `.252` build host is amd64).
3. Wipe dev `underlying.es.price` (`roll-forward-topics.sh --cluster dev ... 192.168.100.102:19092`) once no JSON writer exists.
4. Wipe es4 `es.underlying.es.price.basis-reference` (`--cluster es4`), then install the mirror.
5. Dev readers of `underlying.es.price` (gamma-ladder-path; the web `IndicatorVwapService`) must read Avro before they roll.
The es4 `es-feed` must not be rolled before step 2-4 are done: its Avro-only reference reader would refuse the old text bridge's JSON and the
basis would be absent (es-amt excludes RTH prints without it).

## Production (not done by this change)
On 2026-11-02 production's workloads are all at 0 replicas (its Kafka broker is up, its schema registry is not), so no production JSON writer exists today.
Production's `underlying.es.price` keeps JSON writers and readers until its own cutover, which needs, before the feed rolls: the subject
registered on production's registry (`SCHEMA_REGISTRY_URL=http://192.168.100.252:8082`), `es-spx-align` (basis global store and the CVD
levels aligner), the web `IndicatorVwapService` basis reader, `trade-evidence-api` and the NAS spot archive (`oe-archive-kafka.sh`
`OE_SPOT_AVRO_TOPICS`) converted to Avro readers, `~/oe-ops/basis-health-watch.sh` (reads the topic) updated, and then the same
register -> feed -> wipe (`--cluster production`, `http://192.168.100.252:9092`) -> readers sequence. The es4 `es-feed` basis reference
reads production's topic: until production is cut over it skips every record as undecodable (fail-honest: the SPX superscript hides).
