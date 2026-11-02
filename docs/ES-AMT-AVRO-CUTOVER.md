# es.futures.auction: JSON to Avro on DEV (2026-11-02)

Kafka Serialization Rule (rule.md): record values are Avro through the Schema Registry, JSON is prohibited. Owner ruling 2026-10-18: a topic is rolled forward under its own
name (delete and recreate), no versioned topic names. Triggered by the VWAP acceptance/rejection work (`line-acceptance-service` reads Avro only and found this topic JSON).

**Scope: DEV ONLY.** es4 still runs the JSON writer, production still receives es4's JSON by byte mirror (`Jenkinsfile.es-auction-mirror`, target 192.168.100.252:9092,
untouched). Their cutover is the owner's call; the wipe script refuses `es4` and `production` by name.

## The contract

| | |
|---|---|
| Topic | `es.futures.auction`, 1 partition (EXACT), `cleanup.policy=delete`, `retention.ms=604800000` (7 calendar days), replication 1; key `<tradeDate>|<HH:mm>`, one record per Globex/RTH minute |
| Subject / record | `com.optionsedge.contracts.esamt.EsAuctionMinute` (RecordNameStrategy), fixed id **3201** (`00 00 0c 81`) on every registry, FULL |
| Contracts | `schemas-registry/esamt/EsAuctionMinute.avsc`, codec `EsAuctionAvro` (JSON payload <-> record, refuses a missing/extra/mistyped field; `toJson` reproduces the browser JSON) |
| Registration | `scripts/kafka/ensure-fixed-id-schemas.sh` with `schema-id-maps/es-amt.map`, `SCHEMAS_DIR=<contracts checkout>/schemas-registry/esamt` |
| Wipe | `scripts/kafka/roll-forward-es-amt-auction.sh` (mock-tested by `roll-forward-es-amt-auction-test.sh`; dev only) |

## Who writes, who reads

* **Writer (new on dev):** `es-amt-service`, dev replicas 1 (`k8s/services/es-amt`, services.yaml `envs: [dev]`). Until this cutover dev had **no producer**: its copy was an MM1 byte
  mirror of es4's topic (launchd `com.optionsedge.es-auction-mirror-127-0-0-1-19092-es-futures-auction`). The service reads dev's own ES tape `underlying.es.trades` (32 partitions) and the
  dev SPX price (`underlying.spx.price`, Avro), and publishes Confluent Avro. The engine is unchanged: it still answers with the payload JSON text and `AuctionWire` maps it onto the record
  (a payload the contract refuses is counted `es_amt_avro_refused` and dropped, it never stops the stream). At start it fails closed unless id 3201 is the packaged schema on the registry.
* **Readers (all Avro-only after this change, no JSON fallback):** feed-gateway-service (event `es-auction`), amt-order-bridge, early-auction-verdict-service,
  es-compression-expansion-service, level-absorption-service, market-commentary-service. `trade-evidence-api` (held at 0 on dev) and the offline tools (eav-backtest, market-commentary proof
  scripts) are NOT part of the dev roll.
* **Mirror:** the dev mirror is removed and `Jenkinsfile.es-auction-mirror` now allows `ACTION=install` only for the production target 192.168.100.252:9092 (a byte copy of es4's JSON would put JSON
  into the Avro topic, whichever way the dev broker is spelled). `teardown` and `status` work for every target. The production mirror is unchanged.

## Why merging/applying this early cannot start a mixed-wire topic (fail-closed in the service, not a hold flag)

`es-amt-service` is `replicas: 1` in the dev overlay, so a full dev deploy or a `dev-cleanup start` that happens BEFORE the cutover would start it. That is harmless by construction: at start it
(1) refuses unless id 3201 and the `EsAuctionMinute` subject on the registry carry the packaged schema (`ES_AMT_SCHEMA_ID_ABSENT` / `_MISMATCH`), and (2) scans the retained keys of `es.futures.auction`
and refuses with `ES_AMT_PUBLISHED_SCAN_LEGACY_VALUE` if ANY retained record is not Confluent Avro under 3201. Both checks run before the topic is touched, the producer is fenced or anything is written. So before the
wipe it crash-loops with those messages and writes nothing; after the wipe (topic empty or Avro only) it starts. Do not read a crash-looping `es-amt-service` before step 5 as a defect.

## What the dev record lacks, and why it is acceptable

Dev's ES tape starts at the dev Kafka clean of 2026-11-01 (first print Sun 17:00 ET), so dev has **no Friday session**. The engine's prior-session blocks (references from the prior RTH session, prior
VA/POC, humps of the prior session, the O2/O3 gap and open-type observations) therefore need the prior RTH session to be inside the 34 h replay window and fully covered by the tape:

* trade date **Mon 2026-11-02** (already closed): prior session = Fri, absent -> degraded (`AmtPriorSessionMissingTest`: the prior blocks are withheld rather than invented, the desk stands aside, today's own profile is published).
* trade date **Tue 2026-11-03** (cutover day, 16:05 ET onward): prior session = Mon 11-02 RTH, which is on the dev tape -> full record. The cutover is scheduled so the first Avro minutes are Tuesday's.
* Every later dev clean (`dev-cleanup.sh` wipes `underlying.es.trades`, ES_CLEAN_TOPICS) degrades the **next** trade date's prior blocks the same way. This is a property of dev's tape retention, not of the cutover.
* The history of the old topic is gone by design (the retained JSON is deleted): Monday's JSON minutes are not carried into the Avro topic.

## Order (the whole thing is one window: Tue 11-03 after 16:05 ET and before 18:00 ET)

EAV must not be redeployed between 18:01 and 09:30 ET (its Globex night is in memory), and the market-hours readers are not rolled 09:30-16:00 ET. So: after the 16:00 ET close, and finished before 18:00.

Prerequisites (done earlier, no dev effect): contracts with `EsAuctionAvro` merged; processing (es-amt-service and the readers) and gateway merged; this deploy PR merged; dev has a free `192.168.100.102` registry port 8082.

1. Register the subject (idempotent, collision-fails, FULL):
   `SCHEMA_REGISTRY_URL=http://192.168.100.102:8082 SCHEMAS_DIR=<contracts>/schemas-registry/esamt SCHEMA_ID_MAP=scripts/kafka/schema-id-maps/es-amt.map bash scripts/kafka/ensure-fixed-id-schemas.sh`
2. Record the pre-cutover TopicId: `kafka-topics --bootstrap-server 127.0.0.1:19092 --describe --topic es.futures.auction` (2026-11-02: `iPGdV7d8RsGZ6Tv2lYnIjA`).
3. Tear down the dev mirror, the only JSON writer: Jenkins `es-auction-mirror` with `TARGET_SPX_CLUSTER_IP=127.0.0.1 TARGET_PORT=19092 ACTION=teardown PERMITTED_SHA=<main>`. Prove it: `launchctl list | grep es-auction-mirror-127-0-0-1` prints nothing,
   the plist is gone, and `kafka-consumer-groups --describe --group es-auction-mirror-127-0-0-1-19092-es-futures-auction` (on the es4 broker 192.168.100.4:9092) shows no member. Leave the `192-168-100-252-9092` (production) mirror alone.
4. Stop every reader (strict mode; dev has topic auto-creation on). FIRST record each one's live replica count (`kubectl get deploy -o custom-columns=NAME:.metadata.name,R:.spec.replicas`; all six were 1/1 on 2026-11-02),
   then `kubectl scale --replicas=0` exactly these six by name: `feed-gateway-service amt-order-bridge early-auction-verdict-service es-compression-expansion-service level-absorption-service market-commentary-service`.
   If `es-amt-service` already exists on dev (a pre-cutover deploy), scale it to 0 too. This is a stop, not a bring-up; no scale loop.
5. Wipe: `PATH=/Users/abhinav/development/confluent-7.3.1/bin:$PATH scripts/kafka/roll-forward-es-amt-auction.sh --recreate --cluster dev --pre-cutover-id <TopicId> --writers-and-readers-stopped 127.0.0.1:19092`.
   If it fails on committed offsets, delete those groups (`kafka-consumer-groups --delete-offsets --group <g> --topic es.futures.auction`, or `--delete --group <g>` for a stopped group) and run `--verify --new-topic-id <printed id>`.
6. Roll the readers on their new Avro images, one `service-deploy` each (`ENVIRONMENT=dev BUILD_IMAGES=true`): feed-gateway, amt-order-bridge, es-compression-expansion, level-absorption, market-commentary. They start on an EMPTY topic, so no retained JSON can reach them.
   Each comes up at the replica count its manifest declares; compare with the counts recorded in step 4 and restore any that differ. **early-auction-verdict-service is the exception: its manifest is declaratively `replicas: 0`
   (owner-gated shadow) while the live dev deployment is 1 (hand-set), so `service-deploy` would leave it at 0.** Roll it by image only, before 18:00 ET: build/push its image with Jib, `kubectl set image` to the new digest on the stopped deployment, then
   `kubectl scale --replicas=<recorded count>` for that one name, and check the pod reaches READY with its lock ConfigMap intact. Never in 18:01-09:30 ET.
7. Roll the writer: `service-deploy SERVICE=es-amt ENVIRONMENT=dev BUILD_IMAGES=true`. Its start-up check requires id 3201 (step 1), then it replays the 34 h tape window and publishes every Globex minute of trade date Tue.
8. Verify the wire at both ends of partition 0: `kcat -b 127.0.0.1:19092 -C -t es.futures.auction -p 0 -o beginning -c1 -e -f '%S\n' | xxd | head -1` and the same with `-o -1` both start `00 00 00 0c 81`; decode one record with `kcat -s value=avro`
   or the registry (`curl http://192.168.100.102:8082/schemas/ids/3201`); the gateway `es-auction` frame on https://dev.bleadingoptions.com still carries `levels`, `profile`, `humps`, `call`; `es_amt_avro_refused` stays 0 on the writer's `/metrics`.
9. Re-run step 2's describe: the TopicId differs from the pre-cutover one, 1 partition, `cleanup.policy=delete`, `retention.ms=604800000`.

After the cutover `dev-cleanup.sh` needs nothing: it recreates the topic from `topics.env` at the same shape (plain delete, 7 days, EXACT 1 partition), `es-amt-service` is not in `DISABLED_DEV`/`KEEP_DOWN` (it must run), and the torn-down mirror has no plist
left for the mirror resume list.

## Not done here (owner's decision)

es4 (`k8s/es4/services/es-amt.yaml`, JSON writer) and production (JSON byte mirror of es4) stay as they are. Do not build es4 or production images of the gateway or the readers from main until their own cutover: they are Avro-only and would
refuse es4's JSON. When es4 is converted: register 3201 on `192.168.100.4:8081`, wipe es4's topic, roll es-amt on es4, then reinstall the es4 -> production mirror (a byte copy keeps working because the id is identical on every registry).
