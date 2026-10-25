# es.futures.aggressor-flow: JSON to Avro (2026-10-25)

Kafka Serialization Rule (rule.md): record values are Avro through the Schema Registry, JSON is prohibited. Owner ruling 2026-10-18: a topic is rolled
forward under its own name (delete and recreate), no versioned topic names.

## The contract

| | |
|---|---|
| Topic | `es.futures.aggressor-flow` (es4, dev and production carry the same name; es4 gets the prefix from `TOPIC_PREFIX`) |
| Subject / record | `com.optionsedge.contracts.esflow.EsAggressorFlowSnapshot` (RecordNameStrategy), fixed id **2701** on every registry, FULL |
| Contracts | 0.38.0 (options-edge-contracts #129): `schemas-registry/esflow/EsAggressorFlowSnapshot.avsc`, codec `EsAggressorFlowAvro` |
| Shape | one snapshot per second, key `ES.v.0`. `windows` is an Avro **map** keyed `10s 1m 5m 10m 1h 4h 8h session` (an Avro field name cannot start with a digit); every vocabulary is a string; nullable fields are `[null, T]` defaulting to null |
| Registration | `scripts/kafka/ensure-fixed-id-schemas.sh` with `schema-id-maps/es-aggressor-flow.map`, `SCHEMAS_DIR=<contracts checkout>/schemas-registry/esflow` |

Because the id is identical on all three registries, the es4 to dev/production MM1 mirror (`Jenkinsfile.es-futures-flow-mirror`, a byte copy) keeps working
unchanged: no decode and re-encode job is needed (unlike `underlying.spx.price`, whose ids differ per registry).

## Who writes, who reads (found by grepping every repo under the workspace)

* **Writer:** `es-aggressor-flow-service` (options-edge-processing). Runs on **es4** with the legacy output ON, and on **production** with
  `ES_AGGRESSOR_LEGACY_OUTPUT_ENABLED=false` (it publishes only the Kyle-style slope there), so production and dev get this topic **only** by the mirror from
  es4. Dev has no producer of its own (`es-aggressor-flow` is prod-only in `Jenkinsfile.service-deploy`). The converted writer maps the engine's JSON
  snapshot onto the record through the contracts codec, which refuses a missing, extra or mistyped field.
* **Readers:** the feed gateway (`FeedGatewayService`, event `es-aggressor-flow`, browser frame unchanged through `AvroJson`) and
  `reversal-confirmation-service` (futures-flow enrichment, recording only). `trade-evidence-api` lists the topic only in its freshness registry
  (`registry.py`) and decodes any framed Avro generically by registry id; it is not deployed. Nothing else reads the topic (`option-chain.js` receives the
  gateway frame, not Kafka).
* **Not changed here (still JSON, reported):** the slope output of the same service (`SlopePublisher`), and the input `underlying.es.trades`.
* **`es.options.databento.gex.strike` is NOT part of this cutover:** it is already Avro. Its writer is `databento-gex-service` (`GenericAvroSerde`,
  subject `app.options.gex.GexStrikeSnapshot`, id 8 on dev; the SPX twin `options.databento.gex.strike` shows the 0x00 frame). It has no
  `<topic>-value` subject because the serdes use RecordNameStrategy. It is empty on dev now because retention swept the mirrored es4 history.

## State on 2026-10-25 (measured)

* es4 topic `es.futures.aggressor-flow`: id `fN6dsDdWQ_C4ALYORKo91g`, 1 partition, `cleanup.policy=delete, retention.ms=-1`; last record 01:13:44 UTC, the producer is not running.
* production topic: id `oDcMMkM2QPK9755ETBaT9Q`, same shape; fed by the live launchd mirror `es-futures-flow-mirror-192-168-100-252-9092` on the dev Mac.
* dev topic: 1 partition, `cleanup.policy=delete`, retention at the broker default (this cutover pins `retention.ms=-1`); the dev mirror unit
  `es-futures-flow-mirror-127-0-0-1-19092` is **not loaded** (no JSON writer).

## Order (every cluster): Avro writer first, readers Avro-only, wipe in between is only safe with no JSON writer

1. Contracts 0.38.0 merged (done), subject registered on the cluster's registry (idempotent, collision-fails).
2. Stop every JSON writer of the cluster's topic: the mirror unit for dev/production (`Jenkinsfile.es-futures-flow-mirror` ACTION=teardown), the JSON
   producer on es4.
3. Wipe and recreate: `scripts/kafka/roll-forward-es-aggressor-flow.sh --recreate --cluster <c> --pre-cutover-id <id> --writers-and-readers-stopped|--avro-feed-live --json-writers-stopped <bootstrap>`,
   then `--verify --new-topic-id <id>`. Recreated with `cleanup.policy=delete, retention.ms=-1` (the shape es4 and production carry).
4. Roll the Avro readers (gateway, reversal-confirmation). They refuse any value that is not framed as id 2701, so they must not meet retained JSON; the
   gateway treats the topic as tolerant and drops a stray value instead of stalling the shared Avro consumer.
5. Start the Avro writer (es4 service image with contracts 0.38.0). Then re-install the mirror unit(s) for dev/production.
6. Verify the wire at BOTH ends: `kcat -b <broker> -C -t es.futures.aggressor-flow -o -1 -c1 -e -f '%S\n' | xxd | head -1` starts `00 00 00 0a 8d` (magic 0, id 2701 = 0x0a8d) on the writer's cluster and on every mirror target.

## Dev (done 2026-10-25, authorised)

Registered id 2701 on the dev registry; topic recreated with the script; gateway and reversal-confirmation rolled through Jenkins; because dev has no producer
and the es4 producer must not be touched, the verification writer was the real `EsAggressorFlowRuntime` run on the MacBook against the dev broker
(not on 102), stopped afterwards. See the report of the session for build numbers and the byte checks.

## PENDING the owner's explicit permission (nothing below has been done)

* **es4:** register 2701 on `http://192.168.100.4:8081`; stop the (already idle) es-aggressor-flow-service; wipe/recreate `es.futures.aggressor-flow`
  (`roll-forward-es-aggressor-flow.sh` refuses `--cluster es4` until the owner permits and the cluster case is reviewed); deploy the es-aggressor-flow image
  built with contracts 0.38.0 and the es4 gateway (`es-feed-gateway`) with the Avro binding; verify 0x00 at the source.
* **production:** register 2701 on the production registry; tear down the production mirror unit; wipe/recreate the production topic; roll the
  production gateway and reversal-confirmation; reinstall the mirror (`Jenkinsfile.es-futures-flow-mirror`, TARGET 192.168.100.252:9092).
  **Until then never build a production image of the gateway or reversal-confirmation from main:** they read Avro only and would meet the live JSON mirror.
* **dev mirror:** after the es4 writer is Avro, re-install the dev mirror unit (`Jenkinsfile.es-futures-flow-mirror`, TARGET 127.0.0.1:19092) so the card has live data again.
