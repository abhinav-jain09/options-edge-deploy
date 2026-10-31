# options.spx.vix-option-inteligence-service.current: JSON to Avro (2026-10-31)

Kafka Serialization Rule (rule.md): record values are Avro through the Schema Registry, JSON is prohibited. Owner ruling 2026-10-18: a topic is rolled
forward under its own name (delete and recreate), no versioned topic names. Owner request 2026-10-31: "change this to avro" (the vix service had just been
enabled on dev and was found to publish JSON).

## The contract

| | |
|---|---|
| Topic | `options.spx.vix-option-inteligence-service.current` on dev and production; `es.options.spx.vix-option-inteligence-service.current` on es4 (`TOPIC_PREFIX`). 32 partitions, `cleanup.policy=compact`, `retention.ms=-1`, key `SYMBOL|sessionDate` |
| Subject / record | `com.optionsedge.contracts.zerodte.ZeroDteIntelligenceSnapshot` (RecordNameStrategy), fixed id **3001** on every registry, FULL |
| Contracts | 0.53.0 (options-edge-contracts #145): `schemas-registry/zerodte/ZeroDteIntelligenceSnapshot.avsc`, codec `ZeroDteIntelligenceAvro` |
| Shape | 50 fields; every nullable value is a `[null, T]` union defaulting to null (a missing measurement is null, never 0); string vocabularies; `directionalTriggers`/`reasonCodes` are string arrays; `prediction` is a nullable record `{modelVersion, byHorizon:{H5,H10,H15:{state,pExpansion,pContraction,pBalanced}}}` attached all-or-nothing |
| Registration | `scripts/kafka/ensure-fixed-id-schemas.sh` with `schema-id-maps/vix-option-inteligence.map`, `SCHEMAS_DIR=<contracts checkout>/schemas-registry/zerodte` |
| Wipe | `scripts/kafka/roll-forward-vix-option-inteligence.sh` (mock-tested by `roll-forward-vix-option-inteligence-test.sh`; reads EVERY partition, because the records of one key sit in a single partition of the 32) |

## Who writes, who reads (found by grepping every repo under the workspace)

* **Writer:** `vix-option-inteligence-service` (options-edge-processing #1501), one pod per cluster (dev, production, es4). The only JSON writer of the topic is this
  pod, so "no JSON writer" means "the pod runs the Avro image". The payload is still built as a JSON tree and the contracts serializer maps it onto the record,
  refusing a missing, extra or mistyped field.
* **Readers:** the feed gateway (event `zero-dte-intelligence`; browser frame unchanged through `AvroJson`; options-edge-feed-gateway PR) and nothing else that decodes
  it: `trade-evidence-api` lists the topic only in its freshness registry and decodes any framed Avro generically by registry id (it is not deployed); the web
  receives the gateway frame (`option-chain.js` reads `!payload.prediction`, so a null prediction equals the absent one the JSON era published).
* **Not changed here:** the service's INPUTS (`options.databento.events.raw`, `underlying.vix.price`, `market.spx.market-carry-service.current`).

## Order (every cluster)

1. Contracts 0.53.0 merged; subject registered on the cluster's registry (idempotent, collision-fails):
   `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas-registry/zerodte SCHEMA_ID_MAP=scripts/kafka/schema-id-maps/vix-option-inteligence.map bash scripts/kafka/ensure-fixed-id-schemas.sh`
   (dev registry `http://192.168.100.102:8082`, production `http://192.168.100.252:8082`, es4 `http://192.168.100.4:8081`).
2. Roll the Avro writer (vix-option-inteligence image built with contracts 0.53.0). From this moment nothing writes JSON.
3. Roll the Avro reader (feed gateway). It treats the topic as tolerant: a retained JSON value is counted and dropped, it never stalls the shared Avro consumer.
4. Wipe and recreate (dev: strict mode, see below; the live-writer form is):
   `roll-forward-vix-option-inteligence.sh --recreate --cluster <c> --pre-cutover-id <TopicId> --avro-feed-live --json-writers-stopped <bootstrap> [kafka-bin-dir]`,
   then `--verify --new-topic-id <id>`. The script refuses unless every non-empty partition starts and ends with a Confluent frame under id 3001 (or a tombstone).
5. Verify the wire: `kcat -b <broker> -C -t <topic> -p <partition> -o -1 -c1 -e -f '%S\n' | xxd | head -1` starts `00 00 00 0b b9` (magic 0, id 3001 = 0x0bb9), and the gateway
   frame still carries `atmIv`, `marketDirection`, `prediction` for the page.

**Dev and any broker with topic auto-creation on: use the STRICT mode, not `--avro-feed-live`** (Codex review). A live producer retries into a topic that was just
deleted and the broker can auto-create it first with its defaults (1 partition or 32, not compacted), before the script's own create. So: roll the Avro image (step 2),
scale `vix-option-inteligence` to 0, wipe with `--writers-and-readers-stopped`, `--verify` (partition count and `cleanup.policy=compact` are checked from the broker),
scale it back to 1. `--avro-feed-live` is only for a cluster with auto-creation disabled.

## Pre-cutover facts (measured 2026-10-31)

* dev: TopicId `zLY9HdgqRfCuTv7ic-PFIA`, 32 partitions, `cleanup.policy=compact`, `retention.ms=-1`; 301 retained JSON snapshots, all in partition 13 (key `SPX|2026-10-31`),
  written since the vix service was enabled on dev the same day.
* production and es4: hosts were shut down by the owner on 2026-10-23/26; registration, wipe and the image roll are NOT done there (run the order above when they are up).
