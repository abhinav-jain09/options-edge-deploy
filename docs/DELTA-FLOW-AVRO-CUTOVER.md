# delta-flow-service JSON -> Avro: cutover runbook

Kafka Serialization Rule (rule.md): JSON on Kafka is prohibited; a topic that changes serialization is **wiped and recreated under its own
name** (no `.vN`/`.avro` sibling, no dual publish, no rollback). Contracts 0.54.0 (`schemas/deltaflow/*.avsc`, `DeltaFlowAvro`, `DeltaFlowStateAvro`,
`DeltaFlowJson`): Confluent framing, RecordNameStrategy (subject = record full name, one subject per registry), FULL, `auto.register=false`.
Processing: producer `delta-flow-service` and the readers below in one change set; feed-gateway, options-edge (web monolith), short-premium-agent:
one PR each. Generic wipe tool: `scripts/kafka/roll-forward-topics.sh` (deploy PR 1320, `docs/KAFKA-AVRO-TOPIC-ROLL-FORWARD.md`).

## HARD PREREQUISITE (blocks step 5b; nothing below may start without it)
**Merge options-edge-deploy#1320 (`scripts/kafka/roll-forward-topics.sh`) FIRST.** The destructive wipe in step 5b is performed ONLY by that merged script;
this runbook neither contains nor copies it. At cutover record here, in the cutover change, the merge commit sha of #1320 that the operator ran:
`roll-forward-topics.sh merge commit sha: ____________` and run the script from a checkout at (or after) that sha. Steps 1-4 and the two scripts of this PR
(`ensure-delta-flow-schemas.sh`, `reset-delta-flow-state.sh`) do not need it; step 5b and everything after it do. If #1320 is not merged, STOP at the end of step 5.
`roll-forward-topics.sh --cluster` accepts `dev|production|es4` (the `es4` binding is added by this PR; see "es4" below).

## What is converted
| Item | Shape (scripts/kafka/topics.env) | Subject (`com.optionsedge.contracts.deltaflow.` + name) |
|---|---|---|
| `delta-flow-by-trade`, `delta-flow-diagnostics` | 32 partitions | `DeltaFlowEvent` |
| `delta-flow-10s`, `-30s`, `-1m`, `-5m`, `-15m`, `delta-flow-session`, `delta-flow-dashboard` | 32 partitions | `DeltaFlowSnapshot` |
| `delta-flow-by-strike` | 32 partitions, delete-retention | `DeltaFlowStrikeSnapshot` |
| `delta-flow-corrections` | 32 partitions | `DeltaFlowCorrectionEvent` |
| `delta-flow-positioning`, calibration topic | exist only when `DELTA_FLOW_INPUT_OI_TOPIC` / `DELTA_FLOW_CALIBRATION_TOPIC` are set (not set in any manifest) | `DeltaFlowPositioningSnapshot`, `DeltaFlowCalibrationRecord` |
| `<appId>-trades-by-symbol-repartition`, `<appId>-quotes-by-symbol-repartition` | Streams-internal | `DeltaFlowTradeInput`, `DeltaFlowQuoteState` |
| 8 store changelogs `<appId>-<store>-changelog` | Streams-internal, compacted | `DeltaFlowQuoteState`, `DeltaFlowIntradayState`, `DeltaFlowStrikeStoreValue`, `DeltaFlowRollingState`, `DeltaFlowReversibleContribution`, `DeltaFlowAcceptedWatermark`, `DeltaFlowIndexMarker`, `DeltaFlowPositioningState` |
| 3 global stores (no changelog; local only) | rebuilt from their source topics | `DeltaFlowGreekStoreValue`, `DeltaFlowOiState`, `DeltaFlowCalibrationRecord` |

`<appId>` is the EFFECTIVE Streams application id: `options-edge-delta-flow-service` + `KAFKA_APPLICATION_ID_SUFFIX` (`-dev`, `-prod`, `-es4`) + the
`TOPOLOGY_VERSION_SUFFIX` `-r2` of `DeltaFlowSettings`: `options-edge-delta-flow-service-dev-r2` | `-prod-r2` | `-es4-r2` (read it off the broker with
`kafka-consumer-groups --list | grep delta-flow`). The state dir is an `emptyDir`; stores are in-memory (`KAFKA_STREAMS_STORE_TYPE` unset = memory), so the
changelog is the only durable image of the state. Store names: `latest-quote-store`, `cumulative-delta-flow-store`, `strike-delta-flow-store`,
`rolling-delta-flow-store`, `delta-flow-duplicate-trade-store`, `delta-flow-accepted-watermark-store`, `delta-flow-trading-date-index-store`, `positioning-store`.
Not converted, not ours: `options.opra.tcbbo` and the optional quote / open-interest input topics are JSON written by another component (the service parses them once
at the re-key stage and never writes JSON); `delta-flow.acceleration.current` is written by the web monolith (`DeltaFlowAccelerationPublisher`), a different producer.

## Reader-completeness evidence (origin/main, run 2026-10-19)
```text
for r in options-edge-processing options-edge-feed-gateway options-edge options-edge-deploy options-edge-contracts options-edge-documents \
         options-edge-trading-journal short-premium-agent options-edge-integration-test options-edge-ibkr-feed options-edge-databento-feed options-edge-approach-monitor; do
  git -C $r grep -l -E 'delta-flow-(by-trade|10s|30s|1m|5m|15m|session|by-strike|dashboard|diagnostics|corrections|positioning)|DELTA_FLOW_OUTPUT|DeltaFlowTopics|DeltaFlowSnapshot|DeltaFlowStrikeSnapshot|DeltaFlowEvent' origin/main
done
```
Kafka readers (all converted to Avro-only in this change set unless marked):
* **feed-gateway** (`delta-flow-by-strike`; feed-gateway PR): Avro cache + live consumers, the browser `delta-flow` event is rendered with `DeltaFlowJson` (byte-identical).
* **options-edge web monolith** (`DeltaFlowTopicListener`: session, dashboard, 10s, 30s, 1m, 5m, 15m; options-edge PR): Avro decode, rendered to the JSON-era text the store parses.
* **short-premium-agent** (`delta-flow-dashboard`; short-premium-agent PR).
* **processing**: `strike-intelligence-service` (by-strike), `hedging-pressure-service` (by-strike), `dealer-ledger-service` (session, optional cross-check), `drop-classifier-service`
  and `reversal-confirmation-service` (`es.delta-flow-by-trade`), `trade-evidence-api` (dashboard freshness collector; fastavro, JSON refused).
* **NOT in this change set (open PR branches, owners to change - exact edits are in the processing PR description / report):** `greek-move-authenticity-service`
  (options-edge-processing#1175, `LegacyJsonDeltaFlowReader`) and `strike-invasion-service` (options-edge-processing#1177, String source of `delta-flow-by-strike`).
  **They must be merged with their Avro delta-flow readers BEFORE the by-strike topic is recreated, or each will read Avro bytes as JSON text and drop every record.**
* `early-auction-verdict-service`: its `optionsFlowRefusal` input names `delta-flow-by-strike` with `"serialization":"JSON"` but is `enabled:false` in the dev, prod and live-lock input specs; nothing is read.
* No reader: `delta-flow-diagnostics`, `-corrections`, `-positioning`, the calibration topic, `-10s/-30s/-1m/-5m/-15m` outside the monolith, `delta-flow-by-trade` outside drop-classifier / reversal-confirmation.
* NAS archive: `delta-flow-by-strike` is in NO archived topic list (the 2026-09-17 owner rule archives the raw feed inputs only); only an orphan floor line
  `delta-flow-by-strike:1000` remains in `scripts/ops/archive/oe-topics.env` (a floor for a topic nobody archives). No archive reader is needed. Past JSON-era archive files stay JSON lines.
* No mirror: `git grep -n delta-flow origin/main -- scripts/ops scripts/ci` lists no mirror or bridge row; no Postgres writer reads these topics.

### Cross-broker readers need the es4 registry (new env, this PR)
`drop-classifier-service` and `reversal-confirmation-service` read `es.delta-flow-by-trade` from the **es4 broker** (192.168.100.4:9092) but run against the prod/dev registry
(`SCHEMA_REGISTRY_URL`). Avro schema ids are per registry, so a flow reader pointed at the wrong registry cannot decode. This PR therefore sets
`DROP_FLOW_SCHEMA_REGISTRY_URL` (dev overlay: blank = the dev registry, because dev reads its local broker) and `REVERSAL_DELTA_FLOW_SCHEMA_REGISTRY_URL` to
`http://192.168.100.4:8081` in the base manifests and the regenerated slices. The es4 subjects must therefore be registered on **that** registry too (step 2).

## Order (rule step 4; one environment at a time, off-hours, topic administration frozen)
1. Merge contracts 0.54.0, then processing (producer + readers), feed-gateway, options-edge, short-premium-agent and the greek-move / strike-invasion reader changes, then this change.
   From the first merge on, an image built from main for any converted service reads or writes Avro: build **no prod image** of delta-flow-service, the gateway, the web monolith,
   strike-intelligence, hedging-pressure, dealer-ledger, drop-classifier, reversal-confirmation, greek-move-authenticity or strike-invasion before this environment's cutover.
2. **Register** the 17 subjects on the environment's registry, and on the **es4 registry** (`http://192.168.100.4:8081`) as well for the prod cross-broker readers:
   `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas/deltaflow scripts/kafka/ensure-delta-flow-schemas.sh`
   (sha256-pinned bytes, FULL by read-back, read-everything-then-mutate, canonical = latest; `DEPLOY_DRY_RUN=true` only reports; tests: `tests/test_ensure_delta_flow_schemas.py`).
   Registries: dev `http://host.docker.internal:8082`, production `http://192.168.100.252:8082`, es4 `http://192.168.100.4:8081`.
3. **Stop** (service-deploy paths; the workloads are Jenkins-only, verify that the path you use actually holds replicas at 0): the Avro-incompatible OLD readers -
   feed-gateway, the web monolith's delta-flow listener (the web tier), short-premium-agent, strike-intelligence, hedging-pressure, dealer-ledger,
   drop-classifier, reversal-confirmation, trade-evidence-api, greek-move-authenticity, strike-invasion - and `delta-flow-service`.
4. **Reset the Streams state** with the service at 0:
   `KAFKA_BOOTSTRAP_SERVERS=<bootstrap> DELTA_FLOW_APP_ID=<appId> scripts/kafka/reset-delta-flow-state.sh`
   (group absent/Empty, group deleted, the 2 repartition + 8 changelog topics deleted by exact name, absent ones reported, none left; tests `tests/test_delta_flow_cutover_tooling.py`).
   **Owner decision to confirm:** offsets are deliberately NOT reset to 0 (rule step 4c's literal wording). delta-flow-service is a cold-start-at-the-live-end service
   (`DELTA_FLOW_STREAMS_AUTO_OFFSET_RESET=latest` in every manifest) and a replay of the retained `options.opra.tcbbo` through fresh state cannot be event-time faithful: the greek surface is a
   global store restored to its LATEST value, so every replayed trade older than `DELTA_FLOW_GREEK_MAX_AGE_MS` is rejected as `LOOKAHEAD_GREEK`/`STALE_GREEK` (zero lookahead tolerance), producing mass diagnostics and no flow.
   After the cutover the session aggregates therefore restart from the first accepted print (`sessionObservedFromMs` on session/dashboard/by-strike marks the partial session, as for any mid-day cold start).
   Also empty the pod's `kafka-streams-state` (an `emptyDir`: lost with the pod).
5. **Delete every consumer group that holds an offset on these topics, THEN wipe.** The wipe harness's strict preflight REFUSES any committed offset on a topic it is about to delete
   (committed offsets survive a topic delete, keyed by topic name), so the groups go first. Everything stopped, then:
```text
# runbook-lint:delete-groups
kafka-consumer-groups --bootstrap-server <bootstrap> --describe --all-groups | awk '$2 ~ /^delta-flow-/ {print $1}' | sort -u   # every group with a committed offset on a delta-flow topic
kafka-consumer-groups --bootstrap-server <bootstrap> --delete --group <g>                                                   # each one; a non-Empty group refuses: stop its readers first
kafka-consumer-groups --bootstrap-server <bootstrap> --describe --all-groups | awk '$2 ~ /^delta-flow-/' | wc -l                      # must print 0
```
   Expect the gateway groups `options-edge-feed-gateway-*`, `strike-intelligence`, `hedging-pressure`, `dealer-ledger`, `trade-evidence-api`, greek-move-authenticity and strike-invasion (names as listed by the first command);
   the monolith listener, drop-classifier and reversal-confirmation are assign-mode (no group). The last command must print 0 before the next step.
5b. **Delete and recreate the public topics** with the generic roll-forward tool. **HARD PREREQUISITE: options-edge-deploy#1320 is merged first; record its merge sha above and run the script from that sha.**
   Read each TopicId first (`kafka-topics --bootstrap-server <bootstrap> --describe --topic <t>`, the id is on the first line) and freeze topic administration. The harness takes one `--pre-cutover-id <topic>=<id>` and one
   `--subject <topic>=<record.full.Name>` argument PER TOPIC (no globbing, no alternation); `<cluster>` is `dev` or `production` and `<bootstrap>` that cluster's broker as bound in the script:
```text
# runbook-lint:roll-forward <dev|production>
scripts/kafka/roll-forward-topics.sh --recreate --cluster <dev|production> --topics delta-flow-by-trade,delta-flow-by-strike,delta-flow-10s,delta-flow-30s,delta-flow-1m,delta-flow-5m,delta-flow-15m,delta-flow-session,delta-flow-dashboard,delta-flow-diagnostics,delta-flow-corrections \
  --pre-cutover-id delta-flow-by-trade=<TopicId read in the previous step> \
  --pre-cutover-id delta-flow-by-strike=<TopicId read in the previous step> \
  --pre-cutover-id delta-flow-10s=<TopicId read in the previous step> \
  --pre-cutover-id delta-flow-30s=<TopicId read in the previous step> \
  --pre-cutover-id delta-flow-1m=<TopicId read in the previous step> \
  --pre-cutover-id delta-flow-5m=<TopicId read in the previous step> \
  --pre-cutover-id delta-flow-15m=<TopicId read in the previous step> \
  --pre-cutover-id delta-flow-session=<TopicId read in the previous step> \
  --pre-cutover-id delta-flow-dashboard=<TopicId read in the previous step> \
  --pre-cutover-id delta-flow-diagnostics=<TopicId read in the previous step> \
  --pre-cutover-id delta-flow-corrections=<TopicId read in the previous step> \
  --registry <registry-url> \
  --subject delta-flow-by-trade=com.optionsedge.contracts.deltaflow.DeltaFlowEvent \
  --subject delta-flow-by-strike=com.optionsedge.contracts.deltaflow.DeltaFlowStrikeSnapshot \
  --subject delta-flow-10s=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject delta-flow-30s=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject delta-flow-1m=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject delta-flow-5m=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject delta-flow-15m=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject delta-flow-session=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject delta-flow-dashboard=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject delta-flow-diagnostics=com.optionsedge.contracts.deltaflow.DeltaFlowEvent \
  --subject delta-flow-corrections=com.optionsedge.contracts.deltaflow.DeltaFlowCorrectionEvent \
  --writers-and-readers-stopped <bootstrap> [kafka-bin-dir]
```
   Strict mode: every topic must come back EMPTY at 32 partitions; the shape is copied from the live topic. Then verify partitions=32, cleanup policy, retention and end offset 0. Do NOT use `apply-topics.sh` to recreate the topics ad hoc.
6. **Deploy the Avro readers** (all of the above except delta-flow-service) and verify each Ready with zero records consumed (they must never meet retained JSON).
7. **Deploy the Avro producer** (`delta-flow-service`, image built from the merged tree); verify its first records on every output topic: the first byte is `00` and the 4-byte id resolves
   (`/schemas/ids/<id>/versions`) to the subject of the table above (`kafka-avro-console-consumer --topic delta-flow-by-strike --property schema.registry.url=<registry> --max-messages 1`);
   the repartition and changelog topics hold Avro; `delta_flow_unencodable_price_total` stays 0 (any non-zero site names the input that carried a price with no exact decimal(38,18) image);
   the browser `delta-flow` payload and `/api/delta-flow` are unchanged.
8. Full clean-slate flows (`dev-cleanup.sh`, `prod-clean-slate.sh`, the es4 clean) already delete every non-preserved topic, including all of the above, and recreate the declared set (`scripts/kafka/topics.env`).

## es4 (`es.delta-flow-*`, application id `options-edge-delta-flow-service-es4-r2`)
Same service image, `TOPIC_PREFIX=es.`, own broker and registry (the self-contained ES box: broker `192.168.100.4:9092`, registry `http://192.168.100.4:8081`). The generic harness
now takes it: `roll-forward-topics.sh --cluster es4` (this PR adds the `es4:192.168.100.4:9092` bootstrap binding next to dev/production, with tests in `roll-forward-topics-test.sh`:
es4 refuses the production and dev bootstraps and they refuse the es4 one; a recreate on the es4 bootstrap copies the live shape and binds the shape file to cluster `es4`).
Prod's drop-classifier and reversal-confirmation read these topics, so the es4 conversion is a prerequisite of theirs. Order (same as steps 2-7, es4 objects):
1. Register the 17 subjects on the es4 registry: `SCHEMA_REGISTRY_URL=http://192.168.100.4:8081 SCHEMAS_DIR=<contracts>/schemas/deltaflow scripts/kafka/ensure-delta-flow-schemas.sh`.
2. Stop the es4 delta-flow-service (es4 deploy path, `Jenkinsfile.es4-deploy`) and every reader of `es.delta-flow-*`: es4 dealer-ledger (session), es4 strike-intelligence (by-strike), es4 feed-gateway (by-strike),
   and the PROD drop-classifier and reversal-confirmation (`es.delta-flow-by-trade`, cross-broker; assign-mode consumers with no committed group).
3. `KAFKA_BOOTSTRAP_SERVERS=192.168.100.4:9092 DELTA_FLOW_APP_ID=options-edge-delta-flow-service-es4-r2 scripts/kafka/reset-delta-flow-state.sh` (group + 2 repartition + 8 changelog topics).
4. Read each TopicId: `for t in es.delta-flow-by-trade es.delta-flow-by-strike es.delta-flow-10s es.delta-flow-30s es.delta-flow-1m es.delta-flow-5m es.delta-flow-15m es.delta-flow-session es.delta-flow-dashboard es.delta-flow-diagnostics es.delta-flow-corrections; do kafka-topics --bootstrap-server 192.168.100.4:9092 --describe --topic $t | head -1; done`, and freeze es4 topic administration (the es4 clean must not run meanwhile).
5. Delete every consumer group that holds an offset on an `es.delta-flow-*` topic BEFORE the wipe (strict mode refuses any committed offset on a topic it deletes): the es4 dealer-ledger, strike-intelligence and feed-gateway groups.
   Prod drop-classifier / reversal-confirmation are assign-mode: nothing to delete.
```text
# runbook-lint:delete-groups
kafka-consumer-groups --bootstrap-server 192.168.100.4:9092 --describe --all-groups | awk '$2 ~ /^es\.delta-flow-/ {print $1}' | sort -u   # every group with a committed offset on a delta-flow topic
kafka-consumer-groups --bootstrap-server 192.168.100.4:9092 --delete --group <g>                                                   # each one; a non-Empty group refuses: stop its readers first
kafka-consumer-groups --bootstrap-server 192.168.100.4:9092 --describe --all-groups | awk '$2 ~ /^es\.delta-flow-/' | wc -l                      # must print 0
```
6. Wipe and recreate (everything stopped, strict mode: every topic must come back EMPTY, i.e. end offset 0 on every partition, and no group may hold an offset, which step 5 guaranteed), one `--pre-cutover-id` and one `--subject` argument per topic:
```text
# runbook-lint:roll-forward es4
scripts/kafka/roll-forward-topics.sh --recreate --cluster es4 --topics es.delta-flow-by-trade,es.delta-flow-by-strike,es.delta-flow-10s,es.delta-flow-30s,es.delta-flow-1m,es.delta-flow-5m,es.delta-flow-15m,es.delta-flow-session,es.delta-flow-dashboard,es.delta-flow-diagnostics,es.delta-flow-corrections \
  --pre-cutover-id es.delta-flow-by-trade=<TopicId read in the previous step> \
  --pre-cutover-id es.delta-flow-by-strike=<TopicId read in the previous step> \
  --pre-cutover-id es.delta-flow-10s=<TopicId read in the previous step> \
  --pre-cutover-id es.delta-flow-30s=<TopicId read in the previous step> \
  --pre-cutover-id es.delta-flow-1m=<TopicId read in the previous step> \
  --pre-cutover-id es.delta-flow-5m=<TopicId read in the previous step> \
  --pre-cutover-id es.delta-flow-15m=<TopicId read in the previous step> \
  --pre-cutover-id es.delta-flow-session=<TopicId read in the previous step> \
  --pre-cutover-id es.delta-flow-dashboard=<TopicId read in the previous step> \
  --pre-cutover-id es.delta-flow-diagnostics=<TopicId read in the previous step> \
  --pre-cutover-id es.delta-flow-corrections=<TopicId read in the previous step> \
  --registry http://192.168.100.4:8081 \
  --subject es.delta-flow-by-trade=com.optionsedge.contracts.deltaflow.DeltaFlowEvent \
  --subject es.delta-flow-by-strike=com.optionsedge.contracts.deltaflow.DeltaFlowStrikeSnapshot \
  --subject es.delta-flow-10s=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject es.delta-flow-30s=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject es.delta-flow-1m=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject es.delta-flow-5m=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject es.delta-flow-15m=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject es.delta-flow-session=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject es.delta-flow-dashboard=com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot \
  --subject es.delta-flow-diagnostics=com.optionsedge.contracts.deltaflow.DeltaFlowEvent \
  --subject es.delta-flow-corrections=com.optionsedge.contracts.deltaflow.DeltaFlowCorrectionEvent \
  --writers-and-readers-stopped 192.168.100.4:9092 [kafka-bin-dir]
```
   Its post-checks verify new id, partitions (32), RF and every config equal to the captured live shape, readable offsets on every partition and EMPTY; a topic whose id differs from `--pre-cutover-id` refuses the whole run before any delete.
7. Deploy the Avro readers (es4 services; prod drop-classifier / reversal-confirmation with the es4 registry env), verify Ready with zero records consumed, then the es4 producer; verify as in step 7 above against the es4 registry.

## Rollback
There is none by decision (roll forward only). Before step 3 nothing has changed (registration is additive under FULL). After step 5b the topics are empty by design; a failure is
fixed forward by `--verify --new-topic-id ...` or, if JSON appears, by stopping the JSON writer and wiping again as the generic tool describes.
