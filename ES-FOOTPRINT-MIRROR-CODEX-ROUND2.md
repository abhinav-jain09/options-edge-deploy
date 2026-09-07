## Findings

LOW — [scripts/ops/archive/test-archive-reset.sh:375](/private/tmp/oe-deploy-mirror/scripts/ops/archive/test-archive-reset.sh:375) — The archive regression suite pins only `es.futures.cvd.bars`. It does not pin the newly required `es.futures.footprint.bars` and `.outcomes`, nor prohibit the two always-hot live topics. Removing either history topic from `OE_ES4_TOPICS`, or accidentally adding either live topic, still leaves the suite green because `es4_expected` is derived from the same policy being tested. Add token-exact positive assertions for `.bars` and `.outcomes`, and negative assertions for `es.futures.footprint` and `.evidence`. This is the same silent archive-inventory regression class the existing CVD assertion was introduced to prevent.

## Round-1 findings

Both are fixed.

- The parser at [validate-mirrored-topic-contracts.sh:193](/private/tmp/oe-deploy-mirror/scripts/ci/validate-mirrored-topic-contracts.sh:193) now accumulates the complete `TOPIC` choice declaration and tracks `[`/`]` depth. For all five current mirror Jenkinsfiles, the list opens and closes on either the same line or the following line. For the deliberately unterminated CVD mutation, later balanced lists and character classes contribute net zero, leaving depth positive through EOF and producing `<unterminated>`. The termination is correct for every current mirror Jenkinsfile.
- The wrapped-list mutation at [validate-mirrored-topic-contracts-test.sh:184](/private/tmp/oe-deploy-mirror/scripts/ci/validate-mirrored-topic-contracts-test.sh:184) genuinely detects a reversion: the old parser omits `.outcomes`, so its changed retention is never checked and `expect_fail` fails.
- The unterminated-list mutation at [validate-mirrored-topic-contracts-test.sh:186](/private/tmp/oe-deploy-mirror/scripts/ci/validate-mirrored-topic-contracts-test.sh:186) also genuinely detects a reversion: the old parser neither detects the missing bracket nor emits the sentinel, so the validator incorrectly succeeds.
- All three misleading footprint-compaction comments are corrected. Repository-wide footprint references now consistently describe plain `delete`.

The real validator passed: 12 topics across all 5 mirror jobs. Shell syntax checks passed. Mutation/render suites could not execute in this read-only review sandbox because they require temporary/output files; CI remains responsible for executing them.

## Full contract check

The executable footprint contract is internally consistent:

- Four frozen mirror arms: one partition, `delete`; `.bars` and `.outcomes` retain forever, while live `.footprint` and `.evidence` retain 12 hours.
- Dev/prod and es4 declarations contain all four at exactly one partition.
- All four appear in both exact-partition sets.
- Retention overrides agree on source and targets.
- `.bars` and `.outcomes` are correctly reset-rebuildable on mirror targets; the finite-retention live topics do not require that classification.
- None appears in a compact or pure-compact set.
- Base, dev, production, and experiment gateway manifests contain the same enable flag and four exact topic names.
- Archive inventory correctly includes only the two finite-session history topics, with 500-record completeness floors.

## Other repositories and rollout

This repository supplies topic policy, mirror services, deployment configuration, and archive inventory; it does not implement the producer or gateway relay. No further deploy-repo transport script is needed. Before rollout, the corresponding application implementations must already be merged and built:

- `options-edge-processing`: `es-cvd-service` must implement the four footprint producers and understand the four environment variables.
- `option-edge-feed-gateway`: the gateway must implement `GATEWAY_ES_FOOTPRINT_ENABLED` and the four topic settings.
- A web-repository release is needed only for user-visible rendering, not for Kafka records to reach dev/prod.

Operational order:

1. Fix the archive regression assertions above, pass CI, merge to `main`.
2. Build and publish the footprint-capable processing and gateway images.
3. Run `es4-deploy` with `ACTION=create-topics`.
4. Reconcile dev Kafka topics through the normal dev deploy with Kafka topics enabled.
5. Reconcile production Kafka topics through the normal production deploy with Kafka topics enabled.
6. Deploy `es-cvd` to es4 and verify all four source topics have one partition, `cleanup.policy=delete`, and the declared retentions.
7. Run `Jenkinsfile.es-cvd-mirror` eight times with `ACTION=install`: four topics to `127.0.0.1:19092`, then the same four to `192.168.100.252:9092`.
8. Verify each mirror’s source/target shape checks, launchd unit, consumer-group progress, and increasing target offsets.
9. Deploy the footprint-capable feed gateway to dev and production; deploy experiment only if that environment is intended to consume its configured broker.
10. Run `archive-scripts-deploy` so the two history topics enter the live es4 archive inventory.
11. Verify gateway consumption and the next 17:01 ET archive for `.bars` and `.outcomes`.

VERDICT: REQUEST_CHANGES
