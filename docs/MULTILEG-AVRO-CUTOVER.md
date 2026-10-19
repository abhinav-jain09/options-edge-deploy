# multileg-structure-service Avro cutover (options.multileg.observations / .window.status / .sr.levels)

Topics keep their own names; they are wiped and recreated carrying Avro. Contracts 0.34.0.

## Reader-completeness evidence (origin/main, run 2026-10-19)

```text
for r in options-edge-processing options-edge-feed-gateway options-edge-approach-monitor options-edge-contracts \
         options-edge-deploy options-edge-trading-journal short-premium-agent options-edge options-edge-databento-feed; do
  git -C $r grep -l -E 'multileg\.(observations|window\.status|sr\.levels)' origin/main
done
```

Result: only `options-edge-processing` multileg-structure-service `MultilegSettings.java` (the producer's own topic defaults) and
`trade-evidence-api/inventory.json` (an env-var inventory listing); `options-edge-deploy` k8s manifests (env values),
`scripts/kafka/ensure-multileg-topics.sh` and `topics.env` (topic shape). No consumer, gateway, Postgres writer, archive, mirror or
script reads these topics. `options-edge-feed-gateway`, `approach-monitor`, `contracts`, `trading-journal`, `short-premium-agent`,
`options-edge` (web) and `databento-feed`: no match. The web page reads the service over HTTP/SSE (`/api/multileg/stream`,
`/api/multileg/levels`), whose JSON is unchanged. `options.opra.tcbbo` (JSON, written by the Databento feed) is an INPUT of this
service, not one of its outputs, and is out of scope.

## Cutover order (everything that can fail non-destructively runs BEFORE any destruction)

1. Merge contracts (0.34.0), then processing; build the multileg image against 0.34.0 and confirm its resolved digest.
2. PREFLIGHT - register and verify all three subjects on the TARGET registry (non-mutating on refusal; a failure here stops the
   cutover with production untouched):
   `SCHEMA_REGISTRY_URL=<registry> SCHEMAS_DIR=<contracts>/schemas/multileg scripts/kafka/ensure-multileg-schemas.sh`
   Re-run with `DEPLOY_DRY_RUN=true` to read-only confirm "latest ... is the canonical schema" for each subject.
3. Scale multileg-structure to 0 (the decision point: from here on the topics are wiped).
4. Wipe and recreate the three topics with the generic roll-forward script (PR from the roll-forward agent; confirm flags against its
   final usage text). Shape unchanged: 4 partitions, `cleanup.policy=delete`.
   `scripts/kafka/roll-forward-topics.sh --topics options.multileg.observations,options.multileg.window.status,options.multileg.sr.levels --partitions 4 --cleanup-policy delete`
5. Roll the new image; check that records on all three topics start with magic byte 0. Rollback before step 4 is just scaling
   the old image back up; after it, the topics are empty by design (no history is carried).
