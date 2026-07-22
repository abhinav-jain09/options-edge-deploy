# es-gex-spx-align → es-spx-align cutover + strike-intel enablement runbook

Two independent changes ship together in the deploy rename commit but are enabled on **different
schedules**:

1. **The rename** (`es-gex-spx-align-service` → `es-spx-align-service`) — cut over immediately; the live
   ES-GEX-on-SPX overlay must not break.
2. **The new strike-intel data type** — ships **dark** (`ES_SPX_ALIGN_STRIKE_INTEL_ENABLED=false`); enable
   per-env only after its mirror + input topic are verified.

The processing image (`options-edge-es-spx-align`) rotates the Streams `application.id` to
`es-spx-align-service`, so the new workload starts with fresh state and rebuilds the GEX book from the live
bridge. The GEX data topics are unchanged (`es.options.databento.gex.spxbridge` in, `options.es-gex-spx-aligned`
out), so the overlay is byte-identical once the new pod is caught up.

## Part 1 — rename cutover (do first, any time)

### Why the old workload must die
Both `es-gex-spx-align-service` (old) and `es-spx-align-service` (new) write the SAME compacted
`options.es-gex-spx-aligned`. Two producers with independent `emitEventTimeMs` stamps fight the gateway's
roll-forward (latest-emit-wins) and **flicker the overlay**. `kubectl apply -k` never prunes, so the old
Deployment is reconcile-deleted in `scripts/deploy/apply.sh` as part of the monolith apply.

Every deploy path that could run BOTH identities is covered so there is no dual-producer window:

| Path | Old-deployment delete |
| --- | --- |
| monolith `apply.sh` (prod/dev `service-deploy` full apply, `bring-up-all`) | automatic — reconcile-delete immediately before `kubectl apply -k` |
| `Jenkinsfile.experiment-deploy` (.2 monolith) | automatic — old workload deleted BEFORE `apply -k` (delete-first) |
| standalone fast-path (`service-deploy SERVICE=es-spx-align`) | **manual, delete FIRST** (below) |

### Steps
1. **Monolith path (recommended for the cutover)** — `service-deploy`/`bring-up-all` that runs the monolith
   `apply.sh` will, in one atomic apply: delete `deployment/es-gex-spx-align-service`, then apply the renamed
   `es-spx-align-service`. The experiment (.2) monolith does the same via `Jenkinsfile.experiment-deploy`.
   Prefer a monolith path for the cutover so the delete is guaranteed.
2. **Standalone fast-path** — `service-deploy SERVICE=es-spx-align` applies ONLY the new slice; it does NOT
   run the monolith reconcile-delete. To avoid a dual-producer window, **delete the old workload BEFORE
   deploying the new one** (delete-first ordering — never apply the new slice while the old is still up):
   ```
   # 1) stop the old producer FIRST
   kubectl -n options-edge delete deployment/es-gex-spx-align-service --ignore-not-found=true
   # 2) THEN deploy the renamed service
   #    (Jenkins) service-deploy SERVICE=es-spx-align ENVIRONMENT=<env> BUILD_IMAGES=true
   ```
3. **Readiness validation**: `kubectl -n options-edge rollout status deploy/es-spx-align-service` READY, its
   `/health/ready` 200, and the gateway `es-gex` cache non-empty (overlay still renders on the SPX board).
4. **Rollback**: re-deploy the previous commit (restores `es-gex-spx-align-service`); the old + new share no
   state, so a rollback is a clean redeploy of the prior identity.

### Post-cutover Kafka cleanup (separate, after new pod is healthy — NEVER during the initial deploy)
The old app-id leaves orphan internal topics. Delete only after `es-spx-align-service` is healthy and the old
Deployment is gone:
```
# on the SPX broker (.252): list then delete the orphaned old-identity internal topics
kafka-topics --bootstrap-server <spx-broker> --list | grep '^es-gex-spx-align-service-'
#   → es-gex-spx-align-service-*-changelog, es-gex-spx-align-service-*-repartition
kafka-topics --bootstrap-server <spx-broker> --delete --topic 'es-gex-spx-align-service-.*'
```

## Part 2 — enable strike-intel per env (only after prerequisites pass)

Ships `ES_SPX_ALIGN_STRIKE_INTEL_ENABLED=false`. Do NOT flip it on until, for the target env's SPX cluster:

1. **Mirror running**: an `es-strike-intel-mirror` job (MM1 `kafka-mirror-maker`, same shape as
   `es-gex-mirror`) byte-copies `es.strike-intelligence-by-strike` from es4 → the env's SPX cluster with the
   IDENTITY name (no `es4.` prefix). Confirm records are flowing:
   ```
   kafka-console-consumer --bootstrap-server <spx-broker> --topic es.strike-intelligence-by-strike --max-messages 1
   ```
2. **Input topic present**: `es.strike-intelligence-by-strike` exists on that cluster (the mirror creates it;
   partitions/retention track the es4 source — delete-policy, like the gateway's native strike-intel input).
3. **Output topic**: `options.es-strike-intel-spx-aligned` is self-provisioned COMPACTED by the service at
   boot (`ensureCompactedTopic`), the same way the live `options.es-gex-spx-aligned` is (neither is in the
   topics.env SSOT — the align service owns its output contract).

### Enable
Flip `ES_SPX_ALIGN_STRIKE_INTEL_ENABLED` to `"true"` for the env (base value for all, or a per-overlay patch
for a single env), redeploy `es-spx-align`, then verify:
- `es-spx-align-service` logs `strikeIntel[enabled=true …]`, output topic exists + is compacted;
- the gateway `es-strike-intel` overlay (Stage 3) renders ES signals at translated SPX strikes and withdraws
  on tombstone.

### Rollback
Flip back to `"false"` + redeploy. The dedicated `options.es-strike-intel-spx-aligned` topic can be purged
independently (nothing else uses it) if a clean slate is wanted.
