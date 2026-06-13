# HPSF V2.1 Deployment And Operations Runbook

## Scope

HPSF V2.1 is a Databento-based, signal-only pipeline. It consumes OPRA TCBBO, ES trades, and SPX spot inputs, then emits signal, latest-signal, audit, strike-score, market-flow, DLQ, writer-DLQ, and exit-intent topics.

HPSF must never place orders, call IBKR order placement, or emit `orderInstruction.enabled=true`.

## Market Hours

Use the New York exchange calendar.

- Regular SPX options session: 09:30 to 16:00 America/New_York.
- Early-close sessions: normally 09:30 to 13:00 America/New_York when the exchange calendar marks an early close.
- Pre-market, post-market, weekends, and exchange holidays: HPSF should not emit BUY signals.
- Trade date, 0DTE expiry selection, VWAP session state, and rollover are based on America/New_York market date, not UTC date.

## Deployment Components

- `hpsf-stage-a-service`: Databento OPRA per-strike classification and strike-flow state.
- `hpsf-stage-b-service`: central market evaluator runtime placeholder. Keep topology disabled until the Stage B entrypoint is wired so it cannot run a duplicate Stage A topology.
- `hpsf-postgres-writer-service`: persists HPSF signal/audit/validation records and rejects any `orderInstruction.enabled=true` payload.
- `feed-gateway-service`: publishes HPSF latest-signal UI view models.
- OptionsEdge web UI: renders the HPSF dashboard above the option chain.

## Start Or Restart

```bash
kubectl -n options-edge rollout restart deployment/hpsf-stage-a-service
kubectl -n options-edge rollout restart deployment/hpsf-stage-b-service
kubectl -n options-edge rollout restart deployment/hpsf-postgres-writer-service
kubectl -n options-edge rollout restart deployment/feed-gateway-service
```

Verify rollout:

```bash
kubectl -n options-edge rollout status deployment/hpsf-stage-a-service --timeout=180s
kubectl -n options-edge rollout status deployment/hpsf-stage-b-service --timeout=180s
kubectl -n options-edge rollout status deployment/hpsf-postgres-writer-service --timeout=180s
```

## Stop HPSF

```bash
kubectl -n options-edge scale deployment/hpsf-stage-a-service --replicas=0
kubectl -n options-edge scale deployment/hpsf-stage-b-service --replicas=0
kubectl -n options-edge scale deployment/hpsf-postgres-writer-service --replicas=0
```

Stopping HPSF must not affect IBKR feed services. HPSF is Databento-scoped.

## Run In SHADOW Mode

Set `HPSF_MODE=SHADOW` on HPSF deployments and redeploy. SHADOW mode may maintain paper positions for validation, but it must not place real orders.

```bash
kubectl -n options-edge set env deployment/hpsf-stage-a-service HPSF_MODE=SHADOW
kubectl -n options-edge set env deployment/hpsf-stage-b-service HPSF_MODE=SHADOW
```

Signal-only verification:

```bash
kubectl -n options-edge get configmap options-edge-config -o yaml | grep -E 'HPSF_ORDER_PLACEMENT_ENABLED|ORDER_PLACEMENT_ENABLED'
scripts/smoke/check-hpsf-deployment.sh
```

Both order-placement flags must be `false`.

## Run In REPLAY Mode

Use a unique application id and earliest offsets so replay does not share live state.

```bash
kubectl -n options-edge set env deployment/hpsf-stage-a-service \
  HPSF_MODE=REPLAY \
  HPSF_STREAMS_APPLICATION_ID=options-edge-hpsf-stage-a-replay-$(date +%Y%m%d%H%M%S) \
  HPSF_STREAMS_AUTO_OFFSET_RESET=earliest
```

Replay must use Databento records and HPSF topics only. Do not involve IBKR feed services.

## Verify Topics

```bash
scripts/kafka/create-hpsf-topics.sh --dry-run
scripts/kafka/create-hpsf-topics.sh
scripts/kafka/verify-hpsf-topics.sh
```

Required RF=1 constraints:

- `replication.factor=1`
- `min.insync.replicas=1`
- `num.standby.replicas=0`
- `compression.type=lz4`
- `options.hpsf.strike-flow` retention is 2 days.
- `options.hpsf.strike-score` retention is 2 days.
- `options.hpsf.latest-signal` is compacted latest state.

RF=1 has no broker-failure durability. Do not describe this cluster as highly available.

## Verify Latest Signal

```bash
kafka-console-consumer \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --topic options.hpsf.latest-signal \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 10000
```

The UI must consume `options.hpsf.latest-signal` for the current decision, not historical `options.hpsf.signal`.

## Verify DLQs

```bash
kafka-console-consumer --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --topic options.hpsf.dlq --from-beginning --max-messages 1 --timeout-ms 5000 || true
kafka-console-consumer --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --topic options.hpsf.writer-dlq --from-beginning --max-messages 1 --timeout-ms 5000 || true
```

Any DLQ growth should be investigated before trusting BUY signals.

## Databento Replay Recovery

If Databento mapping or source lag causes bad signals:

1. Stop HPSF deployments.
2. Keep raw Databento records immutable.
3. Fix mapper/config issue.
4. Start REPLAY mode with a unique application id and earliest offsets.
5. Compare replayed latest-signal/audit outputs against expected deterministic fixtures.
6. Return to LIVE_SIGNAL_ONLY or SHADOW only after replay matches the expected contract.

## Remote Smoke

```bash
scripts/smoke/check-hpsf-deployment.sh
```

Use `REQUIRE_LATEST_SIGNAL=true` only when deterministic fixtures or live market records are expected to produce a latest-signal message during the smoke window.

## Rollback

Record these before each Jenkins deploy:

- Previous Git SHA.
- New Git SHA.
- Previous image tag.
- New image tag.
- Config version.
- Jenkins build number.

Rollback by redeploying the previous image tag and config version:

```bash
kubectl -n options-edge set image deployment/hpsf-stage-a-service hpsf-stage-a=192.168.100.252:5000/options-edge-hpsf-processing:<previous-tag>
kubectl -n options-edge set image deployment/hpsf-postgres-writer-service hpsf-postgres-writer=192.168.100.252:5000/options-edge-hpsf-postgres-writer:<previous-tag>
```

## No Live-Money Approval

There is no approval path for live order placement in HPSF V2.1. If a config, ticket, or operator asks for real orders, reject that part and keep HPSF signal-only.
