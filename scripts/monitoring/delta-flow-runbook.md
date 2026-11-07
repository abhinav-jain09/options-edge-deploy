# Delta-Flow — monitoring runbook

Operator runbook for the `delta-flow.*` alert groups in `hpsf-alert-rules.yaml` (R19). Covers the
signal's sign convention, expected normal ranges, threshold tuning, and the Streams-state mapping.

## What the signal means (sign convention)

`signedDeltaFlow = sign(aggressor) × delta × contracts × contractMultiplier`, unit **SHARE_DELTA**.

- **Positive** = net **buyer-aggressor** share-delta at the strike (aggressive buying of positive-delta
  exposure / aggressive selling of negative-delta exposure).
- **Negative** = net **seller-aggressor** share-delta.
- It is **not** directly "bullish/bearish" — interpretation depends on the call/put mix and the
  dealer/customer perspective. The per-strike UI tooltip shows the call-net and put-net components plus
  the high-confidence subset so the operator can see what drives the net.

The R4 `confidenceWeightedNetDeltaFlow` field is **structurally 0 on OPRA** (the R11 side-accuracy gate
never calibrates because OPRA trades carry no aggressor side), which is why the UI surfaces
`sessionNetDeltaFlow`, not the confidence-weighted field.

## Expected normal ranges (from real backdata — SPXW 0DTE)

| Signal | Normal | Notes |
|--------|--------|-------|
| at-touch HIGH-confidence share | ~60% of trades | drives `highConfidenceNetDeltaFlow` |
| unknown-aggressor (midpoint) | ~15% | baseline for `DeltaFlowUnknownAggressorRateHigh` (fires >30%) |
| normal-market share | ~99.9% | `INVALID_SPREAD` ~1% |
| side-accuracy CI-low (OPRA) | N/A — uncalibrated | R11 stays UNKNOWN on OPRA by design |

## Threshold tuning process

Thresholds in `hpsf-alert-rules.yaml` are **backdata defaults**, not yet live-calibrated. Before
enforcing (paging) in a new environment:

1. Run the rules in **warning-only / no-page** mode for a few full RTH sessions.
2. Pull the actual distributions for each alert's metric (`delta_flow_stale_greek_total`,
   `delta_flow_unknown_aggressor_rate`, `delta_flow_condition_rejected_total`, `..._ci_low`, etc.).
3. Set each threshold a margin above normal variance (e.g. p99 of the quiet-market distribution).
4. Re-check after any producer/classifier change — a code change can shift the baselines.

## Market-hours gating (`DeltaFlowNoFreshEvents`)

`DeltaFlowNoFreshEvents` has a **coarse UTC** PromQL gate (weekday 13:30–21:00 UTC, covering RTH under
both EDT and EST). It is **not** holiday/early-close aware — it can false-page on a market holiday. The
correct long-term fix is a service-emitted `delta_flow_market_open` metric; replace the time predicate
with it when available, and wire the OPRA/NYSE holiday calendar.

## Kafka Streams state ordinals (`DeltaFlowStreamsError`)

The alert assumes numeric `delta_flow_kafka_streams_state` ordinals:

`CREATED=0  REBALANCING=1  RUNNING=2  PENDING_SHUTDOWN=3  NOT_RUNNING=4  PENDING_ERROR=5  ERROR=6`

Healthy = 1 or 2; `> 2` = shutdown/error. `-1` = feature disabled, `-2` = not started. If the service's
metric mapping ever changes, update this rule and this table together.

## Alert → action quick reference

| Alert | First check |
|-------|-------------|
| DeltaFlowStreamsError / NotReady | pod logs; is Kafka reachable; did Streams die at boot? |
| DeltaFlowNonFiniteOutput | **page** — a math/guard defect produced a non-finite signed-delta |
| DeltaFlowStaleGreekRateHigh | GEX surface lagging — check the gex producer |
| DeltaFlowUnknownAggressorRateHigh | quote quality / classifier regression (baseline ~15%) |
| DeltaFlowConditionRejectSurge | feed condition/action metadata degraded (fail-closed spike) |
| DeltaFlowSideAccuracyBelowFloor | classifier regressed (only meaningful where an aggressor benchmark exists) |
| DeltaFlowClusterUncalibrated | cluster calibration off → weighted signal suppressed (inert on OPRA) |
| DeltaFlowCancelUnlinkableRateHigh | feed lacks prior-trade ref (R16) — expected on OPRA |
| DeltaFlowPositioningUnknownRateHigh | OI missing/stale (R1) — only when positioning enabled |
