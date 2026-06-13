# HPSF V2.1 Release Notes

## Release Scope

HPSF V2.1 is a Databento-based, signal-only SPX option-flow signal system. It consumes Databento OPRA TCBBO option flow, Databento GLBX.MDP3 ES trades, and SPX spot reference data, then emits HPSF signal topics and UI-ready views.

This release does not add live trading. It must not place real orders, call IBKR order placement, or emit an order instruction with `enabled` set to true.

## Modes

- `LIVE_SIGNAL_ONLY`: emits informational HPSF signals only.
- `SHADOW`: SHADOW mode emits informational signals and paper-position validation only.
- `REPLAY`: uses isolated application ids and earliest offsets for deterministic replay.

## Release Gate Summary

Before trusting SHADOW or live signal-only observation, these gates must pass:

- Source docs aligned.
- Contracts frozen.
- Databento mapping tests pass.
- Kafka topics created with RF=1.
- Stage A emits strike-flow only.
- Stage B emits exactly one signal per evaluation id.
- `options.hpsf.latest-signal` works for current state.
- Final signal JSON matches `optionedge_hpsf_v2_expected_output_contract.md`.
- `orderInstruction.enabled` is always false.
- Freshness and lag gates force NO_TRADE.
- VWAP soft/hard retest logic works.
- Mixed flow blocks trades.
- Bad liquidity blocks trades.
- Far OTM anchor cannot become the execution strike.
- Postgres writer is separate from signal generation.
- Writer DLQ works.
- Validation ledger works.
- Shadow paper positions work without real orders.
- Exit intent is informational only.
- UI shows signal, execution strike, flow anchor, data health, reasons, and shadow validation result.
- Monitoring and runbook exist.
- Replay mode is isolated.
- No IBKR order placement code path exists in HPSF.

## RF=1 Warning

RF=1 has no broker-failure durability. Abhinav's current two-broker Kafka cluster uses replication factor 1, `min.insync.replicas=1`, and `num.standby.replicas=0`. This is a capacity constraint, not high availability. A broker failure can lose HPSF topic data.

## Signal-Only Warning

HPSF V2.1 is signal-only. BUY_CALL_* and BUY_PUT_* are informational outputs, not order instructions. Do not enable any order-placement flag for HPSF.

## Deployment Notes

- HPSF runtime artifacts are `options-edge-hpsf-processing` and `options-edge-hpsf-postgres-writer`.
- HPSF deployment is Databento-scoped.
- Do not modify or depend on IBKR feed/services for this release.
- UI current state must come from `options.hpsf.latest-signal`, not historical `options.hpsf.signal`.

## Stage B Runtime

`hpsf-stage-b-service` runs the dedicated Stage B Kafka Streams topology. It consumes `options.hpsf.strike-flow`, Databento ES trades, and SPX spot reference data, then emits `options.hpsf.signal`, `options.hpsf.latest-signal`, `options.hpsf.market-flow`, `options.hpsf.strike-score`, and `options.hpsf.audit`. Do not deploy Stage B with topology disabled in a release profile.
