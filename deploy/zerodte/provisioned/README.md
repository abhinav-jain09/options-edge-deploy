# Confirmed provisioning receipts (one per environment)

`<env>.yaml` is committed by the operator from the `PROVISIONED` (or `ALREADY_PROVISIONED`) receipt line the Job printed — the wrapper prints
the exact block. Increment 8's runtime render reads `ZERO_DTE_PROVISIONING_GENERATION` and `ZERO_DTE_LEDGER_TOPIC_ID_EXPECTED` from it
and refuses a missing or mismatched identity. Never hand-entered; a re-provisioning (a new generation) replaces the block through review.

```yaml
environment: dev
symbol: SPX
generation: 1
eraId: 1
ledgerTopicId: "<32 lowercase hex — the live DEPLOYMENTS topic id>"
clusterId: "<the broker cluster id>"
provisionedDigest: "<64 hex — sha256 of the canonical PROVISIONED fields without operator/ts>"
ledgerOffset: 0
```
