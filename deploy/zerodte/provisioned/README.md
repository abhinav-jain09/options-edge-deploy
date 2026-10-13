# Confirmed provisioning receipts (one per environment)

`<env>.yaml` is committed by the operator from the `PROVISIONED` (or `ALREADY_PROVISIONED`) receipt line the Job printed — the wrapper prints
the exact block. Increment 8's runtime render reads `ZERO_DTE_PROVISIONING_GENERATION` and `ZERO_DTE_LEDGER_TOPIC_ID_EXPECTED` from it
and refuses a missing or mismatched identity; the VIRGIN values (`ZERO_DTE_VIRGIN_LEDGER_TOPIC_ID`, `ZERO_DTE_VIRGIN_CLUSTER_ID`,
`ZERO_DTE_ENVIRONMENT_LINEAGE_ID`) it derives ONLY from the attestation entry selected by this receipt's lineage. CI binds the receipt's
static fields (symbol, lineage, bootstrapKind, generation, eraId, eraStartSession) to the declaration of the same environment — the era's
start session is QUOTED (as every date in these files) and must equal the declaration's, which the Job verified against its own clock and
stored. Never hand-entered; a re-provisioning (a new generation) replaces the block through review.

```yaml
environment: dev
symbol: SPX
environmentLineageId: 6b2c7c1a-5d3e-4a8f-9b41-2f0d7e9c4a10
bootstrapKind: VIRGIN
generation: 1
eraId: 1
eraStartSession: "2026-10-05"
ledgerTopicId: "<32 lowercase hex — the live DEPLOYMENTS topic id>"
clusterId: "<the broker cluster id>"
provisionedDigest: "<64 hex — sha256 of the canonical PROVISIONED fields without operator/ts>"
ledgerOffset: 0
```
