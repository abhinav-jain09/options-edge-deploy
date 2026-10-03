# 0DTE Stage-B provisioning (design STAGE-B-FRAMES-DESIGN.md §5.1 / §5.3 / §5.4, "BUILD decisions for increment 7")

What lives here is REVIEWED INPUT to the provisioning Job (`Jenkinsfile.zerodte-provision` → `scripts/ops/zerodte-provision.sh` →
`k8s/jobs/zerodte-provision-job.yaml` → the service image's `ZeroDteProvisioner`, options-edge-processing increment 7a):

* `provisioning/<env>.yaml` — ONE file per environment: the desired topology and generation DECLARATION. Topics are named by Kafka
  topic NAME, never by id (ids exist only after creation and are read live by the Job). `eraId` is a reviewed positive integer (a dry run
  must not allocate a database sequence value). A generation bump is explicit (`migration: true`, `previousGeneration`, `recreatedTopics`,
  `modeChange`). Validated by `scripts/ci/validate-zerodte-provisioning.sh` with the SAME rules the Job's parser enforces.
* `virgin-attestation.yaml` — the ONE-SHOT external VIRGIN attestation: `lineages` (an environment is a LINEAGE: a UUID minted once, with
  the OWNER's recorded approval; a clone names its parent) and append-only `entries` ({symbol, environmentLineageId, ledgerTopicId,
  clusterId, createdAt, operator, prevEntryHash}), hash-chained: the first `prevEntryHash` is 64 zeros, each later one is the SHA-256 of
  the canonical TLV encoding of the COMPLETE preceding entry. `ledgerTopicId`, `clusterId` and `prevEntryHash` are written QUOTED (YAML
  would read digits as a number). The Job VERIFIES this file and never appends to it: an append is a reviewed Git change
  (`scripts/ci/validate-zerodte-attestation.sh` refuses any change that removes or alters an existing entry or lineage, or breaks the chain).
* `provisioned/<env>.yaml` — the CONFIRMED receipt of a provisioning (generation, eraId, ledgerTopicId, clusterId, provisionedDigest),
  committed by the operator from the Job's receipt line. The runtime render of increment 8 takes `ZERO_DTE_LEDGER_TOPIC_ID_EXPECTED` and
  `ZERO_DTE_PROVISIONING_GENERATION` from it and refuses a missing or mismatched identity. Never hand-entered.

## The order (one Job run = one receipt line; dry run first, confirm on CONFIRM)

1. the research schema is exactly v7 (increment 9's migration — a live provisioning cannot complete before it);
2. the declaration's output contract; every PRESENT topic verified (inputs are other services' topics: verified, never altered);
3. absent outputs created with the registered policy (§5.4) and verified in turn;
4. the live cluster id and topic ids;
5. the deployment ledger read whole under its HMAC key;
6. VIRGIN only: the attestation must name exactly this (ledgerTopicId, clusterId) — a first run stops with `ATTESTATION_REQUIRED` and
   prints the exact entry to add; the operator commits it through review; the next run continues;
7. the era row (create-or-verify under both unique keys);
8. `PROVISIONED` appended transactionally and READ BACK; the receipt line names the live ledger topic id.

## Owner-only items (stated once)

* The ledger key `ZERO_DTE_LEDGER_KEY` (≥ 64 hex): a Jenkins secret-text credential (`zerodte-ledger-key` / `zerodte-ledger-key-dev`)
  synced into `options-edge-runtime-secrets` by `Jenkinsfile.secrets-sync`. The Job receives it by `secretKeyRef`; it never appears in a
  manifest, a log or a receipt.
* The lineage approval: every lineage row of `virgin-attestation.yaml` carries `approvedBy` — the owner's name, written by the owner. The
  rows shipped by increment 7b carry the marker `UNAPPROVED` and are not usable until replaced.
* Branch protection of this repository (signed commits, a second reviewer, the append-only check as a required check) — design §5.1.
* Each run's `PERMITTED_SHA` (Deployment Permission Rule).
