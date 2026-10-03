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

## The wrapper's guarantees (`scripts/ops/zerodte-provision.sh`)

* ONE run at a time, cluster-wide: an ATOMIC `create` of the ConfigMap `zerodte-provision-lock` (its holder recorded on it) before the
  inventory and the Job; released by its owner only, and only once no Job of this run can still be running — no Job was created, the Job
  was observed terminal (kept for post-mortem) or absent, or a still-active Job was deleted and its absence re-observed. An unreadable API
  or a failed delete RETAINS the lock and says so. A lock a crashed run left behind is refused with its holder: read that run's log, confirm
  the Job is gone or terminal, then `kubectl -n options-edge delete configmap zerodte-provision-lock` by hand. (The deployer can delete the
  lock: it is a cooperative lock, not an adversarial one — that is the break-glass path, taken only after reading the log.)
* The receipt: `wouldCreate` is `NONE` or a non-empty, duplicate-free, ORDERED subset of `FRAMES,HEAD,CURRENT,PULSE,DEPLOYMENTS`;
  `clusterId` is `[A-Za-z0-9._-]+` on every outcome that carries it.
* The receipt to the letter: every outcome has an EXACT field set (no extra token, no missing field, no empty value, every field counted once
  and checked by its domain — hex lengths, the role names, YES/NO/VERIFY, RESOLVED/PENDING vs the digest), the static identity matched to the
  declaration, and the container's exit code must AGREE with the outcome (PROVISIONABLE/PROVISIONED/ALREADY 0, CONFLICTING 66,
  ATTESTATION_REQUIRED 67, REFUSED its own `exit=`). A receipt the process disagrees with is refused.
* The committed receipt block carries `environmentLineageId` and `bootstrapKind` as well; `validate-zerodte-provisioning.sh` binds a
  committed `provisioned/<env>.yaml` to `provisioning/<env>.yaml` (symbol, lineage, bootstrapKind, generation, eraId must agree).

## Owner-only items (stated once)

* The ledger key `ZERO_DTE_LEDGER_KEY` (≥ 64 hex): a Jenkins secret-text credential (`zerodte-ledger-key` / `zerodte-ledger-key-dev`)
  synced into `options-edge-runtime-secrets` by `Jenkinsfile.secrets-sync`. The Job receives it by `secretKeyRef`; it never appears in a
  manifest, a log or a receipt.
* The lineage approval: every lineage row of `virgin-attestation.yaml` carries `approvedBy` — the owner's name, written by the owner. The
  rows shipped by increment 7b carry the marker `UNAPPROVED`, and BOTH readers (the Job's `VirginAttestation` and
  `scripts/ci/zerodte_attestation.py`) REFUSE a lineage that carries it: until the owner's edit the validators, the wrapper and the Job
  all stop on exactly that marker, so nothing can be provisioned under an unapproved lineage (and append-only forbids changing the row later —
  the approval must be in the founding row). `validate-zerodte-attestation-test.sh` asserts that refusal while the marker is there, and the pass after.
* The cluster pins, `clusters.yaml`: each environment's cluster is identified by the sha256 fingerprint of the CA its deployer kubeconfig
  carries (production by its API server address too) — not by a kubeconfig's cluster NAME, which anyone can relabel, and not by an
  environment variable. A pin changes only through review. Certificates are public; nothing in that file is a secret. The pin binds the TLS
  trust anchor, so a kubeconfig with `insecure-skip-tls-verify` or a CA given as a file path is refused; dev (no server pin) rests on its CA
  being unique to that cluster.
* The ledger key in `Jenkinsfile.secrets-sync`: no credential bound ⇒ the Secret's current value is KEPT (read and decoded; an unreadable
  or undecodable Secret, or a key that is PRESENT BUT EMPTY, stops the sync before any apply — presence is judged separately from the value;
  only a Secret or a key that does not exist yet is written empty); a bound credential must be 64+ hex.
  `scripts/ci/secrets-sync-ledger-key-test.sh` drives the stage's exact shell block through every case (13).
* ONE canonical YAML corpus: `scripts/ci/fixtures/zerodte/corpus/` is the source; the Job's test resources carry a verbatim copy. `corpus.sha256`
  (regenerated by `zerodte_attestation.py --corpus-manifest`) describes the fixtures, and `CORPUS_DIGEST` — the sha256 of `corpus.sha256` — is a
  LITERAL pinned in `zerodte_attestation.py` AND in the Job's `YamlSubsetCorpusTest`: a corpus change must update both literals (the command
  prints the new one), so a copy that drifted from the pinned version fails its runner. The subset both readers share is listed in the design
  ("Increment 7b — AS BUILT" item 2): no BOM, no document markers or directives, plain identifier keys, `key: value` with a space, one line per
  scalar in every style, `[]` written exactly, no flow maps, lower-case booleans, `''` as one apostrophe in single quotes, no escapes in
  double quotes, no tabs in indentation, ≤ 2^20 code points.
* Branch protection of this repository (signed commits, a second reviewer, the append-only check as a required check) — design §5.1.
* Each run's `PERMITTED_SHA` (Deployment Permission Rule).
