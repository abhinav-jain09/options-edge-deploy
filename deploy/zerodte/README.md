# 0DTE Stage-B provisioning (design STAGE-B-FRAMES-DESIGN.md §5.1 / §5.3 / §5.4, "BUILD decisions for increment 7")

What lives here is REVIEWED INPUT to the provisioning Job (`Jenkinsfile.zerodte-provision` → `scripts/ops/zerodte-provision.sh` →
`k8s/jobs/zerodte-provision-job.yaml` → the service image's `ZeroDteProvisioner`, options-edge-processing increment 7a):

* `provisioning/<env>.yaml` — ONE file per environment: the desired topology and generation DECLARATION. Topics are named by Kafka
  topic NAME, never by id (ids exist only after creation and are read live by the Job). `eraId` is a reviewed positive integer (a dry run
  must not allocate a database sequence value). `eraStartSession` (inc 9 consult Q6) is the session the era starts on — the session of the
  run, or the next session once today's has closed: the Job computes the same at the insert and REFUSES a stale value
  (`ERA_START_SESSION_STALE`, 68) rather than moving it, so edit it to the run's session before each attempt; once inserted it is verified
  under both unique keys on every later run (a rerun after the close is ALREADY_PROVISIONED). A generation bump is explicit (`migration: true`, `previousGeneration`, `recreatedTopics`,
  `modeChange`). Validated by `scripts/ci/validate-zerodte-provisioning.sh` with the SAME rules the Job's parser enforces.
* `virgin-attestation.yaml` — the ONE-SHOT external VIRGIN attestation: `lineages` (an environment is a LINEAGE: a UUID minted once, with
  the OWNER's recorded approval; a clone names its parent) and append-only `entries` ({symbol, environmentLineageId, ledgerTopicId,
  clusterId, createdAt, operator, prevEntryHash}), hash-chained: the first `prevEntryHash` is 64 zeros, each later one is the SHA-256 of
  the canonical TLV encoding of the COMPLETE preceding entry. `ledgerTopicId`, `clusterId` and `prevEntryHash` are written QUOTED (YAML
  would read digits as a number). The Job VERIFIES this file and never appends to it: an append is a reviewed Git change
  (`scripts/ci/validate-zerodte-attestation.sh` refuses any change that removes or alters an existing entry or lineage, or breaks the chain).
* `provisioned/<env>.yaml` — the CONFIRMED receipt of a provisioning (generation, eraId, eraStartSession, ledgerTopicId, clusterId, provisionedDigest),
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
  committed `provisioned/<env>.yaml` to `provisioning/<env>.yaml` (symbol, lineage, bootstrapKind, generation, eraId, eraStartSession must agree).

## The research-store migration (increment 9c — `Jenkinsfile.zerodte-research-migrate`)

Before any provisioning can complete, the research store must be at schema v7 (the provisioner refuses otherwise, `SCHEMA_VERSION`). The
migration is the service image's own `ZeroDteResearchMigrator` (increment 9a) run as a Job (`k8s/jobs/zerodte-research-migrate-job.yaml`)
by `scripts/ops/zerodte-research-migrate.sh` under the REVIEWED declaration `research-migration/<env>.yaml` (fromVersion 6, toVersion 7,
the shipped calendar's version digest the image must carry, how far ahead the calendar must reach; validated by
`scripts/ci/validate-zerodte-research-migration.sh`). ONE transaction — the typed v6 archive, the v7 DDL and views, the calendar, the
migration record — EXECUTED AND ROLLED BACK on the dry run (its `MIGRATABLE` receipt carries the real dispositions), committed and
verified on CONFIRM (`MIGRATED`); a rerun at v7 verifies the recorded calendar version, every calendar row and the catalog digest
(`ALREADY_MIGRATED`) and never re-applies.

The wrapper repeats every guarantee of the provisioning wrapper (the CA-pinned cluster, the atomic lock `zerodte-research-migrate-lock`,
the same-build dry-run receipt, HEAD == PERMITTED_SHA re-checked) and holds the receipt to its CANONICAL GRAMMAR: one regular expression
per outcome — the tokens in the exact order `ZeroDteResearchMigrator` prints them, each in its domain, single spaces, nothing else — then
every `(reason, exit)` pair of a `REFUSED` line must be one the migrator emits, the container's exit code must agree, and
`calendarVersion` / `fromVersion` / `toVersion` are bound to the reviewed declaration.

THE QUIESCENCE PROOF (consult Q3). The migrator cannot tell from PostgreSQL whether a legacy (v6) in-process writer still runs, so the
wrapper proves it on the live cluster, UNDER ITS LOCK, from a CLOSED WORLD — `research-migration/legacy-writers.yaml` (the image
repository, the declared writer Deployments, the maintenance Jobs that run the same image and never write) — through
`scripts/ops/zerodte-quiescence.py`:

* every pod of the namespace that runs the service image (judged by spec image AND status `imageID`; app, init and ephemeral containers
  alike; exempt only when a LISTED Job — name and uid — owns it and that Job carries a declared maintenance label) must be Running on the
  digest-pinned image this Job runs, with `ZERODTE_RESEARCH_ENABLED` known OFF: for a RUNNING pod only a LITERAL in its own spec is
  evidence (see below — a ConfigMap, a Secret, a `fieldRef`, an expansion, a duplicate, an `envFrom` source that could carry the key:
  each a refusal); a literal governs over every `envFrom` (kubelet precedence); no entry and no source that could carry the key = OFF;
* a TERMINATING pod (still running through its grace period) or a Pending pod is waited for (`QUIESCE_WAIT_S`, default 120 s) and then
  refused;
* every Deployment / StatefulSet / DaemonSet / ReplicaSet / Job / CronJob whose pod template runs the image must be a declared writer
  Deployment (template pinned to the digest, flag off, rollout SETTLED: observed generation current, updated == available == desired,
  nothing unavailable), a ReplicaSet it owns, or an exempt maintenance Job — anything else is a refusal (a CronJob could create a writer
  pod at any moment);
* an unreadable API is never an empty one; the pod set is digested and RE-LISTED immediately before the Job is created — any change since
  the proof refuses the creation;
* the lock is also the DEPLOYMENT BARRIER: `scripts/deploy/service-deploy.sh` sources `scripts/deploy/zerodte-migrate-barrier.sh` and
  refuses to roll `vix-option-inteligence` while the lock exists (an unreadable lock state refuses too).

Only then is `--legacy-writers-quiesced` rendered into the Job; the migrator refuses without it.

THE BOUNDARY OF THIS PROOF, stated plainly: it covers the `options-edge` NAMESPACE of the pinned cluster. A writer in another namespace,
another cluster or a laptop holding the database credential is outside it — that is a credential / RBAC boundary (the research database's
role and password are a Jenkins-synced Secret of this namespace), not something this proof establishes. "Quiescent" means: no writer-capable
workload of this namespace can be running or be created by a controller of this namespace.

For a RUNNING pod only a LITERAL `ZERODTE_RESEARCH_ENABLED` in its own spec is evidence (a container's environment is captured when it
starts; a ConfigMap or Secret read now says nothing about what it read then), so the V1 compatibility rollout MUST set the flag as a
literal `"false"` (the base Deployment does); a ConfigMap-sourced flag on a live pod is a refusal. Controller TEMPLATES are judged by what
the kubelet will resolve for the next pod (a ConfigMap key is read; a Secret source refuses). A pod is exempt only when a LISTED Job owns it
(name and uid) and that Job carries a declared maintenance label. THE RENDER SOURCES SAY SO: the dev overlay patch
(`k8s/overlays/dev/vix-option-inteligence-dev-patch.yaml`), the generated dev slice and the production / experiment slices all carry
`ZERODTE_RESEARCH_ENABLED: "false"` as a literal (`scripts/ci/zerodte-compat-flag-test.sh` renders every overlay and asserts it) — the
compatibility rollout is the sanctioned deploy of exactly that render.

ORDER ON EVERY ENVIRONMENT (consult Q12): roll the V1 compatibility image with `ZERODTE_RESEARCH_ENABLED` off to the declared Deployment
and let it settle → this job (dry run, then CONFIRM) → the provisioning job → the dedicated v7 writer (9e) → increment 8.
`scripts/ci/zerodte-research-migrate-receipt-test.sh` drives the wrapper through 200+ cases (the count is printed by the test) against a fake kubectl, a fake clock and the
Kubernetes fixtures of `zerodte-research-migrate-fixtures.py` — every outcome and grammar violation, every quiescence refusal, the race,
the timeout, the unreadable log — and CAPTURES the Job manifest the wrapper creates, executing its container's shell block against a fake
`java` to prove the attestation and the mode reach the migrator's argv; `zerodte-migrate-barrier-test.sh` the barrier; 
`zerodte-research-migrate-guard-test.sh` the guard stage through 29.

### Release evidence (capacity) — recorded before the first CONFIRM of each environment

The Job's `activeDeadlineSeconds` is 900 under 896 Mi / 1 Gi (`-Xmx768m`). That ceiling is not assumed: the wrapper prints the Job's wall
time (`job wall time: Ns`) after every run, and the DRY RUN that precedes every CONFIRM in the same build executes the whole migration
against the REAL store and rolls it back. ENFORCED, not only recorded: the dry-run receipt carries `wall=<s>` and the CONFIRM stage of the
same build refuses unless that wall time is at most `CONFIRM_MAX_DRY_RUN_WALL_S` (default 600 s, two thirds of the deadline; a missing or
unmeasurable wall time refuses too). Record each environment's dry-run wall time here as well, from the build log, before its first CONFIRM:

| store | date | build | v6 feature families | dry-run wall time | notes |
|---|---|---|---|---|---|
| local reference (Apple-silicon laptop, Homebrew PostgreSQL 16, synthetic v6 store: 50 001 feature families, 200 004 predictions, 50 000 outcomes, 13 MB feature table) | 2026-10-04 | — (the migrator CLI from the 9a build, `-Xmx768m`) | 50 001 | 17.7 s wall, JVM max RSS 120 MiB | the rate bound (~2 800 families/s on this machine); not an environment |
| dev | — | — | — | — | fill from the dev dry run |
| production | — | — | — | — | fill from the production dry run; CONFIRM only after it is under the ceiling with margin |

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
  prints the new one), so a copy that drifted from the pinned version fails its runner; `scripts/ci/validate-zerodte-corpus-peer.sh` holds the
  two LITERALS to each other (it reads the peer repository's at a ref — a peer that cannot be read is a refusal) and runs in the provisioning
  pipeline's validation stage before any cluster is touched. The subset both readers share is listed in the design
  ("Increment 7b — AS BUILT" item 2): no BOM, no document markers or directives, plain identifier keys, `key: value` with a space, one line per
  scalar in every style, `[]` written exactly, no flow maps, lower-case booleans, `''` as one apostrophe in single quotes, no escapes in
  double quotes, NO TAB ANYWHERE (not in indentation, not after a separator or a list dash, not trailing, not in a comment, not in a quoted
  scalar — SnakeYAML treats a tab as separation in some positions and as an error in others, and no hand parser mirrors that), `key: value`
  with at least one space after the colon (never a space before the colon),
  only printable characters anywhere (a DEL in a comment is refused), line breaks LF or CRLF only (a lone CR, NEL, LINE / PARAGRAPH SEPARATOR
  are refused anywhere), valid UTF-8 (an undecodable file is a refusal, never a crash), ≤ 2^16 code points counted over the RAW text (a CR
  counts; exactly 2^16 passes), plain scalars resolve to null / `true` / `false` / a signed decimal integer within a Java long BEFORE any domain
  is judged (an unquoted `123` in a string field is "… is a string" in both; a quoted `"123"` is a string), dates `yyyy-MM-dd` and instants
  `yyyy-MM-ddTHH:mm:ss[.fraction]Z` judged LEXICALLY in both (year 0001–9999, month 01–12, seconds 00–59, `Z` only) and then as real calendar
  values.
* Branch protection of this repository (signed commits, a second reviewer, the append-only check as a required check) — design §5.1.
* Each run's `PERMITTED_SHA` (Deployment Permission Rule).
