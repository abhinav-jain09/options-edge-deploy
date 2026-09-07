1. **MEDIUM — `scripts/ci/validate-mirrored-topic-contracts.sh:185-187`, triggered by `Jenkinsfile.es-cvd-mirror:31-32`** — The new multiline `choices` list escapes the mirror-contract validator. `job_topics()` examines only the physical line containing `choice(name: 'TOPIC'...)`; consequently the validator reports only the original three CVD topics and checks none of the four footprint arms. A future policy/retention/partition mismatch could therefore merge despite this supposedly fail-closed guard. Make the choice declaration single-line or update the parser and add footprint-specific tests.

2. **LOW — `scripts/kafka/topics.env:502-504`** — The remaining es4 declaration comment still says the service creates the four topics “PURE COMPACT,” directly contradicting the corrected executable policy. The same stale claim appears in `k8s/es4/services/es-cvd.yaml:115-118` and `scripts/ops/archive/oe-topics.env:76-84`. These do not alter Kafka behavior, but they misdocument the runtime contract and could cause the defect to be reintroduced.

All executable contracts otherwise reconcile correctly:

- Source and target: one partition, `cleanup.policy=delete`; bars/outcomes `retention.ms=-1`; live/evidence `43200000`.
- Dev/prod topic declarations, exact-partition membership, retention overrides, reset-rebuildable classification, non-compaction, and archive inventory are complete and consistent with `es.futures.cvd.bars`.
- Gateway variable names exactly match `GatewaySettings`; all values correctly use the mirrored `es.` names. Base/dev/prod/experiment parity holds. Experiment is harmless without a mirror because absent footprint topics are deferred and retried rather than assigned.
- The archive correctly includes only bars/outcomes; continuous live/evidence topics are intentionally excluded.
- Clean resets can rebuild target history from es4.
- The new Bash case arms are syntactically and semantically valid.
- No additional repository change is required: the producer and gateway heads are already ancestors of their respective `origin/main`. Operationally, the topic reconciliation must run first, then the Jenkins mirror must be installed once for each target/topic pair—eight launchd units total—for records to start arriving.

The data-quality reconciliation workflow guided the source/target/declaration cross-check.

VERDICT: REQUEST_CHANGES
