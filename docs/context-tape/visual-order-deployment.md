# Context Tape private execution deployment

Status: deployment source prepared on 17 October 2026. This is not rollout evidence or full-feature approval. Coordinated application drafts: processing #1066 and web #851. Both environments deploy reviewed main commits through the permitted-SHA Jenkins path.

## Runtime wiring

The existing broker-execution process serves its private JWT-authenticated API on port 8131. Its ClusterIP Service has no public ingress or NodePort. The web process proxies to `http://broker-execution-service:8131`, forwarding the original validated bearer and a separate shared credential. Dev uses the same dev issuer as the web deployment; production uses the production issuer. Browser mutation origins are explicit: local port 8090 in dev, `https://bleadingoptions.com` in production. There is no username/email/role allowlist. Existing database account grants and subject ownership remain required; deployment creates no grants.

HTTP `/health` is process health, used by startup/readiness/liveness probes. The trading readiness API separately checks the dispatcher, durable store, reconciled inventory, pinned account, broker session and upstream responsiveness. A healthy pod does not enable an order button by itself. No broker connection means an unavailable/red trading state, including production where no IBKR execution is expected. This change preserves the existing replica counts, PAPER mode, transmission setting, client IDs, source policies and account limits. It does not enable LIVE.

The new process explicitly requests 512 MiB and limits memory to 1 GiB, with a 512 MiB maximum Java heap and 128 MiB direct-memory bound. The remaining container budget is for metaspace, code cache, threads and native libraries. These are preparation settings, not measured throughput or soak claims. Permitted-host performance evidence remains a full-feature release gate; do not profile or load-test the Mac or production.

## Shared credential

Shared infrastructure owns Secret `context-tape-execution-auth`, key `token`. Both deployments reference it without copying secret material into Git or rendered workload manifests. `ensure-context-tape-execution-auth.sh` preserves a valid existing credential, refuses malformed data and creates a random credential only when absent. It never rotates one implicitly. Generation requires the permitted Jenkins guard and job identity, uses a private temporary file, removes that file on exit and prints no credential. A failed read cannot be treated as absence; a failed create must be followed by a valid canonical read.

`common-infra-deploy` supports `CONTEXT_TAPE_AUTH_ONLY=true`. After the normal main/SHA guard, this mode validates the credential plan and, only with `DEPLOY_DRY_RUN=false`, creates it if missing. Production retains an explicit Jenkins input gate. All ordinary manifest, PVC and realm stages are skipped in this mode. It allows credential preparation without applying unrelated shared infrastructure. Normal shared-infra runs also prepare the credential, while their dry-run mode changes nothing.

## Release sequence

1. Finish the full-feature requirement reconciliation, changed-code Claude review and subsequent Codex review. This deployment source and the two application drafts have no final whole-feature approval yet.
2. Recheck exact current main commits, Jenkins main SCM definitions, guard enforcement and automatic triggers. Use full immutable permitted SHAs for each repository/child build.
3. Execute the reviewed broker Avro schema/retained-record boundary cutover described in processing `docs/broker-order-avro-cutover.md`. Old JSON records must not be replayed through the new Avro boundary.
4. Run the guarded credential-only common-infra operation separately for dev and production. Verify the Secret's presence/validity without printing its value.
5. Build the reviewed processing/web main commits in Jenkins, record their image-lock digests, and use the guarded per-service deploy jobs with those exact digests and deployment-repository main SHA. A zero-replica production broker remains zero unless separately authorized to run.
6. Verify the workload digests/replica states and process probes. Verify authenticated generic controls for ordinary users and no-username JWTs, denial of private ungranted data and an unavailable execution state without IBKR. Confirm connected dev readiness only from actual end-to-end evidence. No test order is implied by deployment.
7. Record build URLs, exact source/image identities and smoke outcomes before describing either environment as deployed. Do not downgrade a broker schema/runtime while possibly sent commands or exposure remain unresolved.

## Local validation

`python3 -m unittest discover -s tests -p test_context_tape_execution_deploy.py` passes seven tests: no mutation in dry-run, preservation/redaction, invalid existing credential, guarded creation/temporary-file cleanup, read/create failures, credential-only stage isolation, and rendered issuer/token/port/origin agreement for both environments.

Scoped `scripts/ci/validate-services.sh` checks pass for broker-execution-service and web in dev and production, covering service blast radius, image identity and standalone/monolithic render equivalence. `bash -n` validates the two changed shell scripts. These checks use mocks/local renders and perform no deployment.
