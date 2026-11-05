# Dev web CPU reservation recovery — 2026-11-05

Scope: only dev web in both mirrored overlays. Change CPU request 200m to 100m, retain no CPU limit, 768Mi memory request/4Gi limit, readiness, minReadySeconds, maxSurge=1/maxUnavailable=0. CPU request also sets relative scheduling share under contention; this is not a latency qualification.

Observed on verified dev API https://192.168.100.102:6443: Ready desktop-control-plane, allocatable CPU 16000m, assigned requests 15900m. The only Pending pod is this web replacement requesting 200m, with FailedScheduling/Insufficient cpu. The first replacement at 100m is an exact fit. After old 200m web terminates, steady requests become 15800m, leaving 200m for the next 100m surge. Values are snapshots, not permanent capacity claims.

Coordinator read-only preflight uses the existing `/Users/abhinav/.kube/dd-admin-102.yaml`, after verifying its API is exactly `https://192.168.100.102:6443`. This already-authorized credential supplies node/all-namespace read access. Jenkins keeps its existing namespace-limited `/Users/abhinav/.kube/jenkins-deployer-dev-remote.kubeconfig`; no role is expanded. Use kubectl describe node allocated-request totals for init/overhead-aware accounting, plus live node and all-Pending-pod JSON.

Image provenance: successful `options-edge-web-deploy/1633`, exact source main `d6c7e631ab15f340c5a37114f7233922cce3ecb8`, linux/arm64. Its archived `.jenkins-tmp/options-edge-image-lock.env` was verified for source, build ID and guard version. Required full image reference:
`host.docker.internal:5001/options-edge-web:1633-d6c7e631ab15@sha256:e62d382995d82dd5c80e702b44a44f69bec9accac34cbabf3ef762e2523f7ccf`.
This is reuse of that guarded successful build, not an unbuilt manual image.

Jenkins parameters: `SERVICE=web`, `ENVIRONMENT=dev`, `BUILD_IMAGES=false`, `PERMITTED_SHA=<new merged deployment main SHA>`, `PERMITTED_SHA_GUARD_VERSION=2f6fc2ab7aa51eb8a9e8aa350f2d774cd2fb7a253de2591e43645a19a9886b24`, `REQUIRED_IMAGE=<full reference above>`, `KUBECONFIG_FILE=/Users/abhinav/.kube/jenkins-deployer-dev-remote.kubeconfig`. Other schema, recovery, cutover and restart flags false; no production target. First run `DEPLOY_DRY_RUN=true` and require SUCCESS from server-side apply validation with no env drift. Only then repeat identical parameters with `DEPLOY_DRY_RUN=false`.

Preconditions immediately before the Jenkins recovery submission:

1. Recheck dev API identity, node Ready, allocatable CPU and all assigned pod requests, including init-container/overhead accounting; require at least 100m unreserved CPU.
2. List Pending pods across namespaces. Permit only the superseded web replacement; stop if another workload competes for capacity.
3. Confirm no concurrent dev rollout is running. If still running, stop the superseded blocked Jenkins web rollout and confirm it ended before submitting recovery through the same serialized service-deploy job.
4. Pin the already-reviewed web image digest e62d382995d82dd5c80e702b44a44f69bec9accac34cbabf3ef762e2523f7ccf and the exact merged deployment main SHA. BUILD_IMAGES=false; no production or schema changes.

The scheduler and kubelet retain their admission checks. A resource race may leave the replacement Pending or Failed/Outofcpu while the old Ready pod remains available. Inspect failed pods and retain evidence; do not manually delete the old pod. Never force node assignment, change availability semantics, delete the old pod, or reduce the reservation further to defeat scheduling. Existing service-deploy has a replace --force fallback for a specific env-schema drift. The mandatory clean server-side dry run screens that drift before apply; any fallback line in the actual console is an unexpected failure requiring immediate inspection, not a successful safe rollout. This document does not claim that monitoring can undo deletion if a race introduces drift after dry run. A blocked attempt is not success. Recheck capacity and report the blocker; retain a bounded rollout timeout.

Post-rollout hard gates (not established by this document):

- Jenkins SUCCESS; one Ready web replica using the pinned digest, live CPU request exactly 100m, memory request 768Mi/limit4Gi and no CPU limit. No restart loop or readiness failure.
- Confirm the dev route responds and the authenticated Order tab loads changing real data.
- Native Edge unsent-draft drag, hover, scroll, resize, labels and tray behavior. No actual order/armed exit gesture.
- Capture current-source __otPerf and console evidence. Baseline is recorded in options-edge/docs/order-page-performance-review.md at source d6c7e631: 46.82s, render p95 34.2ms, max46.8ms, 34 long tasks/max375ms, approximately1500 SVG nodes. That streaming snapshot is not a paired drag baseline. Collect a new 45s streaming sample and draft drag sample, with workload/node count recorded. Required drag target: p95 orderInteraction below8ms, no pointer-driven root clears, no undefined-reference errors, retained candles, correct preview/labels. Treat worse full-render p95 than34.2ms or new long tasks over375ms as a regression signal to investigate, not a valid causal comparison across unmatched workloads. No 60fps or latency claim from synthetic fixtures.
- If any behavior fails, fix/review/redeploy/retest before marking that behavior working. Reverting to the prior image through Jenkins remains available; retaining the 100m request still needs the same contention verification.

Merge review gates code readiness. Real-dev checks gate completion. The initial exact fit is disclosed and can fail to schedule if capacity changes; preserving the old endpoint makes that a bounded non-destructive failure rather than a guarantee of rollout success.

Owner policy is read from the local workspace file `/Users/abhinav/Documents/Codex/2026-10-28/where-you-type-it-in-a/work/web-interaction-perf/rule.md`, SHA256 `c72bfb576c23443e8693f22cb95e248ff9131b0679b3fc4cea31a945b95145c6`, lines1030–1053. It is not asserted to exist in commit d6c7e631 or in this deployment repository. The operative instruction is: "the review gates the merge, this test gates the word \"done\"". Every promised behavior must be tested by Codex against real dev before being called working. This runbook independently adopts that completion gate.
