**Verdict: REQUEST_CHANGES.** The code diff is fine, and I'd approve it on its own. The runbook has four gaps that should be fixed before the controlled DEV attempt. I made no edits, deploys or mutations. I only ran read-only `kubectl get` calls against dev with the deployer kubeconfig.

## What I read
- All 30 added lines in `origin/main..HEAD` (commit `1d3dd480`).
- The untracked runbook.
- The previous Codex output (REQUEST_CHANGES) and the previous Claude outputs. The Fable run hit its usage limit and the r2 file is empty, so only the Sonnet APPROVE is usable.
- `Jenkinsfile.service-deploy`, `scripts/deploy/service-deploy.sh` and the web base manifest.

## The diff: PASS
- **Patch files:** both are 13 lines, byte-identical (blob `018f3746`). Each is a strategic-merge patch on `Deployment/options-edge-web`, container `web`, changing only `resources.requests.cpu` to `100m`.
- **Kustomizations:** both get one comment plus one `path:` entry, placed right after `web-dev-env-patch.yaml`. The mirror rule holds.
- **Render (`kubectl kustomize k8s/services/web/overlays/dev`):**
  - CPU request is 100m and memory is 768Mi / 4Gi.
  - There is no CPU limit.
  - `minReadySeconds` is 5, `progressDeadlineSeconds` is 1200, and the strategy is `maxSurge: 1` / `maxUnavailable: 0`.
- **Production:** unchanged at 200m, per the earlier renders.
- **Minor, not blocking:** the kustomization comment "Single-node dev has 100m spare CPU" is a point-in-time number and will go stale. The previous review raised this and it is still open.

## Runbook arithmetic: correct
The snapshot is 15900m of 16000m requested, and that 15900m includes the old 200m web pod.
- The surge pod at 100m brings the total to exactly 16000m, which fits.
- After the old pod terminates it settles at 15800m, leaving 200m free.

A live read confirms the shape: the deployment shows 2 replicas, 1 ready, 1 updated. Its spec still says 200m. The one Pending pod in `options-edge` is `options-edge-web-6b686845b-mbckr`, 11 minutes old.

## Findings that need changes
1. **Preconditions 1 and 2 can't run with the deployer credential.** I verified that `jenkins-deployer-dev-remote.kubeconfig` is forbidden from listing nodes and from listing pods across namespaces (`auth can-i` returns `no`). Allocatable CPU, total assigned requests and cluster-wide Pending pods need an admin read-only credential. `~/.kube/dd-admin-102.yaml` is a candidate, but I haven't confirmed it works. The runbook should name which credential to use, or the gate will be skipped or improvised.
2. **The Jenkins parameter block is incomplete.** It gives the image digest and "the exact merged main SHA" but not the full set of values:
   - **Missing:** `PERMITTED_SHA`, `PERMITTED_SHA_GUARD_VERSION`, `KUBECONFIG_FILE`, `DEPLOY_DRY_RUN` and `FORCE_RESTART`.
   - **Digest format:** `REQUIRED_IMAGE` must be `<registry>/<image>:<tag>@sha256:…`, not a bare digest.
   - **Provenance:** the earlier recovery note pinned a different digest (`258d83…`, build 1586). The runbook's `e62d38…` has no recorded provenance.
   - **Guard:** `Jenkinsfile.service-deploy` documents `REQUIRED_IMAGE` as normally set by a guarded image job, so a manual value needs a stated justification.
3. **`kubectl replace --force` fallback is not addressed.** `service-deploy.sh` around lines 452–517 falls back to a delete and recreate of the Deployment when apply hits a specific env-var `value`/`valueFrom` drift. That would break "no delete" and "no zero availability". Add a precondition: run a `DEPLOY_DRY_RUN=true` pass first and require it to be clean. Also state that a fallback in the console log is an abort condition.
4. **The runbook is in the wrong place and untracked.** It sits at `work/deploy-web-dev-surge/docs/reviews/…` inside the repo, which is `?? work/`, not at `docs/reviews/`, and no `docs/reviews/` exists. Either commit it at the right path in the PR or drop the claim that "merge review gates" it.

## Smaller points
- **Atomic admission:** the scheduler isn't the only gate. The kubelet re-checks requests and can reject a pod with `OutOfcpu`, leaving Failed pods while the ReplicaSet retries. The old endpoint is still retained, so it fails closed. The text should say it can leave Failed pods to clean up.
- **Stopping the blocked build:** `ROLLOUT_TIMEOUT` is 600s, and the Pending pod is 11 minutes old. The blocked build has probably already failed. Make "stop it" conditional on it still running. `disableConcurrentBuilds()` only queues builds, so confirm the old one is gone.
- **Post-rollout gates:**
  - There is no baseline artifact path for the `__otPerf` comparison.
  - There is no threshold for "regression".
  - There is no explicit check that the pod's live requests equal 100m CPU and 768Mi.
- **Zero margin:** "at least 100m unreserved" is exactly the surge need, so it is necessary but not sufficient. Codex asked for positive scheduling margin, which can't exist without freeing CPU elsewhere. The runbook discloses this and the failure mode is non-destructive, so I accept it.
- **`rule.md` §1053:** there is no `rule.md` in this repo (the Jenkinsfile only cites it). I could not check the "post-dev hard gate per rule.md 1053" claim, so treat it as unverified.

## The three bars

| Bar | Result | Reason |
|---|---|---|
| Institutional (accuracy, logic) | **Pass, with edits** | The diff and the CPU arithmetic are right. The runbook's claim that "the scheduler is the atomic gate" is slightly overstated, and the cluster-wide Pending check is not executable as written (finding 1). |
| Military (robustness, completeness) | **Not pass** | Findings 2 and 3 leave the parameter set incomplete and a delete-and-recreate path unguarded. Finding 4 leaves the document out of the merge. |
| NASA (performance, preparation) | **Pass for a controlled attempt, not for a performance claim** | The change keeps `maxUnavailable: 0`, adds no CPU limit and makes no latency claim. The post-rollout gates are the right ones, but they need a baseline and a threshold. |

After findings 1–4 are fixed in the runbook (docs only, no runtime change), I'd approve a controlled DEV attempt. That approval would not cover rollout success or performance, which stay gated on the post-rollout evidence.

That notification is only the first diff command, the one with the over-broad `find /`, finishing in the background. I had already re-run that diff, so nothing new came from it. The review above stands: **REQUEST_CHANGES**. The manifest diff is fine, and the four runbook fixes (credential for the node and Pending checks, full Jenkins parameter block, dry-run guard against the `replace --force` fallback, runbook path and tracking) are the only things needed.
