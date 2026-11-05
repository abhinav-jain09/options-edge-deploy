**Verdict: APPROVE.** This covers merge and a controlled DEV attempt only. It does not qualify runtime behavior.

I read only the two files you named, line by line. I did not open the diff, the Jenkinsfile, `service-deploy.sh` or the cluster. Where the runbook says something was verified, I took that as the author's claim, not something I checked.

## The four findings

| # | Finding | Status | Evidence |
|---|---|---|---|
| 1 | Deployer credential can't run the node and cluster-wide Pending checks | Resolved | Line 7 names `dd-admin-102.yaml` as the read-only preflight credential. It must be verified as exactly `https://192.168.100.102:6443`. Jenkins keeps its namespace-limited kubeconfig and no role is expanded. It uses `describe node` for init/overhead-aware accounting. |
| 2 | Parameter set incomplete, bare digest, no provenance | Resolved | Lines 9–13 give the full `REQUIRED_IMAGE` reference, the build and source SHA, and the lock-file check. They list every parameter, including `PERMITTED_SHA_GUARD_VERSION` and `KUBECONFIG_FILE`. The other restart flags are set false. Line 11 frames this as reuse of a guarded build, which answers the "manual value" concern. |
| 3 | `replace --force` fallback unguarded | Resolved as far as can be, with honest limits | Line 13 requires a dry run with server-side apply validation first, then an identical run with `DEPLOY_DRY_RUN=false`. Line 22 says a fallback line in the console is an unexpected failure, and that the document does not claim monitoring can undo a delete if drift appears after the dry run. |
| 4 | Wrong or untracked doc path, and the `rule.md` reference | Resolved | Line 34 says `rule.md:1030-1053` lives in the options-edge repo at `d6c7e631`, not in this repo. The doc sits at `docs/reviews/` within the deploy repo. Line 32 limits the merge review to code readiness. |

## The smaller points

- **`OutOfcpu` admission:** line 22 covers the kubelet re-check, Failed pods and keeping the evidence.
- **Stopping the blocked build:** line 19 now makes this conditional on the build still running. It also requires confirming it ended.
- **Margin:** the exact fit is disclosed on line 32 as a bounded, non-destructive failure.
- **Post-rollout gates (lines 26–30):** these now include the following.
  - A live check of 100m CPU request, 768Mi request, 4Gi limit and no CPU limit.
  - A named baseline artifact and source, with numbers.
  - A p95 drag target below 8ms.
  - An explicit statement that unmatched workloads give a regression signal, not a causal comparison.
  - No 60fps or latency claim.

## The three bars

- **Institutional (accuracy):** pass. The arithmetic (15900m, then 16000m with the surge pod, then 15800m) matches the earlier review. The overstated "scheduler is the atomic gate" wording is gone.
- **Military (robustness):** pass for a controlled attempt. The preflight credential, the dry run, the stop conditions and the no-delete, no-force rules are all explicit. The remaining risk is the post-dry-run drift race, which the runbook states openly.
- **NASA (preparation):** pass for merge and a controlled attempt. The post-rollout gates are explicitly not established, and success and performance stay gated on live evidence.

## Residual items, not blocking

- I couldn't confirm that `dd-admin-102.yaml` works. The earlier review also left that open, so the runbook's identity check is the safeguard.
- I couldn't confirm that the server-side dry run actually catches the env `value`/`valueFrom` drift that triggers the fallback. The runbook asserts this. It's worth reading the dry-run console once to confirm.
- The stale "Single-node dev has 100m spare CPU" comment in the kustomization is outside this runbook and is still open from the earlier review.
- I did not check the product diff, so `1d3dd480` is unchanged and approved only on your word.
