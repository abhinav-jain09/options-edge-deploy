**Verdict: APPROVE.** I found nothing that blocks merge, only three advisory notes. I made no edits and ran no deploy or mutation. I only read files and rendered locally with `kubectl kustomize`.

## What I checked

**Staged diff, all 4 files (+30 lines, `git diff --cached --check` clean)**
- `k8s/overlays/dev/web-dev-resources-patch.yaml` (new, 13 lines) and `k8s/services/web/overlays/dev/web-dev-resources-patch.yaml` (new, 13 lines).
  - Both are a strategic-merge patch on `Deployment/options-edge-web` in namespace `options-edge`, container `web`, setting only `resources.requests.cpu: 100m`.
  - The two files have the same blob hash `018f3746`, and `diff` confirms they are identical.
- `k8s/overlays/dev/kustomization.yaml` (+2): the patch entry goes right after `web-dev-env-patch.yaml`, so it sits with the other web patches. It is a plain `path:` entry, not a target-scoped one.
- `k8s/services/web/overlays/dev/kustomization.yaml` (+2): the same entry in the same position, which satisfies the file's own "MIRROR RULE" header.

**Base** (`k8s/services/web/base/options-edge-web-deployment.yaml:19-29`, `:145-150`)
- Requests are `cpu 200m` and `memory 768Mi`. The only limit is `memory 4Gi`, with no CPU limit.
- Strategy is `RollingUpdate`, `maxUnavailable: 0`, `maxSurge: 1`, with `minReadySeconds: 5`.
- The base comment says Recreate left a zero-endpoint window that made the Docker Desktop LoadBalancer drop the `:8090` binding. That supports keeping the overlap.
- Readiness is `GET /`, with a 20s initial delay, 10s period, 10s timeout and failureThreshold 5. Liveness has a 40s initial delay and a 20s period.
- There is no `/api/health` or startup-probe change in this diff.

**Rendered output**

| Render | web CPU request | memory request / limit | Strategy |
|---|---|---|---|
| dev monolith | **100m** | 768Mi / 4Gi | RollingUpdate, surge 1, unavail 0 |
| dev standalone slice | **100m** | 768Mi / 4Gi | RollingUpdate, surge 1, unavail 0 |
| production | **200m** (unchanged) | 768Mi / 4Gi | RollingUpdate, surge 1, unavail 0 |

- The web Deployment and Service from the monolith and the slice are equal, so the mirror rule holds. This matches your `validate-services` pass.
- Production does not include the patch and still renders 200m, so production is unchanged.
- I did not check the experiment overlay. The diff does not touch it, so it should keep the base 200m.

## Assessment

- **CPU-share tradeoff: acceptable.**
  - A request is a scheduler reservation and a share weight, not a cap, and this patch adds no CPU limit. The JVM can still burst into idle cores, so only the contended weight halves (200m to 100m).
  - The same precedent exists in `unified-sr-dev-cpu-patch.yaml`: dev is a 16-core node that was 98% reserved.
  - Under real contention the web pod gets a lower share. I cannot certify latency or runtime behaviour from manifests, so I have not.
- **Scheduling arithmetic: it works, with no headroom.**
  - The node is at 15900m of 16000m. During the first roll the old pod holds 200m and the new pod asks for 100m, which fits exactly.
  - Afterwards steady state frees 100m.
- **Readiness: preserved.**
  - The `maxUnavailable: 0` overlap is kept, so the old pod serves until the new one passes `GET /`.
  - Readiness gating on `/` is unchanged. Liveness allows about 40s plus 5×20s, so a slower JVM start at the lower share has margin.
  - `progressDeadlineSeconds: 1200` is set by the existing dev patch.

## Findings (advisory, none blocking)

1. **The first roll has zero scheduler slack.** The node has exactly 100m free and the surge pod needs exactly 100m. Any other dev pod that schedules between now and the roll (the node is already full of 100m-class requests) puts the surge pod back in `FailedScheduling`. I would apply this soon after merge, and if the first roll still pends, free CPU elsewhere rather than lowering web further.
2. **The patch comment states a point-in-time number ("100m spare").** It will go stale. The `unified-sr` patch gives the reason and the "raise THIS value" guidance. Consider adding a one-line note that 100m is a floor for web, with the date. This is cosmetic only.
3. **The new file has no header comment explaining why it exists.** `unified-sr-dev-cpu-patch.yaml` has one. The reason lives only in the kustomization comment, which is thin for a file that is duplicated by design. Cosmetic only.

## Explicit PASS items
- **PASS:** the monolith and slice mirrors are identical and render-equal.
- **PASS:** production is unchanged at 200m, and no CPU cap was added.
- **PASS:** the rolling-update overlap and readiness semantics are preserved, so the zero-endpoint/LoadBalancer regression cannot reappear.

**APPROVE**: the change is minimal, scoped to dev, mirrored identically, and addresses the `FailedScheduling` without touching production or removing the overlap. Finding 1 is the only operational risk and does not affect correctness.
