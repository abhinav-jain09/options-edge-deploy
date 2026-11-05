**REQUEST_CHANGES** — the manifest is correct, but the rollout is not institutionally ready until its two operational hard gates are satisfied.

Every staged line is sound:

- Both `kustomization.yaml` additions correctly register the same dev-only strategic-merge patch immediately after the existing web env patch.
- Each 13-line patch correctly identifies `apps/v1`, `Deployment/options-edge-web`, namespace `options-edge`, and the named `web` container; it changes only `resources.requests.cpu` from 200m to 100m.
- The duplicate patch files are byte-identical. Fresh monolith and standalone-slice renders are identical: 100m CPU request, 768Mi request, 4Gi memory limit, no CPU limit, `RollingUpdate`, `maxSurge: 1`, `maxUnavailable: 0`, and existing probes/min-ready behavior.
- Production freshly renders unchanged at 200m CPU / 768Mi / 4Gi. No production scope leakage.

Three bars:

1. **Manifest / scope correctness — PASS.**  
   The named-container strategic merge preserves memory, the absence of a CPU cap, probes, rollout strategy, and all other base fields. It is dev-only and mirror-safe.

2. **Availability / scheduler robustness — NOT PASS for rollout.**  
   Given the supplied 16,000m allocatable and 15,900m requested snapshot, the surge arithmetic is exactly: existing 200m web + new 100m web = 16,000m. Kubernetes can schedule that exact fit, but it has **0m headroom**. Any concurrent admission, DaemonSet change, or stale resource snapshot returns the rollout to `FailedScheduling`. Keeping overlap is correct: `maxUnavailable: 0` preserves the endpoint and avoids the known Docker Desktop LoadBalancer failure. Do not lower availability semantics; establish verified slack or an exclusive/preflight rollout condition first.

3. **Native DEV CPU/performance qualification — NOT PASS / hard gate pending.**  
   The change removes no CPU limit, so it does not cap bursts on idle capacity; under contention, however, the web pod’s CPU share weight is reduced from 200m to 100m. The supplied validation is structural/render validation only. It does not qualify startup, readiness latency, UI/API behavior, or sustained runtime under actual DEV contention. Native DEV tests remain required evidence.

Claude’s approval is accurate for the narrow manifest review, but insufficient for a deployment approval under the stated robustness standard. The required changes are operational evidence/preconditions, not a production manifest change: verify live allocatable/requested CPU immediately before rollout with positive scheduling margin, then complete the native DEV web hard gate.