REQUEST_CHANGES — the four-file manifest diff is correct and dev-only, but the required rollout runbook contains a false governing-rule provenance claim, so it is not institutionally auditable as written.

- Institutional: fail. Runbook line 34 cites `rule.md:1030-1053` at `d6c7…`; that commit does not contain `rule.md`, and the current file has only 958 lines. Actual review/merge policy is at current `rule.md:533-556`; live verification policy is `:942-956`.
- Military: fail overall. The controlled-attempt safeguards are well specified—fresh read-only `.102` preflight, ≥100m requirement, only superseded Pending web pod, serialized job, server-side dry run, and retained old endpoint—but the governing policy assertion is objectively untraceable.
- NASA: fail overall. The manifest preserves readiness, `maxSurge: 1`, `maxUnavailable: 0`, memory settings, no CPU limit, and mirrored overlays; runbook post-rollout behavior/performance gates are appropriately still pending. But an untracked runbook with invalid source anchors is not a release-quality audit artifact.

Line-by-line diff result: both registrations add the same patch; both identical patches target only `options-edge/options-edge-web`, container `web`, changing only CPU request to `100m`. No production diff exists. The static “100m spare CPU” comments are also not a durable capacity fact; the runbook correctly treats capacity as a fresh snapshot, so those comments should be softened or removed.

Correct the runbook’s rule reference/provenance (and static-capacity wording), then re-review. No deployment, cluster access, Jenkins action, or edits were performed.

**VERDICT: REQUEST_CHANGES — correct manifest, but invalid rule provenance prevents all-three-bar approval.**

