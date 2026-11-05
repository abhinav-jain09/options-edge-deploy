**Verdict: APPROVE** for the gated dev attempt only. Runtime and performance gates remain pending. I made no edits and ran no Bash, deploy or trade. One check is open: I couldn't recompute the SHA256, because the task bars Bash.

**What I checked**
- The diff's runbook hunk (diff lines 7–40) matches the current runbook file (lines 1–34) line for line. The diff has four files: the new runbook and both overlay registrations. Each registration adds `web-dev-resources-patch.yaml` plus the new comment. The two new patch files have the same index hash, `018f3746`. The patches target `options-edge/options-edge-web`, container `web`, and set only `cpu: 100m`.
- The prior blocker is fixed. `rule.md` lines 1030–1053 now hold the "Codex Feature Test Rule", and line 1053 contains the quoted sentence exactly. This is probably why Codex R2 saw only 958 lines: the file has changed since. The runbook now says the rule is a local workspace file, not in commit `d6c7e631` or the deployment repo, so nobody will look for it in git.
- The static comment is now "verify current capacity before rolling" in both overlays. It is no longer a hard-coded capacity fact.

**The three bars**
- **Institutional accuracy:** pass.
  - The scope is exactly one change: CPU request 200m → 100m.
  - The capacity arithmetic holds: 16000m allocatable minus 15900m requested leaves 100m, which is an exact fit. After the old 200m pod terminates, requests are 15800m, leaving 200m for the next 100m surge. The runbook labels these as snapshots.
  - The provenance chain is the successful build 1633, source `d6c7e631`, and the digest-pinned image.
  - The rule citation is accurate, and the runbook says it adopts the gate independently.
  - The performance baseline is stated as not a paired drag baseline, with no 60fps or latency claim.
- **Military robustness:** pass.
  - Preflight is required immediately before the run: API identity, node Ready, at least 100m unreserved, and only the superseded web pod Pending.
  - The deploy is serialized and does a server-side dry run before apply.
  - The old pod is kept, so a failed schedule is non-destructive.
  - A `replace --force` fallback line in the console is treated as a failure, not a success.
  - The runbook admits it cannot undo a deletion if drift appears after the dry run.
  - Never-do items are explicit: force node assignment, delete the old pod, or cut the request below 100m.
  - The rollout timeout is bounded, and revert goes through Jenkins.
- **NASA preparation:** pass for the attempt.
  - The post-rollout gates are marked "not established by this document".
  - They cover digest, live 100m request and 768Mi/4Gi memory with no CPU limit, route and Order tab, draft drag, a new `__otPerf` sample with a p95 `orderInteraction` target under 8ms, and "done" withheld until Codex tests against real dev.
  - Availability settings are unchanged: readiness, `minReadySeconds`, `maxSurge=1` and `maxUnavailable=0`.

**Non-blocking nits**
- Missing spaces: "lines1030–1053", "max46.8ms", "below8ms", "than34.2ms", "limit4Gi".
- Because `rule.md` has grown, the pinned SHA256 is the real safeguard against drift. Verify `c72bfb57…45e6` with `shasum -a 256` before relying on the citation.
- The runbook is still an untracked file. It must be committed to the deployment repo, or the "local file" wording kept, before merge.

**VERDICT: APPROVE** — the rule provenance is now correct and honest, the static comment is fixed, and the controlled dev attempt is accurate, robust and well prepared. Real-dev runtime and performance gates still decide whether it can be called "done".
