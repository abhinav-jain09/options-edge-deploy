# Dev native gateway rollout; browser activation held

PR1545 now prepares the gateway only. It adds `GATEWAY_AUTH_ENABLED=true`, audience `options-edge-web`, and system `Rithmic Paper Trading` to both dev gateway renders. It leaves the browser native flag disabled. No new Jenkins job or stage is introduced. Production, experiment, ES4 collectors and mirrors are untouched.

This split prevents a concurrent web rollout from activating the native browser before gateway authorization is verified. After the gateway checks pass, a separate reviewed change can enable the two browser native keys through the same existing service job. Compiled assets are already enabled and remain enabled.

The immutable release identities and safe preflight evidence are in [release-receipt.json](reviews/dev-native-gateway/release-receipt.json). Gateway build850 passed its Linux suite (1928 tests) and existing footprint campaign (49 killed, one characterized survivor). Its source is `4a2c1c064c0df7c5bd5d5103a16f6fcd9a42bb13`, image digest `sha256:dab69425cc6b9af64ea26d4635d0809503659d0bffd6962e8bdbea386bdada94`. Registry labels match the source, JAR and contracts receipt. The currently deployed gateway source `2a97d779` is an ancestor; the candidate preserves it and the PR302 merge.

Web1658 source `b5f01d1d` contains the PR1583 merge and is already deployed at digest `sha256:592cd5129d18edc0ca098e1af6c176b06d48ca942bde5ac62854ca52af76c994`. A read-only copy of its running JAR supplied the manifest hash and exact native raw-asset comparisons in the receipt. This proves native-capable bytes, not activated native data. The build source and image digest are distinct evidence from source review.

Preflight on dev server `192.168.100.102:6443` found WS authorization enabled, AppSession authorization unset, approval role `replay-admin`, the existing dev in-memory-ticket allowance, and the existing issuer/client/JWKS configuration. Safe decoded current token claims include the expected issuer, audience/authorized party and approval role. Token text and subject value are not recorded. No credentials or new auth exceptions are added.

The unrelated current-main `LINE_HEALTH_RELAY_ENABLED=false` and wall freshness `20000` milliseconds must remain unchanged. The rendered tests pin both. Public asset usage measured 22,552 KiB, below the existing publisher operational cap of 1536 MiB; preserve all old published hashes and the existing 2 GiB claim.

The configured replay orchestrator health connection is refused. The live-selection AppSession path does not invoke that run-backed service. Do not recover it, change its URL, or bypass approval. Run-backed replay remains unavailable and must fail closed.

Release sequence:

1. Finish actual Claude and independent Codex review of the final current-main configuration and receipt. Merge through PR1545; feature branches are never runtime sources.
2. Use the existing `service-deploy` job for `feed-gateway`, environment `dev`, with no image rebuild and the exact build850 digest. Run its dry run first, then apply. Verify the server identity and running image.
3. Check the current authenticated subject, ordinary SPX selection/bootstrap, reconnect, authorization refusal and approval expiry, and native hello/system binding. A healthy pod is insufficient. Closed-market source freshness may prevent changing-price proof; record that limitation and do not claim a live-data acceptance pass.
4. Prepare the separate browser activation only after gateway authorization, subject binding and ordinary selection checks pass. If the market is closed, source/history availability and changing-book acceptance remain explicitly pending. Never manufacture live data or relax freshness checks to obtain a green result.
5. Verify native browser quantities/deletions, history coverage, camera retention, reconnect and smooth unsent-draft interactions through the actual dev URL. Record missing coverage and failures in the 22-requirement checklist.

Whole-programme targets remain idle CPU below 2%, zero unchanged full scenes, SVG below 600 nodes, warm first paint below one second, and interaction work p95 below 8 ms with steady 60 fps. Native history bootstrap is bounded to 45 seconds over the authenticated LAN. Allocation bounds are not heap or aggregate process measurements. Current web1658 native observations still fail the full-scene and zero-idle-render targets, and liquidity is unavailable; no phase is accepted by this configuration.

Rollback must use reviewed configuration and exact images through the same existing Jenkins job. While the browser native flag is still off, gateway rollback is independent. Once the browser is enabled, disable native browser use and verify ordinary UI before restoring prior gateway authority. Keep compiled assets and published hashes available. The old draft rollback PR1548 was based on the former paired activation; reconcile and review it before use rather than applying it blindly.

PR1545 stays draft until final reviews and scoped render validation pass. The release receipt records prerequisites, not a completed runtime activation.
