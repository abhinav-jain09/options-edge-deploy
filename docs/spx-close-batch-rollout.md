# SPX daily-close rollout

The runtime schedule belongs to `oe-close-batch`, job `spxDailyClose`, in the existing
`stock-gex-close-scheduler` workload. Jenkins builds and deploys it; Jenkins does not schedule
its daily capture. Production uses the `scheduler` profile; dev uses `spx-scheduler`.

1. Build the approved `oe-close-batch` main commit using Jenkins `oe-close-batch`, with its exact
   `PERMITTED_SHA`, canonical `PERMITTED_SHA_GUARD_VERSION`, and `PROFILE=prod` or `dev`.
   Require SUCCESS and inspect `.jenkins-tmp/batch-image-lock.env`. `BATCH_IMAGE` is immutable;
   its commit and build must match this run. The pipeline verifies the registry manifest/config
   platform (`linux/amd64` production, `linux/arm64` dev) before recording the lock.
2. Inspect active Spring Batch executions before replacing the production scheduler. Recreate
   terminates the existing process: wait while `closeBoard`, `oiIndex` or another duty is running.
   The ordinary evening starts are 16:10/17:40 ET and 20:30 ET, with morning recovery triggers;
   actual job state is the precondition. Build-only image publication does not stop the worker.
3. Deploy the batch first through Jenkins `service-deploy`: `SERVICE=stock-gex-close-scheduler`,
   `ENVIRONMENT=production` or `dev`, `BUILD_IMAGES=false`, `DEPLOY_DRY_RUN=false`,
   `PERMITTED_SHA=<approved deploy-repository main commit>`, and `REQUIRED_IMAGE=<BATCH_IMAGE>`.
   Both the pipeline and shell entry point reject a missing binding; the shell rejects tag-only
   other-repository and crossed-environment registry/tag bindings. The service is excluded from processing-reactor builds.
4. Require Jenkins SUCCESS, verify the running digest/readiness/restarts, and inspect the dated
   `spxDailyClose` execution and its two steps. If a capture is already SUCCESS in
   `market_reference.spx_daily_close_attempts`, the fetch step acknowledges it without HTTP and
   the batch emits `SPX_DAILY_CLOSE_OK`. If before the cash close, do not force a vendor request:
   verify configuration and the existing saved reference, and leave the capture to its schedule.
5. Only after the batch is verified, build and deploy the coordinated web main commit through
   `options-edge-web-deploy` and `service-deploy SERVICE=web` with its own immutable image lock.
   Confirm the authenticated prior-close endpoint and point/% display still use the saved date.
   Unauthenticated API requests must remain 401. Web performs read-only DB cache refreshes only.

Do not manually apply manifests, change topic policies, delete claims, or retry Yahoo to test a
capture. A failed vendor attempt retains its daily claim. A saved response can retry only the
save step, with a bounded execution count and exponential backoff; permanent rejections are
ABANDONED rather than appended indefinitely. A retry or rollback uses a new approved Jenkins
run with the recorded immutable image and exact source commit.

Implementation anchors: `Jenkinsfile.service-deploy` (own-job exclusion and required-image gate),
`scripts/deploy/require-batch-image-binding.sh` (entry-point binding),
`scripts/deploy/service-deploy.sh` (pin/apply/verify), and the batch's
`scripts/jenkins/verify-pushed-platform.py` and `spxclose/SpxDailyCloseScheduler.java`.

Vendor-evidence limit: the archived Yahoo fixture is from a normal cash session. An archived
13:00 early-close response is not yet available. The session-end check remains fail-closed:
a vendor calendar disagreement spends that day's claim and logs a failed job without saving
an intraday value. Before the next early close (27 November 2026), verify this metadata against
an independently archived early-close response. Monitor SPX job failures and the dated SUCCESS
receipt; never remove a daily claim to force another request.
