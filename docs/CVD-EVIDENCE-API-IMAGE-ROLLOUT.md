# Evidence API image rollout

`cvd-evidence-api-images` builds the existing API from current processing main
and the newest successful current-main contracts release, using one fixed dev
or production architecture and registry. There is no version or source selector.
The ordinary Python suite runs inside the final image before publication, with
512 MiB, no extra swap, one CPU, 128 tasks and a two-minute client limit. A 110-second controller inside the named test
container bounds the test even if its client disappears. Failed, timed out or
log-limited tests trigger a bounded ownership-label check and kill only that
exact container ID. The shared builder is never stopped or removed. Build and
publish clients have finite deadlines and 8 MiB log limits. Publication must
produce the exact image configuration tested; the immutable digest and source
provenance are archived only after that check. Failed or running latest builds
never fall back to a previous release.

The separate `roll-forward` action defaults to server dry run. It reuses the
CVD image-only CAS updater with the fixed target namespace/deployer, architecture
and current SDK/source checks. Only the existing `trade-evidence-api` deployment
with container `api` and replicas exactly zero is accepted. An active workload
refuses even in dry run. No secret, configuration, volume, scale or service
creation operation is performed. Stopped image updates make no readiness claim.
Both architectures receive their own image build and final-image tests.

This path avoids the existing API deployment pipeline, whose production apply
starts the service and manages authentication configuration. The new job has no
schedule or trigger. It runs on the primary build Mac; 192.168.100.201 is outside
scope. Only the API directory is checked out from processing main; unrelated tracked
symlinks stay outside the worktree. Fresh source/cache staging is preserved within count/file/byte budgets;
no recursive filesystem deletion is introduced. Reaching a budget refuses a
new build rather than discarding old evidence.

Ordinary tests stub builds and runtime effects while exercising source/build
identity, final-image test ordering, publication drift, latest-build refusal,
fixed architectures/registries and zero holds. A small child process verifies
the log limit. Those tests are not actual image or runtime qualification; each
Jenkins build must independently pass before its digest can be installed.
