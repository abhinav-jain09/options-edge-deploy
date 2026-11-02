# Evidence API image rollout

`cvd-evidence-api-images` builds the existing API from current processing main
and the newest successful current-main contracts release, using one fixed dev
or production architecture and registry. There is no version or source selector.
The image is built Docker-free. The ordinary Python suite runs on the build Mac from the service's hash-pinned lock and the very
sources that are staged next (`trade-evidence-api/deploy/image_stage.py test`), bounded by a 10-minute deadline and an 8 MiB log limit.
The target-architecture wheels and sources are then staged and Jib (`trade-evidence-api/ci/jib-image/pom.xml`) pushes the image. The
staged tree is content-hashed before and after the push: any difference refuses the receipt. The immutable digest and Jib's image
configuration id are archived only after that check. No Docker daemon, Docker Desktop or buildx is involved. Failed or running latest
builds never fall back to a previous release.

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
