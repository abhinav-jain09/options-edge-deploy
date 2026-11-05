# Dev immutable public assets — preparation only

Depends on options-edge PR1548 and an approved built image containing ContextTapeAssetPublisher. Do not apply these manifests with the earlier web image.

The dev web init container publishes verified public code to a2Gi standard rancher/local-path claim. The app mounts it read-only. Both containers use the same digest via the existing service-deploy pinning of containers and initContainers. The verified existing Jib image uses java -cp @/app/jib-classpath-file app.Main; the publisher substitutes only the main class. Verify the next actual image entrypoint/argument file before applying. CPU pod reservation remains max(web100m,init10m), so the init does not increase the100m pod request. No Kafka, schemas, account data or production changes.

PVC ownership remains with the existing common-infra Jenkins job. It creates the claim; service-deploy already refuses missing claims. Standalone web overlay contains only Deployment/Service, never a PVC. Dev monolith includes the same infra claim and the same web patch. Existing scoped service validation passes.

Two stages: first create-only dev infra PVC after a reviewed Jenkins dryrun, then deploy the approved P5 web image with CONTEXT_TAPE_COMPILED_ASSETS=false. Verify all pods support the immutable endpoint and share complete published assets. Only a subsequent reviewed dev-only flag change may set true, reusing the same digest. This prevents old/new pods missing future hashes. These are existing job invocations, no new service identity or Jenkins stage/job.

Rollback keeps the PVC and older public identities. For stage2, disable the flag through Jenkins (stage1 already has itfalse). Rolling back to a pre-P5 image requires reverting the publisher init/mount manifest too; an image-only rollback would crash the init. Never delete or shrink the claim to roll back. Runtime publication refuses capacity overflow without evicting old releases. Locking contract is the observed single-node local filesystem, not NFS.

This branch is not deployed and not approved. Phase5 and the six-phase programme remain incomplete until review, staged Jenkins rollout and deployed nativeEdge acceptance pass.


Claude R1 requested changes. This PR remains draft and MUST remain unmerged until a real P5 build passes app review and its build number is recorded in the existing web/MIN_IMAGE_BUILD guard. A guessed future build floor is unsafe because an intervening renderer/hover build could lack the publisher. No deployment from this draft is authorized by its preparation tests.

Storage observation2026-11-05: the verified dev kubeconfig API is192.168.100.102:6443, one desktop-control-plane node; storageclassstandard reports provisionerrancher.io/local-path, WaitForFirstConsumer, expansionfalse. This is live-observed configuration, not a generic docker-desktop claim. Recheck before apply. Local-path does not enforce the2Gi request as a filesystem quota; the publisher1536MiB budget is the operational cap. Once full, new releases fail and old releases remain. The claim may remainPending until the first web pod mounts it; existing service preflight correctly requires existence, notBound.

Existing Jib imageUser is unset, hence defaultroot. The init now explicitly runsuid/gid0, writes0755root/0644publicfiles, and has -Xmx128m with UsePerfData disabled. The web mount isread-only and those permissions support a later nonroot reader. NoAPP_PROFILE/envFrom is needed: the publisher is a plain Java main loading only public classpath assets/ObjectMapper, not app.Main or Spring; do not pass broker/Kafka secrets into it. Recheck the actual P5 image user/classpath and run publisher smoke under the bounded heap before apply. A Kubernetes server dryrun cannot mount or write a PVC: it validates API manifests, not filesystem access. The init publication is the runtime write/read integrity gate before app readiness, with maxUnavailable0 preserving the old ready pod if it fails.
