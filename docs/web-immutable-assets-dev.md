# Dev immutable public assets — preparation only

Depends on options-edge PR1548 and an approved built image containing ContextTapeAssetPublisher. Do not apply these manifests with the earlier web image.

The dev web init container publishes verified public code to a2Gi standard rancher/local-path claim. The app mounts it read-only. Both containers use the same digest via the existing service-deploy pinning of containers and initContainers. The verified existing Jib image uses java -cp @/app/jib-classpath-file app.Main; the publisher substitutes only the main class. Verify the next actual image entrypoint/argument file before applying. CPU pod reservation remains max(web100m,init10m), so the init does not increase the100m pod request. No Kafka, schemas, account data or production changes.

PVC ownership remains with the existing common-infra Jenkins job. It creates the claim; service-deploy already refuses missing claims. Standalone web overlay contains only Deployment/Service, never a PVC. Dev monolith includes the same infra claim and the same web patch. Existing scoped service validation passes.

Two stages: first create-only dev infra PVC after a reviewed Jenkins dryrun, then deploy the approved P5 web image with CONTEXT_TAPE_COMPILED_ASSETS=false. Verify all pods support the immutable endpoint and share complete published assets. Only a subsequent reviewed dev-only flag change may set true, reusing the same digest. This prevents old/new pods missing future hashes. These are existing job invocations, no new service identity or Jenkins stage/job.

Rollback keeps the PVC and older public identities; set the flagfalse or restore the previous image/manifest through Jenkins. Never delete or shrink the claim to roll back. Runtime publication refuses capacity overflow without evicting old releases. Locking contract is the observed single-node local filesystem, not NFS.

This branch is not deployed and not approved. Phase5 and the six-phase programme remain incomplete until review, staged Jenkins rollout and deployed nativeEdge acceptance pass.
