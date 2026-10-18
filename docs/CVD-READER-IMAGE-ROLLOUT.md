# CVD reader image rollout

`Jenkinsfile.cvd-image-rollforward` rolls one existing CVD reader on dev or
production, with a server-side admission dry run by default. It never creates a
workload, scales a hold, applies a manifest, changes a Secret or touches Kafka.
The existing es4 image-only job retains its original five-service scope.
Dev has ten allowed readers (nine held at zero); production has nine (eight
held at zero). Level Absorption refuses on production before network access.
Context Tape is active and remains closure-gated.

The wrapper fixes the environment's registry, architecture, deployer kubeconfig
and namespace UID. Dev uses ARM64 images and production uses AMD64 images.
Image selection takes the highest-numbered relevant build in the bounded recent
Jenkins inventory; running, failed, unpublished or missing latest builds refuse
instead of falling back. The SDK must match current main. Processing source must match current main or
pass the component-source identity proof below.
There is no operator image, version or build-number selector.

The existing image-only core checks the published lock and registry manifest
bytes, then fences deployment UID, resource version, replica count, container
name and prior image in an immutable JSON patch. Only the image is replaced.
Admission and read-back must preserve the rest of the live workload spec.
Stopped workloads remain stopped and are never described as runtime-verified.
Active workloads require the explicit completed-reader/schema/runtime closure
acknowledgement and a successful rollout. A failed write or readiness check
keeps its phase receipt and never rolls back to an old JSON reader.

The primary Mac executes this job. It performs no operation on secondary hosts.
In particular, the out-of-scope 192.168.100.201 is not a rollout target. A fresh
read-only source-group census is required before writer activation; the latest
census observed no CVD members from that host.

Validation covers both fixed target mappings, ARM64/AMD64 and registry mismatch
refusals, current SDK and source fences, latest-build refusal, zero holds and the
existing core's non-image drift, failure-receipt and active-closure tests.

The receipt records ready replicas before the patch, so an existing unready
Context Tape baseline is explicit. Image pins must be reconciled to the verified
new digests after rollout so a later manifest deployment stays forward-only.
Literal successful processing locks from dev1901 and production1897 cover the
actual tag formats, rather than an invented environment prefix.

If unrelated processing commits advance main during qualification, the shared
source proof compares exact Git trees for all CVD modules and their transitive
internal Maven dependencies, including the evidence API. Every existing reactor
POM and the shared parent model must remain identical. Only unrelated module
source changes, two explicitly named non-runtime files, and simple appended
modules that cannot shadow an existing dependency are accepted. Unknown shared
build changes or CVD code changes require a fresh full qualification. The proof
is bounded and archived with both actual qualified and observed main commits.
It never selects an older successful build behind a newer relevant running or
failed build, relaxes the current SDK requirement, or waives any campaign.
