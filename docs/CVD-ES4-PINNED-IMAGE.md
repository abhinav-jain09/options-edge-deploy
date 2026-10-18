# CVD image updates on es4

The `update-image` action in the existing es4 Jenkins job updates only the image
of one existing CVD workload. It does not apply manifests, ConfigMaps, Services,
PVCs or state, create workloads, reset topics/groups, or change replica counts.
Indicator and Market Structure services are outside its allowlist.

The five bindings are es-cvd, es-footprint-strike, gamma-migration, context-tape
and close-direction. Each requires its known single application container. The
namespace UID and Jenkins deployer identity are checked before the patch. A
missing workload or unexpected structure refuses rather than creating anything.

There is no image/version selector. The action resolves the newest relevant
production processing build among the latest 64 Jenkins builds. A newer matching
running, failed, aborted or unpublished build refuses; it never falls back to an
older image. Unrelated service builds do not block it. The build must be SUCCESS
with pushed images; its bounded image-lock artifact must match build number,
source/SDK parameters, SDK-byte provenance, canonical guard and linux/amd64. The
service entry must use that build/source tag and match the expected repository.
The processing source must also equal the current remote main branch; a new
main commit requires new qualification. The registry response is hashed to verify the exact selected digest.

Close Direction uses a distinct `options-edge-close-direction` image even though
it shares the Context Tape Maven module. The current CVD related-image batch does
not build that image; its latest qualification must be a separate Close Direction
image build. No Context Tape image is substituted for its different entry point.

A JSON patch tests the live workload UID, resourceVersion, replica count, container
name and old image before replacing only the image. Server-side dry run must
preserve the complete workload spec apart from that image, including zero holds.
The exact immutable in-memory patch is then submitted; the archived patch file is
a review copy, never an execution input. Apply checks the returned spec and waits
for rollout only when the existing replica count is positive. Zero workloads stay
zero and have no runtime-readiness claim. Active pod replacement follows its existing rollout strategy and state recovery.
A concurrency or rollout failure is reported for roll-forward repair; no old-image
rollback runs.

The current environment and shared registry configuration are reused. Caller
coordination must finish schema registration, reader closure, source-domain and
runtime qualification before updating an active workload. Real updates with a
positive existing replica count additionally require the default-false
`CVD_READER_CLOSURE_CONFIRMED` acknowledgement. It records the operator
acknowledgement, not an independent certification of those external prerequisites.
This operation verifies
the existing successful processing pipeline and its image lock; it does not
independently repeat that pipeline's tests, publish schemas or certify Kafka data.
The source tree is verified before tests execute. A receipt is written before
dry run and before the real write, updated after each phase, and archived even
on failure. An apply-requested phase can mean an unknown write outcome; inspect
live state and use a fresh roll-forward Jenkins run, never retry an old patch.
Metadata/patch receipts are archived under this Jenkins build's own directory;
when reading wildcard archives, select that exact build and source.
