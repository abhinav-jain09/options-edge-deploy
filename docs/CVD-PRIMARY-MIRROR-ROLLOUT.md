# Primary CVD mirror roll-forward

Use the existing `es-cvd-mirror` job with `ACTION=roll-forward`, one existing
target/topic unit at a time. `DEPLOY_DRY_RUN=true` is the default for this action.
The source is fixed to es4. Targets are the original primary-Mac dev loopback
broker or the production broker, with their paired registries. The secondary
192.168.100.201 is explicitly out of scope and is never contacted.

The new action does not execute the legacy install or teardown stages. It
creates no topic, schema, group or offset, and changes no original properties.
It derives the newest full related processing qualification, refuses a running,
failed or unpublished latest build, and requires the current SDK. Processing source must match current main or
pass the bounded component-source identity proof described below.
The published JAR, qualification receipt, entrypoint and complete released SDK
member population are verified before installation. No operator version or
build selector exists.

Preflight verifies the existing label, launcher, group, endpoints, committed
coordinates, source/target partition sets and exact released schema graphs.
A bounded manual tail sample runs on Linux CI es4 through Jenkins with the
actual packaged mirror decoder and destination-registry serialization. It uses
no consumer group, commits, Kafka writes or schema registration. Its cgroup has
512 MiB memory, no swap, one CPU and a 90-second deadline. At most 25 retained
offsets per partition, 800 records and 32 MiB including keys are sampled. Zero
sampled records is recorded honestly and is not historical-coverage proof. A
launcher upgrade refuses an empty sample; dry runs may report one.
Transferred qualification bytes are hash checked; test staging is preserved
under bounded quarantine. No heavy replay or performance work runs on a Mac or
production host.

Every launcher upgrade requires `CVD_TARGET_READERS_READY=true` after verifying
compatible destination readers. Mirrors publish Avro even while their
source still contains JSON, so upgrade active destination Context Tape readers
first. This is downstream closure for the selected output, not a requirement to
switch an input writer before its reader.

Prepared runtime files live in private releases. A content-addressed verified
JAR cache is hard-linked into unit releases, with physical-inode byte accounting,
file/count/free-space caps and no recursive cleanup. Source configuration is
copied byte-for-byte, and the new JAR/JDK smoke must pass before stopping the
old process. The Java transport overrides auto offset reset to `none`, keeps the
original group and commits only after destination acknowledgements.

Only that unit's ProgramArguments changes atomically. Loaded old processes stop
before a new process starts; inactive units remain inactive. A local attach
probe must verify the new PID/JAR identity, ready MBean, zero failures, completed
poll and the configured 256 MiB maximum heap cap before runtime readiness is claimed. No public JMX port
is opened. Existing plist fields and source properties are checked unchanged.
Failure receipts retain the exact phase; an old raw-byte mirror is never restored.
A held launcher upgrade is recorded without claiming runtime readiness. Loaded
units without a PID refuse; they are not treated as stopped holds. The new
consumer's committed coordinates must not decrease after restart.

Ordinary tests stub runtime effects and exercise actual handoff orchestration,
zero holds, dry run, failure ordering, offsets and identity refusals. They are
separate from the actual Linux retained-tail preflight and the post-install
readiness check. The full processing qualification must publish before any new
action can pass release resolution; this documentation does not claim rollout
completion or full retained-history/performance coverage.

Preserved staging and cache limits intentionally refuse further runs when full;
they never delete existing files automatically. Owner housekeeping is required
before another release then. A group without committed offsets, even on an empty
topic, refuses rather than inventing an earliest/latest starting position.

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
