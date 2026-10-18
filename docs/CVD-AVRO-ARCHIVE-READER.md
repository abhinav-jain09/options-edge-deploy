# CVD and Direction archive reader preparation

The affected archive-policy intersection is five existing topics: es4 CVD bars,
Footprint bars/outcomes/strike, and the dev Direction ledger. They always use the
existing metadata-bounded committed reader, even if the old committed-topic list
is overridden. The other archive inputs retain their existing paths.

`StrikeArchiveReader` keeps its original LSO boundary, manual assignment, position
check, durable manifest/checkpoint and archive-group marker behavior. An optional
reflection-loaded SDK decoder projects canonical frames to the existing JSON API
before any line prefix is written. Retained JSON stays byte-identical. CVD API
object field order may follow the canonical schema; values remain equal. Strike
and Direction recovery-sensitive text retains its exact ordering. Key, timestamp,
partition and source offset stay in the same five-field line layout. Malformed
frames fail the capture without advancing a successful record count/checkpoint.

`CanonicalArchiveDecoder` binds only those five released contracts. It uses the
shared exact-writer admission and bounded preallocation decoder, with the explicit
old Strike-null role. Framed failures never become JSON fallback. Canonical reads
use max.poll.records=1, 8 MiB fetch limits and no automatic topic creation. The
existing same-topic history and topic identities remain; no group or offset is reset.
Previously unstamped console-path checkpoints enter the existing LEGACY path:
the first committed capture recaptures from retained log start, so old files may
overlap the new capture. This safely duplicates history rather than skipping it.
Offset marking uses its original raw Java17 path and does not load the SDK or
resolve a registry, including when a registry argument is supplied directly.
The filesystem still contains API JSON; this helper writes no Kafka record values.

The native raw reader remains Java-17-source compatible, without static SDK
references. Canonical capture selects an actual verified JDK21+; an explicit bad
JDK or missing sidecar fails instead of falling back. Read-only host inspection
found `/usr/lib/jvm/java-latest-openjdk/bin/java` is JDK26.0.1 on the archive host.
No Java installation is required. Canonical capture uses Xmx512m; 256 MiB native
headroom/768 MiB total is the qualification budget, not an already measured maximum.
Actual source-domain/heap/cadence and native classpath qualification remain open.

The existing archive Jenkins job builds the SDK from its exact permitted latest
main commit, derives its coordinate from that POM, compiles/tests the small helper,
and ships both jars within the existing checksummed sidecar transaction. The old
independent 0.19.0 pin is removed; `contracts.version` states the latest-main policy.
A literal SDK artifact alias preserves the fixed-source scp guard. Tests-only runs
also build/test the decoder and require the contracts source SHA. Existing
Indicator codec compatibility is checked against that same SDK/classpath.

Finite local checks: 56 numbered codec/layout assertions plus exact-text checks
for Strike/Direction, five shell routing tests, two Indicator compatibility tests,
Java17 bytecode compilation for the raw metadata reader, archive-unit completeness
and the changed Jenkinsfile's permitted-source guard. Five actual Java assertion
controls cover binding, null policy, API projection and decode-before-prefix;
known assertion/incidental/missing-method outcomes are calibrated separately.
The Strike positive codec model is synthetic, not a market/runtime fixture; four
other models use the shared source-renderer cuts. No live broker, archive, feed,
replay, load or max-domain qualification ran on the Mac.

One source bar, one prime and five singles measured raw line-write CPU medians
11/12 microseconds before/after, with identical 280-byte allocation
medians. The new framed projection plus line write measured 450 microseconds and
31,224 bytes, within the finite 100 ms/8 MiB ceiling with positive allocation
calibration. The 1-microsecond difference is timer quantization across five singles;
it is not a reliable measured percentage regression. These measurements do not
establish real source maxima or throughput. The 100 ms/8 MiB ceiling is a smoke
bound only.

The 83-test guard suite ran: 81 passed and two failed. An initial dynamic scp path
in this change was corrected to a literal guarded alias; the changed archive job
now passes. Repository-wide validation still reports two dedicated-effect errors
in the untouched `Jenkinsfile.kafka-reset`; its bytes match latest main. This task
neither edits nor triggers that reset job. The baseline proof and failed log are
preserved rather than presenting the entire suite as green.

Before activation: complete the normal Jenkins Linux archive suite and es4 native
classpath/source-domain/mixed-history/cold-registry/metadata-offset qualification,
then deploy readers before writers. No source/main preparation approval is an
application activation or complete migration claim.

The activation throughput gate must compare retained records per partition with
measured sustained records/second under the 900-second capture deadline. There is
no chunking outside UNTIL_TS mode; a timeout discards the capture and advances no
checkpoint, so an oversized legacy recapture would repeatedly fail. Illustratively,
450 microseconds/record permits only about 2.0 million records in 900 seconds before
I/O and startup costs; that single-record timing is not throughput qualification.
Also prove which decoder artifact loads: JDK22+ multi-file source launching may
resolve the adjacent source rather than the tested helper jar. Prove native Kafka
and Confluent classpath linkage separately from the Maven test classpath.

## Compiled capture entrypoint follow-up (2026-10-18)

Canonical captures now execute the compiled StrikeArchiveReader main class from
the first classpath entry, the shipped helper jar. That jar contains the reader
and decoder compiled with release21. Jenkins runs the existing codec/layout
suite against this exact jar and asserts both classes' CodeSource locations.
This removes the JDK22+ multi-file source-launch ambiguity. Marker and unrelated
raw-topic paths retain their original Java17-compatible source launch and do not
load the SDK. Native Confluent/Kafka linkage, sustained retained-history capture
and cold-registry/metadata-boundary qualification remain open before installation.

## Self-contained canonical reader

A read-only inventory found production's external archive classpath has 256 JARs,
while es4 has only 108 members of that set. The canonical reader now bundles its
guarded latest-main SDK and runtime dependencies with Maven Shade, including
service metadata. The canonical branch runs with that JAR alone. Indicator and
raw/offset-marker paths keep their existing classpaths and Java selection.

The Jenkins build installs its exact just-built SDK into a private Maven repository,
packages only the two canonical reader classes, and verifies every non-META-INF SDK
member against the bundled bytes. Metadata is merged by Shade and exercised by the
actual serializer tests. Duplicate ZIP members and oversized packages refuse.
All codec checks assert that the reader, SDK, Avro, Kafka and registry classes load
from the tested package, with no external library fallback. A bounded es4 transient
unit repeats the 56 codec/layout assertions on Linux with 512 MiB heap, a 1 GiB
no-swap unit, two available CPUs, 128 tasks and 60 seconds. It contacts no broker
or production host. Both provenance and test output are archived.

These checks establish packaged-codec compatibility only. Actual retained-history,
source-domain, committed-read boundary and sustained capture throughput remain
separate gates before installing the reader. No archive or offset is changed here.


### Retained-history qualification job

`Jenkinsfile.cvd-archive-retained-qualification` is a main-SCM, manually invoked,
read-only source qualification on ES4. It copies the installed production helper
bytes for execution on ES4, preserves captures under the build-owned
`/home/abhinav/ci/cvd-archive-history/run.*` directory, and never marks consumer
offsets or modifies archive schedules. The source SDK must equal current main;
changed archive component bytes require a new reviewed install first. The current
56-assertion packaged receipt is required conservatively; changes to that receipt
contract require updating this qualifier explicitly.

A result named `PASS_OBSERVED_RETAINED_HISTORY` qualifies only the nonempty topics
listed in `qualifiedNonemptyTopics`; `emptyTopics` remains an explicit coverage
limitation. It is JDK21 evidence, not native archive-host JDK26 compatibility,
cold registry, controlled mixed-history/transaction-boundary proof, or other
services' runtime/performance qualification. The private JVM resolver maps the dev
broker's advertised `host.docker.internal` to 192.168.100.102, matching the archive
host; it does not change the broker or host-wide DNS.

The job preserves at most four runs and refuses further allocation at its
8 GiB/8192-entry budget. When full, an operator must inspect receipts and confirm no
owned transient unit is active, then move completed run directories to a separate
preserved audit location with sufficient space. Do not delete captures, change
archive checkpoints, raise the cap automatically, or interrupt another run. Tool
and runtime prerequisites are checked before allocating a run directory. No heavy
qualification is permitted on the controller or production host.
