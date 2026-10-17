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
existing same-topic history, topic identities, groups, offsets and files remain.
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
11/12 microseconds before/after (+9.09%), with identical 280-byte allocation
medians. The new framed projection plus line write measured 450 microseconds and
31,224 bytes, within the finite 100 ms/8 MiB ceiling with positive allocation
calibration. These measurements do not establish real source maxima or throughput.

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
