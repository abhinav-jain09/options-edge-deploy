# ES Footprint strike interaction — deploy Codex FINAL review (post-merge, 2026-09-11)

Review of `origin/main = 7a55b898` (the merged PR #1028 plus later scoped changes). Verdict: **REQUEST_CHANGES**, six findings. This follow-up PR folds them. The review text is appended verbatim below the dispositions.

## Dispositions

| # | Finding | Disposition | What changed | Pinning test |
|---|---|---|---|---|
| 1 | P1: the strike archive advances past unread committed records (HWM checkpoint under `read_committed`) | **FIXED** | New `scripts/ops/archive/StrikeArchiveReader.java`, a single-file program the archiver runs with the JDK source launcher against `$KAFKA_BIN/../libs/*` (no build step; added to the archive deploy `UNIT`). It uses `KafkaConsumer` with `isolation.level=read_committed`, `assign()` + `seek(from)`, and takes the exclusive boundary from `endOffsets()`, which under `read_committed` is the last stable offset (LSO). In `UNTIL_TS` mode the boundary is `min(LSO, time-bounded offset)`. It polls until `position() >= boundary`, writes only records with `offset < boundary` in the strict layout (`<TimestampType>:<ts>\tPartition:<p>\tOffset:<o>\t<key>\t<value>`, with the real timestamp type and raw TAB/CR/LF escaped and counted), writes a one-line summary, and exits 0 only for `status=COMPLETE`. A deadline before the boundary is `TIMEOUT` (exit 3). `oe-archive-kafka.sh` sends only `OE_COMMITTED_READ_TOPICS` (strike) through this reader. It re-checks the summary itself (status, numeric fields, `position >= boundary`, boundary not past the time bound, file record count == reported count). Then the checkpoint **and** the manifest `offset_to` are the boundary. A completed range with zero application records (only markers or aborted records) is published as an empty file with its manifest line and still advances. Every other topic keeps the console-consumer path: same arguments as before #1028's `read_committed` flag, same checkpoint, and a byte-identical manifest line. | `test-archive-reset.sh` §12a–12o (Debian container), plus a real-broker run (below): committed+aborted; unresolved-then-committed (the retry captures every withheld record exactly once and the ranges stay contiguous); marker/aborted-only range; delayed finalize (open transaction at the checkpoint, so nothing is captured and nothing is skipped); reader timeout (checkpoint unchanged, nothing published); three "lying reader" cases that exit 0; a non-declared topic never reaches the reader and keeps its old arguments and manifest line |
| 2 | P1: "retention −1 makes skipped ranges repairable" is not a recovery guarantee | **PARTIAL.** The claim is removed and an interlock is added. Rewind/supersede stays a documented manual procedure, not automated or tested. | The false claim is gone from `oe-topics.env` and the archiver comment. The inventory note now gives the real conditions: retention stops retention deletion, not a whole-broker wipe; `cleanup-es4.sh` deletes the whole Kafka data dir; so recapture is possible only while the TopicId is unchanged and the range is still retained. The es4 manifest note never made the claim (it only says "one session between nightly cleans"), so it is unchanged. **Interlock:** after each durable strike capture and checkpoint, the archiver (ENV=es4 by default) commits the boundary as the offset of consumer group `oe-archive-committed-boundary` on the es4 broker. The new `scripts/es4/strike-archive-interlock.sh` compares that offset with the strike log end, and `cleanup-es4.sh` refuses to wipe while it is short. The check runs twice: at preflight before any mutation (fresh runs only), and just before `docker compose down` once producers are quiesced. It fails closed on unreadable state, an absent group, or a marker ahead of the log end (a re-created topic). DRY runs report without blocking. The only opt-out is explicit: `ES4_STRIKE_ARCHIVE_INTERLOCK=off`, reachable only through the new default-false Jenkins parameter `ACCEPT_UNARCHIVED_STRIKE_LOSS`, and the wipe then logs exactly which ranges it discards. An idle archive run re-records the marker, so a lost marker cannot block the wipe for ever. **Repair procedure** (lock, verify TopicId and retained range, recapture a superset, supersede old files and manifest lines into `dt=<date>/_superseded/<stamp>/`, re-verify) is written in the `oe-topics.env` inventory note. **Not done:** the reviewer's "test rewind, overlap, interrupted repair and source recreation" as automated tests, and a verifier that understands supersession. The procedure keeps superseded files out of the verifier's glob by moving them, so it does not depend on one. | `scripts/es4/strike-archive-interlock-test.sh`: 16 behavioural cases (37 assertions) and 7 wiring assertions (order in `cleanup-es4.sh` relative to replica capture, quiesce, `compose down` and the data-dir delete; both call sites fatal; the Jenkins parameter defaults false; both on-box invocations pass the mode; the archiver and interlock group names agree). §12n: marker at the checkpoint, re-recorded on an idle rerun, a failed marker write does not fail the archive, and ENV=prod writes none. |
| 3 | P2: the strike presence floor is never checked for es4 | **FIXED** | `OE_ALL_TOPICS_es4="$OE_ES4_TOPICS"` in `oe-topics.env`. New crontab entry `5 20 * * 1-5 ENV=es4 … oe-archive-verify.sh` with its own log. The verifier sends its own alert, exactly as the prod entry does. | §14 runs the real verifier over a tree built from the real inventory and floors: strike folder absent gives rc 1, `MISSING` and an alert delivered for env=es4; 499 records gives rc 1, `PARTIAL`, "below the floor of 500"; 500 gives rc 0, `VERDICT COMPLETE`. Presence only: the comment states 500 proves nothing about transactional completion. The crontab has exactly one es4 verify entry. |
| 4 | P2: the partition-repair path can destroy the preserved strike log | **FIXED** | `es.futures.footprint.strike` is added to the single `OPTIONS_EDGE_NEVER_RECREATE_TOPICS` assignment. `apply-topics.sh` does not re-point it for `TOPIC_SET=es4`, so both sets are covered. | New `scripts/kafka/apply-topics-strike-safety-test.sh`, which drives the real `apply-topics.sh` against mocked CLIs. At the declared shape: no delete, and the retention/bytes contract is reconciled (production, dev, es4). With 4 partitions and `KAFKA_RECREATE_MISMATCHED_TOPICS=true`: non-zero exit, zero delete and zero create calls of any topic, and the refusal names NEVER_RECREATE (production, dev, es4). A mutant `topics.env` without the entry deletes the log in both sets, which shows the test is sensitive to the declaration. |
| 5 | P2: a processing failure can still publish and checkpoint | **FIXED** (both paths) | Console path: every `PIPESTATUS` element is saved (consumer, filter: 0/1 accepted, gzip). `scan_archive_file`'s own status is captured before its output is parsed. Committed path: reader, gzip and scan statuses are all checked, and the file's record count must equal the reader's. Both paths: sha256 and size are computed over the verified bytes before the rename, and a missing checksum fails the capture instead of writing `"unknown"`. A failed checkpoint append is now counted as a failure (it had been ignored). | §12j (committed path) and §13 (console path) inject faults with PATH shims: compressor (`gzip -6` dies mid-stream), decompressor (`zcat` exits 7 after output), scanner (the statistics awk prints then exits 7, the reviewer's reproduction), filter (`grep` rc 2, console path), and publication (`mv` fails). In every case the run fails, the checkpoint is unchanged and no data file is published. A healthy control run archives, and its manifest checksum equals the published file's. |
| 6 | P2: the offset boundary bounds neither contents nor completion time | **FIXED for the strike log.** Console-consumer topics are deliberately unchanged. | The reader enforces the exclusive boundary. Records past it are never written, so they cannot count toward a historical floor. Completion depends on the consumer position reaching the boundary taken at start, not on the message budget, the 60 s idle timeout or the 17:00 finalize. The unconditional idle-completion claim is removed from `oe-topics.env`. Other topics keep the budget and timeouts by design (this PR must not change their capture). | §12e: `UNTIL_TS` with abundant later records. Only offsets below the cutoff are archived, the manifest counts only in-range records, and the checkpoint is the cutoff. Real broker: `--max-end 8` wrote 0,1,2,7 (not 8+), and a read that starts with later records present stops at the LSO. |

Earlier unnumbered items that this PR does not change: the GAP wording that says "records LOST" (a console-path log line, left alone to keep that path byte-for-byte), recovery duration / peak RSS / storage runway (still provisional, not measured), `deploy-all` filename order, and the textual isolation assertion in `es-cvd-mirror-shape-test.sh`.

## Real-broker run of `StrikeArchiveReader.java` (dev Kafka on this Mac, 127.0.0.1:19092, Kafka 4.3.0 clients, JDK 21)

The harness is committed as `scripts/ops/archive/broker-test/strike-reader-broker-test.sh` with `TxnFixture.java`. It refuses any non-local bootstrap, and it is not part of the Jenkins suite because it needs a broker. It ran twice, with identical results: first as a scratch copy (topic `oe-strike-reader-test-1789087201`), then as the committed file (`KAFKA_HOME=…/kafka-options-edge/current BOOTSTRAP=127.0.0.1:19092`, topic `oe-strike-reader-test-1789089959`). Each run created one uniquely named 1-partition topic. A transactional fixture producer wrote to that topic only: committed k0–k2 (k2's value holds a raw TAB and LF) at 0–2 with a marker at 3, aborted a0–a1 at 4–5 with a marker at 6, committed k3–k4 at 7–8 with a marker at 9, and an open transaction o0–o1 at 10–11. After a signal it committed that transaction (marker at 12) and wrote an abort-only transaction (13, marker at 14). Results, all 35 checks ok (0 FAIL):

1. From 0 with the transaction open: `boundary=10` (= LSO), `position=10`, `records=5`, offsets `0 1 2 7 8`, no aborted or open records, `escaped=1` with k2 on one line.
2. After the commit, from 10: `boundary=15`, `records=2` (offsets 10, 11), so the retry captured both withheld records.
3. From 12 (commit marker + aborted record + abort marker only): `COMPLETE`, `records=0`, `position=15`.
4. From 15: `COMPLETE`, 0 records.
5. `--max-end 8`: `boundary=8`, offsets `0 1 2 7` only.
6. After switching the topic to `LogAppendTime`, the record printed as `LogAppendTime:<ts>\tPartition:0\tOffset:15\tlk\t{"lat":1}`.
7. After `kafka-delete-records` to offset 7, a read from 0 gave `FAILED` (`OffsetOutOfRangeException`, `auto.offset.reset=none`), non-zero exit.
8. Unreachable bootstrap with a 3 s deadline gave `FAILED`, non-zero exit.
9. `--mark-group … --mark-offset 16` committed and read back. `kafka-consumer-groups --describe` showed `CURRENT-OFFSET 16, LOG-END-OFFSET 16, LAG 0`.

Library logging stayed off the record file (records go to `--out`, never stdout). After each run the test group and the test topic were deleted and deletion was confirmed. No other topic was written. The transactional producer necessarily also wrote to the broker's `__transaction_state`.

## Validation of this head

- `test-archive-reset.sh`, run in Debian exactly as specified: `docker run --rm -v "$PWD/scripts/ops/archive:/suite:ro" python:3.12-slim bash -c '…; cp -r /suite /s && cd /s && chmod +x *.sh && ./test-archive-reset.sh'`. Result: **`test-archive-reset: ALL PASS`, 191 `PASS` lines, 0 `FAIL`**, repeated on the final tree. `Jenkinsfile.archive-scripts-deploy` now requires at least 191 (it was 50). The first container run had 52 failures: every committed-read case failed with `rc=127` because the call site was `timeout N run_strike_reader`, and `timeout` cannot run a shell function. The fix applies the timeout inside `run_strike_reader`, and the suite is the reason it did not ship.
- New: `scripts/kafka/apply-topics-strike-safety-test.sh` OK (29 ok); `scripts/es4/strike-archive-interlock-test.sh` OK (44 ok). Both are added to `.github/workflows/deploy-validation.yml`.
- OK: `validate-durable-topic-preservation.sh`, `validate-durable-topic-preservation-mutation-test.sh`, `validate-mirrored-topic-contracts.sh`, `validate-mirrored-topic-contracts-test.sh`, `es-cvd-mirror-shape-test.sh`, `validate-services.sh`, `validate-archive-unit-completeness.sh` (19 dependencies; it now also requires every `*.java` the unit names or ships), `verify-topics-pure-compact-test.sh`, `apply-topics-ledger-safety-test.sh`, `validate-jenkinsfile-shell-blocks.sh`, `validate-jenkinsfile-groovy-escapes.sh`, `validate-archived-topic-retention.sh`, `validate-archive-uncompaction-env.sh`, `validate-declared-overrides-are-explicit-test.sh`, `validate-es4-reconciliation-guard-test.sh`, `cleanup-topics-durable-test.sh`, `reset-preserved-topics-test.sh`, `validate-dev-mac-watchdog.sh`, `validate-docker-top-has-pid.sh`, `validate-tests-are-collectable.sh`.
- `python3 -m unittest tests.test_es4_topics_ssot tests.test_es4_topic_partition_contract`: 47 OK. One earlier run reported `created failed on a clean broker` while `validate-services.sh` was running at the same time. Both a rerun on this tree and the same modules on a clean `origin/main` worktree then passed. `tests.test_es4_cleanup_full_reset` has 1 failure and 1 error that are **identical on `origin/main`**: they assert strings (`TOPIC_STATE=…`, "recreating every snapshotted application topic") that main's `cleanup-es4.sh` no longer contains. This PR does not cause them, and the GitHub workflow does not run that module.
- `validate-es4-render.sh` on the committed tree (482aba5d): OK, 36 manifests = 27 rendered + 7 hand-authored + 2 known-stale. Every `sh '''` block of `Jenkinsfile.es4-deploy` (not covered by `validate-jenkinsfile-shell-blocks.sh`, which checks the mirror Jenkinsfiles) parses under `bash -n`.
- `git diff --check`: clean.

## Rollout risks recorded here, not solved here

- **JDK source launcher on the prod host.** The archiver runs `$JAVA_HOME/bin/java … StrikeArchiveReader.java`, and the crontab sets `JAVA_HOME=$(ls -d /usr/lib/jvm/*jre-17*)`. The source launcher needs the `jdk.compiler` module, and some JRE packagings omit it. This could not be checked, because this work did not touch .252. If it is missing, the strike capture fails loudly every night: checkpoint unchanged, `committed-read capture FAILED` in `archive-es4.log`, run `failed=1`. The new es4 verifier then alerts MISSING and the interlock keeps refusing the es4 wipe. Before the deploy, check on .252 that `"$(ls -d /usr/lib/jvm/*jre-17* | head -1)/bin/java" --list-modules | grep jdk.compiler` finds it and that `/opt/kafka/current/libs/kafka-clients-*.jar` exists.
- **The es4 clean-reset now waits for the archive marker.** After merge, a clean-reset refuses until an es4 archive run by the new archiver has recorded `MARK es.futures.footprint.strike` (the 17:01 cron after `archive-scripts-deploy`). A clean-reset in between refuses, which is the safe direction; its message gives the remedy and the explicit opt-out.
- **The first es4 verification may report older gaps** in other es4 topics (they were never verified before). That is the check doing its job, not a new failure.

## Review text (verbatim)

**I cannot approve the current main tree.** The deployment wiring is consistent, but transactional archival remains unsafe. Round 7 limits the exposure; it does not close it. Its unconditional repairability argument is contradicted by es4’s reset path.

Reviewed `HEAD = origin/main = 7a55b898`, including squash `9d525d2c` and subsequent changes to the scoped files. This is a code review; deployed image contents and production archive files were not independently inspected.

Earlier findings are reconciled below. **CLOSED by removal** means the defective implementation is absent; it does not imply that the underlying archival requirement is satisfied.

| Earlier finding | Status | Current-tree evidence |
|---|---|---|
| R1 #1 — Aborted revisions mirrored | **CLOSED** | Generated consumer uses `read_committed`: [mirror:192](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:192). Configuration assertion at [shape-test:29](/private/tmp/oe-deploy-fpstrike/scripts/ci/es-cvd-mirror-shape-test.sh:29); broker integration coverage remains absent. |
| R1 #2 — Incorrect reset classification | **CLOSED** for the requested classification | Strike is preserved at [topics.env:189](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:189) and [premarket-reset:241](/private/tmp/oe-deploy-fpstrike/scripts/ops/premarket-reset.sh:241). Separate destructive paths remain: findings 2 and 4 below. |
| R1 #3 — Unsafe transactional archive integration | **PARTIAL** | Committed-only selection exists at [archive:109](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:109); unproved checkpoint remains at [archive:620](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:620). |
| R1 #4 — Unsupported floor of 100 | **CLOSED** as the requested provisional correction | Rationale and `500` at [inventory:202](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:202). The floor’s operational wiring has a separate defect, finding 3. |
| R2 #1 — Checkpoint beyond proven capture | **STILL OPEN** | Positive short reads qualify at [archive:577](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:577); checkpoint uses original `endoff` at [archive:620](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:620). |
| R2 #2 — Missing byte-retention contract | **CLOSED** | Both declarations: [default:258](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:258), [es4:685](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:685). Create/reconcile: [apply:196](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:196), [apply:305](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:305). Verification: [verify:110](/private/tmp/oe-deploy-fpstrike/scripts/kafka/verify-topics.sh:110). |
| R3 #1 — Undeclared transactional writers remain unsafe | **PARTIAL** | The declaration-dependent checkpoint mechanism is removed. Unlisted topics retain baseline `read_uncommitted`; all accepted captures still checkpoint `endoff`: [archive:552](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:552), [archive:620](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:620). Safe transactional archival is not established. |
| R3 #2 — Boundary crossing discards valid capture | **CLOSED by removal** | The crossing-rejection mechanism is absent; successful captures publish at [archive:577](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:577). Timeout starvation remains R4 #3. |
| R4 #1 — Trimming corrupts binary payloads | **CLOSED by removal** | No trimming between [capture:551](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:551) and [publication:587](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:587). |
| R4 #2 — Ignored scan/trim failures | **PARTIAL** | Removed helpers are gone; capture/statistics failures remain insufficiently checked at [archive:559](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:559). |
| R4 #3 — Active sparse capture cannot finish | **STILL OPEN** | Record-count budget and 900-second timeout at [archive:551](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:551); timeout capture discarded at [archive:627](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:627). |
| R4 #4 — Offset column breaks schema discovery | **CLOSED by removal** | Offset printing is STRICT-only at [archive:557](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:557); original extractor remains at [archive:280](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:280). |
| R5 #1 — Payload continuation forges checkpoint | **CLOSED by removal** | No payload-derived offset parser; checkpoint takes queried `endoff`: [archive:620](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:620). |
| R5 #2 — `LogAppendTime` rejected | **CLOSED by removal** | Acceptance no longer depends on timestamp spelling: [archive:577](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:577). |
| R5 #3 — Timeout publishes incomplete records/bypasses STRICT | **CLOSED by removal** | Status `124` cannot pass [archive:577](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:577). |
| R5 #4 — Future records satisfy historical floor | **STILL OPEN** | Count-bounded read at [archive:553](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:553); entire-file count paired with original bounds at [archive:607](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:607). |
| R5 #5 — Processing failures ignored | **PARTIAL** | Added parser removed; inherited unchecked statuses remain at [archive:559](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:559). |
| R6 #1 — Incomplete inventory gate passes unknown writers | **CLOSED by removal** | Gate removed from the [archive deployment pipeline:88](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.archive-scripts-deploy:88). No replacement transactional-completeness guarantee exists. |
| R6 #2 — Raw strike key forges offset | **CLOSED by removal** | No key-derived checkpoint: [archive:620](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:620). |
| R6 #3 — Timeout publishes malformed tail | **CLOSED by removal** | Successful consumer status required: [archive:577](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:577). |
| R6 #4 — Historical floor counts overshoot | **STILL OPEN** | Same surviving defect as R5 #4: [archive:607](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:607). |
| R6 #5 — Processing statuses partly ignored | **PARTIAL** | [archive:559](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:559), finding 5 below. |
| R6 #6 — Schema extractor misreads `Offset:` key | **CLOSED by removal** | Layout inference removed; fixed original column restored at [archive:280](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:280). |
| R6 #7 — Incorrect timestamp-less spelling | **CLOSED by removal** | No timestamp anchor governs acceptance: [archive:577](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:577). |
| R7 #1 — Isolation change introduces skipped commits | **PARTIAL** | Scope narrowed at [archive:109](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:109); strike’s unsafe checkpoint remains. The repairability assertion at [inventory:139](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:139) is insufficient. |
| R7 #2 — PR body advertises withdrawn implementation | **CLOSED** for those stale implementation/test claims | [Round-7 record:9](/private/tmp/oe-deploy-fpstrike/ES-FOOTPRINT-STRIKE-DEPLOY-CODEX-ROUND7.md:9) agrees with the current [PR body](https://github.com/abhinav-jain09/options-edge-deploy/pull/1028). The body still repeats the defective repairability argument. |

The earlier unnumbered findings and qualifications resolve as follows:

| Earlier item | Status | Evidence |
|---|---|---|
| Recovery duration, peak RSS, missed-clean capacity | **PARTIAL** | Sizing remains explicitly provisional: [manifest:62](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:62). No representative capacity results supplied. |
| Fold budget excludes reconstruction | **CLOSED in inspected sibling source** | Post-reconstruction budget check exists at [StrikeRuntime.java:388](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeRuntime.java:388). This does not attest the deployed image or provide a preemptive deadline. |
| Unbounded dev/prod storage | **PARTIAL** | Unlimited retention remains intentional at [topics.env:250](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:250) and [258](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:258); storage runway remains unmeasured. |
| Archive-first rollout, mirror reinstall, filename ordering | **PARTIAL** | Current PR gives explicit ordering and production levels reinstall; [deploy-all:232](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es4-deploy:232) still follows filenames. Safe archival dependency remains unsatisfied. |
| §12(e) missing status assertion | **CLOSED by removal** | Added section is absent; suite ends at [test:403](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:403), byte-identical to the pre-merge suite. |
| OPB transactional-classification explanation | **CLOSED by removal** | Classification mechanism removed; current scope is [archive:109](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:109). |
| GAP wording overstates record loss | **STILL OPEN** | Offset difference is called “records LOST” at [archive:510](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:510). |
| Tests beyond static declarations | **PARTIAL** | Isolation check is textual at [shape-test:29](/private/tmp/oe-deploy-fpstrike/scripts/ci/es-cvd-mirror-shape-test.sh:29); obsolete sibling compaction fixtures persist at [138](/private/tmp/oe-deploy-fpstrike/scripts/ci/es-cvd-mirror-shape-test.sh:138). |
| Retention plumbing and manifest/settings agreement | **CLOSED**, excluding capacity | [apply:196](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:196), [mirror:265](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:265), [manifest:68](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:68). |
| Earlier whitespace defects | **CLOSED** | `git diff --check 9d525d2c^ HEAD` passes. |

The adversarial sweep produced these findings, most severe first.

1. **P1 — Strike archival still advances past unread committed records.**

   **Evidence:** End offsets are captured at [archive:318](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:318), consumption is committed-only at [552](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:552), any positive non-STRICT capture with consumer status zero passes at [577](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:577), and the original end becomes the checkpoint at [620](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:620).

   Executing the actual acceptance condition produced:

   ```text
   endoff=1200, got=1099, consumer_rc=0
   → ACCEPT checkpoint=1200 records=1099
   ```

   An unresolved transaction beginning at offset `1100` can therefore commit after capture, while the next run resumes at `1200`. Kafka explicitly withholds records behind open transactions; console-consumer timeout can return through its normal termination path. [Kafka isolation semantics](https://kafka.apache.org/39/configuration/consumer-configs/#isolation.level), [console-consumer implementation](https://raw.githubusercontent.com/apache/kafka/3.9.1/tools/src/main/java/org/apache/kafka/tools/consumer/ConsoleConsumer.java).

   **Why it matters:** Scoping protects unrelated topics from this new visibility change. It leaves the newly integrated strike archive exposed. Documenting the omission does not make its checkpoint truthful.

   **Corrected code or test:** Keep committed-only reading. Implement capture using Kafka record metadata:

   ```text
   boundary = read_committed exclusive end offset
   export complete records with offset < boundary
   finish only after consumer position reaches boundary
   durably publish data and manifest
   checkpoint = boundary
   ```

   Add committed, aborted, unresolved-then-committed, marker-only and delayed-finalize tests. The retry must capture every later-committed record previously withheld.

2. **P1 — “Retention −1 makes skipped ranges repairable” is not a valid recovery guarantee.**

   **Evidence:** The guarantee appears at [inventory:139](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:139) and [archive:546](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:546). The manifest itself describes nightly cleans at [manifest:65](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:65). The reset subsequently deletes the entire Kafka data directory at [cleanup-es4:256](/private/tmp/oe-deploy-fpstrike/scripts/es4/cleanup-es4.sh:256), without an archive-completeness interlock.

   **Rewinding is mechanically possible before source loss.** [Archive:414](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:414) selects the **last matching checkpoint line**, not the maximum offset:

   ```text
   append 0=1000 after 0=1200 → next read starts at 1000
   edit an older line only   → next read still starts at 1200
   ```

   But strike files omit individual offsets ([archive:557](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:557)); the manifest records the claimed boundary, not the actual completed position ([607](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:607)). A rewind leaves the old files and claims intact. The verifier sums overlapping captures at [verifier:150](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-verify.sh:150).

   **Why it matters:** Retention settings prevent retention-based deletion, not whole-broker wipes. After recreation, the same offset names a different log. Before recreation, conservative recapture is possible, but the current files cannot identify the exact skipped range or automatically repair the old completeness claims.

   An in-memory execution of the actual verifier reported `COMPLETE` for the short capture above. Adding an overlapping rewind capture also reported `COMPLETE`, with inflated aggregate records.

   **Corrected code or test:** Make source destruction depend on verified durable capture through the finalized session boundary. Until that exists, preserve the suspect source. Add a repair procedure that locks the topic, verifies source identity and retained range, recaptures conservatively, and explicitly supersedes/reconciles old manifests. Test rewind, overlap, interrupted repair and source recreation. Replace “keeps every record for ever” with a conditional statement reflecting these requirements.

3. **P2 — The strike presence floor is declared but never checked for es4.**

   **Evidence:** Strike’s floor is at [inventory:205](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:205). The verifier requires `OE_ALL_TOPICS_${ENV_NAME}` at [verifier:61](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-verify.sh:61), but sourcing the current inventory yields:

   ```text
   OE_ALL_TOPICS_es4 = UNDEFINED
   ```

   The scheduled verifier runs with `ENV=prod` at [crontab:57](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive.crontab:57). The es4 archiver entry at [42](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive.crontab:42) does not verify the floor. The supposed nightly backstop explicitly selects three other topics at [daily:152](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-daily.sh:152).

   **Why it matters:** A missing or undersized strike archive has no functioning es4 presence-floor check. Manually invoking the verifier with `ENV=es4` fails for missing policy rather than checking the date.

   **Corrected code or test:** Define `OE_ALL_TOPICS_es4="$OE_ES4_TOPICS"` after the inventory assignment, schedule independent es4 verification, and wire failure alerting. Test an absent strike directory and 499 records as failures; 500 should satisfy only the presence check, never transactional completeness.

4. **P2 — The partition-repair path can destroy the preserved strike log.**

   **Evidence:** Strike is exact-partition and reset-preserved, but absent from [NEVER_RECREATE:478](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:478). [Apply:341](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:341) consults only that protection before reaching [delete:356](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:356).

   Running the actual apply logic with mocked Kafka commands, four existing partitions and `KAFKA_RECREATE_MISMATCHED_TOPICS=true` produced:

   ```text
   Repairing EXACT-partition topic es.futures.footprint.strike: partitions=4 -> 1
   MOCK_DELETE_STRIKE
   MOCK_CREATE_STRIKE
   exit 0
   ```

   **Why it matters:** A global repair flag can erase the history while consumer-group offsets survive—the recovery failure that motivated preservation in R1.

   **Corrected code or test:** Add strike to the existing `OPTIONS_EDGE_NEVER_RECREATE_TOPICS` assignment. Test both topic sets with partition drift and the global flag enabled: execution must fail before any delete/create call and require an explicit migration.

5. **P2 — Processing failure can still produce a published, checkpointed archive.**

   **Evidence:** [Archive:559](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:559) retains only the consumer’s pipeline status. [561](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:561) hides the statistics helper’s exit status behind `read`.

   Fault injection against those current statements produced:

   ```text
   scan_archive_file prints statistics, then returns 7
   → ACCEPT got=1099 checkpoint=1200
   ```

   **Why it matters:** Partial decompression output or downstream processing failure can qualify as verified capture. A checksum subsequently records the bytes that landed; it does not prove successful capture. This is inherited code, but remains on strike’s production path.

   **Corrected code or test:** Save and validate every pipeline component status immediately; capture and check the statistics helper’s status before parsing it. Validate gzip integrity and checksum generation before advancing. Inject compressor, decompressor, scanner and publication failures; each must leave the checkpoint unchanged.

6. **P2 — The claimed offset boundary does not bound capture contents or completion time.**

   **Evidence:** [Archive:553](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:553) uses `--max-messages=endoff-from`. Transaction markers consume offsets but do not consume that message budget. The file can therefore contain records beyond `endoff`, while [manifest:607](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:607) still claims the original range. Alternatively, continuous sparse traffic prevents idle completion until the 900-second kill, after which [627](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:627) discards the capture.

   **Why it matters:** Historical `UNTIL_TS` captures can include future traffic; active recovery can repeatedly discard a valid prefix. The inventory still makes an unconditional idle-completion claim at [111](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:111), despite its later qualification.

   **Corrected code or test:** Enforce the exclusive boundary inside the reader described in finding 1. Test a historical cutoff with abundant later records and a continuously producing sparse stream. Assert exact in-range contents, truthful manifest counts and completion independent of the record-count budget.

The remaining deployment sweep found consistent strike declarations: one partition in both sets; exact-partition membership; `delete`, unlimited time and byte retention; correct mirror arm and isolation; matching service selectors, port `8153`, single replica with `Recreate`, topic prefixes and settings; and correct gateway environment values. The 300-second fold setting leaves nominal room within the 600-second rollout wait, but does not establish capacity.

Independent validation passed: nine shell/env syntax checks, 15 mirrored-topic contracts, 18 archive-unit dependencies, 26 extracted mirror scenarios, byte-retention create/reconcile and verification cases, preservation’s static checks, 27 in-memory es4 renders, and actual kustomize equality for all three gateway slices. Full temporary-file suites and broker integration tests were not run in this read-only workspace. No files or deployments changed.

**Institutional — Not met.** Configuration is coherent, but archive boundaries and completeness claims are not trustworthy.

**Military — Not met.** Unresolved transactions, processing failures and destructive repair/reset paths can defeat recovery.

**NASA — Not established.** Full-session and missed-clean recovery time, peak RSS and storage runway remain unmeasured; archival completion still depends on inappropriate message-count and timeout behavior.

VERDICT: REQUEST_CHANGES
Make transactional capture and recovery provable, activate es4 completeness verification, and protect strike history from destructive repair.
