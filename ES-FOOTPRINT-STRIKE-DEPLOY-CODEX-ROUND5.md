# ES Footprint strike interaction — deploy Codex round 5 (gpt-6-astra, 2026-09-10)

Review of PR #1028 at 54cd79cd. Verdict: **REQUEST_CHANGES**. Dispositions, folded in the following commit:

**The finding behind the findings.** Rounds 4 and 5 both landed on the same thing: `kafka-console-consumer` writes raw value bytes, so on a topic whose values can contain a newline the offsets and the payload share ONE unframed stream. Round 5's reproduction settles it — a continuation line carrying an in-range `Offset:` advanced the checkpoint past unread records, which is exactly the gap the change existed to close. The claim that a payload could only be conservative was wrong, and no anchoring makes it right. So the universal rule round 3 asked for is withdrawn: it is applied where it is SOUND — topics declared in `OE_TRANSACTIONAL_TOPICS_<env>`, whose values are JSON, where RFC 8259 forbids a raw newline inside a value and one line therefore IS one record — and every other topic keeps the format and the checkpoint it has always had, byte for byte.

That leaves round 3's real objection: a list nobody can keep complete. It is now kept complete by a gate rather than by hope — `scripts/ci/validate-archive-transactional-inventory.sh`, wired into `Jenkinsfile.archive-scripts-deploy`, fails the build when a declared topic is not in that environment's archive inventory, when a known exactly-once writer IS archived and is NOT declared (the table carries every writer you named, each with the manifest that enables EOS), and when the archiver stops gating on the declaration at all.

| # | finding | disposition |
|---|---|---|
| 1 | a payload continuation can advance the checkpoint past unread transactional records | FIXED by scope, as above: the probe runs only for declared JSON topics, where the forgery you demonstrated cannot be expressed. §13(b) keeps an adversarial `Offset:` inside a JSON value and asserts the checkpoint does not move past what the run bounded; §13(a) proves an UNDECLARED binary topic is not even asked for offsets (its shim exits 9 if it is), keeps its old checkpoint and every payload byte. |
| 2 | the anchor stopped archival of `LogAppendTime` topics | FIXED — the anchor accepts `CreateTime`, `LogAppendTime` and `NoTimestampType`. §13(d) archives a `LogAppendTime` topic and advances its checkpoint. (With the scope above these topics never reach the probe anyway; the anchor is correct regardless.) |
| 3 | timeout acceptance publishes incomplete records and bypasses STRICT hole detection | FIXED — a timeout capture is accepted only for a declared topic that is NOT STRICT, and its last record is never credited. A STRICT topic refuses a timeout capture outright, so its whole-span check is never bypassed (§13(e), both halves). The published-artifact concern for a mid-record kill on a declared topic remains: the record is re-read next run, but the first file can carry a truncated line. Recorded as a follow-up together with the framed-capture reader, since both need the same thing — a reader that knows record boundaries. |
| 4 | post-boundary records can falsely satisfy historical session completeness | ACKNOWLEDGED and narrowed by the same scope change: only declared JSON topics can archive a record above their claimed `offset_to`, and only when a producer writes during the capture. The floor is already documented as a presence check that does not establish session completeness. |

Suite after the fix, in the Debian container Jenkins uses: **ALL PASS, 122 assertions**. The new gate passes: 2 es4, 11 prod, 4 dev declarations checked against the inventories.

## Review text (verbatim)

Reviewed all 22 changed files at `54cd79cd`, both requested diff ranges, and the archive consumers. **Five findings prevent approval.** The default checkpoint policy is the right direction, but its text parser does not establish the boundary it claims.

Part A — round-3 findings and notes:

| Item | Status | Evidence and test |
|---|---|---|
| **#1: undeclared transactional writers use unsafe HWM checkpointing** | **CLOSED narrowly** | Offsets and captured-max checkpointing are unconditional at [archiver:604](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:604) and [634](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:634). [§12(h):567](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:567) pins the undeclared-writer case. The inventory loophole is closed; finding 1 below breaks the broader safety claim. |
| **#2: boundary crossing discards valid captures and prevents progress** | **PARTIAL** | Trimming is gone. [§12(f)/(g):484](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:484) now expects complete captures, including post-boundary records. [§13:625](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:625) covers simulated timeout acceptance. Progress improves, but publication integrity and historical completeness remain wrong—findings 3–4. |
| **§12(e): missing exit-status assertion** | **CLOSED** | [Test:483](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:483) explicitly requires a failed run for the marker-only remainder. |
| **Recovery duration, peak RSS, missed-clean capacity** | **PARTIAL** | [Manifest:62](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:62) remains explicitly provisional. Neither §12 nor §13 measures representative recovery. |
| **Unbounded dev/prod storage** | **PARTIAL** | Unlimited retention and preservation remain intentional at [topics:251](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:251) and [189](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:189). No measured storage-runway test is supplied. |
| **Archive-first rollout, levels-mirror reinstalls, filename order** | **CLOSED as documented procedure** | [Round-3 disposition:12](/private/tmp/oe-deploy-fpstrike/ES-FOOTPRINT-STRIKE-DEPLOY-CODEX-ROUND3.md:12) records the sequence. This is an operational instruction, not an automated ordering guarantee. |
| **OPB classification corrections** | **CLOSED** | [Inventory:132](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:132) distinguishes transactional sinks from the control publisher and unused dashboard sink. §12(h) establishes checkpoint independence from that annotation. |
| **Retention plumbing and mirror arm** | **CLOSED** | [Create:196](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:196), [reconcile:301](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:301), [verify:110](/private/tmp/oe-deploy-fpstrike/scripts/kafka/verify-topics.sh:110), and [mirror:265](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:265) agree. Retention regressions are present; nine extracted strike mirror scenarios passed independently. |
| **Shell failure handling** | **PARTIAL** | First-pass decompression failures now stop publication at [archiver:610](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:610). Other processing statuses remain unchecked—finding 5. §13 contains no decompression/awk failure injection. |
| **Manifest/settings agreement** | **CLOSED, excluding capacity** | [Manifest:69](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:69) agrees with `StrikeSettings`: broker, topic resolution, IDs, port, universe, band, series limit and explicit fold timeout. Selector checks and all 27 in-memory es4 renders passed. |

Earlier mirror-isolation, reset-preservation, provisional-floor and whitespace corrections remain closed.

Part B — findings, most severe first:

1. **P1 — A payload continuation can advance the checkpoint past unread transactional records.**

   **Evidence:** [The probe:152](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:152) recognizes physical lines, not Kafka record boundaries. Its `o < end` condition only excludes offsets at or above HWM. It does not authenticate an in-range offset.

   With HWM `1200` and real captured maximum `1099`, a value containing this continuation passes the anchor:

   ```text
   CreateTime:1<TAB>Partition:0<TAB>Offset:1199<TAB>payload continuation
   ```

   Executing the extracted production logic returned:

   ```text
   maxoff=1199
   consumer_rc=0 got=2 checkpoint=1200 span=1200
   ```

   Kafka’s formatter writes raw value bytes, so embedded newlines can create precisely this physical layout. [Kafka 4.3 formatter](https://github.com/apache/kafka/blob/4.3.0/tools/src/main/java/org/apache/kafka/tools/consumer/DefaultMessageFormatter.java#L165)

   **Why it matters:** An unresolved transaction at `1100` can subsequently commit, but the next run starts at `1200`. The claim at [archiver:141](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:141) that matching payloads can only make the checkpoint more conservative is false.

   The [adversarial fixture:598](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:598) tests neither condition required to expose this: its text is inside a line, and `999999` exceeds the boundary. The “byte for byte” assertion at line 609 only counts occurrences of `SECOND`.

   **Corrected text:** “Checkpoint offsets must come from complete Kafka records, independently of payload bytes.” Obtain offsets before formatting and use an unambiguous record format or a separately validated metadata stream. Add an embedded-LF fixture with an exact matching prefix and an offset **between the real captured maximum and HWM**, followed by a later-commit retry. Assert complete payload-byte equality.

2. **P1 — The new anchor stops archival of existing `LogAppendTime` topics.**

   **Evidence:** [Archiver:152](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:152) accepts only `CreateTime`. The repository explicitly records:

   - `es.underlying.es.trades` using `LogAppendTime`: [es-cvd manifest:97](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-cvd.yaml:97).
   - `es.tape-zones.cells` using `LogAppendTime`: [topics.env:482](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:482).

   Both belong to [the es4 archive inventory:145](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:145). Kafka prints the actual timestamp type. [Kafka formatter](https://github.com/apache/kafka/blob/4.3.0/tools/src/main/java/org/apache/kafka/tools/consumer/DefaultMessageFormatter.java#L121)

   The extracted probe returned `-1 1`, status `0`, for a valid `LogAppendTime` record.

   **Why it matters:** A nonempty capture reaches [archiver:637](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:637), becomes `got=0`, and is deleted. These topics can fail every night without advancing. Mixed timestamp history progresses only through the last recognized `CreateTime` record.

   **Corrected text:** Decouple offset identity from timestamp type. Support the actual Kafka timestamp variants without treating append time as event time. Add fixtures for both real es4 topics, including mixed historical timestamp types, and require successful publication and checkpoint advancement.

3. **P2 — Timeout acceptance publishes incomplete records and bypasses STRICT hole detection.**

   **Evidence:** [Archiver:620](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:620) subtracts one **offset** from the credited maximum; it leaves every byte in the published file. The STRICT check at [646](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:646) runs only when `consumer_rc=0`. Timeout status `124` becomes success afterward at [659](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:659).

   Two independent reproductions:

   - STRICT capture with offsets `0`, `2`, and partial `3`, boundary `5`, status `124`: accepted with checkpoint `3`, silently passing the missing interior offset `1`.
   - A partial JSON record followed by a correct retry: the actual corpus reader retained the original `unparseable` error. Complete duplicates collapsed correctly.

   [§13:646](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:646) asserts checkpoint deferral, but never asserts that the published corpus remains readable.

   **Why it matters:** A truncated record is not an equivalent duplicate. [The reader:77](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe_corpus_reader.py:77) retains parse errors; [classification:292](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe_corpus_reader.py:292) marks affected sessions `CORRUPT`. A5.8 uses that reader. Retrying does not repair the earlier published artifact.

   A gzip killed mid-write is rejected by the new decompression check. A consumer killed mid-record can instead leave a perfectly valid gzip containing incomplete application data.

   **Corrected text:** Publish only complete, explicitly framed records. Preserve any incomplete tail separately from normal corpus files. Apply STRICT continuity checks to every accepted exit path. Until complete-record recovery is implemented, reject timeout captures for STRICT topics. Add timeout-plus-retry reader/evaluator tests and an interior-hole fixture.

4. **P2 — Post-boundary records can falsely satisfy historical session completeness.**

   **Evidence:** [Archiver:625](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:625) counts the whole capture and [694](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:694) writes that count alongside the narrower credited range. [§12(g):502](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:502) explicitly expects 200 archived records while checkpointing only through `99`.

   [The verifier:150](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-verify.sh:150) sums those counts and [207](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-verify.sh:207) applies the session floor. It does not inspect payload offsets or enforce the cutoff.

   Using the actual verifier with an in-memory archive containing 99 in-range records and 901 post-boundary records produced:

   ```text
   records=1000 floor=1000 offset_gaps=0 checksums_verified=1 status=OK
   ```

   **Why it matters:** Preserving extra bytes can be safe for recovery, but it does not make them evidence for the claimed historical session. `UNTIL_TS` no longer bounds file contents, and future records can turn an underfilled session green.

   **Corrected text:** Distinguish captured contents from credited session contents. Record actual captured bounds, complete in-range counts, cutoff and overshoot explicitly; make verification use the in-range population. Alternatively, route complete post-boundary records into separate artifacts. Add a historical-cutoff test whose floor must remain unmet despite substantial later traffic.

5. **P2 — Processing failures are still not consistently propagated.**

   **Evidence:** [Archiver:157](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:157) immediately saves the pipeline statuses correctly, but checks only `rc[0]`. Every nonzero decompression status becomes `9`; the caller correctly rejects it. The awk status is discarded.

   [Archiver:625](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:625) still hides `scan_archive_file`’s status behind `read` and command substitution. [Capture:606](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:606) likewise retains only the consumer status.

   Fault injection against extracted production logic showed:

   ```text
   offset awk returns 7 after statistics → scan_offsets returns 0
   archive scan returns 7 after statistics → accepted, checkpoint=2
   truncated gzip → rejected, status=9
   ```

   **Why it matters:** The decompression fix is real but incomplete. A successful first scan does not establish that subsequent processing succeeded. The final guarded `mv` protects rename failure; it cannot validate earlier ignored failures.

   **Corrected text:** Save and validate all capture-pipeline component statuses immediately. Check both offset-scan stages, then capture and check `scan_archive_file`’s status before parsing its output. Keep the checkpoint unchanged on any processing failure. Add separate decompressor, awk, compressor and final-publication fault cases.

The remaining mechanism assessment:

- **Inventory coverage:** Prod and direct-default dev select 189 topics; scheduled dev selects six; es4 selects seven. All now share the same checkpoint rule.
- **Compaction:** A 2,000-record fixture ending at offset `642059` correctly checkpointed `642060`. Sparse offsets alone are safe. Invisible trailing ranges can still fail repeatedly until readable data arrives. Active sparse topics can still consume the full 900 seconds and archive substantial overshoot.
- **Reset/GAP:** Reset detection still uses unclamped `log_end`; historical cutoffs do not trigger false resets. Earliest-offset handling remains intact. The pre-existing “records LOST” wording can overstate loss when the deferred range contains markers or compaction holes.
- **Payload parsing:** Ordinary tabs and inline `Offset:` text do not fool the anchor. A value beginning with that text on its first physical line remains behind the real envelope; a matching continuation after LF does fool it. Empty valid gzip files cannot advance.
- **Schema extraction:** Round-4 finding 4 is **CLOSED for the added-column regression**. The actual extractor returned schema ID `42` for both layouts. [The test:683](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:683) checks only the presence of the selector line, however; it should execute the production extractor instead of maintaining a partial copy.
- **Other deployment changes:** No additional defect found in strike declaration, byte-retention reconciliation, mirror isolation, reset preservation, gateway wiring, service selection or render classification.

Round-4 dispositions therefore stand at: **#1 PARTIAL, #2 PARTIAL, #3 PARTIAL, #4 CLOSED**.

Independent validation passed: ten shell/env syntax checks, both whitespace checks, 15 mirrored-topic contracts, nine extracted strike mirror scenarios, 27 in-memory es4 renders, archive-unit completeness, and the reader/schema fixtures described above. The full temporary-file suites could not run in this read-only environment. Your reported 104-assertion run and the round-4 record’s 119-assertion run are reported evidence, not runs I independently repeated. No files or deployments changed.

**Institutional — Not met:** checkpoints can skip unread records, and historical completeness can count out-of-range data.

**Military — Not met:** existing timestamp types fail, timeout recovery can poison the corpus, and processing failures remain partially unchecked.

**NASA — Not established:** representative recovery time, peak RSS, missed-clean capacity and storage runway remain unmeasured. The added full decompression pass and potentially 900-second sparse reads also contradict the claim that the default rule “costs them nothing.”

VERDICT: REQUEST_CHANGES
Establish boundaries from complete Kafka records, support existing timestamp types, and preserve trustworthy publication and completeness checks.
