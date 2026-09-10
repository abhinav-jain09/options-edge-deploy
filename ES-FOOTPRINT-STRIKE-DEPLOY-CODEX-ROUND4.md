# ES Footprint strike interaction — deploy Codex round 4 (gpt-6-astra, 2026-09-10)

Review of PR #1028 at 71fc70e9 (round-3 delta). Verdict: **REQUEST_CHANGES** — round 3's fix introduced a payload regression. Both round-3 findings CLOSED/PARTIAL, four new ones. Dispositions, folded in the following commit:

| # | finding | disposition |
|---|---|---|
| 1 | **line-based trimming deletes bytes from binary/Avro records, then checkpoints past them** | FIXED by deleting the mechanism. Nothing is trimmed any more: a record that arrived past this run's boundary STAYS in the file and is read again next run — a duplicate, which this archiver has always preferred to a gap. The offset probe now matches only the LEADING metadata (`^CreateTime:<digits>\tPartition:<digits>\tOffset:<digits>\t`), so a continuation line of a multi-line Avro value contributes no offset and is never dropped, and the result is CLAMPED to the end offset captured at start — which is the property that makes the whole thing safe on any payload: a line that happens to look like metadata can only make the checkpoint MORE conservative, never advance it past what the run bounded. Your exact reproduction is now a test: an Avro-shaped value with an embedded newline AND a tab, plus a record whose payload contains `Offset:999999`, archived byte for byte with the checkpoint still at 3. |
| 2 | scan/trim failures can still publish and checkpoint | FIXED — `scan_offsets` returns the DECOMPRESSION status (`PIPESTATUS` of `zcat`), and a non-zero status fails the capture with the checkpoint unchanged, before `scan_archive_file` is consulted. `trim_offsets` is gone entirely, so its status cannot be ignored. |
| 3 | boundary enforcement too late to guarantee progress on active sparse topics | FIXED for progress, in the direction you specified but without a new consumer: a reader stopped by the outer bound (`124`) is no longer a discarded capture — the records it placed are real, and the checkpoint credits the boundary they prove. Because the pipeline's `grep` completes a cut-off last line before `gzip` sees it, a truncated record is indistinguishable inside the file, so the LAST record of such a capture is deliberately not credited (it is re-read next run). Both are pinned. Enforcing the boundary inside the consumer itself needs a reader Kafka's console consumer cannot express; the conservative contract is what is claimed. |
| 4 | unconditional offsets broke Avro schema discovery (`parts[3]` is now the key) | FIXED — the extractor locates the value by the metadata that precedes it (`head = 3 if parts[2].startswith(b'Offset:') else 2`), so it reads BOTH layouts; a legacy archive still resolves to the same schema id. Pinned in both layouts, and the test asserts the archiver's own extractor uses that same rule rather than a copy that can drift. |
| — | `GAP` diagnostic wording overstates loss when the deferred offsets are markers | NOTED, unchanged this round: the wording belongs to the pre-existing reset/GAP branch, not to this PR's change, and altering it here would be an unreviewed edit to the loss-detection path. Recorded as a follow-up. |
| — | coverage / compaction / STRICT / consumers | Your analysis is taken as read and is what the change now relies on: the same captured-max rule for every selection, sparse compacted offsets safe, the STRICT full-span check untouched, and `oe_corpus_reader.py` already parsing the new layout. |

Suite after the fix, in the Debian container Jenkins uses: **ALL PASS, 119 assertions** (was 104). Section 13 is new: binary payloads, an adversarial `Offset:` inside a value, a capture with no metadata at all, a reader stopped by the outer bound, a partial last record, and schema discovery in both layouts.

## Review text (verbatim)

Reviewed both requested diffs at `4076ee61`. **The default checkpoint rule fixes the inventory loophole, but the archive implementation still needs changes.** I found payload corruption, unchecked processing failures, incomplete boundary termination, and a schema-discovery regression.

Part A — round-3 findings and notes:

| Round-3 item | Status | Evidence and test coverage |
|---|---|---|
| **#1: undeclared transactional writers receive unsafe HWM checkpointing** | **CLOSED** | Offsets are unconditional at [archiver:592](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:592); captured-max checkpointing is unconditional at [614](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:614). §12(h), including the [undeclared-writer assertion:566](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:566), pins checkpoint `1100`, not `1200`. This closes the inventory defect; it does not establish payload integrity. |
| **#2: boundary crossing discards the valid prefix and stalls progress** | **PARTIAL** | [Archiver:599](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:599) retains the prefix when capture finishes successfully. [§12(f)/(g):484](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:484) covers concurrent writes and `UNTIL_TS`. It does not cover a continuously active reader reaching the outer timeout—finding 3 below. |
| **§12(e) omitted the exit-status assertion** | **CLOSED** | [Test:483](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:483) now explicitly requires failure for the marker-only remainder. |
| **Recovery duration, peak RSS, missed-clean capacity** | **PARTIAL** | [Manifest:62](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:62) remains explicitly provisional. Producer instrumentation exists, but instrumentation is not representative recovery evidence. §12 tests none of these measurements. |
| **Unbounded dev/prod strike storage** | **PARTIAL** | Unlimited retention and preservation remain intentional: [topics:251](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:251), [preservation:189](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:189). The acknowledged growth estimate still has no measured storage-runway test. |
| **Archive-first rollout; mirror reinstalls; filename order** | **CLOSED as documented procedure** | [Round-3 disposition:12](/private/tmp/oe-deploy-fpstrike/ES-FOOTPRINT-STRIKE-DEPLOY-CODEX-ROUND3.md:12) records the required ordering. This is an operational instruction, not an automated ordering guarantee. |
| **OPB classification corrections** | **CLOSED** | [Inventory:132](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:132) distinguishes the three transactional sinks from `config` and `dashboard`. The list no longer controls checkpoint safety. §12(h) pins that independence; there is no producer-to-inventory correspondence test. |
| **Retention plumbing and mirror arm** | **CLOSED** | Create/reconcile at [apply:196](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:196) and [301](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:301); verification at [verify:110](/private/tmp/oe-deploy-fpstrike/scripts/kafka/verify-topics.sh:110); mirror assertions at [mirror:265](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:265). Covered by the retention and mirror-shape regression cases. |
| **Shell failure handling** | **PARTIAL** | The previously reviewed guarded expansions remain sound. New scan/trim return statuses are unchecked at [archiver:599](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:599)—finding 2. §12 has no corresponding fault injection. |
| **Manifest/settings agreement** | **CLOSED, excluding capacity** | [Manifest:69](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:69) agrees with producer settings: broker, topics, IDs, port, band, series bound, universe and explicit fold timeout. YAML/selector checks passed; all 27 generated es4 manifests matched an in-memory render. |

The carried-forward mirror-isolation, reset-preservation, provisional-floor correction and whitespace dispositions remain closed. Their existing tests establish configuration/classification, not broker-level recovery or measured capacity.

Part B — findings, most severe first:

1. **P1 — Line-based trimming deletes bytes from archived Avro records, then checkpoints past them.**

   **Evidence:** [scan_offsets:142](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:142) treats every physical line as a Kafka record. [trim_offsets:154](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:154) drops any line without a recognized offset. The caller invokes trimming whenever such a line exists, even without boundary crossing.

   The production inventory includes Avro inputs such as `options.databento.display`, `options.ibkr.display` and `options.databento.strike-flow.strike.avro`. These are not hypothetical formats: [RawToDisplayBridge:90](/private/tmp/oe-fpstrike/raw-to-display-service/src/main/java/app/kafka/RawToDisplayBridge.java:90) uses Avro serdes for its output. Kafka’s default formatter writes value bytes directly; it does not escape embedded newlines. [Kafka 4.3 formatter](https://github.com/apache/kafka/blob/4.3.0/tools/src/main/java/org/apache/kafka/tools/consumer/DefaultMessageFormatter.java#L165)

   I reproduced this with offset `7` and a value containing a Confluent header followed by `0x0a HELLO`. The scan returned `7 1 0 1`; trimming removed `HELLO`. The remaining prefix still qualifies for checkpoint `8`.

   **Why it matters:** This change actively removes payload bytes that the previous capture retained. A checksum subsequently authenticates the damaged output, and the checkpoint prevents recovery through an ordinary retry. A continuation containing `Offset:` can also be misclassified as a separate record.

   **Corrected text:** Bound and checkpoint actual Kafka records before formatting, or introduce an explicitly framed, binary-safe capture format with compatible readers. Never discard physical lines as a substitute for filtering records. Reject malformed metadata rather than coercing it: the current parser accepts `Offset:garbage` as offset `0`.

   Add byte-for-byte regressions for Avro, embedded LF/NUL/tab, literal `Offset:`, malformed metadata and boundary crossing. §12’s single-line JSON fixtures cannot detect this corruption.

2. **P2 — Scan and trim failures can still produce a successful publication and checkpoint.**

   **Evidence:** [Archiver:599](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:599) hides `scan_offsets`’ status behind `read` and a here-string. [Line 603](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:603) ignores `trim_offsets`’ status. The script deliberately lacks `set -e`.

   The `&& mv` inside `trim_offsets` correctly prevents replacement when its pipeline fails. However, that leaves the original, untrimmed capture available for the later publication path.

   Fault injection into the extracted production block produced:

   ```text
   trim fails → PUBLISH got=6 checkpoint=5 span=5
   offset scan fails after output → PUBLISH got=5 checkpoint=5 span=5
   archive scan fails after output → PUBLISH got=5 checkpoint=5 span=5
   ```

   A real truncated-gzip test also produced valid-looking scan statistics alongside a nonzero decompression status.

   **Why it matters:** A failed trim can publish offset `6` while claiming `[0,5)`, including records beyond `UNTIL_TS` in the historical session. Decompression failures can be treated as verified captures. The final guarded `mv` protects publication failure, not these earlier processing failures.

   **Corrected text:** Capture and check each helper’s status before parsing its output. On scan, decompression, compression or trim-rename failure, record failure and leave the checkpoint unchanged. Validate the final artifact and derive its checkpoint statistics from that artifact. Preserve/check the capture pipeline’s component statuses as well.

   Add separate failure cases for decompression after partial output, compression failure and the `.inrange` rename.

3. **P2 — Boundary enforcement still happens too late to guarantee progress on active sparse topics.**

   **Evidence:** [Archiver:586](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:586) waits for the console consumer to finish before scanning or trimming. Consumption remains bounded by `--max-messages=endoff-from`, a 60-second idle timeout and a 900-second outer timeout. [Line 635](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:635) rejects every capture with consumer status `124`.

   For the requested compacted example, an offset span of `642,060` with `2,000` readable records plus continuing 1 Hz writes reaches neither the message budget nor the idle timeout. The outer timeout fires despite the complete in-range prefix already having arrived. The extracted acceptance block confirmed:

   ```text
   DISCARD got=2000 candidate_checkpoint=642060 consumer_rc=124
   ```

   **Why it matters:** An active sparse topic can repeatedly discard valid captures, reread the same range and eventually lose records to retention or compaction. Historical capture can also consume substantial post-cutoff traffic unnecessarily. This is the remaining part of round-3 finding #2; §12(f)/(g) use finite shims that exit successfully.

   **Corrected text:** Enforce the exclusive offset boundary during consumption and terminate intentionally once crossing it proves the bounded prefix has been traversed. Distinguish that successful termination from reader failure. Keep conservative no-advance behavior when no record establishes progress.

   Add a sparse, continuously producing fixture whose record-count budget cannot be reached, including an `UNTIL_TS` variant.

4. **P2 — Unconditional offsets break Avro schema discovery.**

   **Evidence:** [Archiver:592](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:592) changes the formatted layout to:

   ```text
   timestamp  partition  offset  key  value
   ```

   But [avro_schema_fragment:325](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:325) still reads `parts[3]` as the value. That is now the key. My old/new-layout reproduction found the schema magic in the old layout and inspected `b'k'` in the new one.

   **Why it matters:** Previously identifiable Avro archives now report `schema_source:"unavailable"`, losing schema ID, subject and version metadata. The verifier does not fail merely because schema information is unavailable.

   **Corrected text:** Update schema extraction to consume the agreed record format and locate the value unambiguously. Support legacy archives where required, and preserve embedded delimiters. Add old/new-format fixtures asserting the same schema ID, subject and version.

The mechanism assessment across the inventories is:

- **Coverage:** The default prod inventory has 189 topics; a direct dev invocation inherits that default, while the scheduled dev wrapper selects the six evidence topics. es4 selects seven topics. Every selection now receives the same captured-max rule.
- **Compaction:** Sparse offsets alone are safe. My 2,000-record fixture ending at offset `642059` yielded checkpoint `642060`. Removed offsets do not cause readable records to be replayed. An invisible trailing remainder can nevertheless remain unadvanced and fail repeated runs until new readable data arrives; that is the explicitly accepted conservative contract.
- **STRICT topic:** `context-tape.direction.ledger` remains outside the transactional annotations and retains the full-span short-read check at [626](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:626). Unconditional offsets do not weaken that check.
- **Parsing and emptiness:** A tab or literal `Offset:` inside a well-formed single-line value is safe because the real metadata offset appears first. An empty gzip returns `-1 0 0 0` and cannot advance the checkpoint. Binary/multiline values and ignored pipeline failures are the exceptions above.
- **Reset/GAP/`UNTIL_TS`:** Reset detection still uses the unclamped `log_end`; normal historical cutoffs do not falsely trigger resets. Earliest-offset clamping remains intact. The GAP diagnostic’s “records LOST” wording can overstate actual record loss when deferred offsets are markers or compaction holes.
- **Consumers:** The actual `oe_corpus_reader.py` accepted a new-format fixture with the correct key and `(partition, offset)=(0,7)`, without errors. A5.8 uses that reader. `oe-archive-verify.sh` reads manifests/checksums rather than fixed payload columns. The broken fixed-column consumer is `avro_schema_fragment`.

Independent validation passed: eight changed shell scripts’ syntax, both whitespace checks, all 15 mirrored-topic contracts, 27 in-memory es4 renders, strike manifest consistency, and the corpus-reader fixture. The fault reproductions above used extracted production logic. Your Debian **104-assertion ALL PASS** result is reported evidence; I could not independently rerun the temporary-file suite in this read-only sandbox. No repository files changed.

**Institutional — Not met:** payload preservation and schema provenance regress.

**Military — Not met:** processing failures do not consistently prevent publication, and active sparse captures can still make no progress.

**NASA — Not established:** representative recovery time/RSS and storage runway remain unmeasured. The new path also adds a full decompression scan to every capture, with another scan and recompression when trimming; “costs them nothing” is not a supported performance claim.

VERDICT: REQUEST_CHANGES
Preserve complete record bytes, check processing failures, enforce boundaries during consumption, and repair schema extraction before approval.
