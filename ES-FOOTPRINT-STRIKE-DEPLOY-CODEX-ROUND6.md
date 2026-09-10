# ES Footprint strike interaction — deploy Codex round 6 (gpt-6-astra, 2026-09-10)

Review of PR #1028 at 17aee509. Verdict: **REQUEST_CHANGES**. Disposition: **the archiver change is WITHDRAWN in full.**

## What this round settled

Round 6 broke the last argument the scoped rule stood on. Two things at once:

- the completeness gate cannot be complete — `strike-intelligence-*`, `options.spx.strike-invasion.*`, `market.spx.market-carry-service.*` and `options.databento.normalized` are archived, exactly-once, and absent from both the table and the declaration, and a new one can be added tomorrow;
- and `options.databento.normalized` is **transactional Avro**, so "declare them all and the JSON argument covers it" is not available either;
- and even for the strike log the JSON argument was wrong in the place it mattered: the archive prints the **key** as well as the value, `StrikeRecords` builds CHECKPOINT keys by concatenating decoded strings, and a valid JSON input could put a newline into the key. (The RFC point is taken too: JSON permits a raw LF as whitespace outside strings; it forbids unescaped control characters inside them.)

So there is no sound way to derive a capture boundary from `kafka-console-consumer`'s text, and no list that makes the unsound path safe. Rounds 2-5 were four attempts at the same impossible thing.

**Everything the archiver gained since round 1 is therefore reverted**: `print.offset` is back to STRICT-only, `scan_offsets`/`trim_offsets` are gone, the checkpoint is the high-water mark for every topic exactly as it has always been, the Avro schema extractor is untouched, `OE_TRANSACTIONAL_TOPICS_<env>` is gone, and `validate-archive-transactional-inventory.sh` is deleted. `test-archive-reset.sh` is back to its round-1 content and passes unchanged. The one archiver change this PR keeps is round 1's: the reader takes `isolation.level=read_committed`, so an ABORTED revision is never archived — which was the finding this PR actually had to answer, and which round 2 marked CLOSED.

**The transactional checkpoint hazard is recorded, not silently dropped.** `oe-topics.env` now states it plainly: this archiver checkpoints every topic at the high-water mark; on a transactional topic that is not a boundary any read_committed reader necessarily reached; that is true today of the OPB outputs, dealer-ledger, corridor-gauge, unified-sr and `es.futures.auction`, and the strike log joins that set rather than creating it. The fix — a capture that takes its boundaries from Kafka metadata rather than from formatted text — is named as its own change with its own review, because it touches every archived topic and none of it is specific to this feature.

| # | finding | disposition |
|---|---|---|
| 1 | the completeness gate passes archived transactional writers it does not know about | WITHDRAWN with the mechanism: the gate and the declaration are deleted rather than left as a promise nobody can keep. The hazard is documented where the inventory lives. |
| 2 | compact JSON values do not prevent forgery through the raw KEY | ACCEPTED, and decisive: it removes the last case where a text-derived boundary could have been called safe. Reverted. |
| 3 | timeout acceptance publishes malformed records | WITHDRAWN with the rest: no timeout capture is accepted any more. |
| — | the `LogAppendTime`, Avro-schema and byte-preservation regressions of rounds 4-5 | GONE by construction: the capture format is unchanged from `origin/main`. |

`test-archive-reset.sh` (Debian container, as Jenkins runs it): **ALL PASS**, at its original assertion count. Every other deploy validator passes; `git diff --check` clean.

## Review text (verbatim)

Reviewed all 25 changed files at `17aee509`, both requested diff ranges, the earlier dispositions, archive consumers, and the relevant producer implementations. **Changes are required.** The round-5 scope change restores the inventory loss path and does not establish safe framing for strike keys.

**Part A — round-3 findings and notes**

| Item | Status | Evidence and test |
|---|---|---|
| **#1: undeclared transactional writers receive unsafe HWM checkpoints** | **STILL OPEN — regression** | [Archiver:607](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:607) gates the probe; undeclared topics retain `ckpt_to=endoff` at line 631. [§12(h):571](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:571) now explicitly expects checkpoint **1200 after capturing only through 1099**. The new gate does not close this—finding 1 below. |
| **#2: crossing the boundary discards valid captures and stalls progress** | **PARTIAL** | Complete captures survive, but their contents remain unbounded. [§12(f)/(g):484](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:484) now expects overshoot and rereading. [Timeout acceptance:659](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:659) improves declared-topic progress while permitting truncated publication. |
| **§12(e): missing exit-status assertion** | **CLOSED** | [Test:483](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:483) explicitly requires failure for the marker-only remainder. |
| **Recovery duration, peak RSS, missed-clean capacity** | **PARTIAL** | [Manifest:62](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:62) correctly labels sizing provisional. Neither §12 nor §13 measures representative recovery. Instrumentation is not capacity evidence. |
| **Unbounded dev/prod storage** | **PARTIAL** | Preservation and unlimited retention remain deliberate at [topics:189](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:189) and line 251. No measured storage-runway test is supplied. |
| **Archive-first rollout, mirror reinstalls, filename order** | **CLOSED as documented procedure** | [Round-3 disposition:12](/private/tmp/oe-deploy-fpstrike/ES-FOOTPRINT-STRIKE-DEPLOY-CODEX-ROUND3.md:12) records the required sequence. Deployment filename order still does not enforce it; no ordering test does. |
| **OPB classification corrections** | **CLOSED narrowly** | [Inventory:132](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:132) correctly distinguishes the three public transactional sinks from `config` and `dashboard`. This does not establish completeness of all archived EOS sinks. |
| **Retention plumbing and mirror arm** | **CLOSED** | [Create:196](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:196), reconcile at line 301, [verification:110](/private/tmp/oe-deploy-fpstrike/scripts/kafka/verify-topics.sh:110), and [mirror:265](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:265) agree. Retention regression cases remain present; nine extracted strike mirror scenarios passed independently. |
| **Shell failure handling** | **PARTIAL** | Declared-topic decompression failure now stops publication at [archiver:609](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:609). Other processing statuses remain unchecked—finding 5. |
| **Manifest/settings agreement** | **CLOSED, excluding capacity** | [Manifest:69](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:69) agrees with [StrikeSettings:35](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeSettings.java:35). All 27 generated es4 manifests matched an in-memory render. |
| **Earlier mirror isolation, reset preservation, provisional floor and whitespace corrections** | **CLOSED** | The fixes remain present. Mirror contract checks and both requested `git diff --check` ranges passed. |

**Part B — findings, most severe first**

1. **P1 — The completeness gate passes existing archived transactional writers that still receive unsafe checkpoints.**

   **Evidence:** [KNOWN_EOS:33](/private/tmp/oe-deploy-fpstrike/scripts/ci/validate-archive-transactional-inventory.sh:33) is a fixed table. [Lines 79–87](/private/tmp/oe-deploy-fpstrike/scripts/ci/validate-archive-transactional-inventory.sh:79) check only its entries. Existing counterexamples include:

   | Archived production topics omitted from both the table and declaration | Producer evidence |
   |---|---|
   | `strike-intelligence-by-strike`, `strike-intelligence-dashboard`, `strike-intelligence-turn-alert` | [Production EOS enabled:85](/private/tmp/oe-deploy-fpstrike/k8s/services/strike-intelligence/overlays/production/manifest.yaml:85); [transactional topology sinks:560](/private/tmp/oe-fpstrike/strike-intelligence-service/src/main/java/com/optionsedge/processing/strikeintelligence/StrikeIntelligenceStreams.java:560). |
   | `options.spx.strike-invasion.current`, `.events` | [Production outputs/EOS:75](/private/tmp/oe-deploy-fpstrike/k8s/services/strike-invasion/overlays/production/manifest.yaml:75); [sinks:347](/private/tmp/oe-fpstrike/strike-invasion-service/src/main/java/com/optionsedge/processing/strikeinvasion/StrikeInvasionStreams.java:347). |
   | `market.spx.market-carry-service.current`, `.history` | [Production outputs/EOS:64](/private/tmp/oe-deploy-fpstrike/k8s/services/market-carry/overlays/production/manifest.yaml:64); [sinks:117](/private/tmp/oe-fpstrike/market-carry-service/src/main/java/com/optionsedge/processing/marketcarry/MarketCarryStreams.java:117). |
   | `options.databento.normalized` | [Production output/EOS:70](/private/tmp/oe-deploy-fpstrike/k8s/services/databento-volume-aggregator/overlays/production/manifest.yaml:70); [Avro sink:164](/private/tmp/oe-fpstrike/databento-volume-aggregator/src/main/java/app/kafka/DatabentoVolumeAggregator.java:164). |

   All occur in [the production archive inventory:64](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:64). The gate returned **OK** unchanged. An in-memory mutation adding another unclassified archive topic also passed.

   **Why it matters:** The extracted production checkpoint block reproduced `captured_max=1099 → checkpoint=1200` for an undeclared writer. Records committing afterward can be skipped permanently.

   Any new topic absent from `KNOWN_EOS`, or an existing unlisted topic switched to transactional production, can pass this gate. The explanatory `why` column is not validated against manifests or source. Several entries depend on processing-repository defaults, so the table is not independently derivable from this deployment repo alone.

   Crucially, `options.databento.normalized` is **transactional Avro**. Simply adding every missing writer to the supposed JSON-only list cannot solve this.

   **Corrected text:** “Every archived topic requires an explicit, verified capture policy. Unknown writers fail validation. Transactional capture obtains boundaries independently of raw key/value text.” Complete the current inventory, include transactional binary sinks, and add mutations for new sinks and EOS changes. Replace [the stale ‘annotation, not safety mechanism’ comments:118](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:118).

2. **P1 — Compact JSON values do not prevent offset forgery through strike’s raw Kafka key.**

   **Evidence:** [The archive prints keys:597](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:597). [BarInput:83](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/BarInput.java:83) accepts any nonempty textual symbol/timeframe. [StrikeRecords:74](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeRecords.java:74) concatenates those decoded strings directly into CHECKPOINT keys; episode identities do likewise at [StrikeState:48](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeState.java:48).

   A valid input JSON string can therefore contain escaped characters that become this raw key:

   ```text
   CHECKPOINT|ES
   CreateTime:1<TAB>Partition:0<TAB>Offset:1199<TAB>forged|1m|1
   ```

   With real record offset `1099` and boundary `1200`, formatter-equivalent bytes passed through the **actual extracted probe** returned:

   ```text
   maxOffset=1199 lines=2 rc=0
   ```

   **Why it matters:** The next checkpoint becomes `1200`, skipping later commits. The value remains valid compact JSON throughout. [Kafka’s formatter writes both key and value bytes without escaping them](https://github.com/apache/kafka/blob/4.3.0/tools/src/main/java/org/apache/kafka/tools/consumer/DefaultMessageFormatter.java#L165).

   The producer-specific conclusions differ:

   - **Strike values:** compact serialization at [StrikeRecords:47](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeRecords.java:47) escapes embedded control characters. Its **keys lack that protection**.
   - **Auction:** both normal and degraded values use the private default mapper at [AmtEngine:1119](/private/tmp/oe-fpstrike/es-amt-service/src/main/java/com/optionsedge/processing/esamt/AmtEngine.java:1119) and line 1229. [Its key:101](/private/tmp/oe-fpstrike/es-amt-service/src/main/java/com/optionsedge/processing/esamt/EsAmtSession.java:101) comes from formatted date/time. I found no newline path in that producer’s emitted keys or values.

   The general RFC argument is also misstated: JSON permits raw LF as whitespace outside strings. It prohibits unescaped control characters **inside strings**. [RFC 8259, §§2 and 7](https://www.rfc-editor.org/rfc/rfc8259.txt).

   **Corrected text:** Establish boundaries from framed records or independently encoded metadata. If retaining a restricted text path, enforce its contract for **both keys and values**, including existing records. Add the valid-JSON/newline-key fixture above. [§13(b):632](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:632) tests only inline text; its `printf` also emits raw tabs inside the purported JSON string.

3. **P2 — Timeout acceptance still publishes malformed records; this must be fixed now.**

   **Evidence:** [Archiver:619](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:619) decrements the credited offset but leaves the partial record in the file. [Line 659](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:659) converts timeout status into success.

   The extracted production logic accepted complete offsets `0–2` followed by truncated JSON at offset `3`:

   ```text
   consumer_rc=0 got=4 checkpoint=3
   valid gzip=true; last JSON complete=false
   ```

   **Why it matters:** Rereading offset `3` does not remove the malformed line from the earlier published artifact. It remains a corrupt JSONL record and contributes to manifest counts. A checksum faithfully authenticates those malformed bytes.

   The STRICT restriction **does close the earlier timeout bypass**: current `context-tape.direction.ledger` does not take this acceptance path. Consequently, the previous direct A5.8 impact should not be carried forward unchanged. Publication integrity for strike and auction remains broken.

   **Corrected text:** Publish only complete records. Until capture can identify their boundaries reliably, reject timeout captures instead of publishing an incomplete tail. Implement bounded record-aware consumption to recover progress. Replace [§13(e)’s complete-line `exit 124` shim:681](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:681) with a real partial-tail case and verify every published record after retry.

4. **P2 — Historical cutoff captures still count future records toward the historical floor.**

   **Evidence:** [Archiver:625](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:625) counts the entire file, while lines 634 and 694 credit a narrower range. [§12(g):502](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:502) explicitly expects **200 records with checkpoint 99**. The [verifier:150](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-verify.sh:150) sums that unrestricted count.

   Executing the actual verifier body with 99 in-range records, 101 overshoot records and a floor of 150 produced:

   ```text
   records=200 floor=150 offset_gaps=0 checksums_verified=1 status=OK
   ```

   **Why it matters:** Calling the floor a presence check does not make future records evidence for the requested historical period. This also occurs with records already present beyond `UNTIL_TS`; it does **not** require a producer to write during capture.

   **Corrected text:** Record captured bounds separately from credited bounds, and use complete in-range records for historical counts and verification. Prefer enforcing the exclusive boundary before publication. Pin a cutoff case that remains below its floor despite abundant later records.

5. **P2 — Processing failures remain partially ignored.**

   **Evidence:** [scan_offsets:152](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:152) checks `zcat` status but discards awk status. [Capture:601](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:601) retains only the consumer status. [Statistics:625](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:625) hides the helper’s failure behind `read`.

   Independent fault injection returned:

   ```text
   offset awk exits 7 after statistics → scan_offsets returns 0
   statistics helper exits 7 after output → read returns 0, got=2
   decompression failure → scan_offsets returns 9
   ```

   **Why it matters:** The decompression fix is real but incomplete. A guarded final rename cannot validate earlier ignored processing failures.

   **Corrected text:** Save every capture-pipeline status immediately; validate both scan stages and the statistics helper before using their output. Refuse checkpoint advancement on processing failure. Add separate filter, compressor, decompressor, awk and publication failure cases.

6. **P2 — Schema discovery still changes undeclared topics and can misread a legitimate legacy key.**

   **Evidence:** [Schema extraction:329](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:329) treats any third field starting `Offset:` as metadata. Without printed offsets, that field is the Kafka **key**.

   Executing the production extractor with mocked file input yielded:

   ```text
   legacy ordinary key → schema 42
   offset-bearing layout → schema 42
   legacy key "Offset:key" → unavailable
   ```

   **Why it matters:** This contradicts the unconditional “no schema change” claim for undeclared topics. No key constraint prevents that collision.

   **Corrected text:** Pass the known capture layout into schema extraction; do not infer it from arbitrary key bytes. Execute the production extractor in regression tests, including a no-offset key beginning `Offset:`.

7. **P3 — The timestamp-less anchor accepts a spelling Kafka does not emit.**

   **Evidence:** [The anchor:147](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:147) accepts `NoTimestampType:<number>`. Kafka prints **`NO_TIMESTAMP`**, without a numeric suffix. [Formatter implementation](https://github.com/apache/kafka/blob/4.3.0/tools/src/main/java/org/apache/kafka/tools/consumer/DefaultMessageFormatter.java#L121). The actual probe returned `-1 1` for that envelope.

   **Why it matters:** The claimed third timestamp variant is untested and unsupported. I found no current timestamp-less strike/auction emission, so this is not an identified nightly outage on those producers.

   **Corrected text:** Match the actual `NO_TIMESTAMP` envelope and add its fixture alongside `LogAppendTime`.

The remaining mechanism checks resolve as follows:

- **Trimming/mv:** `trim_offsets` is gone. There is no trim/recompression/trim-rename path to assess at HEAD. The final [publication `mv`:673](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:673) is guarded.
- **Compacted topics:** `underlying.spx.price` remains undeclared and retains its prior format/HWM checkpoint. Sparse readable counts alone do not fail it. Declared compacted topics use captured offsets; invisible trailing ranges can repeatedly fail until readable data arrives. Active undeclared sparse topics still reject the 900-second timeout.
- **Parsing:** Inline tabs and `Offset:` text do not fool the leading anchor. Empty captures cannot advance. Newlines in keys defeat the framing assumption.
- **Reset/GAP:** Reset detection still uses unclamped log end; `UNTIL_TS` does not create a false reset. Existing GAP wording can overstate record loss for deferred markers.
- **Consumer compatibility:** The normal offset-bearing layout works with `oe_corpus_reader.py`; A5.8 uses that reader. The STRICT topic already printed offsets before this PR. Undeclared non-STRICT topics retain their formatter columns, but “byte-for-byte unchanged behavior” is too broad: `read_committed` now applies globally, and schema extraction changed.
- **Deployment wiring:** No additional defect found in strike topic shape, retention reconciliation, mirror isolation, reset preservation, gateway variables, service selection, or manifest settings.

Independent validation passed: 11 changed shell/env syntax checks, both whitespace checks, archive-unit completeness, 15 mirrored-topic contracts, nine extracted strike mirror scenarios, and 27 in-memory es4 renders. The archive, parser, schema, verifier and gate reproductions above ran independently. Full temporary-file suites could not run in this read-only environment. Your reported 104 assertions and round 5’s recorded 122 assertions are reported evidence, not independently repeated runs. No files or deployments changed.

**Institutional — Not met:** existing transactional outputs can lose later commits, and historical counts include out-of-range records.

**Military — Not met:** raw keys can forge checkpoints; timeout publication and processing-failure handling remain unsafe.

**NASA — Not established:** recovery time, peak RSS, missed-clean capacity and storage runway remain unmeasured. Capture still relies on record-count budgets and potentially 900-second reads instead of boundary-driven completion.

VERDICT: REQUEST_CHANGES
Establish complete transactional coverage and trustworthy record boundaries before publishing or checkpointing archive captures.
