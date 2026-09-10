# ES Footprint strike interaction — deploy Codex round 3 (gpt-6-astra, 2026-09-10)

Review of PR #1028 at 71fc70e9. Verdict: **REQUEST_CHANGES** (two archive findings; retention.bytes CLOSED). Dispositions, folded in the following commit:

| # | finding | disposition |
|---|---|---|
| 1 | the transactional INVENTORY leaves undeclared EOS writers on the unsafe high-water-mark path (dealer-ledger in prod AND dev, corridor-gauge, unified-sr, the calibration scorer, `es.futures.auction` on es4 — and the sweep found more) | FIXED by taking your first option and deleting the mechanism, not by completing a list: `print.offset=true` is now unconditional and EVERY archived topic checkpoints the greatest offset it actually captured, plus one. A list that must name every EOS writer in every environment forever is exactly the thing that was wrong; the safe rule is now the default, so a writer nobody declared is safe. This costs the non-transactional topics nothing: a delete-retained topic with no markers returns every offset in its range, so the proved boundary IS the high-water mark; a COMPACTED topic's proved boundary sits at its last live record and the next run resumes there, re-reading only offsets whose records compaction already removed — no duplicate, no gap. `OE_TRANSACTIONAL_TOPICS_<env>` survives only as an annotation that exempts a STRICT topic from the "records read == offset span" check, so an omission there is at worst a false FAIL, never a gap; it is corrected to the writers you named (and your two OPB corrections are taken: `-config` is a non-transactional control publisher and `-dashboard` is not a sink of the current topology, so neither is listed). |
| 2 | crossing the initial boundary discards the whole valid capture and can stall progress indefinitely | FIXED — the capture is trimmed to the boundary this run started from: records at or past `endoff` (an active producer, or a `--max-messages` budget markers made reachable) are dropped from the file, the in-range prefix is KEPT, and the checkpoint is that prefix's greatest offset + 1. The dropped count is named in the log. Cases (f) and (g) of `test-archive-reset.sh` §12 pin exactly your reproduction (hwm 6, records 0–4, a marker at 5, a record committed at 6 → five records archived, checkpoint 5, then the next run captures exactly the deferred record) and the `UNTIL_TS` variant with records already present beyond the cutoff. |
| — | §12(e) did not assert the exit status of the marker-only case | FIXED — it now asserts the run reports failure rather than claiming completion. |
| — | recovery capacity still unmeasured (fold time, peak RSS, missed-clean) | ACKNOWLEDGED and still PROVISIONAL in the manifest note. The producer round 3 adds a measured retention estimate, a fold-duration metric and `log_retained_bytes_estimate`, so the first es4 run reports the real figures; the sizing is re-derived from them rather than from an assertion. |
| — | unbounded dev/prod strike storage; `deploy-all` filename order is not the rollout order | ACKNOWLEDGED, unchanged: R13 requires every revision to survive, and the PR body carries the rollout order explicitly (archive scripts and inventory first, then image → es4 service → mirrors + the levels-mirror reinstall → gateways → web). |

Suite after the fix, run in the Debian container exactly as `Jenkinsfile.archive-scripts-deploy` runs it: **ALL PASS, 104 assertions** (was 90).

## Review text (verbatim)

Reviewed all 20 changed files at `71fc70e9`, both requested diff ranges, and the current producer implementations. **Two archive defects still prevent approval.** The retention.bytes correction and strike deployment wiring are sound.

Part A — round-2 dispositions:

| Finding / note | Status | Evidence and test coverage |
|---|---|---|
| **1. Transactional archive checkpoints an unproved HWM** | **PARTIAL** | [Archiver:566](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:566) now uses captured-max + 1; [manifest/checkpoint:620](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:620) consistently records that boundary. §12 covers committed records, unresolved transactions, subsequent commits, aborted records, and marker-only remainders. However, the inventory misses transactional writers, and boundary crossing discards valid captures—findings below. |
| **2. Missing retention.bytes contract** | **CLOSED** | Declarations at [topics.env:259](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:259) and [642](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:642); create/reconcile at [apply:196](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:196) and [301](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:301); verification at [verify:110](/private/tmp/oe-deploy-fpstrike/scripts/kafka/verify-topics.sh:110). Pinned by [pure-compact test:131](/private/tmp/oe-deploy-fpstrike/scripts/kafka/verify-topics-pure-compact-test.sh:131) and [mirror test:144](/private/tmp/oe-deploy-fpstrike/scripts/ci/es-cvd-mirror-shape-test.sh:144). |
| **Resource claim / recovery capacity** | **PARTIAL** | The unsupported capacity claim is removed: [manifest:62](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:62) explicitly makes sizing provisional. [StrikeLogFoldTest:179](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/test/java/com/optionsedge/processing/esfootprintstrike/StrikeLogFoldTest.java:179) checks estimated retention for 2,000 synthetic episodes; it does not measure representative es4 recovery time, peak RSS, or missed-clean recovery. |
| **Archive-first rollout and mirror reinstalls** | **CLOSED** | [Disposition:10](/private/tmp/oe-deploy-fpstrike/ES-FOOTPRINT-STRIKE-DEPLOY-CODEX-ROUND2.md:10) agrees with the current [PR rollout instructions](https://github.com/abhinav-jain09/options-edge-deploy/pull/1028): archive/inventory and reconciliation first; service; strike mirrors and existing levels-mirror reinstall; gateways; web. This is an operational instruction, not an automated ordering test. |
| **Unbounded dev/prod storage** | **PARTIAL** | Intent is explicit at [disposition:11](/private/tmp/oe-deploy-fpstrike/ES-FOOTPRINT-STRIKE-DEPLOY-CODEX-ROUND2.md:11). Unlimited retention plus [preservation:189](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:189) preserves history correctly. The stated growth estimate is not measured capacity evidence; archiving does not reclaim those broker copies. No supplied test establishes storage runway. |
| **Round-1 report trailing whitespace** | **CLOSED** | Both requested ranges pass `git diff --check`; the correction recorded at [disposition:12](/private/tmp/oe-deploy-fpstrike/ES-FOOTPRINT-STRIKE-DEPLOY-CODEX-ROUND2.md:12) is present. |

The carried-forward round-1 conclusions remain: mirror isolation **CLOSED** ([generated property:192](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:192), pinned by the shape test’s text assertion); reset classification **CLOSED** ([preserved list:189](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:189), [premarket exclusion:241](/private/tmp/oe-deploy-fpstrike/scripts/ops/premarket-reset.sh:241)); provisional floor correction **CLOSED** ([500 and rationale:190](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:190)). Transactional archival remains **PARTIAL**. Classification tests do not simulate recovery after deletion, and the isolation assertion is not a broker-level aborted-transaction test.

**The captured-max alternative is defensible.** Within an ordered `read_committed` partition read, checkpointing immediately after the last durably captured record does not jump over an earlier unresolved transaction. Kafka withholds subsequent records behind that transaction. [Kafka consumer semantics](https://kafka.apache.org/41/configuration/consumer-configs/#isolation.level)

Likewise, leaving a marker-only remainder unadvanced is conservative and acceptable. The implementation cannot prove that the invisible remainder contains only markers. It reports failure through [archiver:639](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:639), rather than claiming completion. [§12(e):472](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:472) pins the unchanged checkpoint and diagnostic, **but does not assert the exit status**. This contract provides incremental capture safety; it does not establish session completeness or permission to wipe the source.

Part B — findings, most severe first:

1. **P1 — The transactional inventory leaves existing archived writers on the unsafe HWM path.**

   **Evidence:** [oe-topics.env:127](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:127) lists only strike/CVD levels for es4, five OPB names for prod, and nothing for dev. Unlisted topics retain `ckpt_to="$endoff"` at [archiver:565](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:565).

   Confirmed counterexamples include:

   | Environment | Missing transactional archive inputs | Producer evidence |
   |---|---|---|
   | Prod **and dev** | `dealer-ledger-profile`, `dealer-ledger-state`, `dealer-ledger-signal-fired` | EOS explicitly enabled in [prod:101](/private/tmp/oe-deploy-fpstrike/k8s/services/dealer-ledger/overlays/production/manifest.yaml:101) and [dev:143](/private/tmp/oe-deploy-fpstrike/k8s/services/dealer-ledger/overlays/dev/manifest.yaml:143); [Streams sinks:734](/private/tmp/oe-fpstrike/dealer-ledger-service/src/main/java/com/optionsedge/processing/dealerledger/DealerLedgerStreams.java:734). These are in the actual [dev archive set:16](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:16). |
   | Prod and dev | `dealer-ledger-outcome-scored` | [ScorerConfig:78](/private/tmp/oe-fpstrike/dealer-ledger-calibration-service/src/main/java/com/optionsedge/processing/dealerledger/calibration/scorer/ScorerConfig.java:78) defaults EOS on; [138](/private/tmp/oe-fpstrike/dealer-ledger-calibration-service/src/main/java/com/optionsedge/processing/dealerledger/calibration/scorer/ScorerConfig.java:138) applies it. |
   | Prod | `corridor-gauge-state`, `corridor-gauge-event-log` | [CorridorGaugeStreams:295](/private/tmp/oe-fpstrike/corridor-gauge-service/src/main/java/app/kafka/CorridorGaugeStreams.java:295) unconditionally enables EOS; sinks at 331/334. |
   | Prod | `options.spx.strike-sr.current`, `options.spx.strike-sr.history` | [UnifiedSrSettings:115](/private/tmp/oe-fpstrike/unified-sr-service/src/main/java/app/kafka/UnifiedSrSettings.java:115) unconditionally enables EOS; [output sinks:350](/private/tmp/oe-fpstrike/unified-sr-service/src/main/java/app/kafka/UnifiedSrStreams.java:350). |
   | es4 | `es.futures.auction` | [EsAmtRuntime:421](/private/tmp/oe-fpstrike/es-amt-service/src/main/java/com/optionsedge/processing/esamt/EsAmtRuntime.java:421) begins a transaction, sends the auction record, and commits. The topic is explicitly archived at [inventory:131](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:131). |

   The sweep also found omitted archived EOS outputs from strike-intelligence, strike-liquidity-heatmap, spot-vol-regime, greek-move-authenticity, strike-invasion, and market-carry. Therefore adding just the dealer-ledger examples would not complete the inventory.

   **Why it matters:** The original loss scenario remains: HWM `1200`, captured records only through `1099`, checkpoint `1200`; later-committed records are skipped permanently. I reproduced that result using the extracted checkpoint block with an unlisted topic.

   §12 cannot catch this: [line 409](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:409) replaces the production inventory with `oe.test.reset`. It tests the declared-topic branch, not inventory completeness.

   **Corrected text:** Make captured-record checkpointing the conservative default for archived topics, or complete the per-environment inventory against actual deployed sinks and enforce that correspondence in CI. An unknown writer must not silently receive HWM checkpointing. Add regressions using the real prod/dev/es4 inventories, including dealer-ledger and auction, with an unresolved transaction followed by a later commit. Preserve strict-topic hole detection separately.

2. **P2 — Crossing the initial boundary discards the entire valid capture and can prevent progress indefinitely.**

   **Evidence:** [archiver:550](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:550) still bounds consumption by **record count**. [Lines 567–574](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:567) reject the capture whenever its greatest offset reaches or exceeds the initial boundary, setting `got=0`; [line 640](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:640) deletes it.

   Reproduction: `from=0`, initial end `6`, committed records `0–4`, marker `5`, then a newly committed record `6`. `--max-messages 6` returns offsets `0,1,2,3,4,6`. The new guard discards all six—including the five valid records below the boundary. The diagnostic incorrectly says no in-range offset was readable.

   **Why it matters:** An active producer can repeat this every run. A historical `UNTIL_TS` capture is worse: [line 416](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:416) keeps the cutoff fixed, so records already present beyond it can cause every retry to fail. Kafka’s console consumer counts returned messages, not traversed offsets. [Console-consumer implementation](https://github.com/apache/kafka/blob/4.3.0/tools/src/main/java/org/apache/kafka/tools/consumer/ConsoleConsumer.java#L96)

   **Corrected text:** Enforce the exclusive offset boundary during capture, retain the valid in-range prefix, and checkpoint its captured maximum + 1. Distinguish intentional boundary termination from reader failure. Keep the marker-only no-advance behavior. Add §12 cases for writes arriving during capture and `UNTIL_TS` with later records already present; require successful publication of the in-range prefix.

The remaining fresh-sweep results:

- **OPB classification:** Production enables EOS, and the current [topology:426](/private/tmp/oe-fpstrike/option-price-behavior-service/src/main/java/app/pricebehavior/OptionPriceBehaviorStreams.java:426) transactionally writes the public `by-option`, `session`, and `by-strike` sinks. **The five listed names are not five transactional OPB outputs.** `config` is written by a separate [nontransactional control producer:42](/Users/abhinav/development/workspace/options-edge-vanna-ui/src/app/control/OptionPriceBehaviorConfigPublisher.java:42); `dashboard` is not a sink in this current topology. Their conservative classification is harmless, but the explanatory claim is inaccurate. OPB’s dev default is non-EOS; that does not mean dev has no other transactional writers.
- **Retention plumbing:** The new byte-retention lists stay isolated. [Apply:29](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:29) replaces the default list for es4; [verify:110](/private/tmp/oe-deploy-fpstrike/scripts/kafka/verify-topics.sh:110) selects the corresponding list. Distinct-list mutations confirmed the separation. No reviewed strike apply path writes finite bytes. The legacy verifier’s initial general shape loop still uses the SPX list; that predates this change, and es4’s create wrapper does not invoke that verifier.
- **Mirror arm:** [Line 175](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:175) correctly sets `1/delete/-1/-1`. Source attestation, target creation, and target attestation all include bytes. `BYTES=""` initialization prevents inheritance into sibling arms.
- **Shell behavior:** Optional apply expansions survive `set -e`; the verifier’s guarded no-match pipeline reaches its explicit refusal. `eff` failures remain visible on stderr through the callers’ `|| exit 1`. No additional introduced `set -e`/pipefail defect found.
- **Manifest/settings:** All unchanged keys match [StrikeSettings:35](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeSettings.java:35): es4 bootstrap, prefixed topics, stable group/transactional ID, port `8153`, band `250`, series `64`, and `400000..900000/500` universe—1,001 strikes. One replica plus `Recreate` is correct. Explicit fold timeout is 300 seconds; omitted commit timeout/abort limit remain 30 seconds/5. Gateway refusal/symbol defaults remain appropriate.

Validation performed:

- Read-only adaptations preserving the supplied scenarios and production assertions: **26 mirror-shape cases passed; 16 pure-compact/retention cases passed**.
- Extracted apply functions: create/reconcile bytes passed for dev, production, experiment, and es4; distinct-list isolation passed.
- Extracted archive logic: conservative cases passed; both findings reproduced.
- Mirrored-topic validator: **15 topics passed**.
- In-memory es4 rendering: **27 services, zero drift**. Gateway slices equal actual kustomize renders in all three environments.
- Changed-shell syntax and both `git diff --check` ranges passed.

The unmodified temporary-file suites could not run in this read-only sandbox. The archive suite additionally lacks `flock`; your Debian **90-assertion ALL PASS** result is reported evidence, not a run I independently repeated. No files or deployments changed.

**Institutional — Not met:** transactional archive coverage still permits permanent omissions.

**Military — Not met:** capture progress fails under concurrent writes and historical cutoffs, despite sound preservation and retention guards.

**NASA — Not established:** representative recovery capacity and completion timing remain unmeasured; the archive boundary tests omit material operating conditions.

VERDICT: REQUEST_CHANGES
Complete transactional coverage and preserve bounded capture progress before approval.
