# ES Footprint strike interaction — deploy Codex round 7 (gpt-6-astra, 2026-09-10)

Review of PR #1028 at 78c90c77 (the withdrawal). Verdict: **REQUEST_CHANGES** (2 findings). Both folded:

| # | finding | disposition |
|---|---|---|
| 1 | the remaining `read_committed` change is NOT neutral: a committed-only reader stops below the end offset while a transaction is unresolved, and the unchanged rule checkpoints that end offset — so the change adds a skip risk, and "behaviour unchanged" was wrong | FIXED by scoping it. `read_committed` now applies to `es.futures.footprint.strike` alone (`OE_COMMITTED_READ_TOPICS`); every other topic reads exactly as it does on main, so this PR adds no exposure anywhere else. An omission from that list is today's behaviour, never a new gap — the opposite of the checkpoint list rounds 3-6 could not keep complete. For the one declared topic the residual exposure is stated rather than glossed, and it is REPAIRABLE: retention is −1 and `cleanup.policy=delete`, so the source keeps every record for ever and a skipped range can be re-archived by rewinding the checkpoint in `<topic>.offsets`. That is why the setting is not extended to topics whose retention expires. Your corrected text about the framed reader being a deployment dependency is recorded in the inventory note and the PR body; the unconditional idle-completion claims are gone from both `oe-topics.env` and the archiver comment |
| 2 | the live PR body advertises the withdrawn implementation and its regression coverage | FIXED — the body now states that the only executable archiver change is the scoped `read_committed`, lists what rounds 2-6 withdrew and why, carries the known pre-existing checkpoint hazard with its repairability argument, names the validators run against THIS head, and specifies the **production** `es.futures.cvd.levels` mirror reinstall (the job refuses a dev target for it) |

`test-archive-reset.sh` (Debian container): ALL PASS, unchanged from `origin/main`. `validate-archive-unit-completeness` passes.

## Review text (verbatim)

Reviewed all 23 changed files at `78c90c77`, against `origin/main=7581d307` and round-6 baseline `17aee509`. **The withdrawal is complete, but the remaining isolation change still requires safe checkpointing.** The deployment wiring is otherwise consistent.

The withdrawal checks passed byte for byte:

- Removing the six added comments and `--consumer-property isolation.level=read_committed` from the archiver reproduces `origin/main` exactly.
- `test-archive-reset.sh` is identical to main: 403 lines, SHA-256 `b8114e87f86f85113b4b3e1e0138c4f8e5de94403d1ed8592bfbd1750e144f56`.
- `Jenkinsfile.archive-scripts-deploy` is identical to main.
- No executable `scan_offsets`, `trim_offsets`, `ckpt_to`, `ckpt_span`, `OE_TRANSACTIONAL_TOPICS`, inventory gate, or timestamp anchor survives. Historical review records still mention them.
- `print.offset` is back to [STRICT-only use](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:543); the [schema extractor](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:264) is unchanged.
- The archive-directory diff also retains the intended strike inventory entry and provisional floor—not merely comments and isolation.

1. **P1 — The remaining `read_committed` change can cause permanent omissions through the unchanged checkpoint rule.**

   **Evidence:** The archiver obtains ordinary end offsets at [line 314](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:314), adds committed-only consumption at [line 538](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:538), accepts any positive non-STRICT capture with consumer status zero at [line 563](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:563), and checkpoints the original `endoff` at [line 606](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:606).

   Executing the current acceptance predicate and checkpoint statement with a simulated short read produced:

   ```text
   endoff=1200, got=1099, consumer_rc=0
   → 0=1200 records=1099 span=1200
   ```

   Kafka withholds records behind unresolved transactions in `read_committed` mode. Its console consumer can return normally after a receive timeout; zero status does not establish that the requested boundary was reached. [Kafka isolation semantics](https://kafka.apache.org/41/configuration/consumer-configs/#isolation.level), [console-consumer implementation](https://github.com/apache/kafka/blob/3.9.1/tools/src/main/java/org/apache/kafka/tools/consumer/ConsoleConsumer.java).

   **Why it matters:** If the stable boundary remains at `1100` during capture, checkpointing `1200` skips records that subsequently commit below `1200`. This affects existing transactional inventory members as well as strike.

   The property itself is appropriate for every inventory’s data formats and cleanup policies. Purely nontransactional topics retain the same visibility; compaction does not make the property incompatible. However, **the archiver as a whole is not safe for every topic**: transactional compacted and transactional delete-retained topics share the checkpoint problem. Even nontransactional records behind an unresolved transaction in a mixed partition are withheld.

   The hazard explanation at [oe-topics.env:119](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:119) correctly describes the failure. The assertion at [line 133](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:133) that behavior is unchanged is incorrect relative to main: this PR changes which records the consumer can return before checkpointing.

   Withdrawing the text parser was the right decision. Developing its replacement separately is also reasonable. **Leaving that replacement as a nonblocking follow-up while activating this consumer/checkpoint combination is not sufficient.** Existing transactional producers demonstrate the shared exposure; they do not establish safety.

   **Corrected text:**

   > This PR changes archive consumption to read_committed. The existing end-offset checkpoint rule does not prove committed capture completion and can skip later commits. A metadata-based reader must capture an exclusive stable boundary, export complete records below it, and advance the checkpoint only after reaching that boundary. The reader replacement may be reviewed separately, but safe transactional archival is a deployment dependency.

   Also replace the unconditional idle-completion claims at [inventory:111](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:111) and [archiver:535](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:535): catch-up can exhaust the message budget, reach the outer timeout, or encounter an unresolved transaction. Clock time and a presence floor establish none of those boundaries.

2. **P2 — The live PR body still advertises the withdrawn implementation and regression coverage.**

   **Evidence:** [PR #1028](https://github.com/abhinav-jain09/options-edge-deploy/pull/1028), at the reviewed head, still claims:

   - `OE_TRANSACTIONAL_TOPICS_<env>` controls captured-offset checkpointing.
   - Transactional topics never checkpoint the high-water mark.
   - `test-archive-reset.sh` §12 validates that behavior.
   - Rollout step 1 installs the “corrected archiver.”

   Those mechanisms are absent. The [round-6 disposition](/private/tmp/oe-deploy-fpstrike/ES-FOOTPRINT-STRIKE-DEPLOY-CODEX-ROUND6.md:15) explicitly records their removal, and the [current suite ends at line 403](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:403).

   **Why it matters:** The operational handoff claims a data-loss protection that rollout will not install. Its validation summary also describes an earlier tree.

   **Corrected text:**

   > Strike is included in the nightly es4 inventory with a provisional 500-record presence floor. The only executable archiver change is read_committed. Captured-offset checkpointing, transactional classifications, their CI gate, and added archive cases were withdrawn. The archive suite matches origin/main; it does not validate transactional capture completion.

   Retain the rollout sequence, but make safe archival a dependency and replace the validation summary with evidence for this head. Specify the existing **production** `cvd.levels` mirror reinstall; that topic’s [target guard](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:229) refuses dev.

Part A reconciles every numbered earlier finding below. **WITHDRAWN means its implementation was removed, not that every underlying archive limitation was solved.**

| Earlier finding | Current disposition | Current-tree evidence |
|---|---|---|
| R1 #1 — Aborted revisions mirrored | **CLOSED** | Generated [consumer isolation](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:192); pinned by [shape test:29](/private/tmp/oe-deploy-fpstrike/scripts/ci/es-cvd-mirror-shape-test.sh:29). |
| R1 #2 — Incorrect reset classification | **CLOSED** | [Preserved list:189](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:189) and [premarket exclusion:241](/private/tmp/oe-deploy-fpstrike/scripts/ops/premarket-reset.sh:241). |
| R1 #3 — Transactional archive integration | **PARTIAL** | [Committed-only read:538](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:538) is present; unsafe checkpoint remains. Finding 1 above. |
| R1 #4 — Unsupported floor 100 | **CLOSED** | [Provisional rationale and floor 500](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:194); sourced value verified as `500`. |
| R2 #1 — Unproved end-offset checkpoint | **STILL OPEN** | [Acceptance:563](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:563), [checkpoint:606](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:606). |
| R2 #2 — Missing byte retention | **CLOSED** | [Declarations:258](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:258), [es4:643](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:643), [create:196](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:196), [reconcile:303](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:303), [verify:110](/private/tmp/oe-deploy-fpstrike/scripts/kafka/verify-topics.sh:110). |
| R3 #1 — Undeclared transactional writers remain unsafe | **STILL OPEN** | Classification was withdrawn; [all accepted captures again checkpoint `endoff`](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:606). |
| R3 #2 — Boundary crossing discards the complete capture | **WITHDRAWN** | The crossing guard is absent; [current publication path](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:563) retains successful captures. Other boundary limitations remain below. |
| R4 #1 — Trimming corrupts binary payloads | **WITHDRAWN** | No trimming exists in the [restored capture/publication path](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:537). |
| R4 #2 — Ignored scan/trim failures | **PARTIAL** | Added helpers are gone; inherited [pipeline/statistics status handling](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:545) remains incomplete. |
| R4 #3 — Active sparse capture fails to finish | **STILL OPEN** | [Record-count budget and 900-second bound](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:537), followed by [failure/discard](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:612). This is restored baseline behavior. |
| R4 #4 — Added offset column breaks schema discovery | **WITHDRAWN** | [STRICT-only offsets:543](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:543); original [value-column extraction:276](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:276). |
| R5 #1 — Payload continuation forges checkpoint | **WITHDRAWN** | No text-derived offset probe; [checkpoint:606](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:606) uses the queried end. |
| R5 #2 — `LogAppendTime` rejected | **WITHDRAWN** | Timestamp-dependent offset acceptance is absent from the [restored capture path](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:537). |
| R5 #3 — Timeout publication corrupts records/bypasses STRICT | **WITHDRAWN** | [Acceptance requires status zero](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:563); status `124` is rejected. |
| R5 #4 — Future records satisfy historical floor | **STILL OPEN** | [Count-bounded capture:539](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:539) can overshoot; [manifest:594](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:594) counts the whole file. Restored baseline limitation. |
| R5 #5 — Processing failures ignored | **PARTIAL** | Offset-scan failure path removed; [consumer-only pipeline status and unchecked statistics](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:545) remain. |
| R6 #1 — Incomplete inventory gate passes | **WITHDRAWN** | Gate deleted; [archive Jenkins pipeline:96](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.archive-scripts-deploy:96) equals main. The underlying checkpoint issue remains R2 #1. |
| R6 #2 — Strike raw key forges offset | **WITHDRAWN** | Probe deleted. Additionally, current [BarInput:97](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/BarInput.java:97) rejects `\|` and characters below `0x20`, including the demonstrated newline. |
| R6 #3 — Timeout publishes malformed tail | **WITHDRAWN** | [Status-zero acceptance:563](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:563); independently checked rejection of `124`. |
| R6 #4 — Historical floor counts overshoot | **STILL OPEN** | Same surviving baseline behavior as R5 #4; [manifest:594](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:594). |
| R6 #5 — Processing statuses partly ignored | **PARTIAL** | Added scan removed; inherited [status handling:545](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:545) remains. |
| R6 #6 — Schema extractor misreads `Offset:` key | **WITHDRAWN** | Layout inference removed; [extractor:276](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:276) equals main. |
| R6 #7 — Incorrect timestamp-less spelling | **WITHDRAWN** | No timestamp anchor remains in the [capture path](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:537). |

The earlier unnumbered findings and qualifications resolve as follows:

| Item | Disposition | Evidence |
|---|---|---|
| Recovery time, peak RSS, missed-clean capacity | **PARTIAL** | [Manifest sizing remains explicitly provisional](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:62). No representative measurements were supplied. |
| Fold budget excludes reconstruction | **CLOSED** | Current [runtime:355](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeRuntime.java:355) checks the budget after reconstruction. This is not a preemptive wall-clock kill. |
| Unbounded dev/prod storage | **PARTIAL** | [Unlimited byte retention](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:258) and preservation are intentional; storage runway remains unmeasured. |
| Archive-first rollout, mirror reinstall, filename order | **PARTIAL** | Sequence in the PR body is appropriate; its archive claims are stale. [Deploy-all:232](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es4-deploy:232) still uses filename order. |
| §12(e) missing exit-status assertion | **WITHDRAWN** | Entire added section removed; [suite:403](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/test-archive-reset.sh:403) equals main. |
| OPB classification explanation | **WITHDRAWN** | Transactional classifications were removed from [inventory:118](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:118) onward. |
| GAP wording overstates record loss | **STILL OPEN** | [Existing diagnostic:506](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:506); unchanged baseline issue. |
| Test coverage beyond static declarations | **PARTIAL** | [Isolation assertion:29](/private/tmp/oe-deploy-fpstrike/scripts/ci/es-cvd-mirror-shape-test.sh:29) is textual, not an aborted-transaction integration test; [sibling fixtures:138](/private/tmp/oe-deploy-fpstrike/scripts/ci/es-cvd-mirror-shape-test.sh:138) still supply obsolete compaction policies independently of the real arms. |
| Earlier whitespace defects | **CLOSED** | Both requested `git diff --check` ranges pass. |

Part B’s fresh deployment sweep found no additional blocking implementation defect.

| Area | Assessment |
|---|---|
| Topic declarations | Strike occurs once in each [default declaration](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:308) and [es4 declaration](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:575), at one partition. Both exact-partition lists include it. It is absent from compacted/rebuildable lists and exclusively reset-preserved. |
| Complete retention contract | Both sets declare `retention.ms=-1` and `retention.bytes=-1`. Extracted create/reconcile functions set bytes for dev, production, experiment and es4; unrelated topics receive no byte override. |
| Verification | The new [byte check](/private/tmp/oe-deploy-fpstrike/scripts/kafka/verify-topics.sh:110) selects the correct list and explicitly rejects finite or unreadable values. The pre-existing initial verifier loop still uses the default topic set; this PR does not make the entire verifier es4-aware. |
| Mirror | Eight allowed topics; strike’s [arm:175](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:175) is `1/delete/-1/-1`. Bytes are asserted on both ends and supplied during target creation. `BYTES=""` prevents sibling inheritance. Committed-only consumption is correct; MM1 still provides record copying, not atomic replication of source transactions. |
| Manifest/settings | [Manifest env:68](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:68) agrees with current [StrikeSettings:35](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeSettings.java:35): broker `.4:9092`, prefixed input/output, stable group/transactional ID, health `8153`, band `250`, series `64`, universe `400000..900000/500`—1,001 strikes. Omitted commit timeout/abort limit retain `30000` ms/`5`. |
| Lifecycle | One replica and `Recreate` are appropriate. Selectors, Service port, probes and namespace agree. Folding remains live but unready. The explicit 300-second fold budget leaves nominal margin within the 600-second rollout wait; scheduling, initialization and actual resource sufficiency are not proven by that arithmetic. |
| Deployment integration | [Service selection:41](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es4-deploy:41), [hand-authored classification:112](/private/tmp/oe-deploy-fpstrike/scripts/ci/validate-es4-render.sh:112), image/build target and [digest verification](/private/tmp/oe-deploy-fpstrike/scripts/es4/pin-and-apply.sh:82) agree. |
| Gateway environments | Correct topic values in [base:79](/private/tmp/oe-deploy-fpstrike/k8s/base/feed-gateway-deployment.yaml:79), [dev:138](/private/tmp/oe-deploy-fpstrike/k8s/services/feed-gateway/overlays/dev/manifest.yaml:138), [production:116](/private/tmp/oe-deploy-fpstrike/k8s/services/feed-gateway/overlays/production/manifest.yaml:116), [experiment:108](/private/tmp/oe-deploy-fpstrike/k8s/services/feed-gateway/overlays/experiment/manifest.yaml:108), and [es4:133](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-feed-gateway.yaml:133). es4 resolves the unprefixed value through `TOPIC_PREFIX=es.`. |
| Gateway readiness | The [actual fifth-topic gate](/private/tmp/oe-gw-fpstrike/src/main/java/app/feedgateway/FeedGatewayService.java:855) includes strike. Missing topics are deferred; existing invalid topics refuse startup. Therefore successful gateway rollout alone does not prove mirror availability, including experiment. |
| Rollout order | Correct dependency order is safe archive capability/inventory and topic reconciliation → compatible strike image/service → strike mirrors plus production levels reinstall → gateway topic attestation/deployment → web. Use explicit service deployments for initial rollout. |

Independent validation passed: nine changed shell/env syntax checks; both whitespace checks; 15 mirrored-topic contracts; 18 archive-unit dependencies; 26 mirror-shape scenarios through a read-only harness; extracted apply/byte-verification cases; 27 in-memory es4 renders; and actual kustomize equality for all three gateway slices.

The full temporary-file suites, broker integration tests, deployed image contents and capacity measurements were not independently exercised in this read-only environment. No files or deployments changed.

**Institutional — Not met:** declarations and deployment contracts are consistent, but committed archive completeness is still falsely implied by checkpoints and the PR body.

**Military — Not met:** withdrawing the parser eliminates its corruption and forgery regressions; unresolved-transaction capture can still advance past unread data.

**NASA — Not established:** reconstruction is now budget-checked, but representative recovery duration, peak RSS, missed-clean recovery and mirror storage runway remain unmeasured.

VERDICT: REQUEST_CHANGES
Make committed archive checkpointing safe and update the PR body to describe the implementation actually being deployed.
