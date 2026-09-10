# ES Footprint strike interaction — deploy Codex round 1 (gpt-6-astra, 2026-09-10)

Review of PR #1028 at cfa33721. Verdict: **REQUEST_CHANGES**. Dispositions (folded in the following commit):

| # | finding | disposition |
|---|---|---|
| 1 | mirror copies aborted revisions (read_uncommitted) | FIXED — `isolation.level=read_committed` in the generated consumer.properties (every topic this job mirrors; cvd.levels is transactional too), pinned by es-cvd-mirror-shape-test.sh, plus strike shape cases (compacted target, finite retention, 4 partitions, compacted source, dev target) |
| 2 | RESET_REBUILDABLE is wrong for source and mirrors | FIXED — RESET-PRESERVED in topics.env with the reasoning; premarket-reset.sh PRESERVE_TOPICS_REGEX arm; validate-durable-topic-preservation + its mutation test pass |
| 3 | archive: transactional log read uncommitted, record count vs offset span, idle claim | FIXED — the archiver reads `--consumer-property isolation.level=read_committed` (all topics); the oe-topics.env note rewritten: repeated episode keys, CHECKPOINT keys, markers occupying offsets so the bounded read ends on the idle timeout, the producer idle once the 17:00 finalize bar is consumed, the floor (not the clock) says whether the day is whole. The LSO-bounded reader/checkpoint contract you specify is a change to the archiver for EVERY transactional topic and is recorded as a follow-up, not silently claimed |
| 4 | floor of 100 unsupported | FIXED — 500 (the source-bars floor: every well-formed bar leaves records or a CHECKPOINT), marked provisional |
| — | resource adequacy / fold budget vs the 600 s rollout wait | ADDRESSED — heap 768 MiB, limit 1 GiB (the es-cvd precedent), `FOOTPRINT_STRIKE_FOLD_TIMEOUT_MS=300000` so a stuck fold fails closed inside the deploy wait; the producer-side fold now releases closed episodes (processing round 1 #9) |
| — | rollout order | RECORDED in the PR body: image → es4 service (creates its output) → mirrors (dev, prod) → gateways → web |

## Review text (verbatim)

I reviewed all 11 changed files against `origin/main..HEAD`, the design, service implementation, and repository precedents. **The wiring is largely correct, but transaction isolation and reset preservation need changes before merge.**

1. **P1 — The mirror can publish aborted strike revisions as committed records.**

   **Evidence:** The new topic enters the mirror at [Jenkinsfile.es-cvd-mirror:173](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:173), but its consumer properties at line 177 omit `isolation.level`. The service publishes records before committing the transaction and can abort them: [StrikeRuntime.java:123](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeRuntime.java:123), line 148.

   **Why it matters:** Kafka defaults to `read_uncommitted`, which includes aborted records. MM1 republishes those records nontransactionally, so the gateway’s own `read_committed` setting cannot filter them afterward. Dev/prod can display a revision that never became authoritative on es4. [Kafka consumer configuration](https://kafka.apache.org/26/configuration/consumer-configs/).

   **Corrected text:** Add this to the generated `consumer.properties`, immediately after `auto.offset.reset=earliest`:

   ```properties
   isolation.level=read_committed
   ```

   Add a regression that aborts an emitted revision, then commits another transaction: only the committed records may reach the target. The existing shape tests do not exercise this.

2. **P1 — `RESET_REBUILDABLE` is wrong for both the authoritative source and established mirrors.**

   **Evidence:** [topics.env:200](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:200) classifies strike as rebuildable. However, [StrikeRuntime.java:76](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeRuntime.java:76) folds the output, then lines 79–82 resume the committed input-group position. `earliest` at line 241 is only the fallback. Mirror group offsets likewise survive reinstalls: [Jenkinsfile.es-cvd-mirror:179](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:179).

   **Why it matters:** An empty output log plus valid committed input offsets produces an empty recovered state followed by processing of future bars. It does **not** reconstruct the missing episodes, predecessor, or watermark. A wiped mirror also does not automatically replay its source history. The tape-zones preservation rationale at [topics.env:165](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:165) applies directly.

   The design’s as-built “reset-REBUILDABLE” statement at [design:514](/private/tmp/oe-fpstrike/ES-FOOTPRINT-STRIKE-INTERACTION.md:514) therefore conflicts with its normative R6 recovery contract and actual implementation.

   **Corrected text:**

   - Remove `es.futures.footprint.strike` from the existing `OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS` assignment.
   - Insert `es.futures.footprint.strike` into the existing `OPTIONS_EDGE_RESET_PRESERVED_TOPICS` assignment at line 182.
   - Append this alternative **inside the existing quoted** `PRESERVE_TOPICS_REGEX` at [premarket-reset.sh:238](/private/tmp/oe-deploy-fpstrike/scripts/ops/premarket-reset.sh:238):

   ```text
   |^es\.futures\.footprint\.strike$
   ```

   Use this explanatory comment:

   ```sh
   # es.futures.footprint.strike is RESET-PRESERVED on source and mirror brokers.
   # The source folds its own log before resuming committed input offsets.
   # MM1 also resumes committed source offsets; deleting a target does not replay it.
   # Neither path reconstructs a wiped log automatically.
   ```

   Do not add a second preserved-list assignment: [reset-preserved-topics.sh:16](/private/tmp/oe-deploy-fpstrike/scripts/kafka/reset-preserved-topics.sh:16) requires exactly one.

   This protects the ordinary preservation-aware reset paths. It does not make the destructive whole-broker `cleanup-es4.sh` preserve data; that remains dependent on archive/restore.

3. **P1 — The archive integration treats a transactional log as an ordinary record-count-bounded stream.**

   **Evidence:** [oe-topics.env:107](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:107) claims every record has a unique revision and the offset span is reachable. The actual Kafka key excludes revision; CHECKPOINTs have no episode revision: [service contract:89](/private/tmp/oe-fpstrike/es-footprint-strike-service/ES-FOOTPRINT-STRIKE-SERVICE.md:89). The archive consumer at [oe-archive-kafka.sh:531](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:531) omits transaction isolation and uses `--max-messages "$count"`.

   **Why it matters:**

   - Aborted strike records enter the archive under the default isolation.
   - Commit/abort markers occupy offsets but are never returned as application records. Consequently, even an uncompacted transactional log has an unreachable **record-count** offset span. [Kafka transactional consumption](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html).
   - CHECKPOINTs keep output flowing even when no strike has an episode. The producer remains active through processing the 17:00 finalize.
   - Normal caught-up operation should become idle during the session break. Merely following the bars topic does not prove that a delayed/recovering consumer is idle by 17:01. The comment’s “never the 900 s kill” guarantee is unsupported.

   **Corrected text:** Replace lines 107–111 with:

   ```sh
   # Transactional append-only log: episode keys repeat across revisions;
   # CHECKPOINT keys identify bars and carry no episode revision.
   # cleanup.policy=delete preserves revisions, and retention.ms=-1 survives
   # missed archive runs. Transaction markers make record count differ from
   # offset span. Capture committed records to a captured stable offset.
   # The producer becomes idle after consuming the session's finalized bars;
   # 17:01 wall-clock time alone does not prove that processing has completed.
   ```

   The strike archive reader must include:

   ```text
   --consumer-property isolation.level=read_committed
   ```

   That property alone is insufficient for a robust live capture. The required reader/checkpoint contract is:

   ```text
   Capture the read_committed end offset (LSO).
   Export only records below that boundary.
   Complete when consumer.position reaches the boundary.
   Advance the archive checkpoint only to that completed boundary.
   ```

   Test committed, aborted, open-transaction, control-marker-only, and delayed-finalize cases. Keep strike in `OE_ES4_TOPICS`; correct the reader rather than removing the required archive.

4. **P2 — The archive floor of 100 has no supporting session evidence.**

   **Evidence:** [oe-topics.env:171](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:171) introduces `:100`, while the source bars floor is 500. The stated floor policy requires an observed-session rationale at lines 119–122. The engine emits a CHECKPOINT when the bar’s own episode set is empty: [StrikeEngine.java:134](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeEngine.java:134).

   **Why it matters:** Quiet strike interaction does not imply sparse output. A small suffix can exceed 100 records while most of the session is missing. Episode revisions also inflate counts, so any count floor remains a coarse alarm.

   **Corrected text:** Use the existing conservative source-bar floor pending measured calibration:

   ```sh
   es.futures.footprint.strike:500 \
   ```

   Add:

   ```sh
   # Strike provisionally shares the source-bars floor: each new well-formed
   # bar advancing the watermark emits episode records or a CHECKPOINT.
   # This is a coarse presence floor, not proof of session completeness.
   # Calibrate against the first complete archive, including an early close.
   ```

The remaining requested checks resolve as follows.

| Check | Evidence and assessment |
|---|---|
| Default topic declaration | Correct: `es.futures.footprint.strike:1`, [topics.env:293](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:293). |
| es4 declaration | Correct: `:1`, [topics.env:560](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:560). |
| Both exact-partition lists | Correct at [line 218](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:218) and [line 520](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:520). |
| Both retention overrides | Correct: `=-1` at [line 243](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:243) and [line 626](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:626). |
| Compaction/extraneous lists | Absent from active compacted and pure-compact lists; correctly absent from prod-only classifications. Line 68 is an exclusion comment, not compaction membership. |
| Reset classification | Exactly one classification syntactically; semantically incorrect, finding 2. |
| Archive retention | `-1` satisfies the missed-run time-retention requirement. Preservation and transaction-safe capture are separate requirements. |
| Mirror shape/count | Correct `PARTS=1; POLICY=delete; RET=-1`; eight topics, including five footprint streams. [Jenkinsfile:11](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:11), [line 173](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:173). |
| Mutation-test anchor | Preserved: `es.futures.footprint.evidence` remains immediately before `], description:`. The substitution at [test:186](/private/tmp/oe-deploy-fpstrike/scripts/ci/validate-mirrored-topic-contracts-test.sh:186) still matches. |
| Service topic enforcement | `ensureArchivableTopic` writes delete/−1: [KafkaTopics.java:183](/private/tmp/oe-fpstrike/processing-common/src/main/java/app/kafka/KafkaTopics.java:183). Runtime refuses wrong partitions or compaction; retention drift is logged rather than itself being a terminal assertion, [StrikeRuntime.java:179](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeRuntime.java:179). |

Every service-specific setting matches [StrikeSettings.java:35](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeSettings.java:35):

| Setting | Manifest/default assessment |
|---|---|
| Bootstrap | Explicit `192.168.100.4:9092`; overrides the generic bootstrap fallback. |
| Input/output topics | `futures.footprint.bars` / `futures.footprint.strike`; `TOPIC_PREFIX=es.` from [es4-common-env.yaml:17](/private/tmp/oe-deploy-fpstrike/k8s/es4/es4-common-env.yaml:17) produces the required names. |
| Group / transactional ID | `options-edge-es-footprint-strike` / `es-footprint-strike-0`; matches defaults. No competing declaration found in this repo. Transactional uniqueness is broker-scoped. |
| Health | Explicit `8153`; all Service/container/probe references agree. No other manifest uses 8153. |
| Band / series | `250` cents / `64` entries; correct. |
| Universe | `400000..900000`, step `500`: 1,001 strikes; correct. |
| Commit timeout | Omitted intentionally; service default `30000` ms. |
| Consecutive aborts | Omitted; default `5`. |
| Fold timeout | Omitted; default `600000` ms. |

The manifest’s selectors, namespace, labels, envFrom wiring, Prometheus Service annotations, **one replica plus Recreate**, and image naming follow the es-cvd precedent. See [manifest:14](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:14). Its hand-authored declaration at [validate-es4-render.sh:112](/private/tmp/oe-deploy-fpstrike/scripts/ci/validate-es4-render.sh:112) is appropriate; the tape-zones header also establishes that es4-only services need not enter `services.yaml`.

The probes correctly allow folding: liveness remains healthy during STARTING/FOLDING, readiness requires RUNNING, and degradation produces 503 followed by exit 70. [StrikeHealth.java:24](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeHealth.java:24). A startup probe is therefore not required merely because folding takes longer than the initial delay.

**Resource adequacy is unproven.** The manifest supplies a 512 MiB heap, 768 MiB limit and 100m CPU request. Startup scans the **entire retained log**, and [StrikeLogFold.java:29](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeLogFold.java:29) retains each identity’s latest raw bytes and parsed episode, including closed historical episodes, until folding finishes. Bounding series entries does not bound historical identity count. No full-session or accumulated-history replay measurement was provided. Also, the 600-second fold budget leaves no guaranteed startup margin within the [600-second rollout wait](/private/tmp/oe-deploy-fpstrike/scripts/es4/pin-and-apply.sh:82). An arbitrary memory increase would not establish indefinite restartability.

`SERVICE=es-footprint-strike` is correctly selectable at [Jenkinsfile.es4-deploy:41](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es4-deploy:41). The common deployment path digest-pins `:prod`, refuses unresolved/floating images, waits for rollout, and verifies running image digests: [pin-and-apply.sh:24](/private/tmp/oe-deploy-fpstrike/scripts/es4/pin-and-apply.sh:24). Registry availability and the actual contents of `:prod` were not established by this review.

Gateway wiring is correct:

- Base and dev/production/experiment slices all name `es.futures.footprint.strike`; each standalone Deployment is identical to its monolithic render.
- es4 correctly supplies unprefixed `futures.footprint.strike`, [es-feed-gateway.yaml:133](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-feed-gateway.yaml:133).
- The fifth topic enters the actual gate via [FeedGatewayService.java:850](/private/tmp/oe-gw-fpstrike/src/main/java/app/feedgateway/FeedGatewayService.java:850).
- An absent topic is deferred. An existing topic with invalid effective size/compression configuration refuses gateway startup: [FootprintTopicGate.java:91](/private/tmp/oe-gw-fpstrike/src/main/java/app/feedgateway/FootprintTopicGate.java:91). Naming a topic does not provision or mirror it.
- The PR does not establish live topic existence in any environment. Experiment has no separately demonstrated mirror installation. Missing data there is deferred, not a startup failure.
- The current G-R8 static values agree: producer/gateway record ceiling 262144; es4 gateway native headroom 1024 MiB; rendered dev/prod/experiment headroom 2560 MiB. The existing four-topic live attestation in the es4 comment does not attest the new fifth topic.

The rollout should proceed in this order:

1. Publish the required service/gateway/web images; provision the es4 input and topic contracts.
2. Deploy strike on es4 and verify successful recovery, processing, and committed output. Precreating its **output** is optional because the service ensures it; a missing/invalid input fails closed.
3. Install the corrected mirror separately for dev and prod; verify committed history reaches each target.
4. Validate each fifth topic’s effective gateway contract, then deploy the gateway and confirm its fold completes.
5. Deploy the consuming web feature.

The gateway can start before a missing mirror/topic because absence is deferred. Therefore deployment success does **not** enforce that ordering or prove feature readiness. Existing-invalid topics fail closed; missing topics do not. Image pinning also proves artifact identity, not cross-component version compatibility.

The validator coverage has these limits:

| Validator | Result / gap |
|---|---|
| `validate-durable-topic-preservation` | Classification is syntactically accepted; it cannot infer whether reconstruction is real. Add a strike-specific preservation invariant and a mutation moving it back to REBUILDABLE. Full execution stopped at prohibited temporary-file creation. |
| `validate-mirrored-topic-contracts` | **Passed**, including strike and all 15 mirrored topics. Checks shape, not transactional isolation or reset recovery. |
| Its mutation suite | Wrapped-list anchor remains valid. Add strike-specific mutations for each cluster’s shape and the mirror’s transaction-isolation setting. Not executed. |
| `es-cvd-mirror-shape-test` | Tests supplied shape variables, not generated consumer isolation. Its bars/CVD cases still use obsolete `compact,delete` fixtures at [line 129](/private/tmp/oe-deploy-fpstrike/scripts/ci/es-cvd-mirror-shape-test.sh:129). Add `MOCK_TOPIC=es.futures.footprint.strike PARTS=1 POLICY=delete RET=-1`, with source/target partition, policy and retention failures. Not executed. |
| `validate-services` | Relevant gateway slice equality verified through read-only rendering. Does not validate this hand-authored es4 service’s recovery/resource contract. Full suite not executed. |
| `validate-es4-render` | In-memory equivalent rendering: **27 services, zero drift; no unaccounted manifests**. HAND_AUTHORED membership does not validate service semantics. |
| Footprint contingency + test | Static ceiling/headroom checks pass. The validator selects es-cvd and es4 gateway only at [line 29](/private/tmp/oe-deploy-fpstrike/scripts/ci/validate-footprint-gateway-contingency.sh:29); it does not establish strike record-size bounds, fold capacity, live fifth-topic validity, or archive safety. Full scripts not executed. |

Shell syntax checks and `git diff --check` passed. No files or external deployments were changed.

**Institutional:** Not met—reset semantics and committed-history fidelity are incorrect.
**Military:** Not met—abort/reset/archive failure paths are insufficiently protected despite correct fencing and restart wiring.
**NASA:** Not established—full-history recovery capacity, startup margin, and transactional archive completion lack adequate evidence.

VERDICT: REQUEST_CHANGES
Preserve the authoritative log, enforce committed-only transport and archival, and close the archive and recovery-validation gaps.
