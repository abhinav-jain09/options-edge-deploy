# ES Footprint strike interaction — deploy Codex round 2 (gpt-6-astra, 2026-09-10)

Review of PR #1028 at 16c1257c. Verdict: **REQUEST_CHANGES** (two findings). Dispositions, folded in the following commit:

| # | finding | disposition |
|---|---|---|
| 1 | transactional archival checkpoints the high-water mark it never proved it reached (LSO, markers, unresolved transactions; also applies to OPB's transactional outputs in prod) | FIXED, as built: the archiver now carries a per-environment `OE_TRANSACTIONAL_TOPICS_<env>` list (`oe-topics.env`: es4 = the strike log and `es.futures.cvd.levels`; prod = the five `option-price-behavior-*` outputs, which run exactly-once; dev = none). For a declared topic the console reader prints `Offset:` and the checkpoint advances ONLY to the highest offset actually captured + 1 — never to the high-water mark; the manifest's `offset_to`/`offset_span` say the same boundary; a capture that read nothing leaves the checkpoint where it was and is reported (`checkpoint NOT advanced`), so a record that commits after the capture is read next time instead of skipped forever. **Why not the LSO contract verbatim:** no Kafka CLI the archiver runs (`kafka-get-offsets`, `kafka-console-consumer`) exposes the last stable offset, so "capture the read_committed end and complete only when position reaches it" cannot be expressed in this tooling; the captured-max boundary is the one this reader can PROVE it reached and is strictly safe (never past an uncaptured offset). A marker-only remainder therefore does not advance (re-read next run, duplicates not gaps) rather than "completing with zero records" — the archiver cannot tell a marker-only range from a broken reader, and it does not pretend to. `test-archive-reset.sh` §12 pins committed (marker deferred), unresolved transaction (stops at the stable boundary), delayed finalize (the late commits are captured next run, nothing skipped), aborted records (never archived, cleared by a later committed offset), marker-only (not advanced, said), and the plain-topic path unchanged; run in the Debian container exactly as `Jenkinsfile.archive-scripts-deploy` runs it: ALL PASS (90 assertions). The `oe-topics.env` floor note now reads as you specified (the floor detects grossly missing output; idle timeout proves nothing about finalize or the stable boundary). |
| 2 | deployment and mirror omit the retention.BYTES contract | FIXED — `topics.env`: `OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES` and `OPTIONS_EDGE_ES4_TOPIC_RETENTION_BYTES_OVERRIDES` (strike = −1 in both sets); `apply-topics.sh` writes `retention.bytes` at create AND reconcile from the active set; `verify-topics.sh` attests it for the active set and refuses an unreadable value (the no-match grep is guarded so the refusal is SAID, not a silent `set -e` exit — found by the new test); `Jenkinsfile.es-cvd-mirror` strike arm `BYTES=-1`: the target is created with `--config retention.bytes`, and BOTH ends are attested (`eff` reads it; finite or unreadable fails — `eff`'s diagnostic now goes to stderr because every caller is `X=$(eff …) \|\| exit 1`, which swallowed it). Mutations: `es-cvd-mirror-shape-test.sh` (finite target, finite source, UNREADABLE target), `verify-topics-pure-compact-test.sh` (finite, unreadable, es4's own list). |
| — | resource note claims capacity it did not measure | REWORDED as PROVISIONAL in `es-footprint-strike.yaml` (what the fold retains now, and that a full-session and a missed-clean recovery must be measured before the sizing is called capacity). The processing round 3 makes the fold's per-record retention compact and measured (see that record). |
| — | rollout order must include the archive deployment and the transactional mirror reinstalls | RECORDED in the PR body: archive scripts + inventory first, then image → es4 service → strike mirrors (dev, prod) + REINSTALL of the existing `es.futures.cvd.levels` mirror instances (the shared `read_committed` fix only takes effect on reinstall) → gateways → web. `deploy-all`'s filename order is not the rollout order and is not used for it. |
| — | dev/prod strike storage grows unbounded under retention −1 and RESET-PRESERVED | ACKNOWLEDGED, by design (R13: every revision survives; dev's daily wipe deliberately does not reclaim it). Capacity: ~3 KB/record, hundreds of records/session → tens of MB per month per copy; the nightly archive is the long-term home. No change. |
| — | round-1 record trailing whitespace | FIXED (`git diff --check` clean). |

## Review text (verbatim)

Reviewed all 15 changed files at `16c1257c`, both requested diff ranges, and the current producer/gateway implementations. **Two findings prevent approval.** No files or deployed resources were changed.

1. **P1 — Transactional archival can still checkpoint past records it never captured.**

   **Evidence:** [oe-archive-kafka.sh:314](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:314) captures ordinary end offsets. [Line 538](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:538) now consumes `read_committed`, but still stops by `--max-messages "$count"`, where `count=endoff-from`. [Line 563](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:563) accepts any positive capture with consumer exit code zero; [line 605](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-archive-kafka.sh:605) checkpoints the originally captured `endoff`.

   **Why it matters:** The captured end can exceed the last stable offset (LSO). An unresolved transaction can hold the reader below that end until its idle timeout. Kafka’s console consumer catches the receive exception and returns normally, so exit zero does not prove boundary completion. [Kafka consumer semantics](https://kafka.apache.org/41/configuration/consumer-configs/#isolation.level), [console-consumer implementation](https://github.com/apache/kafka/blob/4.3.0/tools/src/main/java/org/apache/kafka/tools/consumer/ConsoleConsumer.java#L96).

   For example, capture end `1200`, read only through stable position `1100`, then time out. The script can checkpoint `1200`; records at `1100–1199` that subsequently commit are permanently skipped. Conversely, transaction markers make the requested record count unreachable within the captured range, so an active producer can cause capture **beyond** that range. A control-marker-only range cannot advance because `got=0`.

   The shared setting also changes existing transactional archive inputs. Production option-price-behavior explicitly enables EOS at [manifest:61](/private/tmp/oe-deploy-fpstrike/k8s/services/option-price-behavior/overlays/production/manifest.yaml:61), and its outputs are already archived through [oe-topics.env:64](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:64). Filtering its aborted records is correct; applying the existing unsafe checkpoint logic to its stable boundary is not.

   **Corrected text:** Implement this reader/checkpoint contract before treating round-1 finding 3 as closed:

   ```text
   Capture each partition's read_committed end offset (exclusive LSO).
   Export only records below that boundary.
   Complete only when consumer.position reaches the boundary.
   Publish the archive and checkpoint only that completed boundary.
   Allow completed ranges containing zero application records.
   A timeout before the boundary is a failed capture.
   ```

   Cover committed, aborted, unresolved-transaction, marker-only, and delayed-finalize cases. Replace [oe-topics.env:115](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:115)’s claim that the floor establishes a whole day with:

   ```sh
   # The floor detects grossly missing output; it does not establish session completeness.
   # Idle timeout does not prove finalized-bar processing or stable-boundary completion.
   ```

2. **P2 — Deployment and mirror validation omit the new byte-retention contract.**

   **Evidence:** Topic creation at [apply-topics.sh:195](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:195) and reconciliation at [line 284](/private/tmp/oe-deploy-fpstrike/scripts/kafka/apply-topics.sh:284) set time retention, policy, and ISR, but never `retention.bytes`. `verify-topics.sh` has no byte-retention check. The mirror likewise creates and checks only policy, partitions, and time retention at [Jenkinsfile.es-cvd-mirror:251](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:251).

   I executed the extracted mirror assertions with mocked effective configs: **`retention.bytes=1024` passed on both source and target**.

   **Why it matters:** `retention.ms=-1` does not prevent byte-based deletion. A newly created mirror can inherit a finite broker cap; a pre-existing cap survives reconciliation. Neither mirror preflight nor the added shape cases detects it.

   The producer now repairs and attests both retention settings at [StrikeRuntime.java:210](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeRuntime.java:210), but that protects its source topic, not the mirrors. Once source history has been deleted, restoring unlimited retention cannot restore it: [line 268](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeRuntime.java:268) refuses a nonzero beginning offset.

   **Direct answer:** I found **no deployment code that explicitly writes a finite `retention.bytes`**. The defect is omission: finite inherited/existing values can remain accepted. An ordinary reconciliation also does not overwrite the producer’s explicit `-1` byte setting.

   **Corrected text:** Extend the strike contract to:

   ```text
   partitions=1
   cleanup.policy=delete
   retention.ms=-1
   retention.bytes=-1
   ```

   Add strike-specific byte-retention declaration and create/update support for both topic sets; set it when creating the mirror target; attest it on both mirror ends and in verification. Add mutations for finite, inherited-finite, and unreadable byte retention.

The four round-1 dispositions are:

| Round-1 finding | Status | Evidence and assessment |
|---|---|---|
| 1 — Mirror exposes aborted revisions | **CLOSED** | The generated consumer configuration now sets `read_committed` at [Jenkinsfile:190](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es-cvd-mirror:190). This is safe for all seven existing topics: the six nontransactional topics retain their records; transactional `cvd.levels` correctly withholds unresolved records and excludes aborted ones. Each topic runs separately, so this introduces no cross-topic transaction barrier. |
| 2 — Wrong reset classification | **CLOSED** | Strike is exclusively preserved at [topics.env:189](/private/tmp/oe-deploy-fpstrike/scripts/kafka/topics.env:189), with the matching premarket exclusion at [premarket-reset.sh:241](/private/tmp/oe-deploy-fpstrike/scripts/ops/premarket-reset.sh:241). The whole-broker es4 reset remains a separate destructive operation. |
| 3 — Unsafe transactional archive | **PARTIAL** | Committed-only consumption and key descriptions are corrected. Boundary completion/checkpointing remains unsafe; see finding 1. The disposition describing this as fixed overstates closure. |
| 4 — Unsupported floor of 100 | **CLOSED** | [oe-topics.env:178](/private/tmp/oe-deploy-fpstrike/scripts/ops/archive/oe-topics.env:178) sets `500`; the rationale explicitly makes calibration provisional. I verified that the actual sourced value parses as `500`. This closes the requested provisional correction, not the question of session completeness. |

Dev preservation behaves consistently across the requested paths:

- [dev-cleanup.sh:493](/private/tmp/oe-deploy-fpstrike/scripts/ops/dev-cleanup.sh:493) excludes the shared preserved list from daily deletion.
- [cleanup-topics.sh:174](/private/tmp/oe-deploy-fpstrike/scripts/kafka/cleanup-topics.sh:174) preserves it during delete/recreate; line 190 also excludes it from temporary retention shrinking.
- [offhours-clean-slate.sh:405](/private/tmp/oe-deploy-fpstrike/scripts/ops/offhours-clean-slate.sh:405) excludes it before constructing either destructive topic list. Its subsequent group resets act on the target broker; MM1’s source offsets live on es4.

This preserves recovery semantics. **Dev/prod strike storage nevertheless grows without a configured bound** when byte retention is unlimited: the daily wipe deliberately does not reclaim it. That is consistent with preserving history, but requires capacity planning. The gateway’s bounded cache does not bound Kafka storage. es4’s whole-broker reset clears both data and offsets; preservation-list membership does not protect that source archive from an unsuccessful pre-reset capture.

For the fresh manifest/runtime sweep:

| Area | Assessment |
|---|---|
| Bootstrap and topics | Correct: explicit `.4:9092`, with `futures.footprint.bars` and `futures.footprint.strike` resolving through `TOPIC_PREFIX=es.`. |
| Writer ownership | Correct: one replica, `Recreate`, stable group and transactional IDs. |
| Domain settings | Match current [StrikeSettings.java:35](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeSettings.java:35): band `250`, series `64`, universe `400000..900000/500`, giving 1,001 strikes. |
| Omitted producer settings | Current defaults are suitable: commit timeout `30000` ms and maximum consecutive aborts `5`. |
| Health and shutdown | Service/container/probes agree on `8153`; selectors and namespace agree. Liveness remains healthy while folding, readiness waits for RUNNING, and terminal failure exits nonzero. A startup probe is not required merely to accommodate folding. |
| Fold budget | Explicit `300000` ms improves margin against the 600-second rollout wait. It bounds the scan, not total scheduling, image-pull, initialization, and recovery time. |
| Image/deployment integration | Service choice, image name, hand-authored classification, digest pinning, rollout wait, and running-image verification are wired correctly. |

The resource note remains **PARTIAL**. [Manifest:62](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-footprint-strike.yaml:62) describes the old fold and claims unmeasured “tens of MB” and an order of magnitude of headroom. The current [StrikeLogFold.java:40](/private/tmp/oe-fpstrike/es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/StrikeLogFold.java:40) releases closed episode snapshots but retains identity metadata and revision digests throughout the scan. Its memory still scales with retained history. The 768 MiB heap/1 GiB limit is plausible provisional sizing, not demonstrated capacity. Measure full-session recovery and accumulated history after missed cleans under representative es4 contention.

**The two new gateway envs need not be explicitly set for this deployment.** [GatewaySettings.java:356](/private/tmp/oe-gw-fpstrike/src/main/java/app/feedgateway/GatewaySettings.java:356) defaults the refusal bound to `10000`, with fail-closed behavior; its symbol default is `ES.v.0`, matching [es-cvd.yaml:85](/private/tmp/oe-deploy-fpstrike/k8s/es4/services/es-cvd.yaml:85). Neither omission creates a current contract mismatch.

Validation established:

- `validate-mirrored-topic-contracts.sh` passes for all 15 mirrored topics.
- In-memory es4 rendering: **27 services, zero drift, no unclassified manifests**.
- Rendered gateway Deployments match their standalone slices in dev, production, and experiment.
- Extracted strike mirror assertions reject wrong source/target partitions, policy, and time retention; both finite-byte cases incorrectly pass.
- Declaration uniqueness, exact partition membership, noncompaction, preservation, manifest singleton settings, and changed-shell syntax pass.
- `git diff --check` reports two Markdown trailing-space lines in the committed round-1 report.

The complete temporary-file-based suites were not executed in this read-only sandbox. The isolation regression is a text-presence check, not an aborted-transaction integration test. The durable validator still checks classification consistency rather than proving strike must remain preserved. The footprint contingency validator selects es-cvd and gateway at [line 29](/private/tmp/oe-deploy-fpstrike/scripts/ci/validate-footprint-gateway-contingency.sh:29); it does not establish strike recovery capacity or transactional archival correctness.

The rollout order should include the archive deployment explicitly:

1. Publish compatible images; deploy the corrected archive scripts and inventory; reconcile input/output contracts.
2. Deploy strike on es4; verify offset-zero recovery and committed processing.
3. Install strike mirrors separately for dev/prod and verify copied history. Reinstall existing transactional-topic mirror instances to activate the shared isolation fix.
4. Attest each gateway’s fifth topic, deploy gateways, and verify the strike view.
5. Deploy the consuming web feature.

`deploy-all` uses filename ordering at [Jenkinsfile.es4-deploy:232](/private/tmp/oe-deploy-fpstrike/Jenkinsfile.es4-deploy:232), placing es-feed-gateway before es-footprint-strike. It does not enforce this rollout sequence. Missing-topic deferral means a successful gateway rollout alone is not feature-readiness evidence.

**Institutional — Not met:** archival completeness can be falsely asserted, and mirror durability omits byte retention.

**Military — Not met:** fencing and preservation are sound, but unresolved-transaction archival and byte-retention drift remain insufficiently protected.

**NASA — Not established:** full-history recovery capacity, total startup margin, and transactional archive completion lack adequate demonstrated validation.

VERDICT: REQUEST_CHANGES
Fix stable-boundary archival and enforce the complete byte-retention contract before approval.
