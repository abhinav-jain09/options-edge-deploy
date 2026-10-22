# TA books: deploy wiring and rollout order (CHART-STRUCTURE-TOOLS, CLASSICAL-CHART-PATTERNS, TICK-TRADE-TOOLS, ELLIOTT-FIB-CYCLE-TOOLS and the market-structure Avro cutover)

Formerly titled by phase letters (phases B, C, D, E and the W0 cutover); the letters map to the processing documents as B = CHART-STRUCTURE-TOOLS, C = CLASSICAL-CHART-PATTERNS, D = TICK-TRADE-TOOLS, E = ELLIOTT-FIB-CYCLE-TOOLS, W0 = MARKET-STRUCTURE-AVRO-CUTOVER (requirement ids B-R*, C-R*, D-R*, E-R* unchanged).

Branch `feat/ta-geometry-deploy`. Nothing here is applied: every flag is the literal `"false"`, both new services are held at `replicas: 0`, and no
destructive step runs from a merge. This file is the order to follow when the owner turns each piece on. Producer, gateway and web are other repositories
(processing `feat/ta-phase-{w0,b,c,d,e}`, gateway `feat/ta-geometry-feed-gateway`, web `feat/ta-*-web`, contracts 0.31.0).

## 1. What this change adds

| piece | where |
|---|---|
| CHART-STRUCTURE-TOOLS `options.spx.market-structure.tools` and CLASSICAL-CHART-PATTERNS `options.spx.market-structure.geometry` (Avro, 1 partition, delete, 172 800 000 ms, EXACT partitions, key `<scheduleId>\|<tf>`) | `scripts/kafka/topics.env`, `ensure-market-structure-topics.sh` (+ test) |
| `MARKET_STRUCTURE_TOOLS_ENABLED`, `MARKET_STRUCTURE_GEOMETRY_ENABLED` = `false` | market-structure base deployment, two generated slices |
| `GATEWAY_STRUCTURE_TOOLS_ENABLED`, `GATEWAY_GEOMETRY_ENABLED`, `GATEWAY_TRADE_TOOLS_ENABLED`, `GATEWAY_WAVE_CYCLE_ENABLED` = `false` (names are `GatewaySettings`'; its topic variables default to the declared topics) | feed-gateway dev and production env patches, two generated slices |
| Fixed-id schema registration for 31 subjects (ids 2201-2405 plus version 2 of thirteen trade-tools subjects, 2321-2332 and 2335, section 3a, and of WaveSet and CycleSet, 2406 and 2407, section 3b; `options-edge-processing docs/ta-books/catalog/SCHEMA-ID-MAP.md`) | `scripts/kafka/ensure-fixed-id-schemas.sh`, `scripts/kafka/schema-id-maps/{market-structure,es-trade-tools,wave-cycle}.map`, service-deploy parameter `REGISTER_TA_SCHEMAS` |
| W0 rollover flag injection (never committed) | `scripts/deploy/service-deploy.sh` block `MS_IGNORE_RECOVERY_ROLLOVER`, parameter `MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER` |
| W0 step 4 and 6 (wipe and recreate the five topics under their own names; verify) | `scripts/kafka/roll-forward-market-structure.sh`, service-deploy parameters `W0_CUTOVER_WIPE`, `W0_EXPECT_IMAGE_DIGEST`, `W0_EXPECT_GATEWAY_DIGEST`, `W0_PRE_CUTOVER_IDS`, `W0_PRODUCERS_ATTESTED` |
| `es-trade-tools-service` (TICK-TRADE-TOOLS) | `k8s/services/es-trade-tools`, seven topics, `ensure-es-trade-tools-topics.sh`, `TT_*` flags `false` |
| `wave-cycle-service` (ELLIOTT-FIB-CYCLE-TOOLS) | `k8s/services/wave-cycle`, three topics, `ensure-wave-cycle-topics.sh`, `WAVE_CYCLE_*` flags `false` |

## 2. Decisions taken (and why), so nobody has to ask

* **Both new services are HELD at replicas 0, named in all three lifecycle lists** (`oe-boot-bringup.sh` KEEP_DOWN, `morning-autostart.sh` KEEP_DOWN,
  `dev-cleanup.sh` DISABLED_DEV). A held deployment is not an omitted one (`mandatory-services-silently-at-zero`): neither is in `prod-mandatory-services.conf`
  and the manifest comments say how to turn each on. Reasons. D: its requirement D-R20.1 step 6 says "dev at replicas 0, then family by family; no
  environment beyond dev before the calibration job", and with `TT_ENABLED=false` it only serves health (state DISABLED, readiness 503 by design).
  E: even with both flags off it resolves its five Avro subjects at boot and halts without them, so scaling it to 1 before registration would crash-loop.
  The dark run (E-R15) is the step after registration.
* **Reset class.** The four retention.ms=-1 D topics and E's recovery topic are `RESET_REBUILDABLE` (the service rebuilds from the retained tape/bars; a reset
  deletes the set together, so no checkpoint outlives its source). The honest cost, recorded in `topics.env`: chart history older than the 7-day tape retention
  is not rebuilt. Moving them to `RESET_PRESERVED` is a three-place change (`topics.env`, a byte contract, `PRESERVE_TOPICS_REGEX`) the owner can make at enablement.
* **The W0 rollover flag is injected at render time, not committed.** The ms-runtime-3 rollover needed a PR to add the flag and a second to remove it.
  Here the Jenkins parameter adds `MARKET_STRUCTURE_IGNORE_RECOVERY=true` to the rendered Deployment for one apply; the next ordinary deploy carries none.
  `RTH_ROLLOVER_GATE` keys on the render, so it refuses the injected flag Mon-Fri 09:30-16:00 America/New_York with no override. A test pins that no committed manifest carries the flag.
* **The tape.** The brief named `es.underlying.es.trades`; the service reads `underlying.es.trades.linearized` (D-D2, the 1-partition total-order tape of
  `es-trade-linearizer`, already EXACT in `topics.env`), a base name `KafkaSettings.topicValue` prefixes (`es.` on es4, empty on dev and production). es4 is not a target.
* **No consumer group** for D or E: both assign partitions themselves and keep their resume offset in their own checkpoint. Transactional ids: D `es-trade-tools-ES.v.0`,
  E `wave-cycle-SPX` (dev and production have separate brokers); market-structure keeps its per-environment ids.
* **Heap.** D: `-Xms384m -Xmx384m` (D-R17.3/6) in a 1Gi limit. E: the image's ENTRYPOINT fixes `-Xmx256m` and wins over `JAVA_TOOL_OPTIONS`, so the manifest declares none (a value would lie);
  512Mi/768Mi pending a measured resident set at the dark run.

## 3a. TICK-TRADE-TOOLS revision 21: registering version 2 of thirteen trade-tools subjects (contracts 0.33.0)

Producer `es-trade-tools-service` (processing #1281) writes `calibration=CALIBRATED`, `validation=MEASURED` and the nullable header field `evidence`. Thirteen subjects (every header-bearing record: PfChartState, PfColumn, PfBreakEvent,
PfLineEvent, PfCountEvent, PfMaCrossEvent, PfVolumeDivergenceEvent, PfMoneyFlowDivergenceEvent, PfBreakVolumeCheck, VolumeNodeSet, FlowBar, ClimaxEvent, TtFamilyStatus) change; `TtCheckpointManifest` / `TtCheckpointChunk` do not.
The change is FULL-compatible (checked read-only with `POST /compatibility/subjects/<s>/versions/latest` on the dev registry 192.168.100.102:8082 and the production registry 192.168.100.252:8082: `is_compatible: true`, subject config FULL, no 42201).

* **A new schema VERSION of an existing subject gets its OWN fixed id.** A registry id names exactly one schema text, so version 2 cannot reuse the version-1 id. Version 2 of each subject is `version-1 id + 20`:
  2321-2332 (PfChartState 2321 ... ClimaxEvent 2332) and 2335 (TtFamilyStatus). 2316-2340 were free on both registries. Version 1 stays registered under 2301-2312 / 2315 and is never deleted (records already on the topics are framed with those ids).
* **What `ensure-fixed-id-schemas.sh` does now (changed by this PR).** Before this PR it admitted exactly one version per subject and would have stopped on the first of the thirteen ("holds versions [1] ... collision"). Map rows take an optional
  fourth column, the registry version (default 1); a subject's rows must number 1, 2, ... in order, each with its own id. For a version N > 1 row the script: (1) accepts an already-canonical schema at (id, version N) when the subject holds exactly versions 1..K with N <= K <= the highest
  version the map declares; (2) otherwise requires the subject to hold exactly versions 1..N-1 (or, on a registry that lacks them, the map's earlier rows to import them) and the id to be free; (3) asks the registry, read-only, whether the schema is compatible with the latest version
  (`POST /compatibility/subjects/<s>/versions/latest`, under the subject's FULL); (4) imports it in subject-level IMPORT mode with `id` and `version` N and restores the mode; (5) reads back version N with the fixed id and sets/verifies FULL. The read-only PREFLIGHT pass judges every row before the first import, so one refusal changes nothing; for a subject that already holds version 1 (dev, production) that includes the registry's compatibility verdict, and the subject must be at compatibility FULL already (the verdict is given under the subject's own level). On a registry that never saw the subject, version 1 is imported first and the version-2 verdict is asked right after it. A stray version or a different schema at a declared version is a collision and fails; nothing is repaired or deleted.
* **Files.** `schema-id-maps/es-trade-tools.map` lists 15 version-1 rows (13 point at the frozen `v1/<Record>.avsc`, the two checkpoint records at the flat file) and then the 13 version-2 rows (flat `<Record>.avsc`, the contracts checkout's `schemas-registry/ta`).
  Both directories come from options-edge-contracts at `CONTRACTS_PERMITTED_SHA` (>= the 0.33.0 merge).
* **Procedure, per environment (dev registry first, then production), registry and Jenkins parameters only; nothing else is needed for registration:**
  1. `service-deploy SERVICE=es-trade-tools REGISTER_TA_SCHEMAS=true DEPLOY_DRY_RUN=true CONTRACTS_PERMITTED_SHA=<contracts 0.33.0 merge sha> BUILD_IMAGES=false`: expect, per environment, `registered: version 1` for the 13 old rows and `free: would import as version 2 under id 23xx` + `compatible with the latest version` for the 13 new ones, `OK`.
  2. The same run with `DEPLOY_DRY_RUN=false`: imports ONLY the 13 version-2 rows (dev and production already hold version 1 under 2301-2315); re-running is a no-op that re-asserts FULL.
  3. Verify: `GET /subjects/com.optionsedge.contracts.tradetools.FlowBar/versions` = `[1,2]`; `GET /subjects/.../versions/2` has id 2331 and a schema equal to contracts `schemas-registry/ta/FlowBar.avsc`; `GET /subjects/.../versions/1` has id 2311.
  4. Only then roll the new es-trade-tools image: its start-up `getId(subject, embedded schema)` resolves each embedded (version-2) schema to its new id and refuses to start if one is missing.
* **Order against the other pieces.** Registry first (step 1-3), then the producer image, then the gateway and web images (they read the writer schema by id from the registry; the old and the new gateway/web both decode either version, because FULL).
* A registry that never saw the family (a rebuilt or new registry) takes both versions from the same map in one run; the ids are then identical to dev and production.

## 3b. ELLIOTT-FIB-CYCLE-TOOLS revision 21: registering version 2 of WaveSet and CycleSet (contracts 0.36.0)

Producer `wave-cycle-service` (processing #1283, 063897ee) publishes the measured validation result: `WaveCount.measurement`, `WaveSet.calibration`, `CycleSet.calibration` (all optional, default null, every vocabulary a string; `options-edge-processing docs/ta-books/catalog/ELLIOTT-FIB-CYCLE-TOOLS-SCHEMA-CHANGES.md`).
`WaveSet` and `CycleSet` change; `TimeframeCheckpoint`, `SeriesCheckpoint`, `ControlCheckpoint` do not. FULL-compatible (read-only `POST /compatibility/subjects/<s>/versions/latest` on the dev registry 192.168.100.102:8082 and the production registry 192.168.100.252:8082: `is_compatible: true`; each subject holds version [1], config FULL). Ids 2406 and 2407 are free (404) on both.

* **Ids.** Version 2 has its own fixed id (a registry id names one schema text): `WaveSet` 2406, `CycleSet` 2407 (version 1 stays under 2401 and 2402, never deleted). The offset is +5, not the +20 of section 3a.
* **Files and map.** `schema-id-maps/wave-cycle.map` lists 5 version-1 rows (`WaveSet` and `CycleSet` point at the frozen `v1/<Record>.avsc` of the contracts checkout, the three checkpoint records at the flat files), then the two version-2 rows (flat `WaveSet.avsc`, `CycleSet.avsc`).
  `ensure-fixed-id-schemas.sh` is unchanged by this section (it learned later versions in section 3a); contracts must be at `CONTRACTS_PERMITTED_SHA` >= the 0.36.0 merge.
* **Procedure, per environment (dev first, then production), before the new wave-cycle image starts (the service resolves the id of its embedded schema at boot and refuses to start if it is missing):**
  1. `service-deploy SERVICE=wave-cycle REGISTER_TA_SCHEMAS=true DEPLOY_DRY_RUN=true CONTRACTS_PERMITTED_SHA=<contracts 0.36.0 merge sha> BUILD_IMAGES=false`: expect `registered: version 1` for the five version-1 rows and, for WaveSet and CycleSet, `compatible with the latest version` + `would import as version 2 under id 2406` / `2407`, then `OK`.
  2. The same run with `DEPLOY_DRY_RUN=false`: imports ONLY the two version-2 rows; a re-run is a no-op that re-asserts FULL.
  3. Verify: `GET /subjects/com.optionsedge.contracts.wavecycle.WaveSet/versions` = `[1,2]`; version 2 has id 2406 and the schema of contracts `schemas-registry/ta/WaveSet.avsc` (SHA-256 `167d6adf36c95d1a58342c36b09fbd0297161fd9ac23acde1d67f12efb0aff33`); `CycleSet` version 2 is id 2407 (`00fd1f9fb68b9aca1ee0e7622ea5e26184d490d2df5377c84562721e74e9ef7b`).
* **Order.** Registry, then the producer image, then the gateway and web images (both decode either version; FULL). Producers already running version 1 are unaffected.
* **Release configuration of the new image** (the producer's document, section 6, for the deploy owner): the registered default parameter set (no tuning variable: another hash makes `calibration` null and the counts `NOT_CALIBRATED`), `WAVE_CYCLE_ENABLED=true`, `WAVE_CYCLE_SEASONALITY_ENABLED=true`, and the `WC_DAILY_SNAPSHOT_*` variables of the release snapshot. This change does not touch the manifests or flags.

## 3. W0, in the runbook's order (processing `MARKET-STRUCTURE-AVRO-CUTOVER.md` section 6)

Always dev first, production after; each step after the 16:00 ET close on a trading day. Production images are built with `ENVIRONMENT=production BUILD_IMAGES=true`
(`environment-dev-builds-only-the-dev-image-tag`: a production roll after a dev-only build silently redeploys the OLD `:prod` image) and the digest rolled is compared with `W0_EXPECT_IMAGE_DIGEST`.

1. **Contracts and registry.** Contracts 0.31.0 merged. `service-deploy SERVICE=market-structure REGISTER_TA_SCHEMAS=true CONTRACTS_PERMITTED_SHA=<sha>` (dry run first) registers the 11
   market-structure subjects (W0 2201-2208, tools 2211, geometry 2221-2222) under fixed ids, IMPORT mode, FULL, on that environment's registry; a collision fails and is never repaired.
   Run it BEFORE any W0 image is built, with `BUILD_IMAGES=false`: the run re-applies the manifest, which adds the two `false` flag env vars and so restarts the OLD image once, harmlessly.
2. **Processing image.** W0 merged first (CHART-STRUCTURE-TOOLS and CLASSICAL-CHART-PATTERNS are stacked on it); `service-deploy SERVICE=market-structure BUILD_IMAGES=true MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER=true`. Verify
   `market_structure_topics_verified 1`, `market_structure_recovery_ignored 1`, restarts 0, the day re-emitted as Avro.
3. **Gateway image** that decodes Avro (`feed-gateway-deploy` build, then `service-deploy SERVICE=feed-gateway`); the four new gateway flags stay `false`. Applying feed-gateway rolls the gateway pod.
4. **Wipe and recreate**, a run of its own: `service-deploy SERVICE=market-structure BUILD_IMAGES=false W0_CUTOVER_WIPE=true W0_EXPECT_IMAGE_DIGEST=sha256:... W0_EXPECT_GATEWAY_DIGEST=sha256:... W0_PRE_CUTOVER_IDS='ui=..,sequence=..,dlq=..,patterns=..,recovery=..' W0_PRODUCERS_ATTESTED=true`
   (ids read off the LIVE broker with `kafka-topics --describe` immediately before). The run's Deploy stage first re-applies the manifest WITHOUT the rollover flag, which restarts the pod
   (expected); then `roll-forward-market-structure.sh --recreate` runs behind gates G1-G9 (bootstrap is the named cluster; after the close; service `SESSION_ENDED`; the W0 digest is the only market-structure image on
   the cluster and one pod is Running; the Avro feed-gateway digest `W0_EXPECT_GATEWAY_DIGEST` is the only gateway image on the cluster (G9); the owner's maintenance-stop attestation; broker-side producer evidence; a 3-minute proof that the input and the five topics and the five producer counters did not move; every topic carries its
   pre-cutover id). If G3 refuses right after the roll, re-run the same job (nothing changes, so nothing restarts). A part-way failure is resumed with `--already-recreated name=NEWID,...`. `DEPLOY_DRY_RUN=true` passes `--gates-only`: every gate and the quiescence proof run and report, nothing is deleted. All W0 arguments are validated before the rollout stage.
   The input topic, the tools topic and the geometry topic are never named by the script.
5. **Restart once more**: `service-deploy SERVICE=market-structure FORCE_RESTART=true BUILD_IMAGES=false` (no rollover parameter), and the gateway if its consumers hold positions past the new log ends (the script lists them).
6. **Verify**: `roll-forward-market-structure.sh --verify` (ids, shape, first and last record of every non-empty partition is Avro), then the runbook section 7 **full-decode verifier** and the gateway frame proof. CHART-STRUCTURE-TOOLS and CLASSICAL-CHART-PATTERNS may deploy only after this passes.

Roll forward only; there is no rollback and no reader that accepts JSON.

## 4. CHART-STRUCTURE-TOOLS tools, then CLASSICAL-CHART-PATTERNS geometry (each, after step 6)

Order for EACH layer: subjects already registered (step 1) -> topic ensured (every market-structure service-deploy run does it) -> gateway image with the relay live (flag `false`) ->
`GATEWAY_STRUCTURE_TOOLS_ENABLED` / `GATEWAY_GEOMETRY_ENABLED` true (the gateway waits for records, hello has the field) -> `MARKET_STRUCTURE_TOOLS_ENABLED` / `MARKET_STRUCTURE_GEOMETRY_ENABLED` true
(a restart: the flag is sampled once per schedule and recorded in the checkpoint). Processing notes B/C once said the flag-enabling change should bump `RecoveryCheckpoint.METHOD`; in this order it does not: the stack is merged W0, then B, then C, W0 took `ms-runtime-5`, CHART-STRUCTURE-TOOLS's branch-local `ms-runtime-4` is dropped when B is rebased onto W0 (B publishes the tools state under the stack's METHOD, `-4` stays unused, one bump per release) and CLASSICAL-CHART-PATTERNS as built does not bump it. If a later image does bump it, roll it with `MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER=true` exactly as in step 2. The web layer has no flag: the hello field is its gate. Flip one layer at a time, outside the session.

## 5. TICK-TRADE-TOOLS `es-trade-tools`, ELLIOTT-FIB-CYCLE-TOOLS `wave-cycle`

Registering the service in the estate (this PR, then): the processing repository must also carry the image build (its root `Jenkinsfile` BUILD_TARGET map and `build_image` call and
`EXPECTED_PROCESSING_IMAGES`; the two modules have a Dockerfile and nothing else), otherwise `BUILD_IMAGES=true` fails with `Unknown BUILD_TARGET`. Then, per environment, dev first:

1. `service-deploy SERVICE=<es-trade-tools|wave-cycle> BUILD_IMAGES=true REGISTER_TA_SCHEMAS=true CONTRACTS_PERMITTED_SHA=<sha>`: subjects registered, topics ensured by the barrier, image built and digest-pinned, the Deployment applied at **0 replicas**. Nothing runs.
2. To enable (the owner; the manifest comment lists the same six edits): the precondition is met (D: the linearizer publishes `underlying.es.trades.linearized` on that environment, and production waits for the calibration job; E: bars are Avro) ->
   set `replicas: 1` in a per-environment enable patch (`k8s/overlays/<env>/<service>-<env>-enable-patch.yaml`, registered in that overlay's kustomization; never in the shared base, which would enable dev and production together) -> regenerate slices -> remove the service from the three lifecycle lists -> `service-deploy`.
   D: also `TT_ENABLED=true` and ONE family flag at a time (`TT_PF_ENABLED`, `TT_VP_ENABLED`, `TT_FLOW_ENABLED`, then `TT_CLIMAX_ENABLED`, which needs FLOW), then `GATEWAY_TRADE_TOOLS_ENABLED`.
   E: first the **dark run** (replicas 1, both flags `false`; the fold runs and is checkpointed; `wave_cycle_*` metrics), then `WAVE_CYCLE_ENABLED` and `GATEWAY_WAVE_CYCLE_ENABLED`; `WAVE_CYCLE_IGNORE_CHECKPOINT` is never committed true.

## 5a. PRODUCTION runs both services live (2026-10-22)

Owner standard: every service is calibrated on historical data first and then runs live, never in shadow. The calibrated releases (processing #1281, #1283; contracts 0.36.0 `4ddf5a36`; version-2 subjects registered on production 2026-10-22: trade tools 2321-2332 and 2335, WaveSet 2406, CycleSet 2407) are enabled in PRODUCTION ONLY by
`k8s/overlays/production/es-trade-tools-prod-enable-patch.yaml` and `wave-cycle-prod-enable-patch.yaml` (replicas 1; D: `TT_ENABLED` and the four family flags; E: `WAVE_CYCLE_ENABLED`, `WAVE_CYCLE_SEASONALITY_ENABLED` and the hash-pinned daily snapshot variables), registered in the production overlay and mirrored in the regenerated slices. Both services are removed from
`KEEP_DOWN` in `morning-autostart.sh` and `oe-boot-bringup.sh` (production-only lists) and stay in `DISABLED_DEV` (dev is handled separately). The shared bases remain replicas 0 / flags `false`. The gateway relay flags `GATEWAY_TRADE_TOOLS_ENABLED` and `GATEWAY_WAVE_CYCLE_ENABLED` are NOT part of this change: they follow once a gateway and web that understand the version-2 records are live on production.
Order: registry (done) -> `service-deploy SERVICE=es-trade-tools` (topic barrier creates the seven topics, the Deployment starts at replicas 1) -> `service-deploy SERVICE=wave-cycle` -> gateway -> web -> gateway relay flags. There is no earlier checkpoint on production for either service, so no state-topic reset is involved.

## 6. Not in this change

* The processing-side image build entries above, and the runbook section 7 full-decode verifier (a Java tool with the registry client; `--verify` here is a sample).
* The web and gateway flags' final values: all `false` until the owner enables a layer. The gateway relay and the web layer are in their own branches.
* Applying anything: no Jenkins job was run, no topic touched, no image built, nothing started.
