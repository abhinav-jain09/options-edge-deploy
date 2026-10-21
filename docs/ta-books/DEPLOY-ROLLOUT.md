# TA books: deploy wiring and rollout order (phases B, C, D, E and the W0 cutover)

Branch `feat/ta-geometry-deploy`. Nothing here is applied: every flag is the literal `"false"`, both new services are held at `replicas: 0`, and no
destructive step runs from a merge. This file is the order to follow when the owner turns each piece on. Producer, gateway and web are other repositories
(processing `feat/ta-phase-{w0,b,c,d,e}`, gateway `feat/ta-geometry-feed-gateway`, web `feat/ta-*-web`, contracts 0.31.0).

## 1. What this change adds

| piece | where |
|---|---|
| Phase B `options.spx.market-structure.tools` and Phase C `options.spx.market-structure.geometry` (Avro, 1 partition, delete, 172 800 000 ms, EXACT partitions, key `<scheduleId>\|<tf>`) | `scripts/kafka/topics.env`, `ensure-market-structure-topics.sh` (+ test) |
| `MARKET_STRUCTURE_TOOLS_ENABLED`, `MARKET_STRUCTURE_GEOMETRY_ENABLED` = `false` | market-structure base deployment, two generated slices |
| `GATEWAY_STRUCTURE_TOOLS_ENABLED`, `GATEWAY_GEOMETRY_ENABLED`, `GATEWAY_TRADE_TOOLS_ENABLED`, `GATEWAY_WAVE_CYCLE_ENABLED` = `false` (names are `GatewaySettings`'; its topic variables default to the declared topics) | feed-gateway dev and production env patches, two generated slices |
| Fixed-id schema registration for 31 subjects (ids 2201-2405, `ta-books-catalog/08_SCHEMA_ID_MAP.md`) | `scripts/kafka/ensure-fixed-id-schemas.sh`, `scripts/kafka/schema-id-maps/{market-structure,es-trade-tools,wave-cycle}.map`, service-deploy parameter `REGISTER_TA_SCHEMAS` |
| W0 rollover flag injection (never committed) | `scripts/deploy/service-deploy.sh` block `MS_IGNORE_RECOVERY_ROLLOVER`, parameter `MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER` |
| W0 step 4 and 6 (wipe and recreate the five topics under their own names; verify) | `scripts/kafka/roll-forward-market-structure.sh`, service-deploy parameters `W0_CUTOVER_WIPE`, `W0_EXPECT_IMAGE_DIGEST`, `W0_EXPECT_GATEWAY_DIGEST`, `W0_PRE_CUTOVER_IDS`, `W0_PRODUCERS_ATTESTED` |
| `es-trade-tools-service` (Phase D) | `k8s/services/es-trade-tools`, seven topics, `ensure-es-trade-tools-topics.sh`, `TT_*` flags `false` |
| `wave-cycle-service` (Phase E) | `k8s/services/wave-cycle`, three topics, `ensure-wave-cycle-topics.sh`, `WAVE_CYCLE_*` flags `false` |

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

## 3. W0, in the runbook's order (processing `W0-MARKET-STRUCTURE-AVRO-CUTOVER.md` section 6)

Always dev first, production after; each step after the 16:00 ET close on a trading day. Production images are built with `ENVIRONMENT=production BUILD_IMAGES=true`
(`environment-dev-builds-only-the-dev-image-tag`: a production roll after a dev-only build silently redeploys the OLD `:prod` image) and the digest rolled is compared with `W0_EXPECT_IMAGE_DIGEST`.

1. **Contracts and registry.** Contracts 0.31.0 merged. `service-deploy SERVICE=market-structure REGISTER_TA_SCHEMAS=true CONTRACTS_PERMITTED_SHA=<sha>` (dry run first) registers the 11
   market-structure subjects (W0 2201-2208, tools 2211, geometry 2221-2222) under fixed ids, IMPORT mode, FULL, on that environment's registry; a collision fails and is never repaired.
   Run it BEFORE any W0 image is built, with `BUILD_IMAGES=false`: the run re-applies the manifest, which adds the two `false` flag env vars and so restarts the OLD image once, harmlessly.
2. **Processing image.** W0 merged first (B and C are stacked on it); `service-deploy SERVICE=market-structure BUILD_IMAGES=true MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER=true`. Verify
   `market_structure_topics_verified 1`, `market_structure_recovery_ignored 1`, restarts 0, the day re-emitted as Avro.
3. **Gateway image** that decodes Avro (`feed-gateway-deploy` build, then `service-deploy SERVICE=feed-gateway`); the four new gateway flags stay `false`. Applying feed-gateway rolls the gateway pod.
4. **Wipe and recreate**, a run of its own: `service-deploy SERVICE=market-structure BUILD_IMAGES=false W0_CUTOVER_WIPE=true W0_EXPECT_IMAGE_DIGEST=sha256:... W0_EXPECT_GATEWAY_DIGEST=sha256:... W0_PRE_CUTOVER_IDS='ui=..,sequence=..,dlq=..,patterns=..,recovery=..' W0_PRODUCERS_ATTESTED=true`
   (ids read off the LIVE broker with `kafka-topics --describe` immediately before). The run's Deploy stage first re-applies the manifest WITHOUT the rollover flag, which restarts the pod
   (expected); then `roll-forward-market-structure.sh --recreate` runs behind gates G1-G9 (bootstrap is the named cluster; after the close; service `SESSION_ENDED`; the W0 digest is the only market-structure image on
   the cluster and one pod is Running; the Avro feed-gateway digest `W0_EXPECT_GATEWAY_DIGEST` is the only gateway image on the cluster (G9); the owner's maintenance-stop attestation; broker-side producer evidence; a 3-minute proof that the input and the five topics and the five producer counters did not move; every topic carries its
   pre-cutover id). If G3 refuses right after the roll, re-run the same job (nothing changes, so nothing restarts). A part-way failure is resumed with `--already-recreated name=NEWID,...`. `DEPLOY_DRY_RUN=true` passes `--gates-only`: every gate and the quiescence proof run and report, nothing is deleted. All W0 arguments are validated before the rollout stage.
   The input topic, the tools topic and the geometry topic are never named by the script.
5. **Restart once more**: `service-deploy SERVICE=market-structure FORCE_RESTART=true BUILD_IMAGES=false` (no rollover parameter), and the gateway if its consumers hold positions past the new log ends (the script lists them).
6. **Verify**: `roll-forward-market-structure.sh --verify` (ids, shape, first and last record of every non-empty partition is Avro), then the runbook section 7 **full-decode verifier** and the gateway frame proof. Phases B and C may deploy only after this passes.

Roll forward only; there is no rollback and no reader that accepts JSON.

## 4. Phase B tools, then Phase C geometry (each, after step 6)

Order for EACH layer: subjects already registered (step 1) -> topic ensured (every market-structure service-deploy run does it) -> gateway image with the relay live (flag `false`) ->
`GATEWAY_STRUCTURE_TOOLS_ENABLED` / `GATEWAY_GEOMETRY_ENABLED` true (the gateway waits for records, hello has the field) -> `MARKET_STRUCTURE_TOOLS_ENABLED` / `MARKET_STRUCTURE_GEOMETRY_ENABLED` true
(a restart: the flag is sampled once per schedule and recorded in the checkpoint). Processing notes B/C once said the flag-enabling change should bump `RecoveryCheckpoint.METHOD`; in this order it does not: the stack is merged W0, then B, then C, W0 took `ms-runtime-5`, Phase B's branch-local `ms-runtime-4` is dropped when B is rebased onto W0 (B publishes the tools state under the stack's METHOD, `-4` stays unused, one bump per release) and Phase C as built does not bump it. If a later image does bump it, roll it with `MARKET_STRUCTURE_IGNORE_RECOVERY_ROLLOVER=true` exactly as in step 2. The web layer has no flag: the hello field is its gate. Flip one layer at a time, outside the session.

## 5. Phase D `es-trade-tools`, Phase E `wave-cycle`

Registering the service in the estate (this PR, then): the processing repository must also carry the image build (its root `Jenkinsfile` BUILD_TARGET map and `build_image` call and
`EXPECTED_PROCESSING_IMAGES`; the two modules have a Dockerfile and nothing else), otherwise `BUILD_IMAGES=true` fails with `Unknown BUILD_TARGET`. Then, per environment, dev first:

1. `service-deploy SERVICE=<es-trade-tools|wave-cycle> BUILD_IMAGES=true REGISTER_TA_SCHEMAS=true CONTRACTS_PERMITTED_SHA=<sha>`: subjects registered, topics ensured by the barrier, image built and digest-pinned, the Deployment applied at **0 replicas**. Nothing runs.
2. To enable (the owner; the manifest comment lists the same six edits): the precondition is met (D: the linearizer publishes `underlying.es.trades.linearized` on that environment, and production waits for the calibration job; E: bars are Avro) ->
   set `replicas: 1` in a per-environment enable patch (`k8s/overlays/<env>/<service>-<env>-enable-patch.yaml`, registered in that overlay's kustomization; never in the shared base, which would enable dev and production together) -> regenerate slices -> remove the service from the three lifecycle lists -> `service-deploy`.
   D: also `TT_ENABLED=true` and ONE family flag at a time (`TT_PF_ENABLED`, `TT_VP_ENABLED`, `TT_FLOW_ENABLED`, then `TT_CLIMAX_ENABLED`, which needs FLOW), then `GATEWAY_TRADE_TOOLS_ENABLED`.
   E: first the **dark run** (replicas 1, both flags `false`; the fold runs and is checkpointed; `wave_cycle_*` metrics), then `WAVE_CYCLE_ENABLED` and `GATEWAY_WAVE_CYCLE_ENABLED`; `WAVE_CYCLE_IGNORE_CHECKPOINT` is never committed true.

## 6. Not in this change

* The processing-side image build entries above, and the runbook section 7 full-decode verifier (a Java tool with the registry client; `--verify` here is a sample).
* The web and gateway flags' final values: all `false` until the owner enables a layer. The gateway relay and the web layer are in their own branches.
* Applying anything: no Jenkins job was run, no topic touched, no image built, nothing started.
