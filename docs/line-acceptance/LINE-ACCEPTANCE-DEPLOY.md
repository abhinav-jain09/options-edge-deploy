# line-acceptance-service: deploy wiring (VWAP-ACCEPTANCE-REJECTION)

Design: options-edge-documents `VWAP-ACCEPTANCE-REJECTION-DESIGN.md` sections 9.1 and 15. **Status since 2026-11-02: LIVE on dev, UNVALIDATED.** The owner explicitly overrode design K9 (no run before the backtest report) and switched the service on for dev, every output labelled UNVALIDATED, alerts OFF (a per-viewer opt-in on the page). It was first deployed dark (stage 0); production is untouched and not authorised.

## Deploy facts

| | |
|---|---|
| Registry entry | `services.yaml` `line-acceptance` (standalone, `envs: [dev]`, `sliceGenerated`) |
| Deployment / container | `line-acceptance-service` / `line-acceptance` |
| Image | `options-edge-line-acceptance`, tag `dev`; variable `LINE_ACCEPTANCE_IMAGE` (dev-only lists in `apply.sh`, `image-lock.sh`, `resolve-images.sh`, `verify-running-images.sh`; `image-tags/dev.yaml`) |
| Replicas / flag | `1` and `LINE_ACCEPTANCE_ENABLED "true"` in the base (the only environment that renders it is dev, so there is no enable patch) and therefore in the dev slice |
| Gates | `GATE_STRUCTURE_ENABLED`, `GATE_GEOMETRY_ENABLED`, `GATE_VWAP_ENABLED`, `GATE_FOOTPRINT_ENABLED`, `GATE_CANDLES_ENABLED` = `true` (enabled one at a time, in that order, each verified before the next); `GATE_PHASEF_EVIDENCE` = `false`; `GATE_BASIS_SERIES` default `ES_PRICE_AVRO` (Avro `underlying.es.price`); `GATE_VWAP_ANALYSIS` must stay unset (the service refuses to start with it true) |
| Env | `LINE_ACCEPTANCE_ENABLED`, `LINE_ACC_HEALTH_PORT=8172`, `ENVIRONMENT=dev`; input topic names are the service's own defaults |
| Health | port `8172` (`8171` is break-classifier), `/health/ready`, `/health/live` |
| Single replica | `Recreate`; `transactional.id = line-acceptance-<ENVIRONMENT>`; a second instance fences the first |
| Held down | **No longer.** Removed from `DISABLED_DEV` (`dev-cleanup.sh`) and from `KEEP_DOWN` of `morning-autostart.sh` and `oe-boot-bringup.sh` on 2026-11-02 (`tests/test_line_acceptance_deploy.py` pins the absence; `tests/test_keep_down_lists_agree.py`) |
| Topics | six, 1 partition each, in `scripts/kafka/topics.env`; barrier `scripts/kafka/ensure-line-acceptance-topics.sh` (dev only, fails closed) |
| Schemas | `scripts/kafka/schema-id-maps/line-acceptance.map`, ids 3101-3111 (contracts #149), `service-deploy SERVICE=line-acceptance REGISTER_TA_SCHEMAS=true CONTRACTS_PERMITTED_SHA=<sha>` |
| Production | none: no overlay, slice, image tag, gateway flag; the deploy job refuses `ENVIRONMENT=production` |

Topic shapes: `options.spx.line-acceptance` delete 7 d; `.monitor` compact; `.status` delete, `retention.ms=-1`, `retention.bytes=-1`, **not compacted** (RESET-PRESERVED: every `CalibrationStatus` version survives);
`.checkpoint` compact with a topic-level `max.message.bytes` 9437184; `.journal` delete 7 d with the same topic-level limit; `.dlq` delete 7 d. `topics.env` is shared with production, so a guard at its end strips every `options.spx.line-acceptance*` token from every `OPTIONS_EDGE_*` variable when `ENVIRONMENT` is the explicit string `production`: a production source declares none of the six topics and no production script creates, configures or expects them (dev and unset still declare them). The names stay in the static text of the RESET_PRESERVED / REBUILDABLE assignments (parsers and the validator need them there); that can only spare a topic.

`dev-cleanup.sh` recreates the six topics from `topics.env` but does **not** run the barrier (the clean never runs per-service barriers): run the deploy barrier before raising the service after a wipe.

## Rollback

One reviewed change, all of it together: in `k8s/services/line-acceptance/base/line-acceptance-deployment.yaml` set `LINE_ACCEPTANCE_ENABLED` back to `"false"` (health reads DISABLED, nothing is consumed or published) and, to stop the pod, replicas to `0`; put `line-acceptance-service` back into `DISABLED_DEV` and both `KEEP_DOWN` lists (replicas 0 alone is lifted by the next start / boot); run `scripts/deploy/generate-service-slices.sh`; update `tests/test_line_acceptance_deploy.py`; deploy. Immediate hand rollback on dev: `kubectl -n options-edge scale deploy/line-acceptance-service --replicas=0` (as `jenkins-deployer`). The six topics keep their records (status is retention -1 and RESET-PRESERVED).

## History: how stage 0 was meant to be left (superseded)

The original plan (design section 15 stage B) was to enable it only after the backtest report. The owner overrode that on 2026-11-02; the enablement ladder (structure, geometry, VWAP, footprint, candles) was still taken one gate at a time. Production needs its own separately reviewed design; nothing here authorises it.
