# Standalone Service Deployment — operations

Design: `options-edge-documents/design/STANDALONE-SERVICE-DEPLOYMENT-DESIGN.md`
(Codex-approved Gate-1). This doc covers what landed in **Phase 1 (common infra) +
Phase 2 (web pilot)** and how to operate it.

> **Hands-on runbook:** `docs/deploy-single-service.md` — deploy one service step by
> step, rollback, Jenkins job setup, and onboarding the next service.

## The idea

Infra is COMMON (one shared layer, applied rarely). Each service is deployable as a
SINGLE STANDALONE UNIT — a routine web change ships in minutes without rebuilding the
other ~28 images or risking their preflight/PVC/topic failure modes.

```
k8s/
  infra/                      # COMMON: namespace, shared ConfigMaps, ALL streams-state
    base/                     #   PVCs, ops CronJobs; prod overlay adds Keycloak
    overlays/{dev,production,experiment}     # per-env configmap values + the env's
                                             # immutable PVC storageClassName
  base/                       # app Deployments/Services still deployed by the monolith
  services/
    web/                      # PILOT service slice: Deployment + Service ONLY
      base/
      overlays/{dev,production,experiment}   # standalone-only; mirror the monolith's
                                             # env patches (drift = CI failure)
services.yaml                 # SERVICE REGISTRY — single source of truth
```

## Jenkins jobs

| Job (Jenkinsfile) | What | When |
| --- | --- | --- |
| `common-infra-deploy` (`Jenkinsfile.common-infra`) | Applies `k8s/infra/overlays/<env>`: shared ConfigMaps, creates missing PVCs (create-only), ops CronJobs, prod Keycloak. DRY-RUN BY DEFAULT; production applies need a manual approval; PVC immutable-class mismatch fails before any mutation. | Rarely — only when infra changes (new PVC, new config key, new topic-era change). |
| `web-service-deploy` (`Jenkinsfile.web-service`) | The FAST PATH: builds the web image natively on the target machine (dev = Mac arm64, prod = .252 amd64 via ssh — same contract as bring-up-all), digest-pins it, applies ONLY the web Deployment+Service, rollout + health gate, prints the rollback command. | Every routine web change. |
| `options-edge-deploy` (monolith) | Unchanged behaviour for everything else. Renders infra + web via the same files, so nothing drifts. | Full deploys / all other services until they migrate. |

## Guarantees (from the design's §13, enforced in code)

* **Blast radius** — a per-service deploy renders ONLY Deployment/Service(/HPA/
  ServiceMonitor/Ingress). PVCs/ConfigMaps/Secrets/CronJobs in a service slice fail the
  deploy (`service-deploy.sh`) and CI (`validate-services.sh`).
* **Immutable images** — the applied Deployment is digest-pinned (`@sha256`); a mutable
  tag never reaches the API (same `pin-image.sh` fail-closed resolver the monolith uses).
* **Rollback without rebuild** — the job records the previously-running digest before
  rollout and prints `kubectl set image … <previous digest>` + `rollout undo`.
* **Health gate** — after `rollout status`: running imageID must match the pinned
  digest, restartCount must be 0, and (dev/prod) the web endpoint must answer 200.
* **PVC storageClassName per env** — infra base is class-NEUTRAL; dev pins `standard`,
  production/experiment pin `local-path` (matching the live immutable classes). This
  removes the "spec: Forbidden: spec is immutable" failure that broke prod deploys
  (#490/#502). PVCs are create-only: never re-applied, never patched.
* **Registry completeness** — `services.yaml` must register every Deployment the
  overlays render; a new unregistered service fails `tests/test_service_registry.py`
  (the gap that let delta-flow/maxpain ship without a prod image).
* **Mirror rule** — the standalone web overlays must render EXACTLY what the monolithic
  overlays render for web; `validate-services.sh` diffs them per env.

## Operational notes

* `common-infra-deploy` re-applies the shared ConfigMaps from git. Keys that the
  monolithic deploy patches at RUNTIME (`IB_EXPIRY`, `KAFKA_RAW_TOPIC`,
  `DATABENTO_EXPIRY`, `UNUSUAL_WHALES_EXPIRY`, bootstrap/schema-registry URLs) will
  reset to their static manifest values — the dry-run diff shows exactly this before
  you approve. Off-hours this is harmless (the next monolithic deploy re-patches);
  during market hours prefer deferring infra applies.
* Kafka topic create/reconcile intentionally stays with the existing pipeline stages;
  the declarative topic-reconcile step is the next phase of the design.
* Deploy locking is `disableConcurrentBuilds()` per job (plugin-free). The
  common-infra job should not be run while a monolithic deploy is mid-flight.

## Migration state

| Service | managedBy |
| --- | --- |
| web | **standalone** (pilot) |
| everything else | monolith (migrate in batches per design §9 — gateway next, then one low-risk processing service) |

## Creating the Jenkins jobs (one-time, post-merge)

Two new pipeline jobs point at the repo's Jenkinsfiles (same SCM config as
`options-edge-deploy`):

* `common-infra-deploy` → Script Path `Jenkinsfile.common-infra`
* `web-service-deploy` → Script Path `Jenkinsfile.web-service`
