# How to deploy a SINGLE service (and how the Jenkins jobs were set up)

Runbook for the standalone-service-deployment fast path. Architecture background:
`docs/standalone-service-deployment.md` (this repo) and the design doc
`options-edge-documents/design/STANDALONE-SERVICE-DEPLOYMENT-DESIGN.md`.

**The idea in one line:** infra is common and applied rarely; a single service ships
on its own in minutes — no all-services build, no shared failure modes.

---

## 1. Deploy one service (web — the pilot)

### Jenkins (the normal way)

1. Open **`web-service-deploy`** → *Build with Parameters*.
2. Parameters:

   | Param | Value | Notes |
   | --- | --- | --- |
   | `ENVIRONMENT` | `dev` \| `production` \| `experiment` | picks the overlay + deployer kubeconfig (oeProfile) |
   | `BUILD_IMAGE` | `true` (default) | builds the image first, natively on the target machine. `false` = deploy whatever `:dev` tag is already in that env's registry |
   | `DEPLOY_DRY_RUN` | `false` | `true` renders + digest-pins + server-side validates, changes nothing |
   | `HEALTH_URL` | empty | empty = per-env default (dev `http://localhost:8090/`, prod `http://192.168.100.252:8094/`) |

3. Build. The stages:
   - **Validate service registry** — `scripts/ci/validate-services.sh`: completeness,
     blast radius, mirror rule. Fails fast if the service slice drifted.
   - **Build web image (native)** — triggers `options-edge-web-deploy` with the same
     per-env contract `bring-up-all` uses (dev builds on the Mac arm64; prod builds on
     `.252` amd64 over ssh — images are always the right arch because they are built ON
     the target machine).
   - **Deploy (service-scoped)** — `scripts/deploy/service-deploy.sh`:
     renders `k8s/services/web/overlays/<env>` → **blast-radius guard** (only
     Deployment/Service/HPA/ServiceMonitor/Ingress may render; PVC/ConfigMap/Secret =
     FATAL) → digest-pins the image (`pin-image.sh`, fail-closed) → records the
     **previously-running image** → `kubectl apply` of ONLY this service's docs →
     `rollout status` → **health gate** (running imageID must match the pinned digest,
     restartCount 0, HEALTH_URL answers 200).

4. The log always ends with the **rollback command** (see §3).

### CLI (what the job runs — for debugging)

```bash
# from the repo root, with the env's DEPLOYER kubeconfig exported
export KUBECONFIG=~/.kube/<env-deployer>.yaml
SERVICE=web ENVIRONMENT=dev DEPLOY_DRY_RUN=true  bash scripts/deploy/service-deploy.sh   # rehearse
SERVICE=web ENVIRONMENT=dev DEPLOY_DRY_RUN=false bash scripts/deploy/service-deploy.sh   # real
```

Notes:
- The cluster enforces a jenkins-only admission policy — a real apply needs the
  jenkins-deployer identity (the Jenkins job has it; your admin kubeconfig will be denied).
- Reducing anything is not this path's job: the deploy only rolls the workload. Infra
  (topics, PVCs, shared config) belongs to `common-infra-deploy`.

---

## 2. Common infra (when you actually changed infra)

Run **`common-infra-deploy`** only when `k8s/infra/**` changed (new PVC, config key,
Keycloak, ops CronJob):

1. First run with `DEPLOY_DRY_RUN=true` (the default) — the log shows a full
   `kubectl diff` plan and validates server-side. **PVCs are create-only**: an existing
   claim is never patched; a storageClassName mismatch vs the live immutable class
   fails BEFORE any mutation.
2. Re-run with `DEPLOY_DRY_RUN=false`. **Production additionally stops at a manual
   approval gate** — review the diff from stage 1 before clicking *Apply to production*.

⚠️ The shared ConfigMaps re-apply from git: runtime-patched keys (`IB_EXPIRY`,
`DATABENTO_EXPIRY`, `KAFKA_RAW_TOPIC`, `UNUSUAL_WHALES_EXPIRY`) reset to manifest
values until the next monolithic deploy re-patches them. Prefer off-hours.

---

## 3. Rollback (never rebuilds)

Every `web-service-deploy` log prints, before and after the rollout:

```
previous image (rollback target): host.docker.internal:5001/options-edge-web:dev@sha256:<old>
=== rollback (no rebuild needed — re-points to the recorded digest) ===
  kubectl -n options-edge set image deployment/options-edge-web web=<previous digest ref>
  # or: kubectl -n options-edge rollout undo deployment/options-edge-web
```

Run either command with the env's deployer identity. Digests are immutable, so the
rollback target is exactly the binary that was running before — no rebuild, ever.

---

## 4. How the Jenkins jobs were set up

Both jobs are plain **Pipeline (from SCM)** jobs pointing at THIS repo, branch
`*/main` — the pipeline code reviews/merges like any other change:

| Job name | Script Path | Purpose |
| --- | --- | --- |
| `web-service-deploy` | `Jenkinsfile.web-service` | standalone web deploy (build native + deploy one workload) |
| `common-infra-deploy` | `Jenkinsfile.common-infra` | the shared infra layer, dry-run-first, prod approval |

Created via the Jenkins REST API by cloning the SCM block of the existing
`options-edge-deploy` job (same repo URL/credential) and swapping the script path:

```bash
# 1) take an existing job's config as the template (carries the git URL + credential)
curl -su admin:$TOKEN http://localhost:8085/job/options-edge-deploy/config.xml -o tpl.xml
# 2) keep only the <definition> (CpsScmFlowDefinition) block; set:
#      <scriptPath>Jenkinsfile.web-service</scriptPath>   and   <name>*/main</name>
#    wrap it in a minimal <flow-definition plugin="workflow-job"> document
# 3) create the job (needs a crumb):
curl -su admin:$TOKEN -H "$CRUMB" -H 'Content-Type: application/xml' \
  --data-binary @job.xml -X POST 'http://localhost:8085/createItem?name=web-service-deploy'
```

Gotchas learned the hard way (already handled in the Jenkinsfiles, listed so nobody
re-discovers them):
- A brand-new pipeline job has **no parameters until its first run** parses the
  Jenkinsfile — `buildWithParameters` returns 400; trigger a plain `/build` once (the
  defaults are safe: dev + dry-run-first where applicable).
- Jenkins **drops `environment {}` entries whose value is the empty string** — the sh
  steps default every such variable (`${VAR:-}`) under `set -u`.
- Job parameters are never Groovy-interpolated into `sh` script text (injection risk);
  they reach the shell as real environment variables.
- Concurrency: both jobs use `disableConcurrentBuilds()` (plugin-free locking).

---

## 5. Onboarding the NEXT service to the fast path

Per the design, migrate in small batches (gateway next, then one low-risk processing
service). For a service `<svc>`:

1. **Manifests** — move its Deployment (+Service) from `k8s/base/` to
   `k8s/services/<svc>/base/`; add `k8s/services/<svc>/overlays/{dev,production,experiment}`
   that MIRROR every top-level patch the monolithic overlays apply to it (the mirror
   rule); make the monolithic overlays include `../../services/<svc>/base` so their
   render is unchanged.
2. **Registry** — flip its `services.yaml` entry to `managedBy: standalone` and fill
   `container`, `buildJob`, `envs`. `scripts/ci/validate-services.sh` (and
   `tests/test_service_registry.py`) then enforce slice existence, blast radius, image
   name, and render equivalence — run it locally before pushing.
3. **Jenkinsfile** — copy `Jenkinsfile.web-service`, swap the service name, the build
   job trigger, and the HEALTH_URL defaults. `scripts/deploy/service-deploy.sh` is
   generic — no changes needed.
4. **Job** — create `<svc>-service-deploy` per §4.
5. **Prove it** — `DEPLOY_DRY_RUN=true` on dev must end with the workload docs
   `unchanged (server dry run)` against the live cluster (the strongest mirror check),
   then a real dev deploy, then production.
