# OptionsEdge — Jenkins Jobs & dev/prod Image Guide

> **The one rule:** **dev runs on the Mac (arm64), prod runs on Linux (amd64) — so the Docker image must be built for a different architecture per environment.** An arm64 image built on the Mac will **not** run on the prod Linux nodes (`ImagePullBackOff` / `exec format error`), and an amd64 image won't run on docker-desktop. Always build with the matching `BUILD_PLATFORM`.

---

## Where Jenkins runs

| Item | Value |
|---|---|
| Jenkins controller | pod **`jenkins-0`**, namespace `jenkins`, in the **docker-desktop** k8s cluster **on the Mac** |
| Mac host | **`192.168.100.102`** (macOS, Apple Silicon / arm64) |
| Jenkins URL (from the Mac) | **http://localhost:8085** |
| How that URL works | `kubectl -n jenkins port-forward svc/jenkins 8085:8080` (agent: `svc/jenkins-agent 55100:50000`) |
| Reach it from another LAN host | re-run the forward with `--address 0.0.0.0`, then **http://192.168.100.102:8085** |
| Build agent label | **`local-mac`** (jobs run on the Mac itself) |
| Login | admin; password = `kubectl get secret -n jenkins jenkins -o jsonpath='{.data.jenkins-admin-password}' \| base64 -d` |

---

## The two environments — different cluster, arch, image

| | **DEV** | **PROD** |
|---|---|---|
| Cluster | docker-desktop (on the Mac) | k3s: **A `192.168.100.252`** + **B `192.168.100.4`** |
| OS / CPU | macOS / Apple Silicon | CentOS Stream 9 / **x86_64** |
| **Image platform (`BUILD_PLATFORM`)** | **`linux/arm64`** | **`linux/amd64`** |
| Image registry | local docker-desktop (`host.docker.internal:5001`) | **`192.168.100.252:5000`** |
| Push image? (`PUSH_IMAGE`) | `true` (push to the dev registry) | `true` (push to the registry) |
| kube access (from the Mac agent) | context **`docker-desktop`** | **`~/.kube/oe-prod.yaml`** |
| Kubernetes API | in-cluster | **https://192.168.100.252:6443** |

`~/.kube/oe-prod.yaml` = A's `/home/options-edge/config/kubeconfig` with the server rewritten `127.0.0.1` → `192.168.100.252:6443`.

### Why the image must differ (do not skip)
The build runs on the **Mac (arm64)**. To get an image that runs on the **prod Linux nodes (amd64)** you must cross-build:
```bash
docker buildx build --platform linux/amd64 -t 192.168.100.252:5000/<svc>:prod --push .   # PROD
docker buildx build --platform linux/arm64 -t <svc>:dev --load .                          # DEV
```
Building the wrong arch → the pod never starts (`exec format error` → `CrashLoopBackOff`, or `ImagePullBackOff` on a manifest-mismatch). The child jobs already do this via the `BUILD_PLATFORM` parameter — just set it correctly per environment.

---

## The jobs

| Job | Does | dev params | prod params |
|---|---|---|---|
| `options-edge-processing` | **builds** the stream-app images | `BUILD_PLATFORM=linux/arm64`, `IMAGE_REGISTRY=` (local), `PUSH_IMAGES=false` | `BUILD_PLATFORM=linux/amd64`, `IMAGE_REGISTRY=192.168.100.252:5000`, `PUSH_IMAGES=true` |
| `options-edge-databento-feed-deploy` | **builds** the databento feed image | `ENVIRONMENT=dev`, `PUSH_IMAGE=false`, `DEPLOY_TO_KUBERNETES=false` | `ENVIRONMENT=prod`, `IMAGE_REGISTRY=192.168.100.252:5000`, `PUSH_IMAGE=true` |
| `options-edge-ibkr-feed` | **builds** the IBKR feed image | `BUILD_PLATFORM=linux/arm64`, `PUSH_IMAGE=false` | `BUILD_PLATFORM=linux/amd64`, `IMAGE_REGISTRY=192.168.100.252:5000`, `PUSH_IMAGE=true` |
| `option-edge-feed-gateway` | **builds** the feed-gateway image | `BUILD_PLATFORM=linux/arm64`, `PUSH_IMAGE=false` | `BUILD_PLATFORM=linux/amd64`, `IMAGE_REGISTRY=192.168.100.252:5000`, `PUSH_IMAGE=true` |
| `options-edge-deploy` | **deploys** the whole `options-edge` namespace (`kubectl kustomize k8s/overlays/<env> \| apply`) | `ENVIRONMENT=dev` | `ENVIRONMENT=prod` |
| `options-edge-web-deploy` | web UI (Docker container — not k8s) | `PROFILE=dev`, `BUILD_PLATFORM=linux/arm64`, `PUSH_IMAGE=false`, `RUN_LOCAL=true`, `IMAGE=options-edge-web:dev` | `PROFILE=prod`, `BUILD_PLATFORM=linux/amd64`, `PUSH_IMAGE=true`, `RUN_LOCAL=false`, `IMAGE=192.168.100.252:5000/options-edge-web:prod` |
| `options-edge-bring-up-all` | **umbrella**: build all images → one deploy → web | `PROFILE=dev` | `PROFILE=prod` |
| `loki-deploy` | Loki/Promtail logging stack (Ansible) | `DEPLOY_ENVIRONMENT=dev`, `KUBECONFIG_FILE=` | `DEPLOY_ENVIRONMENT=prod`, `KUBECONFIG_FILE=/Users/abhinav/.kube/oe-prod.yaml`, `CONFIRM_DEPLOY=true` |
| `vol-premium-ledger-publish` (`Jenkinsfile.vol-premium-ledger-publish`) | **publishes** the vol-premium event calendar (`scripts/vol-premium/calendars/calendar-<version>.json`) to the Postgres ledger `vol_premium_calendar_ledger` by running the engine image's `VolPremiumLedgerPublisher` as a deployer-created Job (`k8s/jobs/vol-premium-ledger-publish-job.yaml`); dry run ALWAYS, the write only on `CONFIRM=true`; needs `PERMITTED_SHA` (main only). Runs BEFORE the deploy that switches vol-premium to the engine image. Job item not yet created in Jenkins — see the Jenkinsfile header. | — (production only) | `ENVIRONMENT=production`, `KIND=calendar`, `FILE=scripts/vol-premium/calendars/calendar-2026.1.json`, `CONFIRM=false` then `true`, `PUBLISHED_BY=<you>`, `PERMITTED_SHA=<40-hex>` |

> The umbrella derives the dev/prod knobs from one `PROFILE` choice and fans the right params out to each child — so for a full bring-up you only pick `PROFILE`.

---

## Every deploy job now needs a permitted commit

Since the permitted-commit guard landed (options-edge rule.md, "Deployment Permission Rule"; deploy PR #1043,
gateway #188, web #738, processing #836), a guarded job refuses to do anything unless it is told which commit it may
deploy. A run without it stops in the `Permitted commit guard` stage and says so:

```text
permitted-sha-guard: REFUSED — PERMITTED_SHA is not set ...
permitted-sha-guard: verdict=REFUSED (no deployment effect may follow)
```

That is the guard working, not a broken job. What to pass:

| Parameter | Value | Which jobs |
|---|---|---|
| `PERMITTED_SHA` | the full 40-character SHA you are deploying: the job's checkout must be exactly this commit, and this commit must be on the environment's branch (`main` for prod jobs) | every guarded job |
| `PERMITTED_SHA_GUARD_VERSION` | leave the default — it is the sha256 of `scripts/jenkins/permitted-sha-guard.sh` and a caller compares it against its own copy | every guarded job |
| `CONTRACTS_PERMITTED_SHA` | tip of `options-edge-contracts` `main` | jobs that clone contracts and build against it (processing, gateway) |
| `DEPLOY_PERMITTED_SHA` | tip of `options-edge-deploy` `main` | image jobs that then trigger `service-deploy` |
| `REQUIRED_IMAGE` | `repo@sha256:…` from the building job's image lock | a deploy that must roll exactly the image another guarded build produced |
| `PROCESSING_PERMITTED_SHA` | tip of `options-edge-processing` `main` | `service-deploy` with `BUILD_IMAGES=true` — it refuses without it |
| `WEB_PERMITTED_SHA` | tip of `options-edge` `main` | `web-service` with `BUILD_IMAGE=true` (the image is built by `options-edge-web-deploy`) |
| `NIFTY_PERMITTED_SHA` | tip of the nifty source `main` | `nifty-gex-service`, which clones and builds that source |

A job that builds an image needs the permission for the *source* repository as well as its own: those last three rows
are not optional on that path, and the build refuses with `BUILD_IMAGES=true needs PROCESSING_PERMITTED_SHA: …` (or the web
and nifty equivalents) before triggering anything. Each job's own **Build with Parameters** page lists exactly what it
requires, with the reason in the parameter description.

Get the SHA with `gh api repos/abhinav-jain09/<repo>/branches/main -q .commit.sha`, and pass it in the same build.
What the guard checks, in the order it checks it: the running guard's sha256 equals `PERMITTED_SHA_GUARD_VERSION`;
the workspace is a git working checkout and `HEAD` resolves to a full commit id; the selected ref and the job's SCM
metadata name the environment's branch; `origin/<branch>` is fetched (a fetch that fails is a refusal); the
checked-out `HEAD` is an *ancestor of* that freshly fetched tip; `PERMITTED_SHA` is present and a full lowercase
40-character commit id; and finally `HEAD` equals `PERMITTED_SHA` exactly.

Two things follow. The ancestry test is against the branch tip but the equality test is against your checkout, so it
is the checkout — never the tip — that must be the permitted commit: if `main` moves on after you read the SHA, a
build that still checks out your commit satisfies both tests. And the equality and ancestry tests are two of seven,
not the whole guard: a run can still refuse for a guard-version mismatch, a workspace that is not a checkout, SCM
metadata naming another branch, or a failed fetch. Read the SHA and start the build together anyway, so you deploy
what you looked at.

A few consequences worth knowing:

- **A build started by a push refuses too.** An SCM-triggered run has no permitted commit, so it stops at the guard
  with a red build and publishes nothing. That is intended: an automatic push is not a deployment permission.
- **The first run of a job after the guard merges is expected to fail.** Jenkins only learns a job's new parameters by
  running it once; that run refuses at the guard and registers them. Done on 2026-09-20 for `service-deploy`,
  `options-edge-web-deploy`, `web-service-deploy` and `option-edge-feed-gateway`.
- **A job may only hand work to a child that enforces the same guard.** The caller reads the child's definition from
  its repository at the commit being forwarded and checks it; an unguarded or out-of-date child is refused rather than
  triggered.
- **Cron-scheduled jobs are deliberately outside the guard** — `premarket`, `premarket-reset`,
  `offhours-clean-slate`, `morning-autostart`, `gateway-nightly-restart` and `stockgex-oi-snapshot`: requiring a SHA
  would fail every scheduled run. They remain the owner's to start. Being a stockgex job is not the reason:
  `stockgex-close-board` has no trigger and *is* guarded.

`scripts/ci/jenkins-permitted-sha-scope.txt` classifies each of the 40 root `Jenkinsfile*` files in this repository
as in or out of scope, with the reason, and the CI check fails if a root Jenkinsfile is missing from it.

---

## How to deploy

### Dev (to docker-desktop on the Mac)
1. `options-edge-bring-up-all` → **Build with Parameters** → `PROFILE=dev` → Build.
   (Builds `linux/arm64` images locally, applies to docker-desktop.)

### Prod (to the Linux k3s cluster A/B)
1. Make sure A (`.252`) and B (`.4`) are powered on and `kubectl --kubeconfig ~/.kube/oe-prod.yaml get nodes` shows both **Ready**.
2. `options-edge-bring-up-all` → **Build with Parameters** → `PROFILE=prod` → Build.
   (Builds `linux/amd64` images, pushes to `192.168.100.252:5000`, then `options-edge-deploy` applies them.)

---

## Known prerequisite (caused build #318 to fail)
`options-edge-deploy` needs an **admin kubeconfig at `/var/jenkins_home/config/kubeconfig`** inside the `jenkins-0` pod (used by `scripts/jenkins/bootstrap-kubernetes-deploy-guard.sh` to set up the Jenkins-deployer RBAC). It is **not** managed by the Helm chart, so it must be restored after any Jenkins reinstall, or the job fails at *Bootstrap Jenkins Kubernetes Guard* with `Missing admin kubeconfig for deploy guard bootstrap`.
