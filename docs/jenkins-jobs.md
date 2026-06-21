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
| Push image? (`PUSH_IMAGE`) | `false` (load into local docker) | `true` (push to the registry) |
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

> The umbrella derives the dev/prod knobs from one `PROFILE` choice and fans the right params out to each child — so for a full bring-up you only pick `PROFILE`.

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
