# Onboarding a New Service → Jenkins Job

When a new OptionsEdge service needs CI/CD, use this template so it matches every other
service: a **build job** (builds the image per-arch) + the **k8s manifests** that
`options-edge-deploy` applies. (Build and deploy are separate — this job only builds.)

> Reminder: **dev = Mac/arm64**, **prod = Linux/amd64** — images must be built per-arch
> (`linux/arm64` for docker-desktop, `linux/amd64` for the k3s nodes). See `docs/jenkins-jobs.md`.
> Jenkins runs in docker-desktop on the Mac → **http://localhost:8085** (`192.168.100.102`).

---

## Steps

### 1. Add a Dockerfile to the new service repo
A normal Dockerfile that builds the service (the template build is multi-arch via buildx).

### 2. Add the build Jenkinsfile
Copy the template into the new repo as `Jenkinsfile`:
```bash
cp options-edge-deploy/templates/Jenkinsfile.new-service <new-service-repo>/Jenkinsfile
```
Edit one line — set the default `SERVICE_NAME` (e.g. `options-edge-foo`). Commit + push to `main`.

### 3. Create the Jenkins job (one command)
```bash
options-edge-deploy/scripts/jenkins/create-service-job.sh \
  options-edge-foo  git@github.com:abhinav-jain09/options-edge-foo.git
```
This clones an existing build job's config (so it reuses the git credentials), repoints it
at the new repo + `Jenkinsfile`, strips the SCM poll (manual job), and creates
`http://localhost:8085/job/options-edge-foo/`.
*(Or in the UI: New Item → Pipeline → Pipeline script from SCM → the repo → `Jenkinsfile`.)*

### 4. Add the service to the k8s deploy
So `options-edge-deploy` deploys it with everything else:
- add `k8s/base/foo-deployment.yaml` (+ `foo-service.yaml` if it serves traffic)
- list them in `k8s/base/kustomization.yaml`
- the deployment's `image:` must match what the build job pushes:
  `192.168.100.252:5000/options-edge-foo:<tag>` for prod.

### 5. (Optional) Add it to the bring-up-all umbrella
If it should come up with a full platform bring-up, add a stage to `Jenkinsfile.bring-up-all`
(Build images parallel block) passing `BUILD_PLATFORM` / `IMAGE_REGISTRY` / `PUSH_IMAGE`.

---

## Running the new job

| | DEV (docker-desktop, Mac) | PROD (k3s A/B) |
|---|---|---|
| `ENVIRONMENT` | `dev` | `prod` |
| `BUILD_PLATFORM` | `linux/arm64` | `linux/amd64` |
| `IMAGE_REGISTRY` | *(empty)* | `192.168.100.252:5000` |
| `PUSH_IMAGE` | `false` (load locally) | `true` (push to registry) |
| `IMAGE_TAG` | `dev` | `prod` (or build number) |

1. **Build with Parameters** → set the values above → Build (builds + pushes/loads the image).
2. Deploy: run **`options-edge-deploy`** (`ENVIRONMENT=dev|prod`) — it applies the whole
   namespace, including the new manifests.

---

## Files this provides
- `templates/Jenkinsfile.new-service` — the parameterized build pipeline (copy into the new repo).
- `scripts/jenkins/create-service-job.sh` — clones a working job to create the new one via the Jenkins API.
- `docs/jenkins-jobs.md` — the full dev/prod + per-job reference.
