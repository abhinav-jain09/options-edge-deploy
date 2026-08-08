# Gate-1 — `fullfunding` tenant namespace on the prod k3s node, and the req-portal's migration into it

**Status: GATE-1 REQUIREMENTS / PROPOSED — rev 8. AWAITING USER APPROVAL (gatekeeping Gate-1).
Not implemented.**

**rev 8 — eight internal inconsistencies corrected, no requirement changed.** Every one was a place
where an earlier revision's number or claim survived a later decision. Three were found reading the
document end-to-end for approval; Codex's review of those corrections found five more of the same
class, listed after them.

Found on the first pass:
1. **§6 step 1's abort threshold was unreachable.** It still read `R > 11.25 Gi`, the threshold for
   the superseded **4 Gi** tenant budget. D-3 fixed the tenant at **2.25 Gi**, which NS-4's formula
   makes feasible while `R ≤ 13.01 Gi`, gated conservatively at **13.0 Gi**. Against the expected measurement (`R ≈ 13 Gi`, the
   off-hours floor) the old figure would have aborted the rollout every time — a feasibility gate
   that had silently become an unconditional stop. Now `R > 13.0 Gi`, with the derivation shown and
   the rounding rule stated — the formula's knife-edge is 13.01 Gi, but its inputs are rounded to
   0.01 Gi, so the gate rounds down rather than claiming a precision the arithmetic lacks.
2. **NS-2's prose contradicted its own YAML** — "`limits.memory` 12 Gi" against `limits.memory:
   10Gi` in the block directly above it. NS-8's peak is **5 CPU / 10 Gi**, so the YAML was right and
   the prose was left over from the 4 Gi envelope. Prose corrected, and it now states that the
   memory limit equals NS-8's peak exactly.
3. **§10's gate status was stale** — it described rev 4 awaiting Codex re-review, and listed D-1 and
   D-3 as Gate-2 blockers after the user had closed them. It now records revs 5–8 and names the two
   blockers that actually remain: **D-6**, and **a named consumer for Postgres**.

Found by Codex on review of those three, all now fixed: NS-4's operative prose and the D-3 decision record still aborted at `R > 13 Gi` (so
`13.00 < R ≤ 13.01` both fitted and blocked); **R-21** still measured slack against the 4 Gi budget;
**§4** still said D-1 awaited a decision; **NS-8** still described a rollout surge that `Recreate`
makes impossible and a reduction trigger that D-3 had already applied; and the revision history
quoted superseded thresholds without marking them as such. The lesson is recorded rather than just
the fix: a closed decision has to be propagated to every operative number it touches, not only to
the decision table.

**USER DECISIONS RECORDED 2026-08-04 — D-1, D-3 and D-7 are CLOSED:**
1. **D-3 → the host reservation wins.** `system-reserved` keeps the full measured host requirement
   (≈13 Gi), and the **tenant's `requests.memory` is fixed at 2.25 Gi**, not 4 Gi. NS-8's entire
   workload budget is re-sized to that envelope; the web tier loses its rollout surge.
2. **D-1 → Bugzilla stays on MariaDB** (rev-11's pinning, image and backup design untouched), and
   **Postgres is additionally provisioned** as the namespace platform database. A named consumer for
   Postgres must still be recorded before Gate 2.
3. **D-7 → literal zero on `/`.** `/var/lib/kubelet` and `/var/log/pods` are relocated onto `/home`
   (**NS-21**), so nothing of the tenant's — not even rotated container logs — is stored on the root
   filesystem. This also benefits OptionsEdge, since `/` has only 38 GiB free.

**Revision history.** Every threshold quoted below is the value that revision computed for the
**4 Gi** tenant budget, which D-3 later superseded. None of them governs: the operative numbers are
`tenant ≤ 15.26 − R` and an abort at `R > 13.0 Gi` (NS-4, §6 step 1).

rev 1 → Codex REQUEST_CHANGES (15 blockers); rev 2 → REQUEST_CHANGES (18);
rev 3 → REQUEST_CHANGES (12 + corrections). **rev 4 implements every rev-3 finding**, the material
ones being: the Ingress quota is raised to 1 (a `0` quota would have rejected the platform-owned
Ingress it was meant to protect); NS-4 gains an explicit **`P_platform`** term and the feasibility
threshold tightens to **R ≤ 11.5 Gi** (for the then-current 4 Gi budget); NS-8's arithmetic is corrected and restructured as
**steady state + exactly one transient** — rev 4 claimed the quota itself enforces that shape, which
**NS-8 has since withdrawn**: a ResourceQuota constrains aggregate consumption, not the shape of it;
the direct-Pod denial is
re-expressed on the **requesting identity** (as this cluster's existing policy already does) so
controller-created Pods are unaffected and the rule is unforgeable, and it is extended to
`options-edge` so the CPU-limit prohibition has no Pod-level bypass; the disk-wall guard becomes an
**independent host timer verifying the loop device by UUID** with an enumerated quiescence
procedure; every operational identity is enumerated (nine, not five); Secret rollback is explicitly
excluded with its consequence stated; **NS-19's clause map is delivered now as Appendix A** and
moved to step 0.5; NS-V16 gains a bounded pilot load so RTH coexistence evidence is not vacuous;
and NS-18 gains a budgeted, policy-compatible telemetry path.

**rev 3 implemented every rev-2 finding**, the material
ones being: the NS-4 arithmetic is corrected (it omitted the eviction reserve — the real slack at a
14 Gi reservation is **2.26 Gi, not 4.3 Gi**) and is now expressed as a **feasibility formula with a
quantified margin**, which surfaces the document's most important result — **the tenant's then-4 Gi
memory budget is only feasible if the measured reservation is small enough** (§3 NS-4 — rev 3's own
figure was ≤ 12 Gi; the corrected formula puts a 4 Gi tenant at `R ≤ 11.26 Gi`, and D-3 has since
replaced the 4 Gi budget entirely); the
request-accounting methodology is restated in the actual scheduler formula and **certified against
this cluster** (zero restartable-init sidecars, zero pod overhead); ephemeral storage is no longer
called a hard ceiling; the LimitRange-versus-explicit-resources contradiction is resolved by
enforcing on **controller templates**, where LimitRange defaulting does not apply; the disk wall now
fails closed for **already-bound PVs** when the mount disappears; the host-service network fallback
is withdrawn as unimplementable and replaced by an explicit **launch block**; NetworkPolicy,
Ingress and traefik Middleware move to the **platform plane** so the tenant credential cannot
dismantle its own boundary; the backup transport is made implementable and its failure domain
stated honestly (**off-volume, not off-host**); RBAC becomes **five named credentials**; and the
unsafe acceptance tests (node-pressure eviction, guardrail removal, deliberate OOM alerts) are
replaced with safe equivalents.

**A live measurement that corrects a rev-2 statement:** this node's **effective** `evictionHard` is
**`{imagefs.available: 5%, nodefs.available: 5%}`** — k3s's own values, *not* the upstream kubelet
defaults rev 2 quoted, and **there is no memory eviction threshold at all today**. NS-4's addition of
`memory.available` is therefore a genuine new protection, not a re-statement of an existing one.

Everything in §2 is **verified on `192.168.100.252` on 2026-08-04**, method recorded per row, with
inference labelled as inference. Everything from §3 onward is intended future behaviour.

**Date:** 2026-08-04  **Owner:** Abhinav
**Repos:** options-edge-deploy (namespace overlay, tenant manifests, admission policies, Jenkins job)
**Hosts:** 192.168.100.252 (prod k3s, single node)
**Supersedes, in part:** `docs/req-portal-bugzilla-keycloak-sso.md` rev 11 — §4 is the normative
disposition table; NS-19 is the clause-level completion requirement.

---

## 1. Goal, and what it does and does not promise

Run a **second, unrelated project on the existing production machine** inside the same k3s cluster,
in a dedicated namespace **`fullfunding`**, hosting the requirement-intake portal
(`req.fullfunding.nl` — Bugzilla + Keycloak realm `req`, designed in rev 11), with its own Postgres
and Kafka, sized for a **data-entry** workload.

**Threat model, stated before the guarantees, because everything below depends on it.** The
`fullfunding` tenant is **not an untrusted party**. It is the same operator, deploying through the
same `main`-only Jenkins path, onto the same host, as OptionsEdge. What this design defends against
is therefore **accident, resource contention and blast radius** — a runaway process, a filled disk,
a misconfigured workload, a login storm — **not a malicious insider with deploy rights**. Where a
control would only stop a deliberate operator (for example: the tenant credential could point its
own Service at the Bugzilla admin port, or add a sidecar that proxies it), that control is documented
as a **guardrail against mistakes, not a boundary against intent**, and is not claimed as isolation.
Anything requiring defence against a hostile deployer needs a separate cluster, not a namespace.

**Subject to that model, the objective is bounded, measured interference — not isolation.**

| Dimension | Delivered | Mechanism | Not delivered |
|---|---|---|---|
| CPU | Bounded scheduler weight and ceiling | NS-2 | Zero interference: up to 6 CPU of burst on a 24 CPU node, by design |
| Memory | Hard ceiling on the tenant; tenant preferred as eviction victim | NS-2, NS-3, NS-4 | A guarantee that OptionsEdge is never the victim (NS-3) |
| Persistent disk | Hard ceiling (a separate, preallocated filesystem) | NS-7 | Fair sharing *within* the tenant — one PVC can consume the whole image (R-23) |
| Ephemeral disk on `/` | **Nothing of the tenant's is stored on `/` at all** — disk-medium `emptyDir` denied, root filesystems read-only, and kubelet state + pod logs relocated to `/home` | **NS-20 + NS-21** (D-7 closed), NS-15(6)(11) | Nothing outstanding once NS-21 lands; before then the interim residue is ≤0.9 GiB of rotated logs |
| Node-level escape | Blocked at admission | NS-15 | Protection against a cluster-admin, the host root, or the node itself |
| Network | Default-deny, port-specific, platform-owned | NS-6, NS-9 | Protection of shared Keycloak/traefik/cloudflared/registry capacity (R-19) |
| Availability | — | — | **Nothing.** One node, one kernel, one k3s (NS-13) |

Success is a **measured** statement (NS-12): after the tenant is live, OptionsEdge's pipeline lag,
error rate and pod-restart behaviour stay inside their pre-change baseline bands. If they do not,
the tenant is scaled down and the answer becomes a second machine (the `.4`/es4 pattern), not a
tighter quota.

The change of record versus rev 11: that revision specified a host Docker-Compose deployment on
loopback ports 8093/8095. This document moves the hosting model into Kubernetes.

## 2. Current state (as-is, verified 2026-08-04)

### 2.1 Node and cluster

| Fact | Value | Method |
|---|---|---|
| k3s | v1.35.5+k3s1, **single node**, control-plane, containerd 2.2.3 | `kubectl get nodes -o wide` |
| k3s data-dir | `/home/options-edge/data/k3s` | `k3s.service` ExecStart |
| Node **capacity** | cpu **24**, memory **65257092Ki (62.23 Gi)**, ephemeral-storage **71645Mi (69.97 Gi)**, pods **110** | `.status.capacity` |
| Node **allocatable** | **identical to capacity** — nothing is reserved today | `kubectl describe node` |
| `system-reserved` / `kube-reserved` | **not set** | unit + config read |
| **Effective `evictionHard` (live kubelet config)** | **`{imagefs.available: 5%, nodefs.available: 5%}`** — k3s's values. **No `memory.available` threshold exists today**; memory exhaustion is handled only by the kernel OOM killer | node `/configz` |
| **Effective `podPidsLimit`** | **`-1` (unlimited)** | node `/configz` |
| **nodefs** (emptyDir, container logs, ephemeral-storage accounting) | **`/`** — 69 GiB capacity, **37 GiB available** | node `stats/summary` |
| **imagefs** (images, container writable layers) | **`/home`** — 1759 GiB capacity, 1157 GiB available | node `stats/summary` |
| Namespaces | `default`, `kube-node-lease`, `kube-public`, `kube-system`, `loki`, `options-edge` — **no `fullfunding`** | `kubectl get ns` |
| ResourceQuota / LimitRange | **none, anywhere** | `kubectl get resourcequota,limitrange -A` |
| Pod Security Admission | **no `pod-security.kubernetes.io/*` label on any namespace** | `kubectl get ns -o jsonpath` |
| **ValidatingAdmissionPolicy** | **GA at `admissionregistration.k8s.io/v1`**, already in use: `options-edge-jenkins-only-workloads` (47 d, `failurePolicy: Fail`, binding `[Deny]`, matching core/apps/batch/networking resources, asserting the requesting username) | `api-resources`; policy + binding read |
| NetworkPolicy objects | **none, anywhere**. `--disable-network-policy` is not set, so k3s's controller is expected to be active — **inference; enforcement is proven only by NS-V9** | `kubectl get netpol -A`; unit + config |
| StorageClass | **one**: `local-path` (default), path `/home/options-edge/data/k3s/storage` | `kubectl get sc`; `local-path-config` |
| IngressClass / traefik | `traefik`; LoadBalancer on `192.168.100.252:80,443` | `kubectl get ingressclass`, `svc traefik` |
| Existing Ingress objects | one: `options-edge/oe-keycloak` → `auth.fullfunding.nl`, path prefixes `/realms/optionsedge` and `/resources` **only** | `-o jsonpath` |
| **The browser path to Keycloak does NOT use that Ingress** | cloudflared routes `auth.fullfunding.nl` **directly to the Keycloak ClusterIP** `http://10.43.127.26:8080` with `httpHostHeader: auth.fullfunding.nl`, with only `/admin` and `/realms/master` intercepted as `http_status:404`. **Every other path — including `/realms/req/*` — is forwarded**, so realm `req` needs **no change to any shared Keycloak route** | `/etc/cloudflared/options-edge-stable.yml` read 2026-08-04 |
| ⚠️ Existing fragility this project now depends on | that route is **pinned to a ClusterIP**. Recreating the `oe-keycloak` Service changes the IP and breaks `auth.fullfunding.nl` for the trading UI **and** the portal. Pre-existing; recorded as **R-25**, not introduced here | same |
| **traefik Ingress routing works** | Host-matched `curl` → **200** | direct test |
| Design primitives accepted by this API server | `PriorityClass value: -100`; quota keys `services.loadbalancers: "0"`, `local-path.storageclass.storage.k8s.io/persistentvolumeclaims: "0"`, `<class>.storageclass.storage.k8s.io/requests.storage` | `--dry-run=server` — **syntax/admission acceptance only, never semantics** |

### 2.2 The measured footprint

**Methodology, restated in the scheduler's own formula.** For each pod, per resource, the effective
request is
`max( Σ requests(app containers) + Σ requests(restartable init containers) , max requests(regular init containers) ) + pod overhead`.
This cluster is **certified against the simplifying conditions**: **zero restartable-init sidecars**
and **zero pods with a `runtimeClassName` or `overhead`** (measured 2026-08-04), so the formula
reduces to `max( Σ app containers , max init container )`, which is what was computed. Replica
weighting uses `max(spec.replicas, 1)` so deployments currently at 0 under the off-hours lifecycle
are counted at their **running intent**; DaemonSets at 1 (single node).

| Scope | CPU requests | Memory requests |
|---|---|---|
| `options-edge` Deployments (55) | 12.97 | 37.06 Gi |
| `options-edge` Jobs (6) + StatefulSet (1) | 0.70 | 1.75 Gi |
| `kube-system` Deployments (4) + Jobs (2) + DaemonSets (6) | 0.40 | 0.16 Gi |
| `loki` StatefulSet + DaemonSet | 0.00 | 0.00 Gi |
| **TOTAL — the schedulable footprint** | **14.07** | **38.97 Gi** |

Pod count at running intent: **68** (Deployments + StatefulSets + DaemonSets) plus 6 Jobs ≈ **74** of
the **110** pod cap. Currently running: 64.

Two facts that bound what any arithmetic can claim:
- Declared **memory limits** in `options-edge` sum to **149.1 Gi** — 2.4× the node. Fitting by
  requests proves schedulability, **not** runtime safety.
- **Zero deployments set `limits.cpu`.** "OptionsEdge stays uncapped on CPU" is the measured status
  quo, not a change; NS-2 makes it an admission-enforced rule.

PVCs cluster-wide: 26, declared total 1141 Gi.

### 2.3 The half of the platform that is not in Kubernetes

kubelet does not represent these as pod requests or namespace usage; it observes only their
aggregate effect as node pressure.

| Component | Where | Port | Data |
|---|---|---|---|
| Kafka (KRaft) | host systemd | 9092/9093 | `/home/kafka/kraft-combined-logs` (dedicated 1.9 T NVMe) |
| PostgreSQL | host systemd | 5432 | `/home/postgres/data` |
| Schema Registry / AKHQ | host systemd | 8081 / 8082 | — |
| Prometheus + node-exporter / Grafana | host systemd | 9090 / 3000 | — |
| httpd + php-fpm | host systemd | — | — |
| cloudflared (`options-edge-option-chain`) | host systemd | — | `/etc/cloudflared/options-edge-stable.yml` |
| Docker registry | container | **5000** | — |
| `options-edge-admin-app` | container | **8091** | — |
| Internal Bugzilla web + MariaDB | containers | **8092**, 3306 | `/home/options-edge/data/bugzilla/` |

Host memory resident at off-hours (most OptionsEdge deployments at 0): **≈13 Gi**, plus 16 Gi
buff/cache.

### 2.4 Disks

| Mount | Device | Size | Free | Options | Role |
|---|---|---|---|---|---|
| `/` | `cs-root` xfs | 70 G | **37–38 G** | — | **kubelet nodefs**: emptyDir, logs, ephemeral-storage |
| `/home` | `cs-home` xfs | 1.8 T | 1.2 T | **`noquota`** | k3s data, imagefs, Postgres, Docker |
| `/home/kafka` | `nvme0n1p1` xfs | 1.9 T | 1.7 T | `noquota` | host Kafka only |

- `local-path` is a hostPath bind: it **does not enforce PVC capacity**.
- `/home` is `noquota`; enabling XFS project quota needs `/home` unmounted, where k3s data, Postgres
  and the Docker root live. Out of scope (NS-7 avoids it).
- **`/` is the small, exposed filesystem** and is *not* where PVCs live: `emptyDir` and container
  logs attack `/`, not the tenant image.

### 2.5 Ports and the existing collision

8093/8095 are free; recorded only because rev 11 used them — **the Kubernetes design uses neither**.
Host port **8091** is held by container `options-edge-admin-app` while LoadBalancer Service
`options-edge/feed-gateway-service` also claims it; its `svclb` pod shows **24 restarts,
`Terminated / Exit Code: 255`**. The Service is at 0 replicas (off-hours), so this is an
**indication, not a proven diagnosis** — NS-V13 closes it at RTH.

## 3. Requirements (NS-1 … NS-19)

### NS-1 — Object planes and ownership
Namespace **`fullfunding`**, label `tenant: fullfunding`. Five ownership classes, each with its own
credential (NS-9). Every object belongs to **exactly one**:

1. **Tenant plane** (in-namespace, tenant-deploy credential): Deployments, StatefulSets, CronJobs,
   **ClusterIP** Services, ConfigMaps, Secrets, PVCs, ServiceAccounts.
2. **Platform-Kubernetes plane** (platform-k8s credential): the Namespace, `ResourceQuota`,
   `LimitRange`, PSA labels, `PriorityClass fullfunding-low`, `StorageClass fullfunding-storage`,
   the second provisioner + its ClusterRole/Binding and PVs, all `ValidatingAdmissionPolicy` objects
   and bindings, **all `NetworkPolicy` objects**, **the public `Ingress`**, and **the traefik
   `Middleware`** (NS-16). NetworkPolicy, Ingress and Middleware are deliberately *not* tenant-owned:
   a tenant that can edit its own boundary does not have one.
3. **Host-platform plane** (host-root credential): the kubelet reservation (NS-4), the loopback
   image + mount unit (NS-7), the cloudflared ingress rule, the host backup agent (NS-17).
4. **External plane** (external credential): the Cloudflare DNS record.
5. **Shared-service plane** — **owned, not unowned**: the Keycloak realm `req` and its client are
   **owned by the OptionsEdge Keycloak operator** (the existing Keycloak admin credential, per
   rev-11 REQ-1), applied through the existing Keycloak deploy path; traefik, cloudflared and the
   registry remain OptionsEdge-owned shared infrastructure.

The project is therefore **not operationally independent** of OptionsEdge (R-19).
**Acceptance:** NS-V1 — every object maps to exactly one class with a named credential; zero
unattributed objects; no tenant-plane credential holds any platform-plane verb.

### NS-2 — Resource budget
- **OptionsEdge:** no CPU limits, enforced by an admission policy over **controller templates**
  (NS-15(9)), not by CI.
- **`fullfunding`:** `ResourceQuota` + `LimitRange`, with production manifests declaring their own
  values explicitly (NS-15(5)).

```yaml
requests.cpu: "1"                 limits.cpu: "6"
requests.memory: 2304Mi           limits.memory: 10Gi   # 2.25Gi — fixed by D-3, see NS-4
requests.ephemeral-storage: 256Mi limits.ephemeral-storage: 1Gi   # NS-20: nothing belongs on /
pods: "18"
count/services: "8"
count/ingresses.networking.k8s.io: "1"   # exactly one, platform-owned (NS-5)
count/secrets: "15"   count/configmaps: "15"
count/cronjobs.batch: "5"   count/jobs.batch: "20"
persistentvolumeclaims: "5"
fullfunding-storage.storageclass.storage.k8s.io/requests.storage: 80Gi
local-path.storageclass.storage.k8s.io/persistentvolumeclaims: "0"
services.loadbalancers: "0"       services.nodeports: "0"
```
`LimitRange` (Container): `defaultRequest` 50m/128Mi/64Mi-ephemeral, `default`
500m/512Mi/128Mi-ephemeral, `max` 2 CPU / 3Gi / **256Mi**-ephemeral (NS-20).

- **`requests.cpu` is the load-bearing number**, because CFS weight derives from requests, not
  limits. It does **not** preserve all CPU for OptionsEdge: the tenant may burst to 6 CPU.
- **The Ingress quota is `1`, not `0`.** A ResourceQuota applies to every admission request in the
  namespace regardless of which credential makes it, so rev 3's `0` would have rejected the
  platform-owned Ingress it was meant to protect. The layering is: **RBAC** denies the tenant
  credential any Ingress verb (NS-9), **the quota** prevents a second Ingress, and **NS-15(7)**
  constrains the content of the one that exists.
- **`limits.memory` 10 Gi and `limits.cpu` 6** are deliberately larger than the requests, because
  limits do not consume allocatable. They are derived from NS-8's corrected budget —
  **steady state plus one budgeted transient**, whose peak is **5 CPU / 10 Gi** — not chosen round.
  The memory limit equals that peak exactly; the CPU limit carries 1 CPU over it.
- **Ephemeral storage is bounded at admission and enforced at runtime by eviction, not by an
  instantaneous quota.** A logging burst can overshoot before kubelet reacts (R-22), and container
  images and pulls sit outside the quota entirely. NS-1's table states this honestly.
- `emptyDir` sizing and PID limits: NS-15(6) and NS-4.
- **Job accumulation is a detected, recoverable failure — not an impossibility.** rev 3 claimed
  accumulated Jobs could "never" block the backup CronJob; that was wrong, because
  `ttlSecondsAfterFinished` cleanup is asynchronous, *failed* history is bounded separately, active
  or stuck Jobs are unaffected, other CronJobs share the quota, and a ResourceQuota cannot reserve a
  Job slot for one CronJob. Required instead: `ttlSecondsAfterFinished`,
  `successfulJobsHistoryLimit: 3`, **`failedJobsHistoryLimit: 3`**, `concurrencyPolicy: Forbid`,
  `startingDeadlineSeconds`, `activeDeadlineSeconds` and `backoffLimit` on every CronJob — plus
  **NS-18 alerting on a Job pending or missed for more than 15 minutes**, which is what actually
  makes the failure recoverable.
**Acceptance:** NS-V2, NS-V3, NS-V4, NS-V20a/b.

### NS-3 — Eviction preference (at the strength Kubernetes provides)
`PriorityClass fullfunding-low`, **`value: -100`**, `preemptionPolicy: Never`, on every tenant pod;
no OptionsEdge manifest changes.

**Honest scope:** node-pressure eviction ranks pods first by whether usage exceeds requests, then by
priority. A tenant pod **below** its requests can survive while an OptionsEdge pod **above** its
requests is evicted. `preemptionPolicy: Never` only prevents scheduler preemption. Neither binds the
kernel OOM killer — which, given §2.1 (no `memory.available` threshold today), is currently the
*only* memory backstop this node has. NS-3 delivers a **preference**, not a guaranteed victim; the
real protections are NS-2's small tenant requests and NS-4's headroom.
**Acceptance:** NS-V5 — documentary verification that every tenant pod carries `priority: -100` and
that the ranking inputs are as documented. **No node-pressure rehearsal is performed on this
production node**; a real ordering test requires a disposable node and is explicitly deferred as
out of scope (rev-2 finding 6).

### NS-4 — Node reservation, and the feasibility gate
Add to `/etc/rancher/k3s/config.yaml`:

```yaml
kubelet-arg:
  - "system-reserved=cpu=4,memory=<R>Gi,ephemeral-storage=4Gi"
  - "kube-reserved=cpu=1,memory=1Gi,ephemeral-storage=2Gi"
  - "eviction-hard=memory.available<2Gi,nodefs.available<10%,nodefs.inodesFree<5%,imagefs.available<10%"
  - "enforce-node-allocatable=pods"
  - "pod-max-pids=<P>"
```

1. **`eviction-hard` replaces, it does not merge** — and this node's *live* set is only
   `{imagefs.available: 5%, nodefs.available: 5%}` (§2.1), so the value above both **adds a memory
   threshold that does not exist today** and preserves/raises the two that do. Adding an inode
   threshold is new protection, not a restatement.
2. **Reservation is capacity accounting, not runtime isolation.** `enforce-node-allocatable=pods`
   caps the *pods* cgroup, which is what shields the host processes; it grants the host services
   nothing.
3. **`pod-max-pids` is node-wide, not tenant-scoped.** Today it is `-1` (unlimited) for every pod,
   including OptionsEdge. `<P>` must therefore be chosen from a **measured maximum pid count across
   OptionsEdge pods at RTH**, set with margin, verified in the effective config, and listed in NS-14
   for rollback. If that measurement is not obtained, `pod-max-pids` is **dropped**, and the PID
   vector is removed from every isolation claim rather than being claimed unenforced.
4. **Fitting by requests is not runtime safety** (§2.2: 149 Gi of declared limits).

**Feasibility formula (this is the document's key result).** kubelet computes
`allocatable = capacity − kube-reserved − system-reserved − eviction-hard(memory.available)`. The
tenant is **not** the only new consumer: the platform plane adds pods that sit outside the tenant
quota but inside node allocatable, so an explicit `P_platform` term is required (rev 3 omitted it):

**None of these run in `fullfunding`** — a ResourceQuota is identity-independent, so a
platform-owned pod placed in the tenant namespace would consume the tenant's budget (the same
reasoning that raised the Ingress quota to 1). They run in `kube-system` or a dedicated
`fullfunding-platform` namespace, and are therefore counted **once**, here, against node allocatable
and **not** against the tenant quota:

| Platform-plane workload | Namespace | cpu req | mem req | max concurrent pods |
|---|---|---|---|---|
| second `local-path-provisioner` | `fullfunding-platform` | 100m | 256Mi | 1 |
| its provisioning helper pods (one per PVC operation; bounded by `persistentvolumeclaims: 5`) | `fullfunding-platform` | 50m each | 128Mi each | **5 worst case** |
| NS-11 guardrail checker (CronJob) | `fullfunding-platform` | 50m | 128Mi | 1 |
| **`P_platform` (budgeted allowance, worst-case concurrent)** | | **0.4 CPU** | **1.0 Gi** | **7** |

The NS-7 tenant-guard is a **host systemd timer with a Kubernetes credential** — not an in-cluster
workload, so it appears in no pod budget (rev 4 contradictorily budgeted both). NS-18 needs no
in-namespace collector at all (see NS-18).

```
tenant_requests_memory  ≤  62.23 − R − 1 − 2 − 38.97 − P_platform(1.0) − M
                        =  19.26 − R − M
```
`R` = measured `system-reserved` memory; `M` = **4 Gi**.

**`R` and `M` are not the same margin and are not double counting.** `R` covers *host processes
outside Kubernetes* and carries its own measurement margin for their burst above the observed
high-percentile. `M` is *scheduler headroom inside Kubernetes*, reserved for OptionsEdge growth —
a new service, a replica increase, a raised request — that would otherwise be unschedulable.

| R (measured) | Max feasible tenant `requests.memory` | Against D-3's **2.25 Gi** envelope |
|---|---|---|
| 10 Gi | 5.26 Gi | fits |
| 11.25 Gi | 4.01 Gi — the superseded 4 Gi budget just fits | fits |
| 12 Gi | 3.26 Gi — the superseded 4 Gi budget does **not** fit | fits |
| 13 Gi | 2.26 Gi | fits (the expected off-hours floor) |
| **13.01 Gi** | **2.25 Gi** | the formula's knife-edge — but **§6 step 1 gates at 13.0 Gi**, rounding down because the inputs are rounded to 0.01 Gi |
| 14 Gi | 1.26 Gi | does **not** fit |

The middle column is the general result; the right-hand column is the one that governs now, since
D-3 closed the tenant at 2.25 Gi. Reading only the middle column is what left §6 step 1 carrying the
4 Gi budget's threshold through rev 7.

**D-3 is closed: the reservation wins and the tenant shrinks.** The user chose to keep the full
measured host reservation rather than trim it for the tenant's benefit. With `R = 13 Gi` (the
off-hours floor, to be confirmed by the §6 step 1 measurement) the formula yields

```
tenant_requests_memory  ≤  19.26 − 13 − 4  =  2.26 Gi
```

so the tenant's `requests.memory` is **fixed at 2.25 Gi (2304Mi)**, and NS-8's workload budget is
sized to that envelope rather than to the 4 Gi rev 4 assumed. **If the measured `R` exceeds
13.0 Gi, launch is blocked** and returns to the user with the same three options (smaller tenant /
smaller reservation / second machine). The formula's knife-edge is 13.01 Gi; the gate rounds down to
13.0 because the formula's inputs are themselves rounded to 0.01 Gi and a 0.01 Gi margin sits inside
that rounding error (§6 step 1). If `R` comes in below 12.76 Gi the tenant may be raised toward
2.5 Gi,
which is a recorded change, not an automatic one. (rev 2's "≈4.3 Gi slack" omitted the eviction
reserve; rev 3 omitted `P_platform`; rev 4's 4 Gi tenant assumed a reservation the user has now
declined. All withdrawn.)

Companion headroom, all recomputed and recorded at NS-V6, each including `P_platform`:
- **CPU:** allocatable `24 − 4 − 1 = 19`; `− 14.07 − 0.4 − 1.0 = 3.53` slack.
- **Pods:** `110 − 74 − 7 − 18 = 11` headroom.
- **Ephemeral:** allocatable ≈ `69.97 − 4 − 2 − 6.99 = 56.98` Gi, minus existing declared requests
  (≈0), minus `P_platform` (≈0.5 Gi), minus the tenant's 2 Gi ⇒ ≈54 Gi of *declaration* headroom.
  This is **not** free disk: the operative number is the **37 GiB actually free on `/`** (R-22), and
  declaration headroom far exceeding real capacity is precisely why ephemeral storage is
  eviction-enforced rather than reserved.

**`R` is derived from high-percentile host usage across ≥5 representative sessions** (open, close,
a volatility spike, the nightly backup window, a maintenance window) plus its measurement margin —
not one snapshot. The off-hours ~13 Gi is a floor. It already exceeded the thresholds that the
earlier **4 Gi** tenant budget required, which is why D-3 was a real choice rather than a
formality — and the user resolved it by keeping the reservation and shrinking the tenant to
2.25 Gi. Against that envelope the same ~13 Gi floor now fits, with the abort threshold at
`R > 13.0 Gi` (§6 step 1).
**Independent value:** this requirement improves the status quo even if the tenant is cancelled.
**Acceptance:** NS-V6 — effective kubelet config dumped and asserted (all four eviction thresholds;
`pod-max-pids` asserted **to the value chosen in D-6, including a verified `-1`/absence if D-6
chooses no limit**), allocatable and all four headroom figures recorded including `P_platform`,
full inventory scheduled.

### NS-5 — Exposure: platform-owned Ingress only
- Public path: **tenant ClusterIP Service → platform-owned traefik `Ingress` → cloudflared**.
- cloudflared rule before the catch-all: `hostname: req.fullfunding.nl` → `http://127.0.0.1:80`.
- **The tenant cannot create Ingress objects** — enforced by **RBAC** (NS-9 grants the tenant
  credential no Ingress verb), with the quota capped at **1** so no second Ingress can exist and
  NS-15(7) constraining the content of the one that does. rev 3's `0` quota is withdrawn: a quota
  applies to every admission request in the namespace irrespective of identity, so it would have
  rejected the platform-owned Ingress itself. rev 2 relied on a host allowlist inside a tenant-created Ingress; that left open a second
  Ingress for the same host without the required middleware, a backend pointed at the admin
  listener, an `ExternalName` Service, or dangerous traefik annotations. Platform ownership closes
  the whole class; NS-15(7) additionally constrains the Ingress content (class, host, path, backend
  service **and port**, required middleware, annotation allowlist) so that even a
  platform-credential mistake is caught, and NS-15(10) denies `ExternalName` Services in the
  namespace.
- Quota `services.loadbalancers/nodeports: "0"` blocks those Service **types** only; `hostPort`,
  `hostNetwork`, `externalIPs`, `hostPath` and privileged pods are blocked by NS-15.
- **LAN origin bypass, explicit:** traefik answers on `192.168.100.252:80` from the LAN, so anyone on
  the LAN can reach the portal with a `Host:` header, bypassing Cloudflare. **Cloudflare is not an
  authentication boundary here**; the portal's own OIDC gate is (rev-11 REQ-5a). Accepted as R-20.
- Publication order and rollback carry from rev-11 REQ-3 verbatim: DNS published last, closed first;
  rollback = (1) point the rule at `http_status:404`, (2) restart cloudflared, (3) **prove** the
  closed response, (4) remove DNS, (5) only then touch the application.
- **The existing `auth.fullfunding.nl` ClusterIP route is left untouched.** Its inline comment
  *"traefik :80 is broken"* is **refuted** (§2.1: Host-matched request → 200; the 404s that motivated
  it are explained by that Ingress declaring only two path prefixes). Correcting it is out of scope.
**Acceptance:** NS-V7, NS-V21, NS-V8.

### NS-6 — Network policy: default-deny, port-specific, platform-owned
Default-deny `Ingress` and `Egress`. "Allow everything inside the namespace" is rejected — it would
leave Bugzilla's **admin listener on container port 81** reachable from any tenant pod. Policies are
per-workload and per-port, and are **platform-owned** (NS-1) so the tenant credential cannot add an
allow-all rule after preflight:

| From | To | Ports |
|---|---|---|
| traefik (kube-system) | portal web pod | **80 only** |
| portal web pod | MariaDB pod | 3306 |
| portal web / app pods | tenant Postgres | 5432 |
| app pods | tenant Kafka | 9092 |
| backup CronJob | MariaDB / Postgres | 3306 / 5432 |
| all tenant pods | `kube-dns` | **UDP 53 and TCP 53** (a rule that omits `protocol` defaults to TCP only) |
| portal web pod | `oe-keycloak` pod in `options-edge` | 8080 (back-channel, below) |

Nothing may reach container port 81 over the pod network; it is reachable only via
`kubectl port-forward` (which traverses the API server and kubelet, not the pod network).

**The load-bearing negative assertion:** a tenant pod must be unable to open **any** listener on
`192.168.100.252`. The test is built from the host's **captured listening-socket inventory** at the
time of the run — not a hand-written six-port list — and every node destination must be denied with
no allowlisted exception. It necessarily includes 9092, 5432, 8081, 8082, 8092 and 5000, and also
**22, 3000, 6443, 8091, 9090, 10250** and every other control-plane and host listener, because if
node-local traffic bypasses NetworkPolicy at all then the exposure is the whole inventory rather
than a chosen subset. NetworkPolicy is *expected* to be enforced (§2.1) but that is
an **inference until NS-V9 proves it**, and egress to the node's own IP is a known CNI weak spot.

**If NS-V9 fails, launch is BLOCKED pending an architectural change — there is no clean fallback,
and rev 2's proposal is withdrawn.** `pg_hba`/SASL rejection of the pod CIDR would block
**OptionsEdge's own pods**, which share that CIDR and legitimately use those services; it would also
leave TCP connecting (contradicting NS-V9's expected result), cover none of Schema Registry, AKHQ,
the registry or internal Bugzilla, and say nothing about every *other* host and control-plane
listener that would be equally exposed. The admissible responses are: (a) demonstrate namespace-
selective enforcement in the CNI, or (b) move the tenant to its own VM/node. Neither is a
same-window fix, which is precisely why NS-V9 runs at §6 step 5, before anything is built on top.

**OIDC back-channel under default-deny (D-5).** Under Compose the container had unrestricted egress,
so `OIDCProviderMetadataURL https://auth.fullfunding.nl/...` simply worked; that hostname resolves
to the **public Cloudflare edge**, which default-deny egress forbids.
- **(ii), recommended — split front/back-channel.** The browser keeps the public authorization
  endpoint (rev-11 REQ-2's exact redirect URI unchanged); mod_auth_openidc's back-channel endpoints
  are set **explicitly** to `http://oe-keycloak.options-edge.svc.cluster.local:8080/...`. The
  **issuer stays the public string**, pinned and verified — a mismatch is a hard failure. Two things
  are **verified, not assumed**, before this is chosen: that the packaged module accepts explicit
  overrides for *every* required endpoint, and that Keycloak emits the public issuer while serving
  an internal plain-HTTP request.
- **(i), fallback — internet egress on 443.** Ordinary NetworkPolicy has no FQDN awareness, so this
  is **broad outbound HTTPS**, not "egress to Cloudflare"; if chosen it is recorded as a widened risk.
**Acceptance:** NS-V9, NS-V18, NS-V19, NS-V22 (complete effective policy set, checked continuously
per NS-11, contains no allow-all rule).

### NS-7 — A disk wall that enforces, and fails closed
- **`fallocate -l 100G /home/fullfunding.img`** — **preallocated, never `truncate`**. A sparse image
  would allocate from `/home` as the inner filesystem filled, giving only a logical ceiling. With
  preallocation the 100 GiB is taken from `/home` **once, at creation**, and is **retained until
  deliberate teardown** (NS-14) — not "permanent". Verified non-sparse (`du` vs `du --apparent-size`)
  before use. The user must accept this 100 GiB cost to `/home` up front.
- `mkfs.xfs`; a **systemd `.mount` unit** (not a bare fstab line) mounted at `/home/fullfunding/data`.
- **Fail-closed for both provisioning and consumption.** A provisioner readiness check only protects
  *new* provisioning; already-bound PVs would still be mounted after the mount disappeared, letting
  tenant pods write into the underlying `/home` and silently defeat the wall. Required, all three:
  1. the **underlying directory** `/home/fullfunding/data` is created empty and made
     **immutable (`chattr +i`) and mode 0000** while unmounted, so any attempt to create a new
     backing path with the image absent **fails** instead of landing on `/home`;
  2. **host-side identity verification, not an in-pod mountpoint check.** rev 3 required tenant pods
     to test `mountpoint`; that is unsound, because a PVC bind-mounted into a container *always*
     presents as a mountpoint regardless of where its host source came from. Verification is
     therefore done on the host — `findmnt -no SOURCE,UUID /home/fullfunding/data` must match the
     **recorded loop-device filesystem UUID** — and the provisioner additionally requires a
     **sentinel file that exists only inside the loop filesystem** (`.fullfunding-volume-<uuid>`)
     before it will provision;
  3. an **independently running tenant-guard** (systemd timer, **not** `RequiredBy` the mount —
     a `RequiredBy` guard is stopped when the mount stops, which is the opposite of a monitor) runs
     the UUID check on a schedule and, on mismatch or absence, executes an **enumerated quiescence
     procedure** with its own Kubernetes credential (NS-9, `tenant-guard`): (a) `kubectl -n
     fullfunding patch cronjob --all -p '{"spec":{"suspend":true}}'`, (b) scale every Deployment and
     StatefulSet to 0, (c) delete any remaining Jobs and standalone Pods in the namespace, (d) raise
     the NS-18 alert. **k3s itself must not depend on the mount** (a boot failure there would take
     OptionsEdge down for a tenant-only concern), and for the same reason the mount unit is `nofail`
     so this single production host never drops to emergency boot. Recovery is documented: re-mount,
     verify SOURCE+UUID, verify the sentinel, then un-suspend and scale back up — in that order.
- Second `local-path-provisioner` instance, own `provisionerName`, own `nodePathMap`, StorageClass
  **`fullfunding-storage`**, `reclaimPolicy: Retain`, `volumeBindingMode: WaitForFirstConsumer`.
  It is a platform-plane object with its own requests/limits, outside the tenant quota.
- **Only `fullfunding-storage` is usable**, enforced by NS-15(3) — the quota key rejects today's
  default but would not reject a future third class.
- **The inner filesystem does not enforce per-PVC sizes**: any one tenant PVC can consume the whole
  image (R-23).
**Acceptance:** NS-V10 — on a **scratch 1 GiB image**, a bounded fill job fails with ENOSPC while the
backing filesystem's **image file size stays fixed and its allocated blocks stay within a stated
tolerance with no growth proportional to inner writes** (rev 2's "byte-identical `df`" is withdrawn
as unsound: extent conversion and outer metadata can move a few blocks). NS-V23 — PVC class
enforcement. NS-V29 — mount-loss rehearsal on the scratch image: writes fail, the guard fires, no
bytes land on `/home`.

### NS-8 — Tenant platform services: a replica-weighted budget that fits
Single replica each, never the host instances. **The three data services are StatefulSets at one
replica**, whose rolling update terminates before it creates and therefore never surges. **Nor does
the web Deployment**: D-3's envelope left no room for a rollout surge, so it is pinned to
`Recreate`, accepting brief downtime on a rollout. Nothing in this budget surges, and the budgeted
transient below is therefore not a rollout at all — it is the backup Job or a debug Job, exactly one
at a time. (`Recreate` / `maxSurge` are Deployment-only fields and are deliberately not used for the
StatefulSets.) Every per-pod figure in the table is stated per pod **and** as a row total, so the
totals cannot be misread.

**Re-sized for D-3's 2.25 Gi envelope.** Every figure is per pod; row totals are stated so they
cannot be misread.

| Workload | kind | requests (cpu/mem) | limits (cpu/mem) | PVC |
|---|---|---|---|---|
| `bugzilla-req-db` (**MariaDB**, rev-11 pinned — D-1) | StatefulSet ×1 | 100m / 384Mi | 1 / 2Gi | 20 Gi |
| `postgres` (tenant platform DB — D-1) | StatefulSet ×1 | 100m / 320Mi | 1 / 2Gi | 15 Gi |
| `kafka` (KRaft, combined, 1 broker) | StatefulSet ×1 | 200m / 640Mi | 1 / 2Gi | 20 Gi |
| `bugzilla-req-web` | Deployment ×1, **`Recreate`** | 150m / 448Mi | 1 / 2Gi | — |
| app pod | Deployment ×1 | 100m / 192Mi | 500m / 1Gi | — |
| backups PVC | — | — | — | 20 Gi |
| **steady state** | | **650m / 1984Mi (1.94 Gi)** | **4.5 CPU / 9 Gi** | **75 Gi of the 80 Gi quota** |
| **+ the budgeted transient** — a backup Job or a debug Job | | **+100m / +256Mi** | **+0.5 CPU / +1 Gi** | — |
| **peak** | | **750m / 2240Mi (2.19 Gi)** — inside the 2304Mi quota | **5 CPU / 10 Gi** | |

Three consequences of the smaller envelope, stated rather than discovered later:
- **The web tier loses its rollout surge.** `bugzilla-req-web` uses **`Recreate`**, so an image
  update causes a brief outage instead of a rolling handover. For an internal requirement-intake
  portal that is an acceptable trade, and it is the honest price of D-3.
- **One application pod, not two.** A second app pod requires re-opening the NS-4 arithmetic.
- **ResourceQuota does not *reserve* a slot for the backup Job.** It admits any combination that
  fits the remaining aggregate; the "one transient" shape is what the numbers permit, not something
  the quota guarantees. If a debug Job is running when the backup fires **and the two together
  would exceed the aggregate**, the backup Job sits **Pending** — which is why NS-18 alerts on a Job
  pending or missed for more than 15 minutes. If they both fit inside the aggregate, both run; the
  quota bounds the total, never the count.

**Kinds are specified because the update strategy depends on them:** the three data services are
**StatefulSets at 1 replica**, whose rolling update terminates before it creates and therefore never
surges (`Recreate` is a Deployment-only strategy and would be invalid here — rev 3 was ambiguous);
**nothing in this budget surges**: the StatefulSets cannot, and the web Deployment is pinned to
`Recreate` precisely so it does not. The budgeted transient is therefore not a rollout surge at all
— it is a backup Job or a debug Job. Stated precisely, because a ResourceQuota constrains aggregate
consumption and not the shape of it: **`100m / 256Mi` is the budgeted STANDARD shape for a transient
Job, not an enforced one.** No control in this design pins a Job to that exact declaration —
NS-15 requires Job and CronJob templates to declare resources explicitly, so the LimitRange's
defaults never apply to them, and its `max` only caps a container at 2 CPU / 3Gi. **What is
enforced is the aggregate**: `requests.memory: 2304Mi` refuses admission once the namespace sum
would exceed it. So several smaller Jobs may run concurrently inside that aggregate — permitted and
budgeted — and the guarantee is the ceiling, not the count.

The peak **requests** row is what must satisfy NS-4's feasibility formula. That reduction has
already been made: D-3 fixed the tenant at 2.25 Gi, and NS-8's table above is the re-sized result —
the app-pod count and the surge allowance were spent to reach it, which is why the web Deployment is
`Recreate` (accepting brief downtime on a rollout). If a measured `R` above 13.0 Gi forced the
envelope down further, the next reductions are recorded before launch, not discovered
at rollout.

- **Kafka's disk bound is the filesystem, not a setting.** `log.retention.bytes` is **per partition**;
  with segment granularity, topic count, internal/KRaft logs and compaction, a naive calculation is
  exceedable. Retention settings are required hygiene; the 100 GiB image is the actual bound.
- **A 2 Gi limit with a 1 Gi heap is validated, not assumed** — JVM native/direct memory and page
  cache are measured in V-pre and the limit revised if the broker is OOM-killed.
- **Kafka runs at 1 replica** (D-2 resolved: the user's requirement states the project needs its own
  Kafka; a zero-replica placeholder is rejected). **Postgres must have a named consumer or platform
  function recorded before Gate 2** — an unused database does not satisfy the requirement.
**Acceptance:** NS-V11.

### NS-9 — Every operational identity, enumerated
A single namespace-scoped ServiceAccount cannot create a Namespace, PriorityClass, StorageClass, PV
or ClusterRole, and no Kubernetes credential can edit `config.yaml`, a mount unit, cloudflared or
DNS. rev 2's two-plane model was therefore not implementable.

**Nine identities, not five** (rev 3 merged two administrative credentials into one row and omitted
every runtime identity):

| # | Identity | Kind | Scope |
|---|---|---|---|
| 1 | **platform-k8s** | kubeconfig, tightly held | create/update on the platform-Kubernetes plane (NS-1(2)); **nothing in `options-edge`** except the NS-15(9) policy |
| 2 | **host-root** | SSH/root on `.252` | kubelet config, mount unit, loopback image, cloudflared, host backup agent, tenant-guard timer |
| 3 | **tenant-deploy** | Jenkins SA, `Role` in `fullfunding` **+ a read-only `ClusterRole`** | mutate the tenant plane only; **read-only** `get`/`list` on PriorityClass, StorageClass, ValidatingAdmissionPolicy and Namespaces so NS-11's preflight can inspect them. **No** Ingress, NetworkPolicy or Middleware verb; no `escalate`, `bind`, `impersonate` |
| 4 | **operator** | human break-glass kubeconfig | `get`/`list` pods + `create pods/portforward` in `fullfunding` only; separate from Jenkins |
| 5 | **cloudflare-dns** | Cloudflare dashboard/API token | the `req.fullfunding.nl` DNS record only |
| 6 | **keycloak-admin** | existing Keycloak admin credential | realm `req` + its client, via the existing Keycloak deploy path (NS-1(5)) |
| 7 | **provisioner SA** | in-cluster ServiceAccount | the second local-path provisioner and its helper pods (PV/PVC verbs only) |
| 8 | **guardrail-checker SA** | in-cluster ServiceAccount | read-only across the objects NS-11 asserts; no mutation |
| 9 | **tenant-guard SA** | in-cluster ServiceAccount used by the host timer | suspend CronJobs, scale workloads to 0 and delete Pods/Jobs **in `fullfunding` only** (NS-7 quiescence) |

**Derived objects** — ReplicaSets, Pods, Jobs, EndpointSlices, provisioner helper pods — are created
by Kubernetes' own controller identities. NS-V1 therefore distinguishes **ownership by owner chain**
(which named object they descend from) from **admission actor** (which identity submitted the
request); a derived object is attributed through its owner chain, not to a human credential.

**Stated honestly, with the exact verbs:** tenant-deploy holds `create`/`update`/`patch`/`delete`
on Secrets (they are tenant-plane objects) — which does **not**, by itself, grant `get`. The more
important residual path is indirect and unavoidable: a principal that can create or modify a
workload can make an allowed consumer mount any namespace Secret and surface its value. Restricting
the `get` verb is therefore not a boundary. NS-15(8) instead carries a **closed allowlist**: the exact Secret names, the exact
ServiceAccounts that may be used, and which workloads may reference which Secret. R-14 records the
residual truth: tenant-deploy, platform-k8s, host-root, any cluster-admin and the node itself can
all reach tenant secrets; namespace RBAC does not and cannot exclude them.
**Acceptance:** NS-V12 — a `kubectl auth can-i` matrix per credential over the enumerated
resource/verb/subresource list, asserting permitted **and** denied cells, in `fullfunding`,
`options-edge` and `kube-system`.

### NS-10 — Jenkins-only, main-only delivery, migration-aware rollback
Manifests at `k8s/tenants/fullfunding/`, applied by a tenant-specific Jenkins job from `main` only,
images digest-pinned from `.252:5000`.
- **Last-known-good is the complete rendered state**: the rendered manifest set, ConfigMap contents,
  the database schema version and the migration checkpoint. **Secret *values* are explicitly
  excluded from rollback** — rev 3 recorded Secret `resourceVersion`s and called the state
  "complete", but Kubernetes cannot restore a historical Secret from a resourceVersion. The
  recoverable source of secret material is the **Jenkins credential store**, and the stated
  consequence is that a rollback across a secret rotation requires re-applying the current secret
  from that store (it is already valid at Keycloak, per rev-11 REQ-9's convergence argument), not
  recovering the previous value.
- **Rollback is migration-aware.** Bugzilla's `checksetup` migrates forward only, so an automatic
  LKG image rollback is permitted **only** when the captured schema version is unchanged; across a
  schema-migrating bump the job **stops and requires a database-restore decision** and never starts
  an older image against a migrated schema.
- Protocol: capture → apply → bounded `rollout status` → classify → conditional LKG → on LKG failure,
  fail loudly and apply NS-5's public fail-closed rollback.
**Acceptance:** NS-V14 — both the same-schema (automatic) and schema-changed (blocked) cases.

### NS-11 — Guardrails re-asserted continuously; drift is an incident
Asserted in **preflight (before apply)**, **after apply**, and **on a schedule between deploys**
(a platform-owned CronJob) — because a boundary checked only at deploy time can be dismantled the
minute after one: quota, LimitRange, PSA labels, NS-15 policies **and their bindings**, PriorityClass
on every tenant pod, the **complete effective NetworkPolicy set** (an added allow-all must fail the
check, not pass it), every PVC on `fullfunding-storage`, no LoadBalancer/NodePort, and the mount
still a mountpoint (NS-7).

**Preflight failure blocks the deploy and raises a security incident**; the job never silently
re-creates a guardrail and then verifies its own repair.
**Acceptance:** NS-V15 — verified against an **isolated fixture namespace** and by policy dry-run,
**never by removing a guardrail from the production namespace** (rev 2's test was itself a risk).

### NS-12 — Baseline and observation
Baseline captured **before** the tenant exists. The window covers **at least one complete regular
trading session including the open and the close**, asserting thresholds agreed at baseline:
- OptionsEdge: pipeline lag, business latency, error rate, pod restarts **with reason codes and
  timestamps**, OOM kills, evictions. CPU throttling is not a meaningful OptionsEdge signal (no CPU
  quota); lag and latency are.
- Node: memory pressure, nodefs and imagefs headroom, load, pid usage.
- Tenant: filesystem **growth slope within an agreed band** (a database in use grows; "flat" was the
  wrong test), PVC usage, restarts.
- All three tunnel hostnames serving.

**The window must not be idle.** External users are provisioned only *after* this window (§6
step 12), so without load the session would prove only that an idle tenant is harmless. A **bounded
synthetic pilot load at the NS-16 SLO profile** (fixed before the run) therefore executes against
the portal during the observation session, and the profile used is recorded with the result.
**Acceptance:** NS-V16.

### NS-13 — What is and is not isolated
NS-2…NS-8 and NS-15 deliver **partial resource governance and connectivity filtering**: bounded CPU
weight and ceiling, a hard tenant memory ceiling, a hard *persistent*-storage ceiling,
admission-bounded and eviction-enforced *ephemeral* storage, blocked node-level escapes, and
default-deny networking. They do **not** deliver disk-I/O or page-cache isolation (R-15), protection
of shared Keycloak/traefik/cloudflared/registry capacity (R-19), instantaneous protection of `/`
against a write burst (R-22), fair sharing within the tenant (R-23), or **any availability
isolation** (R-16). Independent availability requires a second machine.

### NS-14 — Teardown and data disposition
Namespace deletion alone does not restore the pre-change state. The teardown checklist disposes of:
the Keycloak realm `req` and its client, registry images, the Jenkins job and records, **PVs
(`Retain` means they survive namespace deletion by design)**, the loopback image + mount unit + its
100 GiB of `/home`, the immutable underlying directory, the cloudflared rule, the DNS record, PSA
labels, NS-15 policies and bindings, the PriorityClass, the StorageClass and provisioner, the
platform-owned NetworkPolicies/Ingress/Middleware, the host backup agent, the tenant-guard unit,
and the backups.

**Data retention is an explicit, separate decision:** deleting the namespace must not be the
mechanism that destroys tenant data or backups. The NS-4 reservation is **kept** unless shown to
have caused a regression; `pod-max-pids` is reverted to `-1` if it was set.
**Acceptance:** NS-V17 — rehearsed on a scratch namespace against the **enumerated checklist** above
covering cluster, host and external state. `kubectl get all -A` is explicitly **not** the test.

### NS-15 — Admission controls (the layer the isolation claims rest on)
This cluster already runs a `ValidatingAdmissionPolicy` (`failurePolicy: Fail`, binding `[Deny]`),
so the mechanism is proven here.

- **Pod Security Admission** on `fullfunding`: `enforce=restricted` (+`audit`/`warn`) — denies
  privileged pods, host namespaces, `hostPath`, privilege escalation, unsafe capabilities. **All
  three levels carry a pinned `*-version` label** (`enforce-version`, `audit-version`,
  `warn-version`) rather than `latest`, so a k3s upgrade cannot silently change enforcement
  semantics; bumping a pinned version is a reviewed change with its own re-verification of NS-V21.
  No namespace uses PSA today, so this is additive.
- **ValidatingAdmissionPolicies**, all `failurePolicy: Fail`, bindings `[Deny]`, scoped to
  `fullfunding` except (9):
  1. deny `hostPort`, `hostNetwork`, `hostPID`, `hostIPC`;
  2. deny `Service.spec.externalIPs`;
  3. require `spec.storageClassName == "fullfunding-storage"` on every PVC;
  4. require `priorityClassName == "fullfunding-low"` on every pod;
  5. **require explicit `requests` and `limits` (cpu, memory, ephemeral-storage) on every container
     — enforced on controller *templates*, not on Pods.** LimitRange mutates a Pod with defaults
     *before* validating policies see it, so a Pod-level rule cannot distinguish a declared value
     from a default. The policy matches `Deployment`, `StatefulSet`, `DaemonSet`, `ReplicaSet`,
     `Job` and `CronJob` templates, where LimitRange defaulting does not apply;
  5b. **deny Pods created directly by a *user* identity**, expressed on `request.userInfo.username`
     — exactly the mechanism this cluster's existing `options-edge-jenkins-only-workloads` policy
     already uses. rev 3 said "directly created Pods are denied outright", which as written would
     also have blocked the ReplicaSet, StatefulSet and Job controllers and broken every workload.
     Identity-based matching is both correct and unforgeable (userInfo comes from authentication,
     unlike owner references). Consequences, made consistent: **NS-V3 runs in a fixture namespace**,
     not here; and **debugging is an approved Job or Deployment submitted through Jenkins**, not a
     hand-created Pod — which is why NS-8 budgets a debug *Job*;
  6. **deny any `emptyDir` that is not `medium: Memory`** (NS-20), and require `sizeLimit` (≤ 512Mi)
     on the tmpfs ones that remain — a disk-medium `emptyDir` writes to `/`;
  7. constrain the platform-owned `Ingress`: `ingressClassName`, an explicit non-empty host from the
     allowlist (`req.fullfunding.nl`; denying omitted/catch-all hosts, wildcards,
     `fullfunding.nl`, `auth.fullfunding.nl`, `es.fullfunding.nl`), path, **backend service and
     port**, the required middleware annotation, and an annotation allowlist;
  8. a **closed allowlist** of Secret names, permitted consuming workloads and permitted
     ServiceAccounts (NS-9);
  9. **scoped to `options-edge`:** deny `limits.cpu` in any controller template **and in any
     directly created Pod** (with the same controller-identity carve-out as 5b). rev 3 matched
     templates only, leaving a direct-Pod bypass that made "OptionsEdge stays uncapped on CPU"
     unenforced at the API;
  10. deny `Service.spec.type == ExternalName` in the namespace;
  11. **require `securityContext.readOnlyRootFilesystem: true`** on every tenant container (NS-20).
**Acceptance:** NS-V21, NS-V23, NS-V24 — each policy exercised with a violating **and** a conforming
object **for every workload/template kind it matches**, with `failurePolicy: Fail` confirmed (a
policy that fails open is worse than none).

### NS-16 — Service objectives and shared-infrastructure load (claim narrowed)
- Stated SLOs for the portal: expected concurrent users, request rate, p95/p99 latency, error budget
  — modest by construction.
- **Traefik `Middleware` (platform-owned, NS-1):** request-rate limit, in-flight cap, and request
  body size cap on the portal route; connection limits on both tenant databases.
- **What this does and does not bound, stated exactly.** These limits bound load arriving **through
  the portal route**. They do **not** bound a client hitting `auth.fullfunding.nl` (shared Keycloak)
  directly — a login storm can address Keycloak without touching the portal. Bounding that would
  require rate limiting on the Keycloak route, which is shared OptionsEdge infrastructure and out of
  scope; it is recorded as R-19 rather than claimed as solved.
- **Trusted client IP must be defined, not assumed:** requests arrive via cloudflared, so "per
  source" resolves to the tunnel connector unless traefik's forwarded-headers trust and IP strategy
  are configured and verified. Unverified, the rate limit is per-tunnel, i.e. global.
- **An off-hours soak is a safety test, not coexistence evidence.** With most OptionsEdge
  deployments scaled to 0 off-hours, a passing soak cannot demonstrate RTH coexistence. The soak
  (NS-V25) proves the limits engage and nothing breaks; **RTH coexistence is evidenced only by
  NS-12's post-launch observation window.**
**Acceptance:** NS-V25 + NS-V16, with their distinct claims kept distinct.

### NS-17 — Backups: both databases, off-volume, with an honest failure domain
- **Tenant MariaDB and tenant Postgres** are both backed up by a namespace CronJob:
  `mysqldump --single-transaction` / `pg_dump`, per-artifact checksums, a manifest written **last**
  as the completion marker, 14-day retention, secrets excluded — rev-11 REQ-10a's structure.
  **"One atomic generation" means atomic *publication*** (write to a temp directory on the same
  filesystem, then rename), **not a transactionally consistent snapshot across the two databases** —
  independent `mysqldump` and `pg_dump` runs cannot provide that, and no such consistency is claimed. The job's resources are budgeted in NS-8 as the single
  transient — **not "reserved", since a ResourceQuota cannot reserve a slot** — with
  `ttlSecondsAfterFinished` and history limits (NS-2) and the NS-18 pending/missed alert as the
  recovery signal.
- **Transport off the tenant volume — implementable, because the CronJob cannot do it.** Under
  NS-6/NS-15 the job has no `hostPath`, no non-tenant PVC and no egress beyond DNS and the
  databases. The copy is therefore performed by a **host-platform backup agent** (systemd timer,
  host-root, NS-1(3)) that reads completed generations from the tenant filesystem and writes them
  into the existing `.252` daily archive path. Its credentials, schedule, capacity check and
  failure alert are part of that unit, not of the tenant.
- **Copy-race contract** (otherwise retention could delete a generation mid-copy): the agent copies
  to a staging directory at the destination, verifies **source and destination checksums**, then
  publishes atomically by rename; and the CronJob's retention **must not delete any generation newer
  than the agent's last confirmed-copied marker**, which the agent writes back into the tenant
  filesystem after a successful publish.
- **Failure domain, stated honestly:** the archive path is on the **same host**. This protects
  against image corruption, accidental deletion and namespace teardown; it does **not** protect
  against loss of `.252` or of `/home`. **RPO 24 h, RTO hours (manual).** Backups are unencrypted,
  same custody as all prod data (consistent with rev-11 R-9). Genuine off-host copies depend on the
  archive destination, which is outside this document's scope — recorded as R-24, not claimed.
- **Keycloak's `pg_dump` is not a tenant object.** It stays exactly where rev-11 REQ-10a puts it,
  owned by the OptionsEdge side; a tenant CronJob could not reach the Keycloak database under NS-6
  and must not.
- Restore validation per release that changes images or schema, per rev-11's V-restore, with
  `kubectl port-forward` replacing `ssh -L`.
**Acceptance:** NS-V26 — restore of a completed generation into a scratch namespace, verified with
**service-specific known data for each database** (a Bugzilla ticket + attachment checksum + identity
mapping for MariaDB; a named table/row fixture for Postgres — rev 2 wrongly reused Bugzilla data for
both).

### NS-18 — Alerting, and the telemetry path that makes it possible
**Collection architecture, specified rather than assumed** (rev 3 asserted alerts that default-deny
networking would have prevented from ever being collected):
- **Host-side signals need no cluster path:** tenant filesystem usage, mount presence/UUID (NS-7),
  backup generation age and host-agent success are emitted by the **host-platform agent** as
  node-exporter *textfile* metrics, scraped by the existing host Prometheus on `.252:9090`.
- **In-cluster signals** (Kafka lag, database saturation, Ingress 5xx, pod restarts/OOM/eviction)
  are exposed by collectors inside the namespace and scraped by the host Prometheus. This requires
  **one explicit NetworkPolicy ingress rule** allowing the node IP to reach **only** the collectors'
  metrics port — a deliberate, narrow exception to default-deny, listed in the canonical policy set
  (NS-V22) so it is visible rather than incidental.
- Their CPU/memory/pod cost is budgeted in **`P_platform`** (NS-4), not left off the books.

Alerts into the existing prod ops path: tenant filesystem ≥75%/≥90%, PVC growth beyond the NS-12 band, node
memory pressure, any eviction or OOM kill (tenant **or** OptionsEdge), mount loss (NS-7), Kafka disk
and consumer lag, database down or connection saturation, Ingress 5xx rate, backup generation older
than 26 h, host backup-agent failure, and any NS-11 guardrail failure.
**Acceptance:** NS-V27 — each alert's **rule** exercised by synthetic metric injection, and only
those faults that are safe to induce are induced for real. Deliberately causing node MemoryPressure,
an eviction or an OptionsEdge OOM on this production node is **prohibited**.

### NS-19 — Clause-level traceability to rev 11
Every rev-11 verification row and risk row is mapped — unchanged, superseded-by (with the NS id), or
explicitly retired-with-reason — because a REQ-level table cannot prove nothing was stranded.
**The map is delivered in this document as Appendix A**, rather than promised: rev 3 deferred it and
therefore could not demonstrate its own completeness.
**Acceptance:** NS-V28 — Appendix A reviewed against the rev-11 document open side by side, zero
unaccounted rows. **This is a Gate-1 exit criterion**, verified at **§6 step 0.5 — before any node
change** (rev 3 declared it a Gate-2 prerequisite while scheduling it at step 10, after the node
changes, deployment and publication; that contradiction is removed).

### NS-20 — Nothing of the tenant's is stored on the root filesystem (`/`)

**User requirement, 2026-08-04: "nothing has to be stored on root directory."** This is stricter
than rev 4's ephemeral-storage quota, and it is achievable because the exposure was measured rather
than assumed. What actually lands on `/` (69 GiB, **37 GiB free**), verified on the host:

| Writer | Path | On `/`? | Disposition |
|---|---|---|---|
| PVC data (`fullfunding-storage`) | `/home/fullfunding/data` (loop image) | **No** | already off `/` by NS-7 |
| Container **writable layers** | imagefs = **`/home`** | **No** | already off `/`; measured, not assumed |
| Container **images** | imagefs = **`/home`** | **No** | already off `/` |
| **`emptyDir`** (disk medium) | `/var/lib/kubelet/pods/...` on **`/`** | **Yes** | **denied outright** (below) |
| **Container stdout/stderr logs** | `/var/log/pods` on **`/`** | **Yes** | the only residue; bounded, and eliminable (D-7) |

Requirements:
1. **`emptyDir` with disk medium is denied** for the tenant by admission (NS-15(6) changes from
   "must set `sizeLimit`" to **"deny unless `medium: Memory`"**). A `Memory`-medium `emptyDir` is
   tmpfs: it consumes the pod's **memory limit**, not `/`, and must still set `sizeLimit`.
2. **`readOnlyRootFilesystem: true`** is required on every tenant container (NS-15(11)). All
   writable state goes to a `fullfunding-storage` PVC. This is stricter than PSA `restricted`, which
   does not require it.
3. **Container logs would otherwise be the one unavoidable `/` residue**, because kubelet writes
   every pod's stdout to `/var/log/pods` regardless of namespace (bounded by the live
   `containerLogMaxSize: 10Mi` × `containerLogMaxFiles: 5` = 50 MiB per container, so ≈0.9 GiB for
   18 tenant pods). **D-7 is closed in favour of eliminating it rather than bounding it: NS-21
   relocates the directory onto `/home`, so the residue becomes zero.** Until NS-21 is executed,
   the 0.9 GiB bound is the interim state and NS-20 is not yet satisfied.
4. Consequently the tenant's ephemeral-storage quota drops to a token allowance —
   `requests.ephemeral-storage: 256Mi`, `limits.ephemeral-storage: 1Gi` (NS-2) — since with (1) and
   (2) there is nothing legitimate left to write to `/`. Anything approaching that limit is a defect
   to investigate, not headroom to use.
5. **Scope of the word "zero", stated precisely:** this requirement is **zero tenant-owned data**
   on `/` — no `emptyDir`, no container logs, no writable layer, no PVC. It is **not** a claim that
   running the tenant writes literally no bytes to `/`: journald entries for the container runtime,
   containerd/CNI bookkeeping and similar OS metadata are produced by the *platform* on behalf of
   any workload and remain on `/`. They are small, rotated by the OS, and are not tenant data.
   R-27 records the distinction rather than hiding it behind the word "zero".
6. **NS-V30** proves it empirically rather than by argument: after the tenant is running and NS-21
   is in place, a host-side scan finds **zero tenant-owned bytes anywhere on `/`**, with kubelet's
   own state and every pod log resident on `/home`.

**R-22 is closed rather than downgraded once NS-21 lands:** with disk-medium `emptyDir` denied,
root filesystems read-only, and kubelet's state and logs relocated to `/home`, the tenant has no
write path to `/` at all. The `/`-exhaustion risk reverts to being an OptionsEdge/host concern that
NS-21 also improves.

### NS-21 — Relocate kubelet state and pod logs off `/` (D-7)

**Host-platform change (NS-1(3)), node-wide, and valuable in its own right:** `/` is 70 GiB with
**38 GiB free and falling**, and it currently holds kubelet's entire pod state and every container's
rotated logs for **both** projects. k3s's `--data-dir` already moved the rancher/containerd data to
`/home` (imagefs = `/home`, measured), but `/var/lib/kubelet` and `/var/log/pods` were left behind.

- Relocation is by **bind mount declared in fstab** (`/home/k8s/kubelet` → `/var/lib/kubelet`,
  `/home/k8s/pod-logs` → `/var/log/pods`), not by symlink, because kubelet resolves and mounts
  paths beneath these directories.
- **k3s must be stopped and every pod terminated for the move**, since `/var/lib/kubelet` holds live
  volume mounts. It is therefore a full maintenance window — the largest single operation in this
  plan — and it is scheduled at **§6 step 1.5**, immediately after the measurement step and
  **before** the node reservation, so the two node-level changes share one window and one restart.
- **Ordering is a hard requirement:** the mount units must be active **before** k3s starts, or
  kubelet recreates the directories on `/` and the relocation is silently undone.
- **Rollback:** remove the fstab entries, stop k3s, move the directories back, restart. The
  pre-change state (directory sizes, inode counts, `findmnt` output) is captured first.
- **A consequence that must not be missed: after this change kubelet no longer watches `/` at all.**
  Its `nodefs.available` and `nodefs.inodesFree` thresholds follow nodefs, which becomes `/home`
  (1.2 TiB free). `/` still holds the OS, journald and container-runtime metadata, but nothing in
  Kubernetes will evict or alert on it any more. **A host-level `/` capacity alert is therefore a
  hard prerequisite of NS-21, not a follow-up** (NS-18), and NS-4's nodefs thresholds are re-read as
  protecting `/home` — which is also where the tenant image, PVCs and imagefs live, so they become
  the tenant's outer backstop rather than a `/` protection.
- **Post-change gate:** all 55 OptionsEdge deployments Ready, `df /` shows the reclaimed space, and
  `stats/summary` reports **nodefs = `/home`**, not `/`.
**Acceptance:** NS-V31 — nodefs is `/home` after the change; NS-V30 then finds zero tenant bytes on
`/`; OptionsEdge health gate green before the window closes.

## 4. Disposition of the rev-11 req-portal requirements

Where this table says SUPERSEDED, the present document governs. Clause-level completeness is NS-19.

| rev-11 req | Disposition | What changes |
|---|---|---|
| REQ-1 realm `req` | **UNCHANGED**; ownership named | shared-service plane, owned by the Keycloak admin credential (NS-1(5)) |
| REQ-2 OIDC client | **UNCHANGED** | exact redirect URI still holds |
| REQ-3 hostname, published last / closed first | **SUPERSEDED (mechanism)** | cloudflared → traefik; ordering, pre-checks and the 5-step rollback carry over verbatim (NS-5) |
| REQ-4 images | **UNCHANGED in substance** | Jenkins-built from pinned `276673ab6` to `.252:5000`; "compose by digest" → "manifest by digest" |
| REQ-5a exposure, two listeners, header strip, `Require claim` | **PARTLY SUPERSEDED** | Security contract unchanged. Reachability proof changes: public port via ClusterIP + **platform-owned** Ingress; **admin port 81 denied over the pod network by a port-specific, platform-owned policy** — and because NetworkPolicy and Ingress are no longer tenant-writable, the tenant cannot reopen it (the rev-2 weakness). `apachectl -S` remains the vhost proof. New acceptance NS-V18 |
| REQ-5b sessions & offboarding | **UNCHANGED** | container restart → pod restart |
| REQ-5c identity & claim contract | **UNCHANGED** | source-pinned |
| REQ-5d authorization model | **UNCHANGED** | |
| REQ-6 internal Bugzilla untouched | **UNCHANGED, strengthened** | NS-6 makes `:8092` unreachable from the tenant. **No modification to internal Bugzilla access is proposed** — rev 2's `pg_hba`/SASL fallback, which would have touched shared services, is withdrawn (NS-6) |
| REQ-7 cross-system isolation | **UNCHANGED, plus evidence** | NS-V9 output added to the token-level proof |
| REQ-8 Jenkins-only build & deploy | **SUPERSEDED (mechanism)** | kustomize apply + bounded `rollout status`; **the schema-forward rule becomes binding on the rollback path** (NS-10) |
| REQ-9 secrets | **SUPERSEDED (mechanism), new risks** | host `0600` files → k8s `Secret` projected as files (never container env, never in probe text). **Enumerated k8s allowlist:** (a) the Secret object in the k3s datastore **and in any datastore backup**, (b) its projected paths in **every consuming pod** — the web pod, the database pods, application pods, backup Jobs, and any image-pull credential consumer — **and the node-managed projected-volume storage backing those mounts**, (c) the Jenkins credential store. V10's "absent everywhere else" is asserted against the *searchable* surfaces (repo, job logs, archived artifacts, `describe` output, pod env, container logs, backups); rev 3's unqualified "nowhere else" over-claimed, since node-side projected storage and datastore backups necessarily hold the material. **New truths, matching the actual permissions:** unencrypted at rest in the datastore; **readable via the API by tenant-deploy and platform-k8s, and mountable by anything that can create pods**; and **projected-Secret updates are asynchronous while `subPath` mounts never refresh**, so rotation is *not* atomic for the application and **requires an explicit pod restart**, which rev-11's runbook already performs |
| REQ-10a windows & backups | **SUPERSEDED (mechanism), extended** | namespace CronJob + **host-platform agent** for the off-volume copy (NS-17); **tenant Postgres added**; failure domain stated as off-volume, **not off-host**. Keycloak's `pg_dump` stays with OptionsEdge |
| REQ-10b health model | **SUPERSEDED (mechanism)** | healthchecks → probes; a probe is never the security gate |
| REQ-10c verification & observation | **EXTENDED** | rev-11 matrix + NS-V1…**NS-V31** (including the root-filesystem rows NS-V30/NS-V31); window lengthens to a full session (NS-12) |
| REQ-11 login-surface & edge hardening | **UNCHANGED, plus NS-16** | the required traefik `Middleware` is a named platform-plane object (rev 2 omitted it from both plane lists) |
| REQ-12 patch & vulnerability posture | **UNCHANGED** | |
| REQ-13 privacy & data handling | **UNCHANGED** | |
| §8 risks R-1…R-11, R-13 | **CARRIED OVER** — 11 unchanged, **R-2 narrowed** | R-2 ("no formal load/soak") is partially discharged by NS-16 + NS-V25 + NS-V16's pilot load; what remains is that no *production-scale* load test exists. Per-row detail in Appendix A.2 |
| §8 risk **R-12** | **SUPERSEDED, not carried** | **four successors:** its *credential* half → **R-14** + **R-18**; its *shared-fate* half → **R-15** (no I/O isolation) + **R-16** (no availability isolation). rev 4 mapped only the credential half |

**Database engine:** the user's requirement names **Postgres**; rev 11 pins Bugzilla to **MariaDB**.
Switching engines invalidates REQ-4's pinning and REQ-10a's backup design. This document keeps
**MariaDB for Bugzilla** and provisions **Postgres as the namespace platform database**; both are
sized and backed up. **D-1 is CLOSED (user, 2026-08-04)** exactly as described here. What remains
open is narrower: **Postgres needs a named consumer recorded before Gate 2**, since an unused
database does not honestly satisfy the requirement.

## 5. Non-goals

- Not multi-tenancy as a product: one namespace, one tenant, one operator.
- No availability isolation (NS-13); no second node; no HA.
- No disk-I/O or page-cache isolation (R-15); no fair sharing within the tenant (R-23).
- No isolation of shared Keycloak/traefik/cloudflared/registry capacity beyond NS-16's portal-route
  limits (R-19).
- No change to the `optionsedge` realm, the internal Bugzilla, or any OptionsEdge workload — the
  only OptionsEdge-scoped change is the NS-15(9) admission policy, which adds a constraint and
  modifies nothing.
- No XFS project quota on `/home`; no change to the existing `auth.fullfunding.nl` tunnel route.
- No encryption-at-rest for k3s secrets (R-14); no genuine off-host backup (R-24).

## 6. Rollout sequence (fail-closed, ordered)

All steps run **outside** Mon–Fri 09:30–16:15 America/New_York **except the read-only measurement
and observation steps that by definition require live trading hours** (steps 1, 8b and 11), which
change nothing.

0. **Pre-flight:** `req.fullfunding.nl` DNS record **absent**; `/home` free ≥ 400 GiB (so NS-7's
   reserve holds after the 100 GiB image); `/` free recorded; **NS-12 baseline capture begins**
   (it must span a full session, so it starts here).
0.5. **Clause-map verification** (NS-19 / NS-V28) — Appendix A reviewed against rev 11 with zero
   unaccounted rows. **Gate-1 exit criterion; nothing below starts until it passes.**
1. **Measurement (read-only, spans RTH):** high-percentile host CPU/memory across ≥5 representative
   sessions → `R`; **and** maximum pid count per OptionsEdge pod → the D-6 value (NS-4). **Then
   apply NS-4's feasibility formula and confirm the tenant `requests.memory`; if `R > 13.0 Gi`,
   stop and obtain the user's decision.** Resolves D-6; confirms the D-3 envelope. *(Blocks step 2.)*

   The threshold follows from D-3, not from the superseded 4 Gi budget. NS-4's formula is
   `tenant_requests_memory ≤ 19.26 − R − M` with `M = 4 Gi`, i.e. `tenant ≤ 15.26 − R`. D-3 fixed
   the tenant at **2.25 Gi**, so the formula's knife-edge is `R = 13.01 Gi`.

   **The gate is set at 13.0 Gi, not 13.01, deliberately.** The formula's inputs are rounded to
   0.01 Gi — node capacity `62.23 Gi`, existing requests `38.97 Gi` — so a threshold quoted to the
   second decimal claims a precision the arithmetic does not have, and at 13.01 the surviving margin
   (0.01 Gi ≈ 10 MiB) is smaller than that rounding error. Rounding **down**, against the tenant, is
   the only direction that cannot silently overcommit the node. A measurement in
   `13.0 < R ≤ 13.01` is therefore not a pass with a thin margin: it returns to the user, which is
   what a margin inside the noise floor deserves.

   The `11.25 Gi` figure that stood here through rev 7 was the threshold for the **old 4 Gi** tenant
   budget; against the closed D-3 envelope it would have aborted on the expected measurement
   (`R ≈ 13 Gi`, the off-hours floor) every single time — turning a feasibility gate into an
   unconditional stop.
1.5. **Relocate kubelet state and pod logs off `/`** (NS-21, D-7) — full maintenance window: stop
   k3s, move `/var/lib/kubelet` and `/var/log/pods` to `/home`, add fstab bind mounts, verify the
   mounts are active **before** k3s starts. NS-V31 + the OptionsEdge health gate. **Shares the same
   window and restart as step 2.**
2. **Node reservation** (NS-4) — config + restart in the window; NS-V6. Valuable standalone. An
   enumerated rollback (restore the previous `config.yaml`, restart, re-verify allocatable) and an
   **OptionsEdge health gate — all 55 deployments Ready and pipeline lag nominal** — close this step.
3. **Storage wall** (NS-7) — preallocated image, mount unit, immutable underlying directory,
   provisioner, StorageClass; NS-V10, NS-V23, NS-V29 rehearsed on a **scratch 1 GiB image**.
4. **Admission + guardrails** (NS-1, NS-2, NS-3, NS-9, NS-15) — namespace, PSA labels, quota,
   LimitRange, PriorityClass, policies, all nine identities; NS-V1…V5, V7, V12, V20a/b, V21, V23,
   V24.
5. **Network policy** (NS-6, platform-owned) — **NS-V9 runs here. If it fails, launch is BLOCKED**
   (no fallback exists; see NS-6) and the project returns to an architecture decision. NS-V22.
6. **Platform services** (NS-8) — MariaDB, Postgres, Kafka at 1 replica; NS-V11.
7. **Portal workloads** — rev-11 §6 steps 1–7 as adapted by §4; realm `req` + OIDC client created by
   the Keycloak owner; NS-V19 proven **before** any public exposure.
8. **Private verification (V-pre)** — the full rev-11 matrix plus every NS-V that needs neither
   public exposure nor the observation window, via `kubectl port-forward`. **Explicitly deferred:**
   NS-V8 (needs the published path), NS-V16 (step 11), NS-V25 (8a), NS-V13 (8b).
   - 8a. **Soak** (NS-V25) — run **outside** RTH; proves the limits engage, **not** RTH coexistence.
   - 8b. **RTH-only, read-only:** NS-V13.
9. **Publish** (NS-5) — Ingress, then the cloudflared rule, then DNS **last**. NS-V8 plus rev-11
   V3/V4/V6t/V11 immediately after, before any stakeholder user exists.
10. *(moved to step 0.5 — the clause map is a Gate-1 exit criterion, not a post-publication item.)*
11. **Observation** (NS-12/NS-V16) — a full session including open and close, **with the bounded
    synthetic pilot load running** (NS-12), inside thresholds.
12. **Onboarding gate** — every matrix row green, NS-19 complete, window closed. Only then are
    external users provisioned.

Rollback after step 9 uses NS-5's five-step public fail-closed procedure; before step 9 there is no
public exposure and teardown is private (NS-14).

## 7. Verification matrix

| id | Asserts | Exact expected result |
|---|---|---|
| NS-V1 | ownership | every object maps to exactly one of the five planes; every *authored* object names one of the nine identities; every *derived* object (ReplicaSet, Pod, Job, EndpointSlice, helper pod) is attributed through its owner chain, not to a human credential |
| NS-V2 | quota binds | **five conforming 1-CPU containers** (each inside the LimitRange max) are rejected with `exceeded quota` — a single 5-CPU pod would fail the LimitRange first and would not test the quota |
| NS-V3 | LimitRange defaults | run **in a fixture namespace carrying the same LimitRange** (NS-15(5b) denies user-created Pods in `fullfunding`): a resource-less Pod shows exactly 50m/128Mi/256Mi requests and 500m/512Mi/512Mi limits. This does **not** prove production manifests are explicit — NS-V24(5) does |
| NS-V4 | `limits.cpu` denied in `options-edge` | **both** a controller template **and** a directly created Pod declaring `limits.cpu` are denied at the API, while a controller-created Pod is unaffected; `failurePolicy: Fail` confirmed |
| NS-V5 | priority | every tenant pod reports `priority: -100`; ranking inputs verified documentarily. **No production node-pressure rehearsal** |
| NS-V6 | reservation | effective kubelet config lists all four eviction thresholds, and `pod-max-pids` **matches the D-6 decision — including a verified `-1`/absence if D-6 chose no limit**; allocatable plus **CPU, memory, ephemeral and pod-count** headroom recorded, each net of `P_platform`; full inventory schedules |
| NS-V7 | Service types | LoadBalancer and NodePort both rejected with `exceeded quota` |
| NS-V8 | public routing | unauthenticated `req.fullfunding.nl` → **302 to the Keycloak authorization endpoint**; authenticated → 200 (rev 2's bare "200" was wrong for an OIDC-protected origin). `fullfunding.nl` and `auth.fullfunding.nl` unchanged after each cloudflared restart |
| NS-V9 | host services unreachable | the host's **listening-socket inventory is captured at run time** (`ss -ltn`), and from a tenant pod **every** node destination in it — necessarily including 9092, 5432, 8081, 8082, 8092, 5000, 22, 3000, 6443, 8091, 9090, 10250 — **fails to establish TCP** (timeout or refused), with no allowlisted exception. An authenticated rejection over an **established** connection is **not** a pass |
| NS-V10 | disk wall | scratch 1 GiB image: bounded writer fails with ENOSPC; image file size fixed; backing allocated blocks within stated tolerance, with no growth proportional to inner writes |
| NS-V11 | tenant-only data services | every connect string resolves to `*.fullfunding.svc`; no host endpoint in any config |
| NS-V12 | RBAC | every permitted and denied cell asserted, per credential, across 3 namespaces |
| NS-V13 | the 8091 svclb cause | at RTH with feed-gateway scaled up: confirmed or refuted with `ss -ltnp` evidence (reporting only) |
| NS-V14 | rollback | same-schema LKG succeeds; schema-changed case **blocks** and demands a restore decision |
| NS-V15 | guardrail drift | on an **isolated fixture namespace** and by policy dry-run: removal is detected at the next preflight and raises an incident. Production guardrails are never removed to test this |
| NS-V16 | observation | full-session window inside every NS-12 threshold vs a pre-change baseline, **with the bounded synthetic pilot load at the fixed SLO profile running throughout** — an idle session is not coexistence evidence; the profile used is recorded with the result |
| NS-V17 | teardown | enumerated checklist fully dispositioned (cluster, host, external); `kubectl get all -A` not used as the test |
| NS-V18 | admin listener | port 81 unreachable from another tenant pod and from the LAN; the **operator credential is the only namespace-scoped credential intended to reach it via port-forward** — cluster-admin is explicitly outside the boundary (R-14) |
| NS-V19 | OIDC back-channel | login completes under default-deny egress; decoded ID token `iss` equals the pinned public issuer |
| NS-V20a | ephemeral **admission** | a container declaring ephemeral-storage above the LimitRange max, or a set exceeding the quota, is **rejected at admission** |
| NS-V20b | ephemeral **runtime** | a conforming pod writing beyond its `limits.ephemeral-storage` is **evicted**, with a bounded write size and timeout; the delay before eviction is recorded as evidence for R-22 |
| NS-V21 | node-escape denials | `hostPort`, `hostNetwork`, `hostPID`, `hostIPC`, `externalIPs`, `hostPath`, privileged, `ExternalName`, and a **user**-created Pod are each denied, **while a controller-created Pod is admitted** (proving 5b did not break the controllers); PSA pinned versions asserted |
| NS-V22 | effective policy set | the live NetworkPolicy set is compared against the canonical set in the repo as a **normalized semantic projection** — server-defaulted fields, `managedFields`, `resourceVersion`, UIDs, timestamps and ordering removed, rules canonically sorted — and matches exactly. A raw byte comparison is **not** used: API defaulting and server metadata would produce false differences. rev 3's "contains no allow-all" was too weak — it would have accepted a narrow unauthorised rule permitting, say, `.252:9092`. The NS-18 metrics-scrape exception is part of the canonical set. Re-checked on the NS-11 schedule |
| NS-V23 | storage class | PVCs with omitted / `local-path` / an arbitrary third class are rejected; `fullfunding-storage` accepted |
| NS-V24 | policy integrity | each policy exercised with a violating and a conforming object **for every workload/template kind it matches** (Deployment, StatefulSet, DaemonSet, ReplicaSet, Job, CronJob, and the denied bare Pod); all bindings `[Deny]`, all `failurePolicy: Fail` |
| NS-V25 | soak (off-hours) | rate/in-flight/body limits demonstrably engage; nothing breaks. **Not** evidence of RTH coexistence — that is NS-V16 |
| NS-V26 | restore | both databases restored into a scratch namespace and verified with **service-specific fixtures** (MariaDB: known ticket, attachment checksum, identity mapping; Postgres: named table/row) |
| NS-V27 | alerting | every alert **rule** exercised by synthetic metric injection; only safe faults induced for real; inducing node pressure, evictions or an OptionsEdge OOM is prohibited |
| NS-V28 | traceability | every rev-11 clause and V-row mapped; zero unaccounted rows |
| NS-V31 | kubelet state and pod logs off `/` (NS-21) | after the change `stats/summary` reports **nodefs = `/home`**; `/var/lib/kubelet` and `/var/log/pods` are active bind mounts present **before** k3s starts; `df /` shows the reclaimed space; all 55 OptionsEdge deployments Ready |
| NS-V30 | nothing on `/` (NS-20) | after NS-21, a host-side scan finds **zero tenant-owned bytes anywhere on `/`** — no `emptyDir`, no logs, no writable layer; `readOnlyRootFilesystem` asserted on every tenant container |
| NS-V29 | mount-loss fail-closed | on the scratch image: with the mount absent, the host UUID check fails, the provisioner refuses (missing sentinel), new backing paths cannot be created (immutable mode-0000 directory), the independent guard executes the full quiescence procedure — CronJobs suspended, Deployments/StatefulSets scaled to 0, remaining Jobs and Pods deleted — the alert fires, and **no bytes land on `/home`**. Recovery re-mounts, re-verifies SOURCE+UUID and the sentinel, then restores in that order |

## 8. Accepted-risk register (additions to rev-11 R-1…R-11, R-13; R-12 superseded)

| id | Risk | Why accepted / mitigation |
|---|---|---|
| R-14 | Secrets **unencrypted at rest** in the k3s datastore; readable via the API by tenant-deploy and platform-k8s, mountable by anything that can create pods, and reachable by host-root, any cluster-admin and the node | NS-15(8)'s closed allowlist bounds which pods/ServiceAccounts may use which Secret; same host custody as every other prod secret. Encryption-at-rest is out of scope and recorded as a gap |
| R-15 | **No disk-I/O or page-cache isolation** | Data-entry workload; separate backing filesystem; NS-12/NS-18 monitoring. If it bites, the answer is a second machine |
| R-16 | **Shared availability** | Explicitly accepted (NS-13) |
| R-17 | The loopback filesystem adds I/O overhead and takes **100 GiB of `/home` up front**, retained until teardown | The only hard disk wall available without unmounting `/home`; the cost is stated, not hidden |
| R-18 | The admin surface depends on a **kubeconfig** rather than SSH | Narrower in scope but a new credential to protect (NS-9); rev-11 R-7 still applies |
| R-19 | **Tenant load escapes the quota through shared services** — Keycloak, traefik, cloudflared, the registry and image pulls sit outside the quota; NS-16's limits bound the **portal route only**, not direct load on `auth.fullfunding.nl` | Bounded, not eliminated: portal-route rate/in-flight/body limits, database connection limits, off-hours soak, NS-18 alerting, NS-12 RTH observation. Rate-limiting the shared Keycloak route is out of scope |
| R-20 | **LAN origin bypass** — traefik answers on `.252:80`, so Cloudflare is not an authentication boundary | The portal is fail-closed without Cloudflare (rev-11 REQ-5a). Restricting traefik to loopback would change shared infrastructure |
| R-21 | **Thin memory slack.** D-3's envelope leaves almost nothing in reserve: at the expected `R ≈ 13 Gi` the formula yields 2.26 Gi against a fixed 2.25 Gi tenant, so the launch margin is ~0.01 Gi, and a large OptionsEdge growth event consumes the `M = 4 Gi` scheduler headroom the formula already sets aside | NS-4's formula blocks launch rather than overcommitting; NS-18 alerts on memory pressure; the tenant is the preferred eviction victim (NS-3) |
| R-22 | **(downgraded by NS-20)** `/` (nodefs) has ~37 GiB free and is not covered by the tenant disk wall. With disk-medium `emptyDir` denied and `readOnlyRootFilesystem` required, the tenant's only `/` residue is **kernel-rotated container logs, capped at ≈0.9 GiB** | NS-20 (1)(2), NS-4's `nodefs` thresholds, NS-18 alerting, NS-V30's empirical attribution. Literal zero is D-7 |
| R-26 | **After NS-21, kubelet stops monitoring `/`.** nodefs becomes `/home`, so the eviction thresholds and any nodefs alerting follow it; `/` retains the OS, journald and runtime metadata with no Kubernetes-side watcher | A **host-level `/` capacity alert is a hard prerequisite of NS-21** (NS-18), not a follow-up. The trade is deliberate: `/` gains ~all of the reclaimed space and loses a watcher it only had incidentally |
| R-27 | **"Zero on root" means zero *tenant-owned data*, not zero bytes.** journald entries for the container runtime, containerd/CNI bookkeeping and similar OS metadata are produced by the platform for any workload and remain on `/` | Small, OS-rotated, not tenant data. Stated so the requirement is not read as stronger than it is |
| R-28 | **PID exhaustion remains unbounded if D-6 drops `pod-max-pids`** — today the effective value is `-1` for every pod on the node | If D-6 drops it, the PID vector is removed from every isolation claim (NS-4(3)) and this row is the honest record of what is left unprotected |
| R-29 | **The platform plane escapes the tenant quota by construction** — the `fullfunding-platform` namespace, the host backup agent and the host tenant-guard are outside `fullfunding`'s ResourceQuota | Deliberate and accounted once in `P_platform` (NS-4); they are operator-owned, not tenant-writable, so the exposure is a sizing question rather than a containment one |
| R-25 | **Pre-existing, now also a portal dependency:** `auth.fullfunding.nl` is pinned in cloudflared to the Keycloak **ClusterIP**, so recreating that Service silently breaks authentication for the trading UI **and** the portal | Not introduced by this project and not fixed here (out of scope, shared infrastructure). Recorded so the dependency is visible; a stable-IP or Ingress-based route would remove it |
| R-23 | **No fair sharing inside the tenant** — the inner filesystem does not enforce per-PVC sizes, so one tenant PVC can consume the whole 100 GiB and starve the others (including backups) | Accepted for a single-tenant, single-operator project; NS-18 alerts at 75%/90%; NS-17's host agent copies generations off the volume |
| R-24 | Backups are **off-volume but not off-host** — they do not survive loss of `.252` or `/home` | Stated rather than claimed; genuine off-host copies depend on the archive destination, outside this scope |

## 9. Open decisions

| id | Decision | Status / recommendation |
|---|---|---|
| **D-1** | Bugzilla's database engine | **CLOSED (user, 2026-08-04): Bugzilla stays on MariaDB; Postgres is additionally provisioned as the namespace platform DB.** rev-11's REQ-4 pinning and REQ-10a backup design are untouched. **Still required before Gate 2: a named consumer for Postgres**, or it is an unused database |
| **D-2** | Kafka at launch | **RESOLVED: one broker at 1 replica** — the requirement states the project needs its own Kafka; a zero-replica placeholder is rejected |
| **D-3** | `system-reserved` `R`, and the tenant `requests.memory` | **CLOSED (user, 2026-08-04): keep the full host reservation (≈13 Gi) and shrink the tenant to 2.25 Gi.** `R` is still confirmed by the §6 step 1 measurement; **if it exceeds 13.0 Gi, launch is blocked** and returns to the user. The formula's knife-edge is 13.01 Gi — where headroom equals the fixed 2.25 Gi tenant — but the gate rounds down to 13.0, since the formula's inputs are rounded to 0.01 Gi and a 0.01 Gi margin is inside that error (NS-4, §6 step 1) |
| **D-4** | Tenant egress to the public internet | Default **no**; any runtime need becomes an explicit allowlist entry with its own risk row |
| **D-5** | OIDC back-channel under default-deny egress | **Split front/back-channel (option ii)**, conditional on verifying the packaged module's endpoint overrides and Keycloak's issuer behaviour; fallback (i) is broad outbound HTTPS and must be recorded as such |
| **D-7** | NS-20 literal zero on `/` | **CLOSED (user, 2026-08-04): relocate `/var/lib/kubelet` and `/var/log/pods` onto `/home`** — option (a). Specified as **NS-21**, executed at §6 step 1.5. Node-wide and independently valuable, since `/` has only 38 GiB free |
| **D-6** | `pod-max-pids` value `P` | Set from the RTH measurement at §6 step 1, **or dropped entirely** — with the PID vector then removed from every isolation claim rather than claimed unenforced |

## 10. Gate status

- **Gate 1 (requirements):** this document. rev 1 → Codex REQUEST_CHANGES; rev 2 → REQUEST_CHANGES;
  rev 3 → REQUEST_CHANGES; rev 4 implemented every rev-3 finding; revs 5–7 recorded the user's D-1,
  D-3 and D-7 decisions and the NS-21 side effects. **rev 8 corrects eight internal inconsistencies
  (three found on reading, five more found by Codex reviewing those) and awaits the user's explicit
  approval.** No implementation before that approval.
- **Gate 2 (implementation):** not started. **D-1, D-3 and D-7 are CLOSED.** Remaining Gate-2
  blockers: **D-6** (the `pod-max-pids` value, which comes from the §6 step-1 measurement or is
  dropped along with every PID isolation claim) and **a named consumer for Postgres** — D-1
  provisions it, but an unused database does not honestly satisfy the requirement. NS-19/NS-V28 is
  not a Gate-2 blocker: the map is delivered here as **Appendix A** and verified at §6 step 0.5 as a
  Gate-1 exit criterion.

---

## Appendix A — Clause-level traceability to rev 11 (NS-19 / NS-V28)

Delivered here rather than promised. Every verification row and every risk row of
`req-portal-bugzilla-keycloak-sso.md` rev 11 is accounted for exactly once. "Unchanged" means the
row executes as written once the Compose-specific mechanics are read as their Kubernetes
equivalents (container → pod, `ssh -L` → `kubectl port-forward`, `docker inspect` → `kubectl
describe`/pod spec).

### A.1 — rev-11 verification rows (all 27)

| rev-11 row | Status | Where it lives now |
|---|---|---|
| V1 realm discovery | **Unchanged** | Keycloak is unmoved; §6 step 7 |
| V2 `optionsedge` semantic no-change | **Unchanged** | §6 step 7 |
| V2b non-registered `redirect_uri` rejected | **Unchanged** | §6 step 7 |
| V3 public-listener battery (Host tricks, methods, malformed/oversized) | **Unchanged** | §6 step 9; the listener set is now container ports 80/81 in one pod |
| **V-lan** (`ss -ltn` loopback-only + LAN-refused) | **SUPERSEDED** | The loopback port bindings no longer exist. Replaced by **NS-V18** (admin port unreachable from the pod network and the LAN) + **NS-V7/NS-V21** (no LoadBalancer/NodePort/`hostPort` can create a host binding at all). `apachectl -S` survives as the vhost-separation proof |
| V4 end-to-end login + ID-token decode | **Unchanged, extended** | §6 step 9; **NS-V19** adds the back-channel and issuer assertions that default-deny egress makes necessary |
| V4b email/name change → same account | **Unchanged** | §6 step 9 |
| V4c new `sub` + pre-existing email | **Unchanged** | §6 step 9 |
| V4d email-claim vectors (absent/empty/malformed/null/array shapes) | **Unchanged** | The two-layer contract is hosting-independent; §6 step 9 |
| V5 portal content audit | **Unchanged** | §6 step 9 |
| V6 cross-realm login both directions | **Unchanged** | §6 step 9 |
| V6b spoof battery (with V-env) | **Unchanged** | §6 step 8 (V-pre) |
| V6t `req` token → trading API; cleanup verified | **Unchanged, extended** | §6 step 9; **NS-V9** adds network-level evidence |
| V-env diagnostic CGI env dump, then removed | **Unchanged** | §6 step 8; removal re-verified at the onboarding gate |
| V-authz every REQ-5d matrix cell | **Unchanged** | §6 step 8 |
| V-off (a) KC disable alone (b) full offboarding | **Unchanged** | "restart the web container" reads as "restart the web pod" |
| V7 internal-Bugzilla evidence set | **Unchanged, strengthened** | Evidence is still taken at each declared transition; **NS-V9** additionally proves `:8092` is unreachable from the tenant |
| V7-int internal trading-UI live cards | **Unchanged** | Still deferrable only to the next market-hours window; folded into §6 step 11's RTH session |
| V8 all hostnames after each cloudflared restart | **Unchanged, restated** | **NS-V8**, with the expected result corrected: unauthenticated `req.fullfunding.nl` → **302**, not 200 |
| V9 KC pod post-rollout | **Unchanged** | Keycloak is unmoved |
| V10 secret authorized-location assertion | **SUPERSEDED (surface list)** | The k8s allowlist and its new truths are in §4's REQ-9 row; `docker inspect` becomes the pod spec + `describe`; node-side projected storage and datastore backups are added as authorized locations |
| V11 edge-rule trip + size-limit chain | **Unchanged, extended** | **NS-16** adds the traefik `Middleware` layer (rate/in-flight/body) beneath rev-11's Cloudflare layer; the size chain now reads Bugzilla < Apache < **traefik body cap** < Cloudflare |
| V11b `optionsedge` login unaffected | **Unchanged** | §6 step 9 |
| **V-smoke** bounded concurrency (~10 sessions) | **SUPERSEDED, widened** | **NS-V25** (off-hours soak, limits demonstrably engage) + **NS-V16**'s pilot load. rev-11's R-2 ("no formal load/soak") is correspondingly narrowed, not carried unchanged |
| V-restart forced db-then-web restart | **Unchanged** | Pod restarts; probes replace healthchecks (§4 REQ-10b) |
| V-restore content-verified restore | **Unchanged, extended** | **NS-V26** — both databases, with service-specific fixtures per database; `kubectl port-forward` replaces `ssh -L` |
| V-rollback LKG + public fail-closed rehearsal | **Unchanged, extended** | **NS-V14** — split into the same-schema (automatic) and schema-changed (blocked) cases; the public fail-closed rehearsal is unchanged (NS-5) |

### A.2 — rev-11 risk rows (all 13)

| rev-11 risk | Status | Note |
|---|---|---|
| R-1 no staging env | **Carried** | V-pre honestly scoped; DNS-last; rehearsals now also use fixture/scratch namespaces |
| R-2 no formal load/soak | **NARROWED** | NS-16 + NS-V25 + NS-V16's pilot load partially discharge it; what remains is that no *production-scale* load test exists |
| R-3 KC single-replica blip | **Carried** | Keycloak unmoved |
| R-4 no SBOM/signing | **Carried** | digest pinning = immutability, not provenance |
| R-5 cloudflared restarts blip all hostnames | **Carried** | NS-V8 re-tests every hostname after each restart |
| R-6 no automated uptime probe at launch | **CARRIED, partially reduced** | NS-18's Ingress-5xx and restart alerts do **not** detect DNS failure, tunnel failure, a redirect loop or a broken Keycloak login. Discharging it requires a **synthetic external availability + login probe**, which is not in scope here; rev 4's "discharged" was wrong |
| R-7 break-glass native admin vhost exists | **Carried, re-based** | Its protection is now NS-6's port-specific policy + NS-9's operator credential instead of loopback+ssh; **R-18** records the credential change |
| R-8 no MFA for externals at launch | **Carried** | unchanged |
| R-9 indefinite retention; RPO 24 h / RTO hours; unencrypted backups | **Carried, extended** | **R-24** adds that the off-volume copy is *not* off-host |
| R-10 shared visibility among external orgs | **Carried** | REQ-13's per-onboarding gate is untouched |
| R-11 no notification email | **Carried** | unchanged |
| **R-12 single-host colocation / Docker-admin sees everything** | **SUPERSEDED, mapping to four successors** | Its *credential* half → **R-14** + **R-18** (the Docker-daemon dependency is gone; Kubernetes namespace access replaces it). Its *shared-fate* half → **R-15** (no I/O isolation) + **R-16** (no availability isolation). rev 4 mapped only the credential half and silently dropped the colocation half |
| R-13 ubuntu 22.04 + Bugzilla 5.2 lifetime | **Carried** | REQ-12's monthly review is unchanged |

### A.3 — rev-11 rollout steps

Steps 0–8 of rev-11 §6 are preserved in content and order inside §6 steps 7–12 of this document,
with three insertions ahead of them (node reservation, storage wall, admission/network guardrails)
and one relocation: rev-11 step 2's secret transfer becomes a Kubernetes `Secret` creation by the
platform credential. rev-11's step-7.5 "provisional technical gate" and step-8 "onboarding gate"
survive verbatim as this document's step 12, with the observation window lengthened from 24 h to a
full trading session (NS-12).

**Accounting rule, stated so "exactly once" is unambiguous:** every rev-11 row has **exactly one
disposition row here**, though a disposition may name **more than one successor** (R-12 names four).
**Zero rev-11 rows are unaccounted for:** 27 verification rows (24 unchanged/extended, 3 superseded
with named replacements), 13 risk rows (**12 carried — one of those 12 narrowed rather than carried
verbatim — and 1 superseded** with four successors; 0 discharged), and 16 REQ ids dispositioned
in §4.
