# Gate-1 — `fullfunding` tenant namespace on the prod k3s node, and the req-portal's migration into it

**Status: GATE-1 REQUIREMENTS / PROPOSED — rev 3. AWAITING USER APPROVAL (gatekeeping Gate-1).
Not implemented.**

**Revision history.** rev 1 → Codex 3-bar REQUEST_CHANGES (15 blockers). rev 2 implemented them →
Codex 3-bar REQUEST_CHANGES (18 findings). **rev 3 implements every rev-2 finding**, the material
ones being: the NS-4 arithmetic is corrected (it omitted the eviction reserve — the real slack at a
14 Gi reservation is **2.26 Gi, not 4.3 Gi**) and is now expressed as a **feasibility formula with a
quantified margin**, which surfaces the document's most important result — **the tenant's 4 Gi
memory budget is only feasible if the measured reservation is ≤ 12 Gi** (§3 NS-4); the
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

**The objective is bounded, measured interference — not isolation.**

| Dimension | Delivered | Mechanism | Not delivered |
|---|---|---|---|
| CPU | Bounded scheduler weight and ceiling | NS-2 | Zero interference: up to 6 CPU of burst on a 24 CPU node, by design |
| Memory | Hard ceiling on the tenant; tenant preferred as eviction victim | NS-2, NS-3, NS-4 | A guarantee that OptionsEdge is never the victim (NS-3) |
| Persistent disk | Hard ceiling (a separate, preallocated filesystem) | NS-7 | Fair sharing *within* the tenant — one PVC can consume the whole image (R-23) |
| Ephemeral disk on `/` | **Admission-bounded declarations + eviction-based runtime enforcement** | NS-2, NS-15 | A hard, instantaneous filesystem quota; a burst of logging can overshoot before kubelet reacts (R-22) |
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
requests.cpu: "2"                 limits.cpu: "6"
requests.memory: <see NS-4>       limits.memory: 12Gi
requests.ephemeral-storage: 2Gi   limits.ephemeral-storage: 6Gi
pods: "18"
count/services: "8"   count/ingresses.networking.k8s.io: "0"   # Ingress is platform-owned
count/secrets: "15"   count/configmaps: "15"
count/cronjobs.batch: "5"   count/jobs.batch: "20"
persistentvolumeclaims: "5"
fullfunding-storage.storageclass.storage.k8s.io/requests.storage: 80Gi
local-path.storageclass.storage.k8s.io/persistentvolumeclaims: "0"
services.loadbalancers: "0"       services.nodeports: "0"
```
`LimitRange` (Container): `defaultRequest` 50m/128Mi/256Mi-ephemeral, `default`
500m/512Mi/512Mi-ephemeral, `max` 2 CPU / 3Gi / 2Gi-ephemeral.

- **`requests.cpu` is the load-bearing number**, because CFS weight derives from requests, not
  limits. It does **not** preserve all CPU for OptionsEdge: the tenant may burst to 6 CPU.
- **`limits.memory` 12 Gi and `limits.cpu` 6** are deliberately larger than the requests: limits do
  not consume allocatable, and rev 2's 8 Gi/4 CPU ceiling was **operationally infeasible** — three
  data services at 2 Gi each already consumed 6 Gi, leaving nothing for the web pod, backup pods,
  rollout surge or a debug pod. NS-8 carries the replica-weighted budget that fits inside these.
- **Ephemeral storage is bounded at admission and enforced at runtime by eviction, not by an
  instantaneous quota.** A logging burst can overshoot before kubelet reacts (R-22), and container
  images and pulls sit outside the quota entirely. NS-1's table states this honestly.
- `emptyDir` sizing and PID limits: NS-15(6) and NS-4.
- `count/jobs.batch: 20` with `ttlSecondsAfterFinished` and `successfulJobsHistoryLimit` set on
  every CronJob, so accumulated Job objects can never block the backup CronJob (rev-2 finding 9).
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
`allocatable = capacity − kube-reserved − system-reserved − eviction-hard(memory.available)`:

```
tenant_requests_memory  ≤  62.23 − R − 1 − 2 − 38.97 − M
                        =  20.26 − R − M
```
with `R` = measured `system-reserved` memory and `M` = **required margin, fixed at 4 Gi**.

| R (measured) | Max feasible tenant `requests.memory` |
|---|---|
| 10 Gi | 6.26 Gi |
| **12 Gi** | **4.26 Gi** — the 4 Gi budget just fits |
| 14 Gi | 2.26 Gi — the 4 Gi budget **does not fit** |
| 16 Gi | 0.26 Gi — infeasible |

**Therefore the tenant's `requests.memory` is not fixed in this document.** It is set at §6 step 1
from the measured `R`, capped at 4 Gi. **If `R > 12 Gi`, the user must choose** between a smaller
tenant budget, a smaller reservation (accepting less host protection), or a second machine. Launch
is blocked until that choice is recorded. rev 2's "≈4.3 Gi slack" was arithmetically wrong (it
omitted the 2 Gi eviction reserve) and is withdrawn.

Companion headroom, all recomputed and recorded at NS-V6:
- **CPU:** `24 − 4 − 1 = 19` allocatable; `− 14.07 − 2 = 2.93` slack.
- **Ephemeral:** allocatable ≈ `69.97 − 4 − 2 − 6.99 = 56.98` Gi in *declaration* terms, but the
  operative number is the **37 GiB actually free on `/`** (R-22).
- **Pods:** `110 − 74 − 18 = 18` headroom.

**`R` is derived from high-percentile host usage across ≥5 representative sessions** (open, close,
a volatility spike, the nightly backup window, a maintenance window) plus margin — not one snapshot.
The off-hours ~13 Gi is a floor (D-3).
**Independent value:** this requirement improves the status quo even if the tenant is cancelled.
**Acceptance:** NS-V6 — effective kubelet config dumped and asserted (all four eviction thresholds,
`pod-max-pids`), allocatable and all four headroom figures recorded, full inventory scheduled.

### NS-5 — Exposure: platform-owned Ingress only
- Public path: **tenant ClusterIP Service → platform-owned traefik `Ingress` → cloudflared**.
- cloudflared rule before the catch-all: `hostname: req.fullfunding.nl` → `http://127.0.0.1:80`.
- **The tenant cannot create Ingress objects at all** (`count/ingresses: "0"`, NS-2; ownership in
  NS-1). rev 2 relied on a host allowlist inside a tenant-created Ingress; that left open a second
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
| all tenant pods | `kube-dns` | 53 |
| portal web pod | `oe-keycloak` pod in `options-edge` | 8080 (back-channel, below) |

Nothing may reach container port 81 over the pod network; it is reachable only via
`kubectl port-forward` (which traverses the API server and kubelet, not the pod network).

**The load-bearing negative assertion:** a tenant pod must be unable to open `192.168.100.252` on
**9092, 5432, 8081, 8082, 8092, 5000**. NetworkPolicy is *expected* to be enforced (§2.1) but that is
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
     **immutable (`chattr +i`) and mode 0000** while unmounted, so any write with the image absent
     **fails** instead of landing on `/home`;
  2. the provisioner Deployment and every tenant workload carry a startup/readiness check that the
     path is a **mountpoint**, and fail closed if it is not;
  3. the mount unit is `RequiredBy` a small **tenant-guard unit** that scales the tenant to zero and
     alerts if the mount is lost — **k3s itself must not depend on the mount** (a boot failure there
     would take OptionsEdge down for a tenant-only concern), and for the same reason the fstab/mount
     unit must **not** put this single production host into emergency boot: it is `nofail` at boot,
     with the guard and NS-18 alert providing the loudness instead. Recovery is documented
     (re-mount, verify mountpoint, un-taint) with an explicit rollback path.
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
Single replica each, never the host instances. **Data services use `Recreate` / `maxSurge: 0`** so
rollouts do not double their footprint; the web tier keeps a rolling update with `maxSurge: 1`,
which is accounted below.

| Workload | requests (cpu/mem) | limits (cpu/mem) | PVC |
|---|---|---|---|
| `bugzilla-req-db` (**MariaDB**, rev-11 pinned) | 100m / 512Mi | 1 / 2Gi | 20 Gi |
| `postgres` (tenant platform DB) | 100m / 512Mi | 1 / 2Gi | 15 Gi |
| `kafka` (KRaft, combined, 1 broker) | 200m / 1Gi | 1 / 2Gi | 20 Gi |
| `bugzilla-req-web` | 200m / 768Mi | 1 / 2Gi | — |
| app pods (≤2) | 2×100m / 2×256Mi | 2×500m / 2×1Gi | — |
| backup CronJob (reserved) | 100m / 256Mi | 500m / 1Gi | 20 Gi (shared backups PVC) |
| rollout surge (web, `maxSurge: 1`) | 200m / 768Mi | 1 / 2Gi | — |
| debug/troubleshooting pod (reserved) | 100m / 256Mi | 500m / 1Gi | — |
| **peak total** | **≈1.4 CPU / ≈4.3 Gi** | **≈6 CPU / ≈12 Gi** | **75 Gi of the 80 Gi quota** |

The peak **requests** row is what must satisfy NS-4's feasibility formula; if the measured `R`
forces `requests.memory` below 4.3 Gi, the app-pod count or the surge allowance is reduced first,
and that reduction is recorded before launch rather than discovered at rollout.

- **Kafka's disk bound is the filesystem, not a setting.** `log.retention.bytes` is **per partition**;
  with segment granularity, topic count, internal/KRaft logs and compaction, a naive calculation is
  exceedable. Retention settings are required hygiene; the 100 GiB image is the actual bound.
- **A 2 Gi limit with a 1 Gi heap is validated, not assumed** — JVM native/direct memory and page
  cache are measured in V-pre and the limit revised if the broker is OOM-killed.
- **Kafka runs at 1 replica** (D-2 resolved: the user's requirement states the project needs its own
  Kafka; a zero-replica placeholder is rejected). **Postgres must have a named consumer or platform
  function recorded before Gate 2** — an unused database does not satisfy the requirement.
**Acceptance:** NS-V11.

### NS-9 — Five credentials, enumerated
A single namespace-scoped ServiceAccount cannot create a Namespace, PriorityClass, StorageClass, PV
or ClusterRole, and no Kubernetes credential can edit `config.yaml`, a mount unit, cloudflared or
DNS. rev 2's two-plane model was therefore not implementable.

| Credential | Kind | Scope |
|---|---|---|
| **platform-k8s** | kubeconfig, tightly held | create/update on the platform-Kubernetes plane (NS-1(2)); **nothing in `options-edge`** except the NS-15(9) policy |
| **host-root** | SSH/root on `.252` | kubelet config, mount unit, loopback image, cloudflared, host backup agent |
| **tenant-deploy** | Jenkins SA, `Role` in `fullfunding` **+ a read-only `ClusterRole`** | mutate the tenant plane only; **read-only** `get`/`list` on PriorityClass, StorageClass, ValidatingAdmissionPolicy and Namespaces so NS-11's preflight can inspect them (rev 2 forbade any ClusterRole and thereby made its own preflight impossible). No `escalate`, `bind`, `impersonate`; **no mutation of NetworkPolicy, Ingress or Middleware** |
| **operator** | human break-glass kubeconfig | `get`/`list` pods + `create pods/portforward` in `fullfunding` only; separate from Jenkins |
| **external** | Cloudflare dashboard / Keycloak admin | the DNS record; the realm `req` + client, via the existing Keycloak deploy path (NS-1(5)) |

**Stated honestly:** restricting `get secrets` is **not** a boundary against a principal that can
create pods — it can mount any namespace Secret and read it. Moreover **tenant-deploy legitimately
holds create/update on Secrets** (they are tenant-plane objects), so it can read them through the
API as well. NS-15(8) therefore carries a **closed allowlist**: the exact Secret names, the exact
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
  Secret resource versions, the database schema version and the migration checkpoint.
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
  privileged pods, host namespaces, `hostPath`, privilege escalation, unsafe capabilities. No
  namespace uses PSA today, so this is additive.
- **ValidatingAdmissionPolicies**, all `failurePolicy: Fail`, bindings `[Deny]`, scoped to
  `fullfunding` except (9):
  1. deny `hostPort`, `hostNetwork`, `hostPID`, `hostIPC`;
  2. deny `Service.spec.externalIPs`;
  3. require `spec.storageClassName == "fullfunding-storage"` on every PVC;
  4. require `priorityClassName == "fullfunding-low"` on every pod;
  5. **require explicit `requests` and `limits` (cpu, memory, ephemeral-storage) on every container
     — enforced on controller *templates*, not on Pods.** This resolves rev 2's contradiction:
     LimitRange mutates a Pod with defaults *before* validating policies see it, so a Pod-level rule
     cannot distinguish a declared value from a default. The policy therefore matches
     `Deployment`, `StatefulSet`, `DaemonSet`, `ReplicaSet`, `Job` and `CronJob` templates, where
     LimitRange defaulting does not apply — and **directly created Pods are denied outright** in the
     namespace, so there is no bypass and no "stored-but-unschedulable controller" outage;
  6. require every `emptyDir` to set `sizeLimit` (≤ 1 Gi);
  7. constrain the platform-owned `Ingress`: `ingressClassName`, an explicit non-empty host from the
     allowlist (`req.fullfunding.nl`; denying omitted/catch-all hosts, wildcards,
     `fullfunding.nl`, `auth.fullfunding.nl`, `es.fullfunding.nl`), path, **backend service and
     port**, the required middleware annotation, and an annotation allowlist;
  8. a **closed allowlist** of Secret names, permitted consuming workloads and permitted
     ServiceAccounts (NS-9);
  9. **scoped to `options-edge`:** deny any controller template declaring `limits.cpu` (NS-2);
  10. deny `Service.spec.type == ExternalName` in the namespace.
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
  `mysqldump --single-transaction` / `pg_dump`, one atomic generation, per-artifact checksums, a
  manifest written **last** as the completion marker, 14-day retention, secrets excluded — rev-11
  REQ-10a's structure. The job has reserved quota (NS-8) and `ttlSecondsAfterFinished` +
  history limits (NS-2).
- **Transport off the tenant volume — implementable, because the CronJob cannot do it.** Under
  NS-6/NS-15 the job has no `hostPath`, no non-tenant PVC and no egress beyond DNS and the
  databases. The copy is therefore performed by a **host-platform backup agent** (systemd timer,
  host-root, NS-1(3)) that reads completed generations from the tenant filesystem and writes them
  into the existing `.252` daily archive path. Its credentials, schedule, capacity check and
  failure alert are part of that unit, not of the tenant.
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

### NS-18 — Alerting
Into the existing prod ops path: tenant filesystem ≥75%/≥90%, PVC growth beyond the NS-12 band, node
memory pressure, any eviction or OOM kill (tenant **or** OptionsEdge), mount loss (NS-7), Kafka disk
and consumer lag, database down or connection saturation, Ingress 5xx rate, backup generation older
than 26 h, host backup-agent failure, and any NS-11 guardrail failure.
**Acceptance:** NS-V27 — each alert's **rule** exercised by synthetic metric injection, and only
those faults that are safe to induce are induced for real. Deliberately causing node MemoryPressure,
an eviction or an OptionsEdge OOM on this production node is **prohibited**.

### NS-19 — Clause-level traceability to rev 11
Before **Gate 2 begins**, every rev-11 sub-requirement, acceptance clause and verification row
(V-pre, V3, V4/V4d, V6, V6b, V6t, V7, V7-int, V8, V10, V11, V-lan, V-env, V-off, V-restore,
V-rollback) is mapped: unchanged, superseded-by (with the NS id), or explicitly retired-with-reason.
A REQ-level table cannot prove nothing was stranded.
**Acceptance:** NS-V28 — mapping complete, zero unaccounted rows. **This is a Gate-1 exit criterion
and a Gate-2 entry prerequisite**, not a step-10 item (rev 2 placed it at both, contradictorily).

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
| REQ-9 secrets | **SUPERSEDED (mechanism), new risks** | host `0600` files → k8s `Secret` projected as files (never container env, never in probe text). **Enumerated k8s allowlist:** (a) the Secret in the k3s datastore, (b) its projected paths in the web pod, (c) the Jenkins credential store — and nowhere else (not the repo, job logs, archived artifacts, `describe` output, pod env, container logs or backups). **New truths, matching the actual permissions:** unencrypted at rest in the datastore; **readable via the API by tenant-deploy and platform-k8s, and mountable by anything that can create pods**; and **projected-Secret updates are asynchronous while `subPath` mounts never refresh**, so rotation is *not* atomic for the application and **requires an explicit pod restart**, which rev-11's runbook already performs |
| REQ-10a windows & backups | **SUPERSEDED (mechanism), extended** | namespace CronJob + **host-platform agent** for the off-volume copy (NS-17); **tenant Postgres added**; failure domain stated as off-volume, **not off-host**. Keycloak's `pg_dump` stays with OptionsEdge |
| REQ-10b health model | **SUPERSEDED (mechanism)** | healthchecks → probes; a probe is never the security gate |
| REQ-10c verification & observation | **EXTENDED** | rev-11 matrix + NS-V1…**NS-V29**; window lengthens to a full session (NS-12) |
| REQ-11 login-surface & edge hardening | **UNCHANGED, plus NS-16** | the required traefik `Middleware` is a named platform-plane object (rev 2 omitted it from both plane lists) |
| REQ-12 patch & vulnerability posture | **UNCHANGED** | |
| REQ-13 privacy & data handling | **UNCHANGED** | |
| §8 risks R-1…R-11, R-13 | **CARRIED OVER unchanged** | |
| §8 risk **R-12** | **SUPERSEDED, not carried** | replaced by R-14 and R-18 |

**Database engine:** the user's requirement names **Postgres**; rev 11 pins Bugzilla to **MariaDB**.
Switching engines invalidates REQ-4's pinning and REQ-10a's backup design. This document keeps
**MariaDB for Bugzilla** and provisions **Postgres as the namespace platform database**; both are
sized and backed up. **D-1 — requires the user's decision**, and Postgres needs a named consumer
recorded before Gate 2, since an unused database does not honestly satisfy the requirement.

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
1. **Measurement (read-only, spans RTH):** high-percentile host CPU/memory across ≥5 representative
   sessions → `R`; **and** maximum pid count per OptionsEdge pod → `P` (NS-4). **Then apply NS-4's
   feasibility formula and record the tenant `requests.memory`; if `R > 12 Gi`, stop and obtain the
   user's decision.** Resolves D-3. *(Blocks step 2.)*
2. **Node reservation** (NS-4) — config + restart in the window; NS-V6. Valuable standalone.
3. **Storage wall** (NS-7) — preallocated image, mount unit, immutable underlying directory,
   provisioner, StorageClass; NS-V10, NS-V23, NS-V29 rehearsed on a **scratch 1 GiB image**.
4. **Admission + guardrails** (NS-1, NS-2, NS-3, NS-9, NS-15) — namespace, PSA labels, quota,
   LimitRange, PriorityClass, policies, the five credentials; NS-V1…V5, V7, V12, V20a/b, V21, V23,
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
10. **NS-19 clause map complete** (NS-V28) — a Gate-2 entry prerequisite, verified complete here.
11. **Observation** (NS-12/NS-V16) — a full session including open and close, inside thresholds.
12. **Onboarding gate** — every matrix row green, NS-19 complete, window closed. Only then are
    external users provisioned.

Rollback after step 9 uses NS-5's five-step public fail-closed procedure; before step 9 there is no
public exposure and teardown is private (NS-14).

## 7. Verification matrix

| id | Asserts | Exact expected result |
|---|---|---|
| NS-V1 | ownership | every object maps to exactly one of the five classes with a named credential |
| NS-V2 | quota binds | **five conforming 1-CPU containers** (each inside the LimitRange max) are rejected with `exceeded quota` — a single 5-CPU pod would fail the LimitRange first and would not test the quota |
| NS-V3 | LimitRange defaults | a resource-less **Pod created by the platform for this test** shows exactly 50m/128Mi/256Mi requests and 500m/512Mi/512Mi limits. This does **not** prove production manifests are explicit — NS-V24(5) does |
| NS-V4 | `limits.cpu` denied in `options-edge` | an `options-edge` controller template declaring `limits.cpu` is denied at the API; `failurePolicy: Fail` confirmed |
| NS-V5 | priority | every tenant pod reports `priority: -100`; ranking inputs verified documentarily. **No production node-pressure rehearsal** |
| NS-V6 | reservation | effective kubelet config lists all four eviction thresholds and `pod-max-pids`; allocatable plus **CPU, memory, ephemeral and pod-count** headroom recorded; full inventory schedules |
| NS-V7 | Service types | LoadBalancer and NodePort both rejected with `exceeded quota` |
| NS-V8 | public routing | unauthenticated `req.fullfunding.nl` → **302 to the Keycloak authorization endpoint**; authenticated → 200 (rev 2's bare "200" was wrong for an OIDC-protected origin). `fullfunding.nl` and `auth.fullfunding.nl` unchanged after each cloudflared restart |
| NS-V9 | host services unreachable | from a tenant pod, connections to `.252` on 9092/5432/8081/8082/8092/5000 **all fail to establish TCP** (timeout or refused). An authenticated rejection over an established connection is **not** a pass |
| NS-V10 | disk wall | scratch 1 GiB image: bounded writer fails with ENOSPC; image file size fixed; backing allocated blocks within stated tolerance, with no growth proportional to inner writes |
| NS-V11 | tenant-only data services | every connect string resolves to `*.fullfunding.svc`; no host endpoint in any config |
| NS-V12 | RBAC | every permitted and denied cell asserted, per credential, across 3 namespaces |
| NS-V13 | the 8091 svclb cause | at RTH with feed-gateway scaled up: confirmed or refuted with `ss -ltnp` evidence (reporting only) |
| NS-V14 | rollback | same-schema LKG succeeds; schema-changed case **blocks** and demands a restore decision |
| NS-V15 | guardrail drift | on an **isolated fixture namespace** and by policy dry-run: removal is detected at the next preflight and raises an incident. Production guardrails are never removed to test this |
| NS-V16 | observation | full-session window inside every NS-12 threshold vs a pre-change baseline |
| NS-V17 | teardown | enumerated checklist fully dispositioned (cluster, host, external); `kubectl get all -A` not used as the test |
| NS-V18 | admin listener | port 81 unreachable from another tenant pod and from the LAN; the **operator credential is the only namespace-scoped credential intended to reach it via port-forward** — cluster-admin is explicitly outside the boundary (R-14) |
| NS-V19 | OIDC back-channel | login completes under default-deny egress; decoded ID token `iss` equals the pinned public issuer |
| NS-V20a | ephemeral **admission** | a container declaring ephemeral-storage above the LimitRange max, or a set exceeding the quota, is **rejected at admission** |
| NS-V20b | ephemeral **runtime** | a conforming pod writing beyond its `limits.ephemeral-storage` is **evicted**, with a bounded write size and timeout; the delay before eviction is recorded as evidence for R-22 |
| NS-V21 | node-escape denials | `hostPort`, `hostNetwork`, `hostPID`, `hostIPC`, `externalIPs`, `hostPath`, privileged, `ExternalName`, and a directly created Pod are each denied |
| NS-V22 | effective policy set | the full NetworkPolicy set contains no allow-all ingress or egress rule; re-checked on the NS-11 schedule |
| NS-V23 | storage class | PVCs with omitted / `local-path` / an arbitrary third class are rejected; `fullfunding-storage` accepted |
| NS-V24 | policy integrity | each policy exercised with a violating and a conforming object **for every workload/template kind it matches** (Deployment, StatefulSet, DaemonSet, ReplicaSet, Job, CronJob, and the denied bare Pod); all bindings `[Deny]`, all `failurePolicy: Fail` |
| NS-V25 | soak (off-hours) | rate/in-flight/body limits demonstrably engage; nothing breaks. **Not** evidence of RTH coexistence — that is NS-V16 |
| NS-V26 | restore | both databases restored into a scratch namespace and verified with **service-specific fixtures** (MariaDB: known ticket, attachment checksum, identity mapping; Postgres: named table/row) |
| NS-V27 | alerting | every alert **rule** exercised by synthetic metric injection; only safe faults induced for real; inducing node pressure, evictions or an OptionsEdge OOM is prohibited |
| NS-V28 | traceability | every rev-11 clause and V-row mapped; zero unaccounted rows |
| NS-V29 | mount-loss fail-closed | on the scratch image: with the mount absent, writes to the path **fail** (immutable mode-0000 directory), the guard scales the tenant to zero and alerts, and **no bytes land on `/home`** |

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
| R-21 | **Thin memory slack.** At `R = 12 Gi` the margin is exactly the 4 Gi minimum; a large OptionsEdge growth event consumes it | NS-4's formula blocks launch rather than overcommitting; NS-18 alerts on memory pressure; the tenant is the preferred eviction victim (NS-3) |
| R-22 | `/` (nodefs) has ~37 GiB free, is **not** covered by the tenant disk wall, and ephemeral limits are **eviction-based**, so a write burst can overshoot before kubelet reacts | NS-2 quota, NS-15(6) `emptyDir` limits, NS-4's `nodefs` thresholds, NS-18 alerting; NS-V20b records the observed reaction delay |
| R-23 | **No fair sharing inside the tenant** — the inner filesystem does not enforce per-PVC sizes, so one tenant PVC can consume the whole 100 GiB and starve the others (including backups) | Accepted for a single-tenant, single-operator project; NS-18 alerts at 75%/90%; NS-17's host agent copies generations off the volume |
| R-24 | Backups are **off-volume but not off-host** — they do not survive loss of `.252` or `/home` | Stated rather than claimed; genuine off-host copies depend on the archive destination, outside this scope |

## 9. Open decisions

| id | Decision | Status / recommendation |
|---|---|---|
| **D-1** | Bugzilla's database engine: **MariaDB** (rev-11 pinned, Codex-approved) or **Postgres** as the requirement's wording suggests? | **OPEN — needs the user.** Recommend keeping MariaDB for Bugzilla and running the requested Postgres as the platform DB, with a **named consumer recorded before Gate 2** |
| **D-2** | Kafka at launch | **RESOLVED: one broker at 1 replica** — the requirement states the project needs its own Kafka; a zero-replica placeholder is rejected |
| **D-3** | `system-reserved` `R`, and the tenant `requests.memory` that follows from it | **OPEN — resolved by measurement** at §6 step 1. **If `R > 12 Gi` the 4 Gi tenant budget is infeasible and the user must choose** (smaller tenant / smaller reservation / second machine) |
| **D-4** | Tenant egress to the public internet | Default **no**; any runtime need becomes an explicit allowlist entry with its own risk row |
| **D-5** | OIDC back-channel under default-deny egress | **Split front/back-channel (option ii)**, conditional on verifying the packaged module's endpoint overrides and Keycloak's issuer behaviour; fallback (i) is broad outbound HTTPS and must be recorded as such |
| **D-6** | `pod-max-pids` value `P` | Set from the RTH measurement at §6 step 1, **or dropped entirely** — with the PID vector then removed from every isolation claim rather than claimed unenforced |

## 10. Gate status

- **Gate 1 (requirements):** this document. rev 1 → Codex REQUEST_CHANGES; rev 2 → Codex
  REQUEST_CHANGES; **rev 3 implements every rev-2 finding and awaits Codex re-review, then the
  user's explicit approval.** No implementation before that approval.
- **Gate 2 (implementation):** not started. **Blocked on D-1, D-3, D-6, and on NS-19/NS-V28 (the
  clause-level map) being complete.**
