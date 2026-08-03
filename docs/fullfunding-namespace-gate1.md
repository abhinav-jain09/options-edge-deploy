# Gate-1 — `fullfunding` tenant namespace on the prod k3s node, and the req-portal's migration into it

**Status: GATE-1 REQUIREMENTS / PROPOSED — rev 2. AWAITING USER APPROVAL (gatekeeping Gate-1).
Not implemented.**

**rev 2 changes (Codex 3-bar round 1 = REQUEST_CHANGES; every finding implemented):** the §1 goal no
longer claims non-interference and is restated as a **bounded, measured interference objective**;
the disk wall's sparse-file premise is corrected (**preallocated**, and it consumes 100 GiB of
`/home` up front — `df /home` changes at creation, by design); host-port isolation is no longer
claimed from ResourceQuota alone and is delivered by **Pod Security Admission + ValidatingAdmission
Policies** (NS-15), for which this cluster already has a working precedent; the eviction claim is
weakened to what node-pressure eviction actually guarantees; the capacity arithmetic is redone
**replica-weighted across every namespace and workload kind** and starts from **capacity**, not
allocatable; `eviction-hard` is specified as a **complete** threshold set because the flag replaces
rather than merges kubelet defaults; **ephemeral storage on `/` is added as a first-class exposure**
(newly measured: nodefs is `/` with 37 GiB free, not `/home`); NS-6 gets port-specific policies so
the admin listener is actually unreachable, and the unimplementable pod-IP firewall fallback is
replaced by host-service-side access control; MariaDB is added to the sizing; shared-service
escape (Keycloak/traefik/cloudflared/registry) is recorded and bounded; RBAC is split into a
platform plane and a tenant plane; and SLO, backup, alerting and traceability requirements are
added (NS-16…NS-19).

Everything in §2 is **verified on `192.168.100.252` on 2026-08-04** by direct inspection, with the
method recorded per row and inference labelled as inference. Everything from §3 onward is intended
future behaviour.

**Date:** 2026-08-04  **Owner:** Abhinav
**Repos:** options-edge-deploy (namespace overlay, tenant manifests, admission policies, Jenkins job)
**Hosts:** 192.168.100.252 (prod k3s, single node)
**Supersedes, in part:** `docs/req-portal-bugzilla-keycloak-sso.md` rev 11 — §4 is the normative
disposition table. That document remains authoritative for every hosting-independent contract.

---

## 1. Goal, and what it does and does not promise

Run a **second, unrelated project on the existing production machine** inside the same k3s cluster,
in a dedicated namespace **`fullfunding`**, hosting the requirement-intake portal
(`req.fullfunding.nl` — Bugzilla + Keycloak realm `req`, designed in rev 11), with its own
Postgres and Kafka, sized for a **data-entry** workload.

**The objective is bounded, measured interference — not isolation.** On a single shared node, with
half the platform outside Kubernetes (§2.3), "the tenant cannot degrade OptionsEdge" is not a
deliverable claim, and this document does not make it. What is delivered, and what is not:

| Dimension | Delivered | Mechanism | Not delivered |
|---|---|---|---|
| CPU | Bounded share and ceiling | NS-2 (`requests.cpu 2`, `limits.cpu 4`) | Zero CPU interference — 4 CPU of contention on a 24 CPU node is possible by design |
| Memory | Hard ceiling; tenant preferred as eviction victim | NS-2, NS-3, NS-4 | A guarantee that OptionsEdge is never the victim (NS-3) |
| Disk capacity | Hard ceiling on tenant persistent **and** ephemeral storage | NS-7, NS-2 | Disk **I/O** / page-cache isolation (R-15) |
| Node-level escape | Blocked at admission | NS-15 (PSA + VAP) | Protection against a cluster-admin or the node itself |
| Network | Default-deny both directions, port-specific | NS-6 | Protection of shared Keycloak/traefik/cloudflared/registry capacity (R-19) |
| Availability | — | — | **Nothing.** One node, one kernel, one k3s: both projects fail together (NS-13) |

Success is therefore defined as a **measured** statement, verified in NS-12: after the tenant is
live, OptionsEdge's pipeline-lag, error-rate and pod-restart behaviour stay within their
pre-change baseline bands. If they do not, the tenant is scaled down and the answer becomes a
second machine (the `.4`/es4 pattern), not a tighter quota.

The change of record versus rev 11: that revision specified a host Docker-Compose deployment on
loopback ports 8093/8095. This document moves the hosting model into Kubernetes.

## 2. Current state (as-is, verified 2026-08-04)

### 2.1 Node and cluster

| Fact | Value | Method |
|---|---|---|
| k3s | v1.35.5+k3s1, **single node**, control-plane, containerd 2.2.3 | `kubectl get nodes -o wide` |
| k3s data-dir | `/home/options-edge/data/k3s` | `k3s.service` ExecStart |
| Node **capacity** | cpu **24**, memory **65257092Ki (62.23 Gi)**, ephemeral-storage **71645Mi**, pods **110** | `kubectl get node -o jsonpath=.status.capacity` |
| Node **allocatable** | **identical to capacity** — i.e. **nothing is reserved today** | `kubectl describe node` |
| `system-reserved` / `kube-reserved` / explicit `eviction-hard` | **no explicit override** in the unit or `/etc/rancher/k3s/config.yaml`. kubelet's **built-in defaults still apply** (notably `memory.available<100Mi`, `nodefs.available<10%`, `nodefs.inodesFree<5%`, `imagefs.available<15%`) — the correct statement is "not tuned", not "no thresholds" | unit + config read |
| **nodefs** (emptyDir, container logs, ephemeral-storage accounting) | **`/`** — 69 GiB capacity, **37 GiB available** | node `stats/summary` |
| **imagefs** (images, container writable layers) | **`/home`** — 1759 GiB capacity, 1157 GiB available | node `stats/summary` |
| Namespaces | `default`, `kube-node-lease`, `kube-public`, `kube-system`, `loki`, `options-edge` — **no `fullfunding`** | `kubectl get ns` |
| ResourceQuota / LimitRange | **none, in any namespace** | `kubectl get resourcequota,limitrange -A` |
| Pod Security Admission | **no `pod-security.kubernetes.io/*` label on any namespace** — PSA is not in use | `kubectl get ns -o jsonpath` |
| **ValidatingAdmissionPolicy** | **available at `admissionregistration.k8s.io/v1` (GA)**, and already used: `options-edge-jenkins-only-workloads` (47 d old, `failurePolicy: Fail`, binding action `[Deny]`, matching core/apps/batch/networking resources and asserting the requesting username) | `kubectl api-resources`; policy + binding read |
| NetworkPolicy objects | **none, in any namespace**. `--disable-network-policy` is **not** set, so k3s's controller is expected to be active — **this is an inference; enforcement is proven only by NS-V9** | `kubectl get netpol -A`; unit + config read |
| StorageClass | **one**: `local-path` (default), `rancher.io/local-path`, path `/home/options-edge/data/k3s/storage` | `kubectl get sc`; cm `local-path-config` |
| IngressClass | `traefik` | `kubectl get ingressclass` |
| traefik | LoadBalancer, external IP `192.168.100.252`, ports 80/443 | `kubectl -n kube-system get svc traefik` |
| Existing Ingress objects | one: `options-edge/oe-keycloak` → `auth.fullfunding.nl`, path prefixes `/realms/optionsedge` and `/resources` **only** | `kubectl get ingress -A -o jsonpath` |
| **traefik Ingress routing works** | `curl -H 'Host: auth.fullfunding.nl' http://127.0.0.1:80/realms/optionsedge/.well-known/openid-configuration` → **200**; identical through the traefik ClusterIP | direct test |
| Design primitives accepted by this API server | `PriorityClass value: -100`; quota keys `services.loadbalancers: "0"`, `local-path.storageclass.storage.k8s.io/persistentvolumeclaims: "0"`, `<class>.storageclass.storage.k8s.io/requests.storage` | `kubectl apply --dry-run=server` — **proves syntax/admission acceptance only, never semantics** |

### 2.2 The measured OptionsEdge footprint

**Methodology (stated because rev 1 got this wrong):** replica-weighted sum over **every namespace**
and **every workload kind** (Deployment, StatefulSet, DaemonSet, Job), taking `max(containers)` and
`max(initContainers)` per pod as the effective request, DaemonSets at 1 (single node), and
Deployments at `max(spec.replicas, 1)` so that the many deployments currently scaled to 0 by the
off-hours lifecycle are counted at their **running intent**.

| Scope | CPU requests | Memory requests |
|---|---|---|
| `options-edge` Deployments (55) | 12.97 | 37.06 Gi |
| `options-edge` Jobs (6) + StatefulSet (1) | 0.70 | 1.75 Gi |
| `kube-system` Deployments (4) + Jobs (2) + DaemonSets (6) | 0.40 | 0.16 Gi |
| `loki` StatefulSet + DaemonSet | 0.00 | 0.00 Gi |
| **TOTAL (the schedulable footprint)** | **14.07** | **38.97 Gi** |

Two further facts that bound what any arithmetic can claim:
- Declared **memory limits** in `options-edge` sum to **149.1 Gi** — 2.4× the node. Fitting by
  *requests* proves schedulability, **not** runtime safety; the cluster is already overcommitted and
  relies on workloads staying near their requests.
- **Zero deployments set `limits.cpu`.** "OptionsEdge stays uncapped on CPU" is therefore the
  measured status quo, not a change; NS-2 makes it a written, admission-enforced rule.

PVCs cluster-wide: 26, declared total 1141 Gi (several 100 Gi Kafka-Streams state stores).

### 2.3 The half of the platform that is not in Kubernetes

kubelet does not represent any of these as pod requests or namespace usage; it observes only their
aggregate effect as node pressure. This drives NS-4, NS-6 and NS-7.

| Component | Where | Port | Data |
|---|---|---|---|
| Kafka (KRaft) | host systemd | 9092/9093 | `/home/kafka/kraft-combined-logs` (dedicated 1.9 T NVMe, 12% used) |
| PostgreSQL | host systemd | 5432 | `/home/postgres/data` |
| Schema Registry / AKHQ | host systemd | 8081 / 8082 | — |
| Prometheus + node-exporter / Grafana | host systemd | 9090 / 3000 | — |
| httpd + php-fpm | host systemd | (403 on a non-traefik vhost) | — |
| cloudflared (`options-edge-option-chain`) | host systemd | — | `/etc/cloudflared/options-edge-stable.yml` |
| Docker registry | container | **5000** | — |
| `options-edge-admin-app` | container | **8091** | — |
| Internal Bugzilla web + MariaDB | containers | **8092**, 3306 | `/home/options-edge/data/bugzilla/` |

Docker root `/home/options-edge/data/docker`. Host memory in use at off-hours (most OptionsEdge
deployments scaled to 0): **≈13 Gi** resident, plus 16 Gi buff/cache.

### 2.4 Disks

| Mount | Device | Size | Free | Options | Role |
|---|---|---|---|---|---|
| `/` | `cs-root` xfs | 70 G | **37–38 G** | — | **kubelet nodefs**: emptyDir, container logs, ephemeral-storage |
| `/home` | `cs-home` xfs | 1.8 T | 1.2 T | **`noquota`** | k3s data, imagefs, Postgres, Docker |
| `/home/kafka` | `nvme0n1p1` xfs | 1.9 T | 1.7 T | `noquota` | host Kafka only |

- `local-path` is a hostPath bind: it **does not enforce PVC capacity**. A PVC declared `20Gi` can
  grow until its filesystem is full.
- `/home` is `noquota`, so XFS project quota is unavailable without unmounting `/home` — where k3s
  data, Postgres data and the Docker root all live. Out of scope (NS-7 avoids it).
- **`/` is the small, exposed filesystem** and it is *not* where PVCs live. A tenant that writes to
  `emptyDir` or logs heavily attacks `/`, not the tenant PVC. This is why NS-2 caps ephemeral
  storage and NS-15 caps `emptyDir`.

### 2.5 Ports and the existing collision

`ss -ltn`: **8093 and 8095 are free** (re-checked 2026-08-04). They are recorded only because rev 11
used them; **the Kubernetes design uses neither** (NS-5).

Host port **8091** is bound by container `options-edge-admin-app`, while LoadBalancer Service
`options-edge/feed-gateway-service` also claims 8091; its `svclb` pod shows **24 restarts,
`Terminated / Exit Code: 255`**. The Service is scaled to 0 (off-hours), so this is an **indication,
not a proven diagnosis** — NS-V13 closes it at RTH. It is recorded because it is exactly the failure
mode NS-5 + NS-15 remove for the tenant.

## 3. Requirements (NS-1 … NS-19)

Stable ids. Initial state for all: **TRACKED-PENDING**.

### NS-1 — Tenancy identity, and the two object planes
Namespace **`fullfunding`**, label `tenant: fullfunding`.

"Every tenant object lives in the namespace" is **false as a blanket statement** and is therefore
replaced by an explicit two-plane model, with a closed list of objects that necessarily live
outside it:

- **Tenant plane** (in-namespace, deployed by the tenant credential): Deployments, StatefulSets,
  CronJobs, Services, Ingress, ConfigMaps, Secrets, PVCs, NetworkPolicies, ServiceAccounts.
- **Platform plane** (cluster-scoped or host-level, deployed by the platform credential, NS-9):
  the Namespace itself, `ResourceQuota`, `LimitRange`, `PriorityClass fullfunding-low`,
  `StorageClass fullfunding-storage`, the second local-path provisioner Deployment plus its
  ClusterRole/ClusterRoleBinding, the PVs it creates, the `ValidatingAdmissionPolicy` objects and
  bindings (NS-15), the PSA namespace labels, the kubelet reservation (NS-4), the loopback image +
  fstab entry (NS-7), the cloudflared ingress rule, and the DNS record.
- **Shared, owned by neither**: Keycloak realm `req` and its client (they live in the OptionsEdge
  Keycloak), traefik, cloudflared, the registry.

The project is therefore **not operationally independent** of OptionsEdge: it depends on the shared
Keycloak, ingress and control plane (R-19).
**Acceptance:** NS-V1 — every object is attributable to exactly one plane; no tenant-plane object
outside the namespace; no platform-plane object created by the tenant credential.

### NS-2 — Resource budget (the asymmetry is deliberate, and its limits are stated)
- **OptionsEdge:** no CPU limits. Enforced by a `ValidatingAdmissionPolicy` (NS-15) that **denies**
  any pod-carrying object in `options-edge` whose containers declare `limits.cpu` — a CI check
  alone is not enforcement, because it does not cover live patches, other repos or direct API use.
- **`fullfunding`:** `ResourceQuota` + `LimitRange`. Production manifests **must declare their own
  requests and limits explicitly**; the LimitRange is a backstop for the forgotten case, not the
  sizing method.

```yaml
requests.cpu: "2"                 limits.cpu: "4"
requests.memory: 4Gi              limits.memory: 8Gi
requests.ephemeral-storage: 2Gi   limits.ephemeral-storage: 6Gi
pods: "15"
count/services: "8"   count/ingresses.networking.k8s.io: "2"
count/secrets: "15"   count/configmaps: "15"   count/cronjobs.batch: "5"   count/jobs.batch: "10"
persistentvolumeclaims: "5"
fullfunding-storage.storageclass.storage.k8s.io/requests.storage: 80Gi
local-path.storageclass.storage.k8s.io/persistentvolumeclaims: "0"
services.loadbalancers: "0"       services.nodeports: "0"
```
`LimitRange` (Container): `defaultRequest` 50m/128Mi, `default` 500m/512Mi, `max` 1 CPU / 2Gi,
plus `default.ephemeral-storage` 512Mi and `max.ephemeral-storage` 2Gi.

- **Why `requests.cpu` is the load-bearing number:** under contention the CFS weight of a cgroup
  derives from **`requests.cpu`**, not `limits.cpu`. A tenant that requested 8 CPU would satisfy any
  limit-based quota and still take guaranteed scheduler weight from OptionsEdge. What this does
  **not** do is preserve all CPU for OptionsEdge: the tenant may still burst to 4 CPU.
- **Why a memory limit is non-negotiable:** CPU is compressible, memory is not.
- **Why ephemeral storage is quota'd:** nodefs is `/` with ~37 GiB free (§2.4). PVC quotas do not
  cover `emptyDir`, container logs or writable layers.
- PID exhaustion and `emptyDir` sizing are handled in NS-15.
**Acceptance:** NS-V2 (quota rejects an over-budget pod), NS-V3 (LimitRange defaults applied),
NS-V4 (the admission policy denies an injected `limits.cpu` in `options-edge` **via the API, not
via CI**), NS-V20 (ephemeral-storage and object-count limits reject their respective violations).

### NS-3 — Eviction preference (stated at the strength Kubernetes actually provides)
`PriorityClass fullfunding-low`, **`value: -100`**, `preemptionPolicy: Never`,
`globalDefault: false`, set on every tenant pod. No OptionsEdge manifest changes.

**Honest scope, corrected from rev 1:** node-pressure eviction ranks pods **first** by whether usage
exceeds requests, **then** by priority, then by usage relative to requests. A tenant pod sitting
**below** its requests can therefore survive while an OptionsEdge pod **above** its requests is
evicted, despite the negative priority. `preemptionPolicy: Never` only stops the tenant from
triggering scheduler preemption. Neither mechanism binds the **kernel OOM killer**, which acts on
its own scoring. What NS-3 delivers is a **strong preference**, not a guarantee of the victim.
The real protections against this case are NS-2's small tenant requests (so the tenant is usually
*above* its requests when it matters) and NS-4's headroom.
**Acceptance:** NS-V5 — every tenant pod reports `priority: -100`; the ordering rehearsal runs in a
**scratch namespace on the same node with a deliberately small memory ceiling**, never by inducing
real node-wide pressure on the production node.

### NS-4 — Node reservation for the host platform
kubelet currently reserves nothing (§2.1), so the scheduler treats ~13 Gi of host-process memory as
free. Add to `/etc/rancher/k3s/config.yaml`:

```yaml
kubelet-arg:
  - "system-reserved=cpu=4,memory=<measured>Gi,ephemeral-storage=4Gi"
  - "kube-reserved=cpu=1,memory=1Gi,ephemeral-storage=2Gi"
  - "eviction-hard=memory.available<2Gi,nodefs.available<10%,nodefs.inodesFree<5%,imagefs.available<15%"
  - "enforce-node-allocatable=pods"
```

Four things this requirement must say plainly:
1. **`eviction-hard` replaces, it does not merge.** Supplying a partial map would silently drop
   kubelet's default inode and imagefs thresholds, so the **complete** set is specified above.
2. **Reservation is capacity accounting, not runtime isolation.** With `enforce-node-allocatable=pods`
   the *pods* cgroup is capped, which is what protects the host processes; it does **not** guarantee
   the host services any CPU or memory. Reserving `system-reserved` without a reserved cgroup does
   not police host processes at all — it only shrinks what the scheduler will place.
3. **The measurement basis is not one snapshot.** `<measured>` is derived from **high-percentile
   host usage across at least five representative sessions** covering the open, the close, a
   volatility spike, the nightly backup window and a maintenance window, plus margin. The off-hours
   figure of ~13 Gi is a floor, not the answer (D-3).
4. **Fitting by requests is not runtime safety** (§2.2, 149 Gi of limits).

Arithmetic to be **recomputed and recorded at NS-V6**, starting from capacity:

```
capacity                                   62.23 Gi
− system-reserved <measured> − kube-reserved 1
− eviction-hard memory.available 2
= allocatable                              (recorded)
− measured schedulable footprint           38.97 Gi   (§2.2, replica-weighted, all namespaces)
− fullfunding requests.memory               4.00 Gi
= slack                                    (recorded; must be > 0 with margin, or the tenant
                                            budget or the reservation is revised before launch)
```
With a 14 Gi reservation the slack is ≈4.3 Gi — **thin**. Consequences that are requirements, not
advice: the tenant's `requests.memory` must not exceed 4 Gi without redoing this; and if the
measured reservation exceeds 16 Gi, launch is blocked pending a budget decision by the user.
**Independent value:** this requirement improves the status quo even if the tenant is cancelled.
**Acceptance:** NS-V6 — post-change effective kubelet config dumped and asserted (including every
eviction threshold), allocatable recorded, and the complete workload inventory scheduled.

### NS-5 — Exposure: Ingress only
- The tenant's only public path: **ClusterIP Service → traefik `Ingress` → cloudflared**.
- cloudflared rule before the catch-all: `hostname: req.fullfunding.nl` →
  `service: http://127.0.0.1:80`, Host header preserved.
- Quota `services.loadbalancers/nodeports: "0"` blocks **those Service types only**. It does **not**
  block `hostPort`, `hostNetwork`, `spec.externalIPs`, `hostPath`, privileged pods, or an Ingress
  claiming somebody else's hostname. Those are blocked at admission by **NS-15**, and the
  "structurally impossible" claim rests on NS-15, not on the quota.
- **Host ports 8091/8092/8094 must not be referenced by any tenant object** (NS-15 rule).
- **LAN origin bypass, stated explicitly:** traefik is reachable on `192.168.100.252:80` from the
  LAN, so anyone on the LAN can reach the portal Ingress directly with a `Host:` header, bypassing
  Cloudflare. Cloudflare is therefore **not** an authentication boundary here — the portal's own
  OIDC gate is (rev-11 REQ-5a). This is accepted as R-20 because the portal is fail-closed without
  Cloudflare; if Cloudflare must become a boundary, traefik has to be restricted to loopback, which
  is a change to shared infrastructure and out of scope.
- **Why traefik and not a pinned ClusterIP — a stale claim, corrected:** the live cloudflared config
  routes `auth.fullfunding.nl` straight to Keycloak's ClusterIP and carries an inline comment
  reading *"traefik :80 is broken"*. **Refuted 2026-08-04** (§2.1): a Host-matched request through
  traefik returns 200; the 404s that motivated it are explained by the `oe-keycloak` Ingress
  declaring only two path prefixes. Pinning a tunnel to a ClusterIP is fragile — recreating the
  Service changes the IP and silently breaks the hostname. **The existing `auth.fullfunding.nl`
  route is left untouched**; correcting it is out of scope and not a prerequisite.
- Publication order and rollback wording are carried from rev-11 REQ-3 unchanged: DNS published
  last and closed first; rollback is (1) point the rule at `http_status:404`, (2) restart
  cloudflared, (3) **prove** the closed response, (4) remove the DNS record, (5) only then touch
  the application.
**Acceptance:** NS-V7 (quota rejects LoadBalancer/NodePort), NS-V21 (NS-15 denies `hostPort`,
`hostNetwork`, `externalIPs`, `hostPath`, privileged, and a foreign Ingress host), NS-V8
(`req.fullfunding.nl` serves through traefik; `fullfunding.nl` and `auth.fullfunding.nl` regress
clean after every cloudflared restart).

### NS-6 — Network policy: default-deny, port-specific
Default-deny `Ingress` and `Egress`. **"Allow everything inside the namespace" is not acceptable**
— it would leave the Bugzilla **admin listener on container port 81** reachable from any other pod
in the namespace, contradicting NS-V18. Policies are therefore per-workload and per-port:

| From | To | Ports |
|---|---|---|
| traefik (kube-system) | portal web pod | **80 only** |
| portal web pod | MariaDB pod | 3306 |
| portal web / app pods | tenant Postgres | 5432 |
| app pods | tenant Kafka | 9092 |
| backup CronJob | MariaDB / Postgres | 3306 / 5432 |
| all tenant pods | `kube-dns` | UDP/TCP 53 |
| portal web pod | `oe-keycloak` pod in `options-edge` | 8080 (back-channel, below) |

**Nothing** may reach container port 81 over the pod network; it is reachable only through
`kubectl port-forward` (which traverses the API server and kubelet, not the pod network) by a holder
of the operator credential (NS-9).

**The load-bearing negative assertion:** a pod in `fullfunding` must be unable to open
`192.168.100.252` on **9092, 5432, 8081, 8082, 8092, 5000**. NetworkPolicy is expected to be
enforced (§2.1) but **that is an inference until NS-V9 proves it**, and egress to the node's own IP
is a known weak spot in some CNIs.
**If NS-V9 fails, the fallback is host-service-side access control, not a pod-IP firewall** — a
static firewall cannot distinguish namespaces from churning pod IPs, so rev 1's proposal was not
implementable. The implementable form: `pg_hba.conf` rejecting `10.42.0.0/16` for host Postgres;
Kafka listener binding / SASL so the pod CIDR cannot connect; and the registry and AKHQ bound
appropriately. This is a **launch prerequisite** if NS-V9 fails, not a follow-up.

**OIDC back-channel — a new problem created by this move (D-5).** Under Compose the container had
unrestricted egress, so `OIDCProviderMetadataURL https://auth.fullfunding.nl/...` simply worked.
Under default-deny it does not: that hostname resolves to the **public Cloudflare edge**.
- **(ii), recommended — split front-channel and back-channel.** Browser keeps the public
  authorization endpoint (rev-11 REQ-2's exact redirect URI is unchanged); mod_auth_openidc's
  back-channel endpoints are configured **explicitly** to
  `http://oe-keycloak.options-edge.svc.cluster.local:8080/...`. The **issuer stays the public
  string** and is pinned and verified — a mismatch is a hard failure. Two things must be verified
  before this is chosen, not assumed: that the packaged module accepts explicit overrides for
  **every** required endpoint, and that Keycloak emits the public issuer while serving an internal
  plain-HTTP request.
- **(i), fallback — internet egress on 443.** Ordinary NetworkPolicy has no FQDN awareness, so this
  is **broad outbound HTTPS**, not "egress to Cloudflare". Choose only if (ii) is unworkable, and
  record it as a widened risk.
**Acceptance:** NS-V9 (six negative connection tests), NS-V18 (port 81 unreachable over the pod
network **and** from the LAN), NS-V19 (back-channel completes and the ID token carries the pinned
issuer), NS-V22 (the **complete effective policy set** contains no allow-all rule). Re-run after
every policy change.

### NS-7 — A disk wall that enforces
`requests.storage` bounds only what may be **declared**; `local-path` enforces nothing and `/home`
is `noquota` (§2.4). Therefore:

- A dedicated filesystem for the tenant, **without touching the `/home` mount**:
  **`fallocate -l 100G /home/fullfunding.img`** — **preallocated, never `truncate`**. rev 1 was
  wrong here: a sparse image allocates blocks from `/home` as the inner filesystem fills, so it
  would provide a logical ceiling while still letting the tenant consume `/home`, and `df /home`
  would move. With preallocation the 100 GiB is **taken from `/home` once, at creation**, and the
  tenant can never consume more. This must be stated to the user as a **permanent 100 GiB cost to
  `/home`**, verified non-sparse (`du --apparent-size` vs `du`) before use.
- `mkfs.xfs`, fstab entry, mounted at `/home/fullfunding/data`. **A host free-space reserve applies:
  the image is not created unless `/home` retains ≥ 300 GiB afterwards.**
- A **second `local-path-provisioner` instance**, own `provisionerName` (e.g. `fullfunding.local/path`),
  own `nodePathMap` rooted at that mount, StorageClass **`fullfunding-storage`**,
  `reclaimPolicy: Retain` (deletion must not silently destroy tenant data — see NS-14),
  `volumeBindingMode: WaitForFirstConsumer`.
- **Fail-closed if the mount is absent.** If `/home/fullfunding/data` is not a mountpoint, the
  provisioner must refuse to provision rather than create directories on the underlying `/home` —
  otherwise a reboot that fails to mount the image silently removes the wall. Implemented as a
  mount check in the provisioner pod's startup and readiness probe, plus `nofail` **absent** from
  the fstab entry so the mount failure is loud.
- The provisioner is a platform-plane object with its own requests/limits and its own
  (non-negative) PriorityClass; it is not inside the tenant quota.
- **Enforcement that only `fullfunding-storage` is usable** is by admission, not by quota alone: the
  quota key `local-path.…/persistentvolumeclaims: "0"` rejects today's default, but a PVC naming a
  *future third* StorageClass would pass. NS-15 therefore includes a policy requiring
  `spec.storageClassName == "fullfunding-storage"` on every PVC in the namespace.
**Acceptance:** NS-V10 — on a **scratch 1 GiB image, not the production 100 GiB one**, a fill job
exhausts the inner filesystem, the writer fails, and `df` of the backing filesystem is unchanged
(because the image is preallocated); orphan/reboot behaviour rehearsed. NS-V23 — PVCs with
(a) omitted `storageClassName`, (b) `local-path`, (c) an arbitrary other class are **all rejected**,
and (d) `fullfunding-storage` is accepted.

### NS-8 — Tenant platform services (data-entry sizing)
In-namespace, single replica each, never the host instances. **rev 1 omitted Bugzilla's MariaDB;
it is included here and in the storage arithmetic.**

| Workload | requests | limits | PVC (`fullfunding-storage`) |
|---|---|---|---|
| `bugzilla-req-db` (**MariaDB**, rev-11 pinned) | 100m / 512Mi | 1 / 2Gi | 20 Gi |
| `postgres` (tenant platform DB) | 100m / 512Mi | 1 / 2Gi | 15 Gi |
| `kafka` (KRaft, combined, 1 broker) | 200m / 1Gi | 1 / 2Gi | 20 Gi |
| backups PVC | — | — | 20 Gi |
| `bugzilla-req-web` + app pods | declared per workload | ≤1 / 2Gi | — |
| **totals** | ≤ 2 CPU / 4 Gi | ≤ 4 CPU / 8 Gi | **75 Gi of the 80 Gi quota, inside the 100 GiB image** |

- **Kafka's disk bound is the filesystem, not a setting.** `log.retention.bytes` is **per partition**,
  so with segment granularity, topic count, internal/KRaft logs and compacted topics a naive
  calculation can be exceeded. Retention settings are required for hygiene; the 100 GiB image is
  the actual bound.
- **A 2 Gi memory limit with a 1 Gi heap must be validated, not assumed** — JVM native/direct memory
  and page-cache behaviour are measured during V-pre, and the limit is revised if the broker is
  OOM-killed.
- **Kafka replica count at launch:** running at 1 replica (not 0). rev 1 proposed 0 to save memory,
  but that contradicts "the project gets its own Kafka" and makes NS-V11 untestable. If the tenant
  applications genuinely do not use Kafka at launch, the correct action is to **not deploy it at
  all** and reclaim its budget — that is D-2, and it must be decided before Gate 2, not deferred
  into a zero-replica placeholder.
- Backups: both databases (NS-17).
**Acceptance:** NS-V11 — tenant workloads connect only to in-namespace endpoints; with NS-V9
proving the host instances unreachable.

### NS-9 — RBAC: platform plane, tenant plane, operator
Three credentials, enumerated, because a single namespace-scoped ServiceAccount **cannot** create a
Namespace, PriorityClass, StorageClass, PV, or the provisioner's ClusterRole (rev 1's NS-9 and NS-1
contradicted NS-3 and NS-7 on exactly this point):

| Credential | Scope | Verbs |
|---|---|---|
| **platform** (tightly held; used for bootstrap and for NS-4/NS-7/NS-15 changes) | cluster | create/update on the platform-plane list in NS-1, nothing in `options-edge` |
| **tenant deploy** (Jenkins) | `fullfunding` only, `Role`/`RoleBinding`, no ClusterRole | create/update/delete/get/list/watch on the tenant-plane list in NS-1; **no `escalate`, no `bind`, no `impersonate`** |
| **operator** (human break-glass, e.g. the admin listener) | `fullfunding` only | `get`/`list` pods + `create pods/portforward`; **separate from the Jenkins credential** |

**Stated honestly (rev 1 got this wrong):** restricting the `get secrets` verb is **not** a boundary
against a principal that can create pods — such a principal can mount any Secret in the namespace
and read it. Therefore the tenant deploy credential's power over Secrets is bounded by NS-15
(policies constraining what a pod may mount and which ServiceAccount it may run as), and R-14
records the residual truth: a namespace deployer, a cluster-admin and the node can all reach tenant
secrets. Namespace RBAC cannot and does not exclude an existing cluster administrator.
**Acceptance:** NS-V12 — a `kubectl auth can-i` matrix per credential over the enumerated
resource/verb/subresource list, asserting both the permitted and the denied cells, in
`fullfunding`, `options-edge` and `kube-system`.

### NS-10 — Jenkins-only, main-only delivery, with migration-aware rollback
Manifests at `k8s/tenants/fullfunding/` in options-edge-deploy, applied by a **tenant-specific
Jenkins job** from `main` only, images digest-pinned from the prod registry `.252:5000`.

- **Last-known-good is the complete rendered state**, not just image digests: the rendered manifest
  set, ConfigMap contents, Secret resource versions, the database schema version, and the migration
  checkpoint are captured before apply.
- **Rollback is migration-aware.** rev 11's carried-over rule stands: Bugzilla's `checksetup`
  migrates forward only. Therefore an automatic LKG image rollback is permitted **only** when the
  captured schema version is unchanged; across a schema-migrating bump the job must **stop and
  require a database restore decision**, never start an older image against a migrated schema.
- Deploy protocol: capture → apply → bounded `rollout status` → classify → conditional LKG → on
  failure of LKG, fail loudly and apply the NS-5 public fail-closed rollback.
**Acceptance:** Jenkins evidence with retained artifacts; NS-V14 — LKG rehearsal for both the
same-schema (automatic) and schema-changed (blocked, restore-required) cases.

### NS-11 — Guardrails are re-asserted, and drift is an incident
The job asserts, **in preflight (before apply) and again after apply**: quota, LimitRange, PSA
labels, the NS-15 policies and bindings, PriorityClass on every tenant pod, the **complete effective
NetworkPolicy set** (not merely "a default-deny exists" — an added allow-all would otherwise pass),
every PVC on `fullfunding-storage`, and no LoadBalancer/NodePort.

Drift semantics are defined rather than left ambiguous: **preflight failure blocks the deploy and
is reported as a security incident**; the job does not silently re-create a missing guardrail and
then verify its own repair.
**Acceptance:** NS-V15 — removing any single guardrail object makes the **next preflight** fail and
raises an incident; re-adding it is a recorded, reviewed action.

### NS-12 — Baseline and observation window
Baseline captured **before** the tenant exists, otherwise the comparison is meaningless. The window
covers **at least one complete regular trading session including the open and the close**, not a
bare 24 h, and asserts against thresholds agreed at baseline:

- OptionsEdge: pipeline lag, business latency, error rate, pod restarts **with reason codes and
  timestamps** (an unattributed restart count is not evidence), OOM kills, evictions. "CPU
  throttling" is **not** a meaningful OptionsEdge metric — its containers have no CPU quota — so
  lag and latency are the signals.
- Node: memory pressure, nodefs and imagefs headroom, load.
- Tenant: filesystem **growth slope within an agreed band** (a database in use grows; "flat" was the
  wrong test), PVC usage, restarts.
- All three tunnel hostnames serving.
**Acceptance:** NS-V16 — baseline captured pre-change; window closed within thresholds.

### NS-13 — What is and is not isolated (truth statement)
NS-2…NS-8 and NS-15 deliver **partial resource governance and connectivity filtering**: bounded CPU
share and ceiling, a hard memory ceiling, a hard persistent- and ephemeral-storage ceiling, blocked
node-level escapes, and default-deny networking. They do **not** deliver disk-I/O or page-cache
isolation (R-15), protection of shared Keycloak/traefik/cloudflared/registry capacity (R-19), or
**any availability isolation** (R-16): a reboot, kernel panic, k3s upgrade, `/` exhaustion or node
failure takes both projects down together. No reader may infer an availability guarantee from the
governance guarantees. Independent availability requires a second machine.

### NS-14 — Teardown and data disposition
Namespace deletion alone does **not** restore the pre-change state. A complete teardown must
explicitly dispose of: the Keycloak realm `req` and its client (in shared Keycloak), registry
images, Jenkins job and records, PVs (`reclaimPolicy: Retain` means they **survive** namespace
deletion by design), the loopback image + fstab entry + its 100 GiB of `/home`, the cloudflared
rule, the DNS record, the PSA labels, the NS-15 policies, the PriorityClass and StorageClass, and
the backups.

**Data-retention decision is explicit, not implicit:** deleting the namespace must not be the
mechanism that destroys tenant data or its backups — both live on `Retain` PVs and are removed only
by a separate, deliberate step recorded by the owner. The NS-4 reservation is **kept** unless shown
to have caused a regression.
**Acceptance:** NS-V17 — teardown rehearsed on a scratch namespace against an **enumerated
checklist** of the objects above (host, cluster and external state). `kubectl get all -A` is
explicitly **not** the test; it omits most resource types and all host/external state.

### NS-15 — Admission controls (the layer the isolation claims actually rest on)
This cluster already runs a `ValidatingAdmissionPolicy` (`options-edge-jenkins-only-workloads`,
`failurePolicy: Fail`, binding `[Deny]`), so the mechanism is proven here. Required:

- **Pod Security Admission** on `fullfunding`: `enforce=restricted` (plus `audit`/`warn`), which
  denies privileged pods, host namespaces, `hostPath`, privilege escalation and unsafe capabilities.
  PSA is not currently enabled on any namespace (§2.1), so this is additive and touches nothing else.
- **ValidatingAdmissionPolicies**, all `failurePolicy: Fail`, bindings `[Deny]`, scoped to
  `fullfunding` except where noted:
  1. deny any container declaring `hostPort`; deny `hostNetwork`/`hostPID`/`hostIPC`;
  2. deny `Service.spec.externalIPs`;
  3. require `spec.storageClassName == "fullfunding-storage"` on every PVC;
  4. require `priorityClassName == "fullfunding-low"` on every pod;
  5. require every container to declare `requests` **and** `limits` for cpu, memory and
     ephemeral-storage;
  6. require every `emptyDir` to set `sizeLimit` (≤ 1 Gi);
  7. restrict `Ingress` hosts to an allowlist (`req.fullfunding.nl` and any later tenant hostname) —
     denying `fullfunding.nl`, `auth.fullfunding.nl`, `es.fullfunding.nl` and wildcards;
  8. constrain which Secrets a tenant pod may reference and which ServiceAccount it may run as;
  9. **scoped to `options-edge`:** deny any container declaring `limits.cpu` (NS-2).
- Pod-level PID limiting via kubelet `podPidsLimit`.
**Acceptance:** NS-V21 and NS-V23 above, plus NS-V24 — each policy is exercised with one violating
and one conforming object, and `failurePolicy: Fail` is confirmed (a policy that fails open is worse
than no policy).

### NS-16 — Service objectives and load behaviour on shared infrastructure
Because the portal shares Keycloak, traefik and cloudflared with the trading UI, its load is an
OptionsEdge risk (R-19). Required before launch:

- Stated SLOs for the portal: expected concurrent users, request rate, p95/p99 latency, error-rate
  budget — modest by construction (a data-entry portal for external stakeholders).
- A **load and soak test through the shared path** (traefik → portal, and the OIDC flow against
  Keycloak) at a defined multiple of the expected peak, run **outside RTH**, asserting that
  OptionsEdge's baseline metrics stay within NS-12 thresholds throughout.
- **Rate and concurrency limits** at the Ingress (traefik middleware): request rate per source,
  in-flight request cap, and request body size cap — so a login storm or a scripted client cannot
  convert into unbounded shared-infrastructure load. Database connection limits set on both tenant
  databases.
**Acceptance:** NS-V25 — soak test executed, thresholds met, limits demonstrated by exceeding them.

### NS-17 — Backups: both databases, off-volume, restorable
- **Tenant Bugzilla (MariaDB)** and **tenant Postgres** are both backed up — rev 1 omitted the
  latter. `mysqldump --single-transaction` / `pg_dump` in a namespace CronJob, one atomic generation
  with per-artifact checksums and a manifest written **last** as the completion marker, 14-day
  retention, following rev-11 REQ-10a's structure.
- **Not only inside the 100 GiB image.** Backups written solely to the tenant filesystem do not
  survive image corruption, accidental deletion, or `/home` loss, and would fail exactly when the
  filesystem is full. The retained copy is therefore **also** written off-volume to the existing
  `.252` daily archive path, with the same exclusion of secret material.
- **RPO 24 h, RTO hours (manual restore)**, stated as such. Backups are unencrypted on the host,
  same custody as all prod data — accepted, consistent with rev-11 R-9.
- **Keycloak's `pg_dump` is not a tenant object.** It stays owned by the OptionsEdge side exactly as
  rev-11 REQ-10a specifies, runs from where it runs today, and is **not** placed in a tenant CronJob
  — a tenant CronJob could not reach the Keycloak database under NS-6 anyway, and should not.
- Restore validation per release that changes images or schema, per rev-11's V-restore, with
  `kubectl port-forward` replacing `ssh -L`.
**Acceptance:** NS-V26 — restore of a completed generation into a scratch namespace, content
verified (known ticket, attachment checksum, identity mapping), for **both** databases.

### NS-18 — Alerting
Alerts wired before launch, into the existing prod Discord/ops path: tenant filesystem ≥ 75% / 90%,
PVC growth beyond the NS-12 band, node memory pressure and any eviction or OOM kill (tenant **or**
OptionsEdge), Kafka disk and consumer lag, database down / connection saturation, Ingress 5xx rate,
backup generation older than 26 h, and any NS-11 preflight guardrail failure.
**Acceptance:** NS-V27 — each alert fired once deliberately in rehearsal and observed to arrive.

### NS-19 — Clause-level traceability to rev 11
§4's table is by top-level REQ id. Before Gate 2 begins, a **clause-level mapping** is produced:
every rev-11 sub-requirement, acceptance clause and verification row (V-pre, V3, V4/V4d, V6, V6b,
V6t, V7, V7-int, V8, V10, V11, V-lan, V-env, V-off, V-restore, V-rollback) is listed with its status
— unchanged, superseded-by (with the NS id), or explicitly retired-with-reason. A REQ-level table
cannot prove nothing was stranded.
**Acceptance:** NS-V28 — mapping complete, with zero rows unaccounted for; reviewed with the rev-11
document open.

## 4. Disposition of the rev-11 req-portal requirements

`docs/req-portal-bugzilla-keycloak-sso.md` rev 11 (Codex 3-bar APPROVE, round 11) remains the
authority for everything independent of where containers run. Where this table says SUPERSEDED, the
present document governs. **Clause-level completeness is NS-19; this table is the REQ-level summary.**

| rev-11 req | Disposition | What changes |
|---|---|---|
| REQ-1 realm `req` | **UNCHANGED** | Keycloak stays in `options-edge`; the realm is shared-plane (NS-1), not a tenant object |
| REQ-2 OIDC client | **UNCHANGED** | exact redirect URI `https://req.fullfunding.nl/oidc-callback` still holds |
| REQ-3 hostname, published last / closed first | **SUPERSEDED (mechanism)** | cloudflared target becomes traefik `http://127.0.0.1:80`; ordering, pre-checks and the 5-step fail-closed rollback carry over verbatim (NS-5) |
| REQ-4 images traceable/immutable/digest-deployed | **UNCHANGED in substance** | still Jenkins-built from pinned commit `276673ab6` to `.252:5000`; "compose by digest" → "manifest by digest" |
| REQ-5a exposure, two listeners, header strip, `Require claim` | **PARTLY SUPERSEDED** | Security contract unchanged (two structurally separate listeners, public-only OIDC vhost, enumerated header strip, pinned `Require claim "email~…"`). Reachability proof changes: public port via ClusterIP+Ingress; **admin port 81 denied over the pod network by NS-6's port-specific policy** (not merely "no Service"), reachable only by `kubectl port-forward` with the operator credential. `apachectl -S` remains the vhost-separation proof. New acceptance NS-V18 |
| REQ-5b sessions & offboarding | **UNCHANGED** | "restart the web container" → "restart the web pod"; semantics identical |
| REQ-5c identity & claim contract (`276673ab6`) | **UNCHANGED** | source-pinned, hosting-independent |
| REQ-5d authorization model | **UNCHANGED** | |
| REQ-6 internal Bugzilla untouched | **UNCHANGED, strengthened** | NS-6 additionally makes `:8092` unreachable from the tenant |
| REQ-7 cross-system isolation | **UNCHANGED, plus evidence** | NS-V9 output added alongside the token-level proof |
| REQ-8 Jenkins-only build & deploy | **SUPERSEDED (mechanism)** | compose → kustomize apply + bounded `rollout status`. **The schema-forward rule is not merely carried over but made binding on the rollback path** (NS-10): automatic LKG only when the schema version is unchanged |
| REQ-9 secrets | **SUPERSEDED (mechanism), with new risks** | host `0600` files → k8s `Secret` **projected as files** (never container env, never in probe text). **The k8s allowlist is enumerated here:** (a) the Secret object in the k3s datastore, (b) its projected paths in the web pod, (c) the Jenkins credential store — and **nowhere else** (not the repo, not job logs or archived artifacts, not `kubectl describe`, not pod env, not container logs, not backups). New truths: the secret is **unencrypted at rest in the k3s datastore**; **any principal that can create pods in the namespace can mount and read it** (NS-9); and **projected-Secret updates are asynchronous and `subPath` mounts do not refresh at all**, so rotation is *not* automatically atomic for the application — rotation therefore **requires an explicit pod restart step**, which rev-11's rotation runbook already performs. `OIDCClientSecret exec:` and the two secret lifecycles are unchanged |
| REQ-10a windows & backups | **SUPERSEDED (mechanism), extended** | host cron → namespace CronJob, atomic generation preserved; **tenant Postgres added** and **an off-volume copy required** (NS-17). **Keycloak's `pg_dump` stays with OptionsEdge**, unmoved and explicitly not a tenant CronJob |
| REQ-10b health model | **SUPERSEDED (mechanism)** | Docker healthchecks → liveness/readiness probes; a probe is still never the security gate |
| REQ-10c verification & observation | **EXTENDED** | rev-11 matrix + NS-V1…**NS-V28**; the observation window merges with NS-12 and lengthens to a full session |
| REQ-11 login-surface & edge hardening | **UNCHANGED, plus NS-16** | rate/concurrency limits added at the Ingress |
| REQ-12 patch & vulnerability posture | **UNCHANGED** | |
| REQ-13 privacy & data handling | **UNCHANGED** | |
| §8 risks R-1…R-11, R-13 | **CARRIED OVER unchanged** | |
| §8 risk **R-12** (host-level / Docker-daemon access) | **SUPERSEDED, not carried** | replaced by R-14 and R-18: the tenant no longer needs Docker-daemon access, and gains namespace-scoped Kubernetes access instead |

**Database engine, stated plainly:** the user's requirement names **Postgres**; rev 11 pins Bugzilla
to **MariaDB**. Bugzilla supports both, but switching engines invalidates REQ-4's image pinning,
REQ-10a's backup design and the Codex-approved text. This document keeps **MariaDB for Bugzilla**
and provisions the requested **Postgres as the namespace platform database**. Both are sized and
backed up (NS-8, NS-17). This is **D-1** and must be resolved by the user at Gate 1 — running an
unused Postgres beside MariaDB would not honestly satisfy "the project gets its own Postgres".

## 5. Non-goals

- Not multi-tenancy as a product: one namespace, one tenant, one operator.
- No availability isolation (NS-13); no second node; no HA.
- No disk-I/O or page-cache isolation (R-15).
- No isolation of shared Keycloak/traefik/cloudflared/registry capacity (R-19) beyond NS-16's limits.
- No change to the `optionsedge` realm, the internal Bugzilla, or any OptionsEdge manifest — except
  the `options-edge`-scoped admission policy of NS-2/NS-15(9), which adds a constraint and modifies
  no workload.
- No XFS project quota on `/home` (needs downtime); no change to the existing `auth.fullfunding.nl`
  tunnel route.
- No encryption-at-rest for k3s secrets (R-14 records the gap).

## 6. Rollout sequence (fail-closed, ordered)

All steps run **outside** Mon–Fri 09:30–16:15 America/New_York **except the measurement steps that
by definition require live trading hours** (steps 1 and 8b), which are read-only observation and
change nothing. Steps serialized; each step's verification passes before the next begins.

0. **Pre-flight:** `req.fullfunding.nl` DNS record **absent**; `/home` free space ≥ 400 GiB (so
   NS-7's reserve holds after the 100 GiB image); `/` free space recorded; NS-12 baseline capture
   **begins** (it must span a full session, so it starts here).
1. **Measurement (read-only, spans RTH):** high-percentile host CPU/memory across ≥5 representative
   sessions (open, close, spike, backup window, maintenance) → the NS-4 `system-reserved` value.
   Resolves D-3. *(Blocks step 2.)*
2. **Node reservation** (NS-4) — config + restart in the window; NS-V6. Stands alone in value.
3. **Storage wall** (NS-7) — preallocated image, mount, provisioner, StorageClass; NS-V10 rehearsed
   on a **scratch 1 GiB image**, NS-V23.
4. **Admission + guardrails** (NS-1, NS-2, NS-3, NS-9, NS-15) — namespace, PSA labels, quota,
   LimitRange, PriorityClass, policies, RBAC; NS-V1…V5, V7, V12, V20, V21, V23, V24.
5. **Network policy** (NS-6) — **NS-V9 runs here.** A failure triggers the host-service-side access
   control prerequisite before anything else proceeds. NS-V22.
6. **Platform services** (NS-8) — MariaDB, Postgres, and Kafka per D-2; NS-V11.
7. **Portal workloads** — per rev-11 §6 steps 1–7 as adapted by §4. Realm `req` and the OIDC client
   (REQ-1, REQ-2) created here. NS-V19 back-channel proven **before** any public exposure.
8. **Private verification (V-pre)** — the full rev-11 matrix plus every NS-V that does **not**
   require public exposure or the observation window, reached via `kubectl port-forward`.
   **Explicitly deferred to later steps:** NS-V8 (needs the published path), NS-V16 (needs the
   window), NS-V25 (8b), NS-V13 (needs RTH with feed-gateway scaled up).
   - 8b. **RTH-only checks** (read-only): NS-V13; and NS-V25's soak run scheduled **outside** RTH
     but compared against the RTH baseline.
9. **Publish** (NS-5) — Ingress, then the cloudflared rule, then DNS **last**. NS-V8 plus rev-11
   V3/V4/V6t/V11 immediately after, before any stakeholder user exists.
10. **Onboarding gate** — every matrix row green, NS-19 mapping complete, and the NS-12 window
    (a full session) closed within thresholds. Only then are external users provisioned.

Rollback after step 9 uses the NS-5 five-step public fail-closed procedure; before step 9 there is
no public exposure and teardown is private (NS-14).

## 7. Verification matrix

Every row states an exact expected result; "green" without one is not acceptance.

| id | Asserts | Exact expected result |
|---|---|---|
| NS-V1 | two-plane object attribution | every object maps to exactly one plane; zero unattributed |
| NS-V2 | quota rejects over-budget pod | a 5 CPU pod is rejected with `exceeded quota` |
| NS-V3 | LimitRange defaults | a resource-less pod shows exactly 50m/128Mi requests, 500m/512Mi limits |
| NS-V4 | `limits.cpu` denied in `options-edge` **at the API** | admission denies; policy `failurePolicy: Fail` confirmed |
| NS-V5 | priority + ordering preference | all tenant pods `priority: -100`; scratch-namespace rehearsal evicts the tenant pod first, with the NS-3 caveat recorded |
| NS-V6 | reservation applied | effective kubelet config lists all four eviction thresholds; allocatable and slack recorded; full inventory schedules |
| NS-V7 | LB/NodePort rejected | both rejected with `exceeded quota` |
| NS-V8 | public routing | `req.fullfunding.nl` → 200 through traefik; `fullfunding.nl` and `auth.fullfunding.nl` unchanged status/body after each cloudflared restart |
| NS-V9 | host services unreachable | connect to `.252` on 9092/5432/8081/8082/8092/5000 from a tenant pod: **all six time out or are refused**, none connects |
| NS-V10 | disk wall | on a scratch 1 GiB image: writer fails with ENOSPC; backing `df` **byte-identical** before/after |
| NS-V11 | tenant-only data services | portal/app connect strings resolve to `*.fullfunding.svc`; no host endpoint in any config |
| NS-V12 | RBAC matrix | every permitted cell allowed and every denied cell denied, per credential, in 3 namespaces |
| NS-V13 | the 8091 svclb cause | at RTH with feed-gateway scaled up: confirmed or refuted, with `ss -ltnp` evidence (reporting only) |
| NS-V14 | rollback | same-schema LKG succeeds automatically; schema-changed case **blocks** and demands a restore decision |
| NS-V15 | guardrail drift | removing any guardrail fails the **next preflight** and raises an incident |
| NS-V16 | observation | full-session window inside every NS-12 threshold, against a pre-change baseline |
| NS-V17 | teardown | enumerated checklist fully dispositioned (cluster, host, external); `kubectl get all -A` explicitly not used as the test |
| NS-V18 | admin listener | port 81 unreachable from another tenant pod **and** from the LAN; reachable only via `port-forward` with the operator credential |
| NS-V19 | OIDC back-channel | login completes under default-deny egress; decoded ID token `iss` equals the pinned public issuer |
| NS-V20 | ephemeral/object limits | a pod exceeding `limits.ephemeral-storage` is evicted; the 16th Secret and 9th Service are rejected |
| NS-V21 | node-escape denials | `hostPort`, `hostNetwork`, `externalIPs`, `hostPath`, privileged, and a foreign Ingress host are each denied at admission |
| NS-V22 | effective policy set | the full NetworkPolicy set contains no allow-all ingress or egress rule |
| NS-V23 | storage class | PVCs with omitted / `local-path` / other class are rejected; `fullfunding-storage` accepted |
| NS-V24 | policy integrity | each policy exercised with one violating and one conforming object; all bindings `[Deny]`, all `failurePolicy: Fail` |
| NS-V25 | soak on shared path | at the agreed multiple of peak, OptionsEdge metrics stay inside NS-12 thresholds; rate limits demonstrably engage |
| NS-V26 | restore | both databases restored into a scratch namespace; known ticket, attachment checksum and identity mapping verified |
| NS-V27 | alerting | every NS-18 alert fired deliberately and observed to arrive |
| NS-V28 | traceability | every rev-11 clause and V-row mapped; zero unaccounted rows |

## 8. Accepted-risk register (additions to rev-11 R-1…R-11, R-13; R-12 superseded)

| id | Risk | Why accepted / mitigation |
|---|---|---|
| R-14 | Secrets are **unencrypted at rest** in the k3s datastore, and **any principal able to create pods in the namespace can mount and read them**; cluster-admin and the node always can | NS-15(8) constrains what pods may mount and run as; the datastore has the same host custody as every other prod secret. Encryption-at-rest is out of scope and recorded as a gap |
| R-15 | **No disk-I/O or page-cache isolation** — heavy tenant I/O can raise host Kafka latency | Data-entry workload (NS-8); separate backing filesystem; monitored by NS-12/NS-18. If it bites, the answer is a second machine, not a tighter quota |
| R-16 | **Shared availability** (NS-13) | Explicitly accepted; single-operator scope |
| R-17 | Loopback-file filesystem adds I/O overhead and consumes **100 GiB of `/home` permanently, up front** | Negligible at data-entry volume; it is the only hard disk wall available without unmounting `/home`. The cost is stated, not hidden |
| R-18 | The admin surface now depends on a **kubeconfig** rather than SSH | Narrower in scope (namespace-only) but a new credential to protect (NS-9); rev-11 R-7 still applies |
| R-19 | **Tenant load escapes the quota through shared services** — Keycloak, traefik, cloudflared, the registry and kubelet/containerd image pulls are all outside the `fullfunding` quota, so a login storm can degrade OptionsEdge without breaching any tenant limit | Bounded, not eliminated: NS-16 rate/concurrency limits at the Ingress, connection limits on both databases, a soak test through the shared path, and NS-18 alerting. Isolating Keycloak or ingress capacity is out of scope |
| R-20 | **LAN origin bypass** — traefik is reachable on `.252:80` from the LAN, so Cloudflare is not an authentication boundary | Accepted: the portal is fail-closed without Cloudflare (rev-11 REQ-5a's OIDC gate). Restricting traefik to loopback would change shared infrastructure |
| R-21 | The **thin memory slack** (≈4.3 Gi at a 14 Gi reservation) leaves little room for an OptionsEdge growth event | NS-4 blocks launch if the measured reservation exceeds 16 Gi; NS-18 alerts on memory pressure; the tenant is the preferred eviction victim (NS-3) |
| R-22 | `/` (nodefs) has only ~37 GiB free and is **not** protected by the tenant disk wall | NS-2 ephemeral-storage quota, NS-15(6) `emptyDir` size limits, NS-4's `nodefs` eviction threshold, and NS-18 alerting |

## 9. Open decisions (D-1, D-2 and D-3 must be resolved before Gate 2)

| id | Decision | Recommendation |
|---|---|---|
| **D-1** | Bugzilla's database engine: keep **MariaDB** (rev-11 pinned, Codex-approved) or switch to **Postgres**? | **Keep MariaDB for Bugzilla; run the requested Postgres as the namespace platform DB.** Switching Bugzilla's engine invalidates REQ-4's pinning and REQ-10a's backup design and needs a fresh Codex round. **Requires the user's decision — the requirement says "Postgres".** |
| **D-2** | Is Kafka actually needed by this portal? | Bugzilla does not use Kafka. **Either deploy it at 1 replica because a named tenant application will use it, or do not deploy it at all and reclaim ~1 Gi and 20 Gi.** A zero-replica placeholder is rejected (NS-8) |
| **D-3** | `system-reserved` value | Derived from ≥5 representative sessions (§6 step 1), not the off-hours figure. Launch blocked if it exceeds 16 Gi pending a user decision |
| **D-4** | Tenant egress to the public internet | Default **no**. Any runtime need becomes an explicit, reviewed allowlist entry with its own risk row |
| **D-5** | OIDC back-channel under default-deny egress | **Split front/back-channel (option ii)** — narrowest allowlist; requires verifying that the packaged `mod_auth_openidc` accepts explicit overrides for every endpoint and that Keycloak emits the public issuer over the internal request. Fallback (i) is broad outbound HTTPS and must be recorded as such |
| **D-6** | Does the portal need its own operator alert channel, or the existing prod one? | Existing prod path (NS-18), tagged, until volume justifies separation |

## 10. Gate status

- **Gate 1 (requirements):** this document. Rev 1 was reviewed by Codex (3-bar) → REQUEST_CHANGES;
  every finding is implemented in rev 2. **Awaiting Codex re-review, then the user's explicit
  approval.** No implementation before that approval.
- **Gate 2 (implementation):** not started, and blocked on D-1, D-2 and D-3.
