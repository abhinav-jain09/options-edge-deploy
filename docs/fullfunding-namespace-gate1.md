# Gate-1 — `fullfunding` tenant namespace on the prod k3s node, and the req-portal's migration into it

**Status: GATE-1 REQUIREMENTS / PROPOSED — rev 1. AWAITING USER APPROVAL (gatekeeping Gate-1).
Not yet implemented, not yet Codex-reviewed at the time this line was written.**

Everything in §2 is **verified on `192.168.100.252` on 2026-08-04** by direct inspection (commands
recorded per row). Everything in §3 onward is intended future behaviour.

**Date:** 2026-08-04  **Owner:** Abhinav
**Repos:** options-edge-deploy (namespace overlay, tenant manifests, Jenkins job)
**Hosts:** 192.168.100.252 (prod k3s, single node)
**Supersedes, in part:** `docs/req-portal-bugzilla-keycloak-sso.md` rev 11 — see §4 for the
requirement-by-requirement disposition. That document remains authoritative for every
hosting-independent contract (identity, claims, authorization matrix, realm design).

---

## 1. Goal

Run a **second, unrelated project on the existing production machine** inside the same k3s cluster,
in a dedicated namespace **`fullfunding`**, such that:

1. the new project **cannot degrade OptionsEdge** — not by CPU, not by memory, not by evicting its
   pods, not by taking a host port, not by filling a disk;
2. the new project is **capped** (CPU and memory limits) while **OptionsEdge remains uncapped on
   CPU** — stated by the user as a hard requirement, and already true today (§2);
3. the new project gets its **own Postgres and Kafka**, sized for a **data-entry workload, not a
   streaming one**, and never touches the host-level Kafka/Postgres that OptionsEdge runs on;
4. the **requirement-intake portal** (`req.fullfunding.nl` — Bugzilla + Keycloak realm `req`,
   designed in rev 11) is that project, and is delivered **in this namespace** rather than as a
   host Docker-Compose stack.

Point 4 is the change of record: rev 11 specified a host-Compose deployment. This document moves
the hosting model into Kubernetes and states exactly which rev-11 requirements survive unchanged,
which are superseded, and what replaces them (§4).

## 2. Current state (as-is, verified 2026-08-04)

### 2.1 Node and cluster

| Fact | Value | Source |
|---|---|---|
| k3s | v1.35.5+k3s1, **single node** (`localhost.localdomain`, control-plane), containerd 2.2.3 | `kubectl get nodes -o wide` |
| k3s data-dir | `/home/options-edge/data/k3s` (NOT on `/`) | `/etc/systemd/system/k3s.service` ExecStart |
| Node allocatable | **cpu 24, memory 65257092Ki (≈62.2 Gi)**, pods 110 | `kubectl describe node` |
| `system-reserved` / `kube-reserved` / `eviction-hard` | **none set** (no `kubelet-arg` in the unit; `/etc/rancher/k3s/config.yaml` empty) | unit file + config read |
| Namespaces | `default`, `kube-node-lease`, `kube-public`, `kube-system`, `loki`, `options-edge` — **no `fullfunding`** | `kubectl get ns` |
| ResourceQuota / LimitRange | **none, in any namespace** | `kubectl get resourcequota,limitrange -A` |
| NetworkPolicy objects | **none, in any namespace**; `--disable-network-policy` is **not** set in the unit or `/etc/rancher/k3s/config.yaml`, so k3s's netpol controller is expected to be active — **enforcement itself is not proven by that absence and is proven only by NS-V9** | `kubectl get netpol -A`; unit + config read |
| StorageClass | **one**: `local-path` (default), provisioner `rancher.io/local-path`, path `/home/options-edge/data/k3s/storage` | `kubectl get sc`; cm `local-path-config` |
| IngressClass | `traefik` (`traefik.io/ingress-controller`) | `kubectl get ingressclass` |
| traefik | LoadBalancer, external IP `192.168.100.252`, ports 80/443 | `kubectl -n kube-system get svc traefik` |
| Existing Ingress objects | exactly one: `options-edge/oe-keycloak` → `auth.fullfunding.nl`, path prefixes `/realms/optionsedge` and `/resources` only | `kubectl get ingress -A -o jsonpath` |
| **traefik Ingress routing works** | `curl -H 'Host: auth.fullfunding.nl' http://127.0.0.1:80/realms/optionsedge/.well-known/openid-configuration` → **200**; same through the traefik ClusterIP → 200 | direct test on `.252` |
| Admission-validated design primitives | a `PriorityClass` with **`value: -100`**, a quota with **`services.loadbalancers: "0"`**, and a quota with **`local-path.storageclass.storage.k8s.io/persistentvolumeclaims: "0"`** are all **accepted by this API server** | `kubectl apply --dry-run=server` |

### 2.2 The measured OptionsEdge footprint (this is what the budget is sized against)

| Fact | Value | Source |
|---|---|---|
| Deployments in `options-edge` | **55** | `kubectl -n options-edge get deploy` |
| Declared **CPU requests**, summed over all 55 | **12.97 CPU** | JSON sum over `spec.template.spec.containers[].resources` |
| Declared **memory requests**, summed | **37.1 Gi** | same |
| Declared **memory limits**, summed | **149.1 Gi** (≈2.4× the node — already overcommitted) | same |
| Declared **CPU limits** | **none — zero deployments set `limits.cpu`** | same |
| PVCs cluster-wide | 26, declared total **1141 Gi** (several 100 Gi Streams state stores) | `kubectl get pvc -A` |

The "OptionsEdge is uncapped on CPU" requirement is therefore **not a change** — it is the
existing, measured state. NS-2 turns it into a written, enforced rule.

### 2.3 The half of the stack that is NOT in Kubernetes

kubelet cannot see, account for, or constrain any of the following. This single fact drives NS-4,
NS-6 and NS-7.

| Component | Where | Port | Data |
|---|---|---|---|
| Kafka (KRaft) | host systemd `kafka.service` | 9092/9093 | `/home/kafka/kraft-combined-logs` (dedicated 1.9 T NVMe, 12% used) |
| PostgreSQL | host systemd | 5432 | `/home/postgres/data` (on `/home`) |
| Confluent Schema Registry | host systemd | 8081 | — |
| AKHQ | host systemd | 8082 | — |
| Prometheus + node-exporter | host systemd | 9090 | — |
| Grafana | host systemd | 3000 | — |
| httpd + php-fpm | host systemd | 80 (403) | — |
| cloudflared (`options-edge-option-chain`) | host systemd | — | `/etc/cloudflared/options-edge-stable.yml` |
| Docker registry | container `options-edge-registry` | **5000** | — |
| `options-edge-admin-app` | container | **8091** | — |
| Internal Bugzilla web + MariaDB | containers | **8092**, 3306 | `/home/options-edge/data/bugzilla/` |
| `options-edge-admin-postgres` | container | internal | — |

Docker root: `/home/options-edge/data/docker`. Host memory in use at off-hours (market closed,
most OptionsEdge deployments scaled to 0): **≈13 Gi**, buff/cache 16 Gi separately.

### 2.4 Disks

| Mount | Device | Size | Used | Free | Options |
|---|---|---|---|---|---|
| `/` | `cs-root` (LVM/xfs) | 70 G | 27 G | 44 G | — |
| `/home` | `cs-home` (LVM/xfs) | 1.8 T | 598 G | **1.2 T** | `noquota` |
| `/home/kafka` | `nvme0n1p1` (xfs) | 1.9 T | 207 G | 1.7 T | `noquota` |

**Two facts that matter more than the free space:**
- `local-path` is a hostPath bind — it **does not enforce PVC capacity**. A PVC declared `20Gi` can
  grow until the filesystem is full.
- `/home` is mounted **`noquota`**, so XFS project quota is not available; enabling it requires
  unmounting `/home`, where k3s data, Postgres data and the Docker root all live. That is a
  downtime change and is **out of scope** (NS-7 solves it without downtime).

### 2.5 Ports and the existing collision

`ss -ltn` on `.252`: **8093 and 8095 are free** (re-checked 2026-08-04, as rev 11 required).

Host port **8091** is bound by the Docker container `options-edge-admin-app`, while the k3s
LoadBalancer Service `options-edge/feed-gateway-service` also claims 8091; its `svclb` pod shows
**24 restarts** with `Last State: Terminated, Exit Code: 255`. The service is currently scaled to 0
(off-hours), so this is a **strong indication, not a proven diagnosis** — NS-V13 closes it. It is
recorded here because it is precisely the failure mode NS-5 makes structurally impossible for the
tenant.

## 3. Requirements — namespace platform (NS-1 … NS-14)

Stable ids. Initial state for all: **TRACKED-PENDING**.

### NS-1 — Tenancy identity
Namespace **`fullfunding`**, label `tenant: fullfunding`. Every tenant object lives in it. The
namespace is the unit of budget, policy and RBAC; there is exactly one tenant per namespace.
**Acceptance:** namespace exists with the label; no tenant object outside it (NS-V1).

### NS-2 — CPU and memory budget (the asymmetry is deliberate and stated)
- **OptionsEdge:** **no CPU limits, ever.** Written rule; matches §2.2. A CI check rejects any
  `options-edge` manifest that introduces `limits.cpu`.
- **`fullfunding`:** capped by `ResourceQuota` **and** `LimitRange`, so a pod that omits resources
  still gets limits.

```yaml
requests.cpu: "2"        limits.cpu: "4"
requests.memory: 4Gi     limits.memory: 8Gi
requests.storage: 60Gi   persistentvolumeclaims: "4"
pods: "15"
services.loadbalancers: "0"   services.nodeports: "0"
```
`LimitRange` (Container): `defaultRequest` 50m/128Mi, `default` 500m/512Mi, `max` 1 CPU / 2Gi.

- **Why requests are deliberately small (load-bearing, not a detail):** under contention the Linux
  CFS share of a cgroup is derived from **`requests.cpu`**, not from `limits.cpu`. A tenant that
  requested 8 CPU would pass any limit-based quota and still take a guaranteed scheduler share away
  from OptionsEdge. The cap that protects OptionsEdge is therefore **`requests.cpu: 2`**;
  `limits.cpu: 4` only bounds the burst.
- **Why a memory limit is non-negotiable:** CPU is compressible (throttling), memory is not
  (OOM-kill / node-pressure eviction). A CPU-only cap does not bound the tenant's blast radius.
**Acceptance:** NS-V2 (quota rejects an over-budget pod), NS-V3 (a pod with no resources declared
receives the LimitRange defaults), NS-V4 (CI check fires on an injected `limits.cpu` in
`options-edge`).

### NS-3 — Eviction and preemption ordering, without touching OptionsEdge
`PriorityClass fullfunding-low`, **`value: -100`**, `preemptionPolicy: Never`,
`globalDefault: false`; every tenant pod sets it. OptionsEdge pods stay at the default priority 0
and **no OptionsEdge manifest is modified** — the negative value alone orders the tenant below
them for node-pressure eviction and preemption.
**Acceptance:** NS-V5 — tenant pods report `priority: -100`; a synthetic memory-pressure rehearsal
in the change window evicts the tenant pod and no `options-edge` pod.

### NS-4 — Node-level reservation for the invisible host stack
Because §2.3 is invisible to kubelet, the scheduler currently believes the node is nearly empty
(requests 6% CPU / 4% memory) while ~13 Gi is in use by host services. Add to
`/etc/rancher/k3s/config.yaml`:

```yaml
kubelet-arg:
  - "system-reserved=cpu=4,memory=14Gi"
  - "kube-reserved=cpu=1,memory=1Gi"
  - "eviction-hard=memory.available<2Gi,nodefs.available<10%"
```

Arithmetic that must hold after the change (recomputed at NS-V6, not assumed):

```
allocatable                       62.2 Gi
− system-reserved 14 − kube-reserved 1   = 47.2 Gi schedulable
− options-edge requests (measured)  37.1 = 10.1 Gi
− fullfunding requests.memory        4.0 =  6.1 Gi slack
```

The slack is thin. Two consequences are requirements, not advice: (a) `requests.memory` for the
tenant **must not exceed 4Gi** without redoing this arithmetic; (b) the `14Gi` figure is derived
from an **off-hours** measurement and **must be re-derived from a regular-trading-hours
measurement** before the value is committed (open decision D-3).
**Risk if skipped:** today, with nothing reserved, node memory pressure lets kubelet evict
OptionsEdge pods with no protection at all — this requirement improves the status quo independently
of the tenant.
**Acceptance:** NS-V6 — post-change `kubectl describe node` shows the reduced allocatable, and
scheduling all 55 OptionsEdge deployments plus the tenant still fits with the computed slack.

### NS-5 — Exposure: Ingress only, never a host port
- `services.loadbalancers: "0"` and `services.nodeports: "0"` in the quota make a host-port claim
  **structurally impossible** for the tenant, rather than merely discouraged. This is the direct
  countermeasure to §2.5.
- The tenant's only public path: **Service (ClusterIP) → traefik `Ingress` → cloudflared**.
- cloudflared ingress rule (before the catch-all): `hostname: req.fullfunding.nl` →
  `service: http://127.0.0.1:80` (traefik on `.252`), Host header preserved so the Ingress matches.
- **Why traefik and not a pinned ClusterIP — a stale claim, corrected:** the live cloudflared
  config routes `auth.fullfunding.nl` **directly to the Keycloak ClusterIP** `10.43.127.26:8080`
  and carries an inline comment reading *"traefik :80 is broken"*. **That claim is refuted as of
  2026-08-04** (§2.1: a Host-matched request through traefik `:80` returns 200; the 404s that
  motivated it are explained by the `oe-keycloak` Ingress declaring only the `/realms/optionsedge`
  and `/resources` path prefixes, so every other path legitimately 404s). Pinning a tunnel to a
  ClusterIP is fragile — recreating the Service assigns a new IP and silently breaks the public
  hostname — so the tenant uses the Ingress path. **The existing `auth.fullfunding.nl` route is
  left exactly as it is**; correcting it is out of scope here and is not a prerequisite.
- **Host port 8094 (`fullfunding.nl`) and 8091/8092 must not be referenced by any tenant object.**
- The rev-11 fail-closed publication order and rollback wording are carried over verbatim (§4,
  REQ-3): DNS is published last and closed first, and the rollback is (1) point the rule at
  `http_status:404`, (2) restart cloudflared, (3) **prove** the closed response, (4) remove the DNS
  record, (5) only then touch the application.
**Acceptance:** NS-V7 (quota rejects a `type: LoadBalancer` Service in the namespace), NS-V8
(`req.fullfunding.nl` reaches the tenant through traefik; `fullfunding.nl` and `auth.fullfunding.nl`
regress clean after every cloudflared restart).

### NS-6 — Network isolation, including from the host stack
Default-deny `Ingress` **and** `Egress` in `fullfunding`, with an allowlist for exactly: traffic
within the namespace, DNS to `kube-dns`, ingress from the traefik namespace to the public workload
only, and the OIDC back-channel below. No internet egress by default (D-4).

**The OIDC back-channel is a new problem created by this move, and it must be decided before
implementation.** Under the Compose model the portal container had unrestricted egress, so
`OIDCProviderMetadataURL https://auth.fullfunding.nl/realms/req/.well-known/openid-configuration`
(rev-11 REQ-5a) simply worked. Under default-deny egress it does not: that hostname resolves to the
**public Cloudflare edge**, so the pod's metadata fetch and its token-endpoint call would have to
leave the cluster, cross the internet and re-enter through the tunnel. Two admissible resolutions,
**decided at implementation and asserted live (D-5)**:
- **(ii), recommended — split front-channel and back-channel.** The browser keeps using the public
  `https://auth.fullfunding.nl` authorization endpoint (unchanged, REQ-2's exact redirect URI still
  holds), while mod_auth_openidc's **back-channel** endpoints are configured explicitly to the
  in-cluster Service `http://oe-keycloak.options-edge.svc.cluster.local:8080/...` instead of being
  discovered from the public metadata URL. The **issuer string stays the public one** and must be
  pinned and verified — an issuer mismatch is a hard failure, not a warning. Egress allowlist: the
  `oe-keycloak` pod in namespace `options-edge`, port 8080, and nothing else.
- **(i), fallback — explicit internet egress** to the Cloudflare edge on 443. Simpler to configure,
  but it re-opens general outbound reachability from the tenant and widens the allowlist beyond one
  in-cluster pod. Choose only if (ii) proves unworkable with the packaged module version.

Whichever is chosen, `OIDCProviderIssuer`/discovery must be verified live against a decoded ID token
during rev-11's V4, exactly as rev-11 already requires.

**The load-bearing assertion, which must be tested and not assumed:** a pod in `fullfunding` must be
**unable** to open `192.168.100.252:9092` (host Kafka), `:5432` (host Postgres), `:8081`
(Schema Registry), `:8082` (AKHQ), `:8092` (internal Bugzilla) or `:5000` (registry).
NetworkPolicy enforcement is active in this cluster (§2.1), but egress to the **node's own IP** is
a known weak spot in some CNI implementations. **If NS-V9 shows any of those reachable, a host
firewall rule blocking the pod CIDR `10.42.0.0/24` to those ports (with an exception for the
`options-edge` workloads that legitimately use them) is a launch prerequisite, not a follow-up.**
This requirement is what makes rev-11's REQ-6 ("internal Bugzilla untouched") structurally stronger
than it was under the Compose model: the tenant cannot even reach `:8092`.
**Acceptance:** NS-V9 — negative connection test from a throwaway pod in the namespace to each of
the six ports above; all must fail. NS-V19 — the OIDC back-channel completes under the same policy
and the ID token carries the pinned issuer. Both re-run after every netpol change.

### NS-7 — A disk wall that actually enforces
`requests.storage` in a ResourceQuota bounds only what may be **declared**; `local-path` enforces
nothing and `/home` is `noquota` (§2.4). Therefore:

- Create a dedicated filesystem for the tenant, **without touching the `/home` mount**:
  `truncate -s 100G /home/fullfunding.img` → `mkfs.xfs` → fstab entry
  `loop,defaults` → mounted at `/home/fullfunding/data`.
- Deploy a **second `local-path-provisioner` instance** with its own `provisionerName`
  (e.g. `fullfunding.local/path`) and its own `nodePathMap` rooted at `/home/fullfunding/data`,
  exposed as StorageClass **`fullfunding-storage`** (a second provisioner instance is required — a
  single provisioner's `nodePathMap` selects per node, not per StorageClass).
- **No tenant PVC may use `local-path`, and this is enforced by the API server, not by review.**
  A StorageClass is a cluster-scoped object and a "default StorageClass per namespace" does not
  exist; the enforceable form is a storage-class-scoped quota, **validated against this API server
  by server-side dry-run** (§2.1):
  ```yaml
  local-path.storageclass.storage.k8s.io/persistentvolumeclaims: "0"
  fullfunding-storage.storageclass.storage.k8s.io/requests.storage: 60Gi
  ```
  Every tenant PVC therefore has to name `fullfunding-storage` explicitly; one that omits
  `storageClassName` inherits the cluster default `local-path` and is **rejected** by the first
  line. A CI check on the overlay is defence in depth, not the control.
- Sizing: Postgres 20 Gi, Kafka 20 Gi, backups 20 Gi, spare 40 Gi inside the 100 G image.

Result: whatever the tenant does, it cannot consume more than 100 G, and it cannot touch the
filesystem that k3s, Postgres and Docker share.
**Acceptance:** NS-V10 — a `fill` job inside the namespace exhausts the tenant filesystem and
`df /home` is unchanged; every tenant PVC binds to `fullfunding-storage`.

### NS-8 — Tenant platform services (Postgres, Kafka) — data-entry sizing
In-namespace, single replica each, **never the host instances**:

| Service | requests | limits | PVC | Notes |
|---|---|---|---|---|
| `postgres` | 100m / 512Mi | 1 / 2Gi | 20 Gi | tenant application data |
| `kafka` (KRaft, combined mode, 1 broker) | 200m / 1Gi | 1 / 2Gi | 20 Gi | `-Xmx1g`; **`log.retention.bytes` is mandatory**, not optional — it is the only thing that bounds Kafka's growth inside the 100 G image |
| app pods | per LimitRange | ≤1 / 2Gi | — | |

- The tenant's Kafka is reachable only at `kafka.fullfunding.svc.cluster.local:9092`; NS-6 denies
  the host broker.
- **This is a data-entry workload, not a streaming one.** Partition counts stay small; the
  4-partition and retention conventions used for OptionsEdge topics do not apply here.
- **Kafka at launch:** provisioned but **scaled to 0 replicas until a real consumer exists**, which
  returns ~1 Gi of the tight NS-4 slack. See open decision D-2.
**Acceptance:** NS-V11 — tenant workloads connect to the in-namespace Postgres/Kafka only; NS-V9
proves the host instances are unreachable.

### NS-9 — RBAC and credential separation
Dedicated `ServiceAccount` + `Role`/`RoleBinding` scoped to `fullfunding` (no ClusterRole, no
cluster-admin). The Jenkins job for this tenant uses a **separate kubeconfig credential** bound to
that ServiceAccount; the existing prod kubeconfig credential is never reused. **Secret read
permission is restricted to that ServiceAccount** (see R-14).
**Acceptance:** NS-V12 — the tenant credential can act in `fullfunding` and is denied in
`options-edge` and `kube-system` (`kubectl auth can-i` matrix, both directions).

### NS-10 — Jenkins-only, main-only delivery
Manifests live at `k8s/tenants/fullfunding/` in options-edge-deploy, applied by a **new,
tenant-specific Jenkins job** from `main` only. Images are digest-pinned and pulled from the
existing prod registry `.252:5000`. Deploy protocol: capture pre-deploy digests → apply → bounded
`rollout status` → on failure redeploy the recorded last-known-good digests → if LKG also fails,
fail loudly and apply the NS-5 public fail-closed rollback. No `kubectl` from a workstation.
**Acceptance:** Jenkins evidence; NS-V14 (LKG rehearsal).

### NS-11 — Guardrail verification is part of every deploy
The tenant job asserts, after each apply: quota present and non-zero-limited, LimitRange present,
PriorityClass negative and set on every tenant pod, netpol present with the NS-6 negative test
green, every PVC on `fullfunding-storage`, no LoadBalancer/NodePort Service. Any failure fails the
deploy. A guardrail that is not re-asserted is assumed absent.
**Acceptance:** NS-V15 — removing any single guardrail object makes the next deploy fail.

### NS-12 — Observation window
24 h observation after the first tenant workload is live, checking: no `options-edge` pod evicted,
restarted or throttled beyond its pre-change baseline; node memory pressure absent; tenant
filesystem usage flat; the three tunnel hostnames all serving. The baseline is captured **before**
the tenant is deployed, or the comparison is meaningless.
**Acceptance:** NS-V16 — baseline captured pre-change; window closed clean.

### NS-13 — Availability is explicitly NOT isolated (truth statement)
Compute, memory, disk and network isolation are delivered by NS-2…NS-8. **Availability is not.** A
reboot, kernel panic, k3s upgrade, `/home` exhaustion by a non-tenant, or a node failure takes both
projects down together. Anyone reading this document must not infer an availability guarantee from
the isolation guarantees. If the new project ever needs independent availability, the answer is a
second machine (the `.4`/es4 pattern), not a namespace.
**Acceptance:** stated and accepted (R-16).

### NS-14 — Rollback of the tenant as a whole
Deleting namespace `fullfunding` plus the tenant's cloudflared rule, DNS record, PriorityClass,
StorageClass, provisioner instance and loopback mount returns the node to its pre-change state. The
NS-4 kubelet reservation is **kept** (it is an improvement in its own right) unless it is shown to
have caused a regression. Rehearsed once before launch.
**Acceptance:** NS-V17 — teardown rehearsal on a scratch namespace leaves `kubectl get all -A`
diff-clean apart from the intended reservation.

## 4. Disposition of the rev-11 req-portal requirements

`docs/req-portal-bugzilla-keycloak-sso.md` rev 11 (Codex 3-bar APPROVE, round 11) stays the
authority for everything that is independent of where the containers run. This table is normative:
where it says SUPERSEDED, this document's text governs.

| rev-11 req | Disposition under the k8s model | What changes |
|---|---|---|
| REQ-1 realm `req` (bootstrap + kcadm reconciliation + recoverable state) | **UNCHANGED** | Keycloak stays in `options-edge`; realm work is unaffected by tenant hosting |
| REQ-2 confidential OIDC client `bugzilla-web` | **UNCHANGED** | exact redirect URI `https://req.fullfunding.nl/oidc-callback` still holds |
| REQ-3 public hostname, published last / closed first | **SUPERSEDED (mechanism only)** | cloudflared target becomes `http://127.0.0.1:80` (traefik, Host-matched) instead of `127.0.0.1:8093`. Ordering, pre-checks and the 5-step fail-closed rollback wording are carried over unchanged (NS-5) |
| REQ-4 images traceable, immutable, digest-deployed | **UNCHANGED in substance** | `bugzilla-req-web` / `bugzilla-req-db` still built by Jenkins from the pinned commit `276673ab6` to `.252:5000`; "compose references by digest" becomes "manifest references by digest" |
| REQ-5a exposure model, two listeners, header strip, `Require claim` | **PARTLY SUPERSEDED** | The **security contract is unchanged**: two structurally separate Apache listeners (public `:80`, admin `:81`), the public listener carrying only the OIDC vhost, the enumerated header strip, and the pinned `Require claim "email~…"` expression. What changes is the proof of reachability: `127.0.0.1:8093/8095` port bindings and the `V-lan` loopback test are replaced by — public port exposed through a ClusterIP Service + Ingress; **admin port 81 exposed by no Service and no Ingress**, reachable only via `kubectl port-forward` by a holder of the tenant kubeconfig (replacing `ssh -L`). `apachectl -S` remains the vhost-separation proof. **New acceptance NS-V18:** the admin port is unreachable from the pod network and from the LAN |
| REQ-5b session semantics & offboarding | **UNCHANGED** | "restart `bugzilla-req-web`" becomes "restart the web pod"; session invalidation semantics are identical |
| REQ-5c identity & claim contract (pinned to `276673ab6`) | **UNCHANGED** | source-pinned, hosting-independent |
| REQ-5d application state & authorization model | **UNCHANGED** | |
| REQ-6 internal Bugzilla untouched | **UNCHANGED, and strengthened** | NS-6 additionally makes `:8092` unreachable from the tenant network |
| REQ-7 cross-system isolation (token-level) | **UNCHANGED, plus evidence** | NS-V9 output is added as network-level evidence alongside the token-level proof |
| REQ-8 Jenkins-only build & deploy | **SUPERSEDED (mechanism)** | `docker compose up -d` → kustomize apply + bounded `rollout status`; LKG rollback becomes a digest re-pin. The schema-compatibility rule (checksetup migrates forward only; a rollback across a schema-migrating bump needs a DB restore) is **carried over unchanged** |
| REQ-9 secrets: scoped claim, file-mounted, atomic rotation | **SUPERSEDED (mechanism), with a NEW risk** | Host `0600` env files → k8s `Secret` **projected as files** into the container (never as container env, never in probe command text). The allowlist model is kept, with the allowlist re-enumerated for k8s. **New: the secret now also exists in the k3s datastore, unencrypted at rest, and is readable by any principal with `get secret` in the namespace** — NS-9 restricts that verb, and R-14 records the residual risk. `OIDCClientSecret exec:` and the crypto-passphrase lifecycles are unchanged |
| REQ-10a windows & backups | **SUPERSEDED (mechanism)** | host cron → k8s `CronJob` in the namespace writing one atomic generation (manifest-last completion marker, checksums, 14-day retention) to a PVC on `fullfunding-storage`; `mysqldump --single-transaction` unchanged; Keycloak `pg_dump` unchanged (it is not a tenant object). V-restore's separate-stack, temporary-client, loopback-callback topology is preserved, with `kubectl port-forward` replacing `ssh -L` |
| REQ-10b health model (liveness ≠ readiness ≠ security gate) | **SUPERSEDED (mechanism)** | Docker healthchecks → `livenessProbe`/`readinessProbe`. The distinction is kept: a probe is never the security gate |
| REQ-10c verification scope & observation | **EXTENDED** | rev-11 matrix plus NS-V1…NS-V18; the 24 h observation window merges with NS-12 |
| REQ-11 login-surface & edge hardening | **UNCHANGED** | |
| REQ-12 patch & vulnerability posture | **UNCHANGED** | |
| REQ-13 privacy & data handling | **UNCHANGED** | |
| §8 risk register R-1…R-13 | **CARRIED OVER**, plus R-14…R-18 (§8 below) | R-12 (host-level compromise) is **narrowed**: the tenant no longer needs Docker-daemon access, but gains namespace-scoped Kubernetes access |

**Database engine, stated plainly:** the user's requirement names **Postgres**; rev 11 pins Bugzilla
to **MariaDB** (built from the pinned checkout's `Dockerfile.mariadb`). Bugzilla supports both, but
switching engines invalidates the image pinning, the schema/backup design and the Codex-approved
REQ-4/REQ-10a text. **This document keeps MariaDB for Bugzilla and provisions the requested Postgres
as the namespace's platform database for the data-entry applications that follow.** This is open
decision **D-1** and is the one item most likely to need the user's correction.

## 5. Non-goals

- Not multi-tenancy as a product: one namespace, one tenant, one operator.
- No availability isolation (NS-13), no second node, no HA.
- No disk **I/O** isolation — see R-15.
- No change to the `optionsedge` realm, to the internal Bugzilla, or to any OptionsEdge manifest
  (NS-3 is deliberately designed to require zero OptionsEdge edits).
- No XFS project quota on `/home` (needs downtime; NS-7 avoids it).
- No migration of any other OptionsEdge component into or out of the cluster.

## 6. Rollout sequence (fail-closed, ordered)

All steps outside Mon–Fri 09:30–16:15 America/New_York, serialized. Each step's verification must
pass before the next begins.

0. **Pre-flight:** re-verify 8093/8095 free (still recorded for the admin path), `req.fullfunding.nl`
   DNS record **absent**, disk/memory headroom, and capture the NS-12 pre-change baseline.
1. **RTH measurement** for NS-4 (`kubectl top` + host RSS during regular trading hours) → commit the
   `system-reserved` value. *(Blocks step 2; resolves D-3.)*
2. **Node reservation** (NS-4) — k3s config + restart in the window; re-verify all 55 OptionsEdge
   deployments schedule. This step stands alone and is valuable even if the tenant is cancelled.
3. **Storage wall** (NS-7) — loopback image, mount, second provisioner, `fullfunding-storage`.
4. **Namespace + guardrails** (NS-1, NS-2, NS-3, NS-6, NS-9) — quota, LimitRange, PriorityClass,
   netpol, RBAC. **NS-V9 negative network test runs here**; a failure triggers the host firewall
   prerequisite before anything else proceeds.
5. **Platform services** (NS-8) — Postgres; Kafka manifested at 0 replicas.
6. **Portal workloads** — `bugzilla-req-web` + `bugzilla-req-db` per rev 11 §6 steps 1–7, adapted
   per §4. Realm `req` and the OIDC client (REQ-1, REQ-2) are created here.
7. **Private verification (V-pre)** — the full rev-11 matrix plus NS-V1…NS-V18, with **no public
   exposure yet**, reached through `kubectl port-forward`.
8. **Publish** (NS-5) — Ingress, then the cloudflared rule, then DNS **last**. V3/V4/V6t/V8/V11 run
   immediately after, before any stakeholder user is provisioned.
9. **Onboarding gate** — every matrix row green **and** the 24 h NS-12 window closed clean. Only
   then are external users provisioned.

Rollback at any point after step 8 uses the NS-5 five-step public fail-closed procedure; before
step 8 there is no public exposure and teardown is private (NS-14).

## 7. Verification matrix (namespace additions)

| id | Asserts | Method | Blocks |
|---|---|---|---|
| NS-V1 | namespace + label exist; no tenant object outside it | `kubectl get` | step 5 |
| NS-V2 | quota rejects an over-budget pod | apply a 5 CPU pod → expect `exceeded quota` | step 5 |
| NS-V3 | LimitRange defaults applied to a resource-less pod | apply + inspect effective resources | step 5 |
| NS-V4 | CI check fires on an injected `limits.cpu` in `options-edge` | deliberate bad manifest in CI | merge |
| NS-V5 | tenant pods priority −100; pressure rehearsal evicts tenant first | `kubectl get pod -o …priority` + rehearsal | step 8 |
| NS-V6 | post-reservation allocatable + full scheduling fit | `describe node` + schedule all 55 | step 3 |
| NS-V7 | quota rejects `type: LoadBalancer` and NodePort | apply → expect rejection | step 8 |
| NS-V8 | `req.fullfunding.nl` serves via traefik; `fullfunding.nl` + `auth.fullfunding.nl` regress clean after each cloudflared restart | HTTP checks | step 9 |
| NS-V9 | tenant pod **cannot** reach `.252` on 9092/5432/8081/8082/8092/5000 | throwaway pod, per-port connect test | step 5 |
| NS-V10 | tenant cannot consume beyond the 100 G image; `df /home` unaffected | fill job | step 8 |
| NS-V11 | tenant uses only in-namespace Postgres/Kafka | config + connection inspection | step 8 |
| NS-V12 | tenant credential denied in `options-edge`/`kube-system`, allowed in `fullfunding` | `kubectl auth can-i` matrix | step 8 |
| NS-V13 | the 8091 svclb restart cause — confirmed or refuted at RTH with feed-gateway scaled up | `describe pod` + `ss -ltnp` | reporting only |
| NS-V14 | LKG digest rollback rehearsal | Jenkins | step 8 |
| NS-V15 | removing any guardrail object fails the next deploy | deliberate removal in rehearsal | step 8 |
| NS-V16 | 24 h observation closed clean against a pre-change baseline | comparison | step 9 |
| NS-V17 | full teardown returns the node to pre-change state | scratch-namespace rehearsal | step 8 |
| NS-V18 | admin listener (container `:81`) unreachable from the pod network and the LAN; only `port-forward` reaches it | negative tests both paths | step 8 |
| NS-V19 | under default-deny egress the OIDC back-channel resolves and completes, **and** the issuer in a decoded ID token is the pinned public issuer | login through the portal + token decode (with rev-11 V4) | step 8 |

## 8. Accepted-risk register (additions to rev-11 R-1…R-13)

| id | Risk | Why accepted / mitigation |
|---|---|---|
| R-14 | **Secrets at rest in the k3s datastore**, unencrypted, and readable by any principal with `get secret` in the namespace — weaker than rev-11's host `0600` files | NS-9 restricts the verb to the tenant ServiceAccount; the datastore lives under `/home/options-edge/data/k3s` with the same host custody as every other prod secret. Encryption-at-rest for k3s secrets is a possible later hardening, out of scope here |
| R-15 | **No disk-I/O or page-cache isolation.** A tenant doing heavy I/O can raise host Kafka latency; no k8s control bounds this | The workload is data-entry, not streaming (NS-8); the tenant filesystem is a separate loopback image; monitored in NS-12. If it ever bites, the answer is a second machine |
| R-16 | **Shared availability** (NS-13) — reboot / kernel / k3s upgrade takes both projects down | Explicitly accepted and stated; single-operator scope |
| R-17 | Loopback-file filesystem adds a small I/O overhead vs a native volume | Negligible at data-entry volume; it is the only way to get a hard disk wall without unmounting `/home` |
| R-18 | The admin surface now depends on possession of the tenant kubeconfig rather than SSH to `.252` | Narrower than SSH in scope (namespace-only), but a new credential to protect (NS-9); rev-11's R-7 for the admin vhost still applies |
| R-12 (narrowed) | Host-level compromise | The tenant no longer requires Docker-daemon access (a reduction); it gains namespace-scoped Kubernetes access instead |

## 9. Open decisions

| id | Decision | Recommendation |
|---|---|---|
| **D-1** | Bugzilla's database engine: keep **MariaDB** (rev-11 pinned, Codex-approved) or switch to **Postgres** as the user's wording suggests? | **Keep MariaDB for Bugzilla; run the requested Postgres as the namespace platform DB for the data-entry apps that follow.** Switching Bugzilla's engine invalidates REQ-4's image pinning and REQ-10a's backup design and would need a fresh Codex round |
| **D-2** | Is Kafka needed **at launch**, or is it for later data-entry work? | Manifest it at **0 replicas** until a real consumer exists; that returns ~1 Gi to the thin NS-4 slack. Bugzilla itself does not use Kafka |
| **D-3** | `system-reserved` value (14 Gi is derived from an **off-hours** measurement) | Measure at RTH first (§6 step 1) and commit the measured number. Do not ship the off-hours figure |
| **D-4** | Does the tenant need egress to the public internet (e.g. package fetches at runtime)? | Default **no** — NS-6 allowlist has no internet egress. If a runtime need appears, it is an explicit, reviewed allowlist entry |
| **D-5** | OIDC back-channel path under default-deny egress (NS-6): split front/back-channel with in-cluster Keycloak endpoints, or allow internet egress to the Cloudflare edge? | **Split (option ii)** — narrowest allowlist (one in-cluster pod), public issuer pinned and verified in V4. Falls back to (i) only if the packaged mod_auth_openidc cannot take explicit endpoint configuration |

## 10. Gate status

- **Gate 1 (requirements):** this document. Refined against verified system state; **awaiting Codex
  3-bar review, then the user's explicit approval.** No implementation before that approval.
- **Gate 2 (implementation):** not started. Manifests, Jenkins job and the rev-11 portal work follow
  only after Gate 1 is approved.
