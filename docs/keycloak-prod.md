# Production Keycloak (OptionsEdge) — operator runbook

Stands up the production identity provider that secures the option-chain UI and feed-gateway behind login.
Deployed **into the existing `options-edge` namespace** via the normal Jenkins pipeline (`options-edge-deploy`
`k8s/base`), so the Jenkins-only admission policy and deployer RBAC already cover it. The apps are OAuth2
**resource servers** — they validate Keycloak's JWTs and never run Keycloak themselves; these manifests
stand up the issuer they point at.

## What it deploys (namespace `options-edge`)

| File | Resource | Purpose |
|------|----------|---------|
| `keycloak-realm-configmap.yaml` | ConfigMap `oe-keycloak-realm` | Hardened realm import (client `options-edge-web`, no test user, origins pinned to the public web origins — both domains during the bleadingoptions.com migration; see docs/domain-migration-bleadingoptions.md) |
| `keycloak-postgres.yaml` | headless Service + StatefulSet + **PVC** | Durable Keycloak database (state on disk, not H2/container layer) |
| `keycloak-deployment.yaml` | Deployment + **ClusterIP** Service (`:8080`) + headless Service + `oe-keycloak-lan` **LoadBalancer** (`192.168.100.252:8089`, LAN admin console) | Keycloak in production mode (`start --import-realm`) on Postgres |
| `keycloak-ingress.yaml` | traefik Ingress | Routes `Host: auth.fullfunding.nl` `/realms/optionsedge` + `/resources` → `oe-keycloak:8080` (admin paths get no route → 404) |

## Edge / exposure model

```
browser ──HTTPS──▶ Cloudflare edge (TLS terminates) ──tunnel──▶ cloudflared on prod host
         ──▶ http://10.43.127.26:8080 (oe-keycloak ClusterIP, Host header forced per auth hostname)
```

- The public path does **NOT** traverse traefik: the prod node's traefik `:80` proved broken, so the
  cloudflared ingress targets the `oe-keycloak` ClusterIP directly (hardcoded IP — the canonical
  tunnel file documents the re-read command if the Service is ever recreated). The traefik Ingress
  in `keycloak-ingress.yaml` remains as an in-cluster routing definition only.
- Keycloak is additionally published on the LAN as the `oe-keycloak-lan` **LoadBalancer**
  (`192.168.100.252:8089`) for admin-console access (`KC_HOSTNAME_ADMIN`); this is a deliberate
  LAN-only admin path, so the original "ClusterIP only, LAN cannot reach it" claim no longer holds.
  The public edge still cannot reach `:8089`.
- The **management port `:9000`** (health/metrics) is a containerPort used by the probes in-cluster; the
  Service never publishes it.
- `/admin` and `/realms/master` are **edge-blocked by the cloudflared 404 rules** for every public
  auth hostname (both domains during the migration). The traefik-route block only applies on the
  in-cluster Ingress path, which public traffic no longer takes — the edge rules are the real
  control. Realm/user admin is performed via `kcadm.sh` in-pod or the LAN admin console, never over
  the public internet.

## Before deploy — operator prerequisites (NOT done by the pipeline)

### 1. Create the Secret `oe-keycloak-secrets` (out-of-band; never in git)

```sh
# On the prod host (or with the prod kubeconfig). Choose strong values.
# Two keys only: Postgres AND Keycloak's JDBC both read POSTGRES_PASSWORD (single source of truth — they
# cannot drift), and KC_BOOTSTRAP_ADMIN_PASSWORD seeds the initial admin.
kubectl -n options-edge create secret generic oe-keycloak-secrets \
  --from-literal=POSTGRES_PASSWORD='<strong-db-password>' \
  --from-literal=KC_BOOTSTRAP_ADMIN_PASSWORD='<strong-admin-password>'
```

The Postgres StatefulSet and the Keycloak Deployment both fail to start without this Secret — create it
**before** the Jenkins deploy.

### 2. DNS — add `auth.fullfunding.nl` (Cloudflare account)

In the Cloudflare dashboard for `fullfunding.nl`, add a **proxied** record for `auth` pointing at the same
tunnel as `fullfunding.nl` (a CNAME to `<tunnel-id>.cfargotunnel.com`, or via `cloudflared tunnel route dns
options-edge-option-chain auth.fullfunding.nl`).

### 3. cloudflared ingress — route the auth hostnames to Keycloak

The reviewed tunnel config lives at
[`infra/prod/cloudflared/options-edge-stable.yml`](../infra/prod/cloudflared/options-edge-stable.yml).
Deploying a change is a fail-safe sequence — stage, validate, back up, install atomically, restart
the EXACT unit, verify (never edit the live file in place):

```sh
# on the prod host, with the merged repo copy staged at ~/options-edge-stable.yml.new
cloudflared tunnel --config ~/options-edge-stable.yml.new ingress validate
# authoritative preflight: cloudflared itself resolves every public route against the STAGED file
for u in https://fullfunding.nl/ws/events https://fullfunding.nl/ \
         https://bleadingoptions.com/ws/events https://bleadingoptions.com/ \
         https://auth.fullfunding.nl/admin https://auth.fullfunding.nl/realms/optionsedge \
         https://auth.bleadingoptions.com/admin https://auth.bleadingoptions.com/realms/optionsedge \
         https://es.fullfunding.nl/ws/events https://es.bleadingoptions.com/ws/events; do
  cloudflared tunnel --config ~/options-edge-stable.yml.new ingress rule "$u"
done
sudo cp /etc/cloudflared/options-edge-stable.yml /etc/cloudflared/options-edge-stable.yml.bak-$(date +%Y%m%d-%H%M%S)
sudo cp ~/options-edge-stable.yml.new /etc/cloudflared/options-edge-stable.yml.tmp   && sudo mv /etc/cloudflared/options-edge-stable.yml.tmp /etc/cloudflared/options-edge-stable.yml
sudo systemctl restart options-edge-cloudflared-stable.service   # the unit name — NOT bare "cloudflared"
systemctl is-active options-edge-cloudflared-stable.service
journalctl -u options-edge-cloudflared-stable.service -n 20 --no-pager   # no ERR lines
scripts/ops/verify-prod-tunnel.sh                                        # phase-appropriate gate
# rollback (exact, using the backup taken above):
#   sudo cp /etc/cloudflared/options-edge-stable.yml.bak-<timestamp> /etc/cloudflared/options-edge-stable.yml
#   sudo systemctl restart options-edge-cloudflared-stable.service
``` (An earlier revision of this doc inlined a
sample config here; it drifted — `/ws/events` must target the gateway NodePort 30097, never the
:8091 ServiceLB, and the web backend moved to `.252:8094`. The canonical file carries the incident
notes.) The `/admin` + `/realms/master` 404 rules MUST precede each auth hostname's catch-all rule
(first match wins), for every auth hostname (`auth.fullfunding.nl` and, since the domain migration,
`auth.bleadingoptions.com` — see docs/domain-migration-bleadingoptions.md).


## Deploy (via Jenkins)

`options-edge-deploy` job, `ENVIRONMENT=production`, `DEPLOY_BRANCH=main` (after merge). The pipeline renders
`k8s/overlays/production` (which includes `../../base`) and applies it. KC's `quay.io/keycloak/keycloak` and
`postgres` are public images pinned by `tag@sha256:digest` — exempt from the `/options-edge-*` registry-digest
pin gate (which only covers in-house images), but still reproducible.

Watch it come up:

```sh
kubectl -n options-edge rollout status statefulset/oe-keycloak-postgres
kubectl -n options-edge rollout status deployment/oe-keycloak
```

## First admin login + create a user (in-cluster, no public /admin)

Use `kcadm.sh` inside the pod — this targets the local server and does not need the public admin console:

```sh
ADMIN_PW=$(kubectl -n options-edge get secret oe-keycloak-secrets -o jsonpath='{.data.KC_BOOTSTRAP_ADMIN_PASSWORD}' | base64 -d)
POD=$(kubectl -n options-edge get pod -l app.kubernetes.io/name=oe-keycloak -o name | head -1)
kubectl -n options-edge exec "$POD" -- /opt/keycloak/bin/kcadm.sh config credentials \
  --server http://localhost:8080 --realm master --user admin --password "$ADMIN_PW"
# create a login user for the option-chain UI
kubectl -n options-edge exec "$POD" -- /opt/keycloak/bin/kcadm.sh create users -r optionsedge \
  -s username=<user> -s enabled=true
kubectl -n options-edge exec "$POD" -- /opt/keycloak/bin/kcadm.sh set-password -r optionsedge \
  --username <user> --new-password '<strong-pw>'
# (optional) grant replay roles for the replay UI
kubectl -n options-edge exec "$POD" -- /opt/keycloak/bin/kcadm.sh add-roles -r optionsedge \
  --username <user> --rolename replay-admin
```

After bootstrap, create a permanent admin and rotate `KC_BOOTSTRAP_ADMIN_PASSWORD` out of the Secret.

## Verify the issuer

```sh
# Public OIDC discovery (through the tunnel) — issuer must be the HTTPS hostname:
curl -s https://auth.fullfunding.nl/realms/optionsedge/.well-known/openid-configuration | jq .issuer
# expect: "https://auth.fullfunding.nl/realms/optionsedge"
# Admin console must NOT be public:
curl -s -o /dev/null -w '%{http_code}\n' https://auth.fullfunding.nl/admin/    # expect 404
```

## Point the apps at it (next phase — NOT in this change)

Built into the web image and feed-gateway env once this issuer is verified:

```sh
# option-chain web (APP_PROFILE=prod)
VITE_AUTH_ENABLED=true
VITE_AUTH_ISSUER=https://auth.fullfunding.nl/realms/optionsedge
VITE_AUTH_CLIENT_ID=options-edge-web
AUTH_AUDIENCE=options-edge-web
# feed-gateway (oc.bearer WS auth — the web app's path; NOT the ticket/GATEWAY_AUTH_ENABLED mode,
# which needs a replay orchestrator + approval gate prod doesn't run). Set on the feed-gateway Deployment.
WS_AUTH_ENABLED=true
WS_AUTH_ISSUER_URI=https://auth.fullfunding.nl/realms/optionsedge
WS_AUTH_AUDIENCE=options-edge-web
WS_ALLOWED_ORIGINS=https://fullfunding.nl   # explicit allow-list required once auth is on
```

The app prod profiles **fail closed**: they refuse to start unless auth is enabled with a non-loopback HTTPS
issuer and explicit audience/client id.

## Known limitations / follow-ups

- **Backups:** Postgres uses k3s `local-path` (node-affine). The PVC survives pod restarts but not node loss
  or deletion/corruption. Add a `pg_dump` CronJob or volume snapshots before treating this as DR-grade.
- **Realm drift:** `--import-realm` only seeds a *new* realm; later changes are not reconciled on boot. Make
  realm edits via `kcadm.sh` (above) or a dedicated import job.
- **traefik header trust:** traefik on `:80` is LAN-reachable. The prod LAN is trusted and Cloudflare is the
  only public path, but for full closure firewall `:80/:443` to the cloudflared host or set the traefik
  entrypoint `forwardedHeaders.trustedIPs`.
- **Admin console:** reachable only in-cluster (kcadm / port-forward) by design. To use the web console,
  front a `/admin` route with Cloudflare Access (operator's CF dashboard) rather than unblocking it openly.
