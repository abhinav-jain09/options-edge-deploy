# Domain migration: fullfunding.nl → bleadingoptions.com

Operator runbook for moving every public hostname to `bleadingoptions.com`. Three phases; outside
the scheduled Phase-1 blips and the declared Phase-2 outage window (both sections below state their
bounds), the old domain keeps serving until Phase 2 completes. The canonical tunnel desired state lives in
[`infra/prod/cloudflared/options-edge-stable.yml`](../infra/prod/cloudflared/options-edge-stable.yml)
(this doc references it, never repeats it); `scripts/ops/verify-prod-tunnel.sh` proves live == repo
across BOTH domains and is the acceptance gate for each phase.

| Old | New | Backend |
|---|---|---|
| `fullfunding.nl` | `bleadingoptions.com` | prod web `.252:8094`, WS `.252:30097` |
| `es.fullfunding.nl` | `es.bleadingoptions.com` | es4 es-web `.4:30080`, WS `.4:30091` |
| `auth.fullfunding.nl` | `auth.bleadingoptions.com` | Keycloak ClusterIP `:8080` (`/admin` + `/realms/master` edge-404) |
| `req.fullfunding.nl` | `req.bleadingoptions.com` | dark today (realm client exists; no DNS, no tunnel route) — renamed in the realm at Phase 3, wired up only if the portal ever ships |

Both zones sit in the SAME Cloudflare account (same nameserver pair), so no registrar/account moves
are involved; the tunnel (`options-edge-option-chain`, `976f76d2-e3c8-4887-a11d-21c27f5e8bed`) is
shared and hostnames from either zone route to it.

## Phase 1 — additive dual-run (scheduled maintenance; bounded old-domain blips)

Phase 1 does not change any old-domain behavior, but two of its steps DO briefly interrupt live
old-domain connections, so run it while the market is closed:

- the cloudflared restart (step 2) drops every open tunnel connection for a few seconds
  (systemd restart; clients reconnect on their own), and
- each feed-gateway deploy (step 4) is a single-replica `Recreate` rollout — open WebSockets
  disconnect and the endpoint is absent until the new pod passes readiness (~30-60 s each,
  observed; the Jenkins job waits for rollout + health before finishing).

Executed 2026-08-07/08 after the Friday close (CME closed until Sunday 18:00 ET).

1. **Cloudflare dashboard** (zone `bleadingoptions.com`): proxied CNAMEs `@`, `es`, `auth` →
   `976f76d2-e3c8-4887-a11d-21c27f5e8bed.cfargotunnel.com`.
2. **Tunnel**: new-domain ingress blocks per the canonical YAML (applied 2026-08-08; backup
   `options-edge-stable.yml.bak-20260808-domain-migration` on the host).
3. **Keycloak LIVE realm** (kcadm, in-pod — `--import-realm` skips existing realms, so the realm
   configmap alone never mutates a live realm): client `options-edge-web` redirectUris/webOrigins/
   post-logout now carry both domains (applied 2026-08-08). The realm configmap in this repo holds
   the same values as source-of-truth parity.
4. **feed-gateway** (this PR): `WS_ALLOWED_ORIGINS` accepts both apex origins (prod) / both es
   origins (es4). Deploy: per-service `feed-gateway` production job + `es4-deploy deploy-service
   SERVICE=es-feed-gateway`.

**Phase-1 verification matrix** (`verify-prod-tunnel.sh` automates the tunnel/auth rows):

| Check | Old domain | New domain |
|---|---|---|
| Page serves (200, login redirect) | ✅ must stay green | ✅ |
| OIDC discovery 200 / admin paths 404 | ✅ | ✅ |
| Unauthed WS upgrade → 401 (apex + es) | ✅ | ✅ |
| Authenticated login round-trip | ✅ | ✅ (issuer still `auth.fullfunding.nl` — by design) |
| REST `/api/*` from the browser | ✅ | ❌ **known-blocked**: the served page points at absolute `https://fullfunding.nl` API bases and the web backend serves no CORS headers, so cross-origin fetches are browser-blocked until the Phase-2 env flip. Do not "fix" this with CORS — Phase 2 removes the cross-origin condition itself. |

## Phase 2 — cutover (a declared AUTH/UI OUTAGE WINDOW, not a seamless flip)

Be explicit about what this window is: from the Keycloak deploy until the last consumer deploy +
redirect, authentication is NOT continuously available on either domain, and there are transient
mixed states (Keycloak's RollingUpdate briefly runs old-issuer and new-issuer pods side by side, so
a token minted in that overlap may be rejected by a consumer; after the web env flip and before the
redirect, an old-domain page calls new-domain APIs cross-origin and those fetches are
browser-blocked). None of these states are avoidable with single-replica services and a
single-valued `KC_HOSTNAME` — the mitigation is scheduling, not engineering: run the whole window
while the market is closed (weekend), expect every session to be invalidated, and verify the new
domain end-to-end before leaving the window. Bound: minutes per deploy step, dominated by the
Keycloak and feed-gateway rollouts.

No web image rebuild: the served URLs come from RUNTIME env (`RuntimeProfileConfig` injects
`window.__OPTIONS_EDGE_ENV__` from pod env each request). The Jenkins URL matrices are build-time
defaults/documentation only — keep them in sync, but the deployed truth is the k8s env.

One coordinated change-set (single PR, single deploy window):

1. Keycloak `KC_HOSTNAME` → `https://auth.bleadingoptions.com` (+ config-nonce bump). This changes
   the TOKEN ISSUER — partial deploys leave services rejecting every token, so the whole set below
   ships together.
2. Every issuer/JWKS consumer: prod feed-gateway `WS_AUTH_ISSUER_URI`, spx-mission-control
   `MISSION_AUTH_ISSUER_URI` + JWK URI, es4 es-feed-gateway issuer, web `VITE_AUTH_ISSUER`.
3. Web runtime URLs (prod web deployment + es4 es-web): `VITE_API_BASE_URL`, `VITE_WS_URL`,
   `APP_FEED_GATEWAY_WS_URL`, `VITE_MISSION_CONTROL_URL`, `VITE_REPLAY_ORCHESTRATOR_URL` → new
   domain; add `APP_ES_OPTIONS_URL=https://es.bleadingoptions.com` (the WebNav default still
   points at the old domain).
4. Jenkinsfile URL matrices (`Jenkinsfile.web-service`, `Jenkinsfile.bring-up-all`) → new domain.
5. Deploy order: keycloak (base pipeline — the common-infra path owns Keycloak only, it does NOT
   apply service workloads) → feed-gateway (per-service job) → spx-mission-control (per-service
   job; it declares 0 replicas today, but its desired state must not stay pinned to a dead issuer
   or the next scale-up boots broken) → web (`web-service-deploy ENVIRONMENT=production
   BUILD_IMAGE=false`; confirm the `:prod` tag still resolves to the running digest first) → es4
   `deploy-service` es-feed-gateway + es-web. Prod before es4 (es4 pulls the same image/pattern).
6. Verify: full matrix above goes green on the NEW domain including REST; old-domain UI now serves
   pages pointing at new-domain APIs — immediately shadowed by the redirect (next step).
7. **Cloudflare dashboard** — three separate Single Redirects on the `fullfunding.nl` zone (one
   per hostname; a fixed target cannot host-map, and in wildcard patterns `${1}` is the FIRST
   wildcard, so keep the hostname out of the pattern's wildcards). Preserve query string; start
   every rule as **307** (temporary) for rollback safety — a cached permanent redirect cannot be
   recalled from browsers — and promote to **308** (not 301: 301 may rewrite POST to GET; 308
   preserves the method) after the soak:

   | # | Wildcard pattern | Target | Status |
   |---|---|---|---|
   | 1 | `https://fullfunding.nl/*` | `https://bleadingoptions.com/${1}` | 307 → 308 |
   | 2 | `https://es.fullfunding.nl/*` | `https://es.bleadingoptions.com/${1}` | 307 → 308 |
   | 3 | `https://auth.fullfunding.nl/*` | `https://auth.bleadingoptions.com/${1}` | 307 → 308 |

   Test each rule before promotion: `curl -sI https://fullfunding.nl/board?x=1` must return the
   307 with `Location: https://bleadingoptions.com/board?x=1`, and likewise for the `es.`/`auth.`
   hostnames.

Rollback (before the 308 promotion): revert the PR, redeploy the same set, drop the 307 rules.
Old-issuer tokens die at the flip in both directions — that is expected, not a defect.

## Phase 3 — retire

After soak: remove old-domain ingress blocks from the canonical YAML + live (verifier keeps them
honest), drop old-domain entries from the realm (live via kcadm + configmap parity), rename the
`req` realm client, sweep docs/bookmarks, shrink `APEX_HOSTS`/`ES_HOSTS`/`AUTH_HOSTS` defaults in
the verifier to the new domain only.
