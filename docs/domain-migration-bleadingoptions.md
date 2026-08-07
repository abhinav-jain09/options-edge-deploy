# Domain migration: fullfunding.nl → bleadingoptions.com

Operator runbook for moving every public hostname to `bleadingoptions.com`. Three phases; the old
domain stays fully functional until Phase 2 completes. The canonical tunnel desired state lives in
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

## Phase 1 — additive dual-run (zero risk to the old domain)

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

## Phase 2 — cutover (off-hours only; every session is invalidated)

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
5. Deploy order: keycloak (base pipeline) → feed-gateway → web (`web-service-deploy
   ENVIRONMENT=production BUILD_IMAGE=false`; confirm the `:prod` tag still resolves to the running
   digest first) → es4 `deploy-service` es-feed-gateway + es-web. Prod before es4 (es4 pulls the
   same image/pattern).
6. Verify: full matrix above goes green on the NEW domain including REST; old-domain UI now serves
   pages pointing at new-domain APIs — immediately shadowed by the redirect (next step).
7. **Cloudflare dashboard**: redirect rule `*fullfunding.nl/*` → `https://bleadingoptions.com/$1`
   (host-mapped; es→es). Start as **307** (temporary) for rollback safety — a cached 301 cannot be
   recalled from browsers — promote to 301 after the soak.

Rollback (before the 301 promotion): revert the PR, redeploy the same set, drop the 307 rule.
Old-issuer tokens die at the flip in both directions — that is expected, not a defect.

## Phase 3 — retire

After soak: remove old-domain ingress blocks from the canonical YAML + live (verifier keeps them
honest), drop old-domain entries from the realm (live via kcadm + configmap parity), rename the
`req` realm client, sweep docs/bookmarks, shrink `APEX_HOSTS`/`ES_HOSTS`/`AUTH_HOSTS` defaults in
the verifier to the new domain only.
