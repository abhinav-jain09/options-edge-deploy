# Domain migration: fullfunding.nl → bleadingoptions.com

Operator runbook for moving every public hostname to `bleadingoptions.com`. Three phases; outside
the scheduled Phase-1 blips and the declared Phase-2 outage window (both sections below state their
bounds), the old domain keeps serving until Phase 2 completes. The canonical tunnel desired state lives in
[`infra/prod/cloudflared/options-edge-stable.yml`](../infra/prod/cloudflared/options-edge-stable.yml)
(this doc references it, never repeats it); `scripts/ops/verify-prod-tunnel.sh` proves live == repo
and is the acceptance gate — the steady state is `--phase retired` (its default). `--phase dual`
and `--phase redirect` are the historical Phase-1/2 gates, kept for auditability. It fails closed: a
check it cannot run (unreachable kubectl, etc.) is a FAIL, not a skip (`--network-only` exists for
credential-less diagnostics only, never for acceptance).

| Old | New | Backend |
|---|---|---|
| `fullfunding.nl` | `bleadingoptions.com` | prod web `.252:8094`, WS `.252:30097` |
| `es.fullfunding.nl` | `es.bleadingoptions.com` | es4 es-web `.4:30080`, WS `.4:30091` |
| `auth.fullfunding.nl` | `auth.bleadingoptions.com` | Keycloak ClusterIP `:8080` (`/admin` + `/realms/master` edge-404) |
| `req.fullfunding.nl` | `req.bleadingoptions.com` | dark (realm client only; no DNS, no tunnel route) — renamed at Phase 3, wired up only if the portal ever ships |

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

Execution status (as of 2026-08-08, Friday after the close; CME closed until Sunday 18:00 ET):
steps 2 and 3 APPLIED; step 1 (DNS CNAMEs) PENDING with the operator; step 4 (gateway deploys)
PENDING on this PR's merge. Phase 1 is accepted only when `--phase dual` goes fully green.

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

**Phase-1 verification matrix.** `timeout 600 scripts/ops/verify-prod-tunnel.sh --phase dual` automates the edge, issuer,
allow-list (running-pod env, both clusters), live-realm-client and unauthenticated web rows. The
**login round-trip and the browser REST behavior are a MANDATORY MANUAL gate** — a curl cannot
exercise the browser-injected API base, the OIDC redirect dance, or CORS. The manual gate MUST
include the authenticated WebSocket: the 2026-07-31 ServiceLB incident returned a clean
unauthenticated 401 while every AUTHENTICATED upgrade died, so "401 seen" proves routing, not a
working socket. Two parts, because in Phase 1 the pages still target the OLD `wss://` hostnames —
opening the new pages exercises the new Origins but NOT the new WS routes:

1. In the browser (both apex and es pages): log in, confirm the network tab's WS request reaches
   `101 Switching Protocols` and live board data updates — and note WHICH Request URL that was.
2. Authenticated direct probe of each NEW WS hostname the pages did not traverse. The browser
   carries the token as a WebSocket SUBPROTOCOL (`["oc.bearer", <accessToken>]`), NOT an
   Authorization header — copy the token from the page's WS request (network tab →
   `Sec-WebSocket-Protocol` request header, second value). The token is a LIVE credential — it
   must never enter shell history or any process argv, so it is read silently and passed to curl
   through a config stream:

   ```sh
   read -rs -p "paste token: " TOK; echo
   for host in bleadingoptions.com es.bleadingoptions.com; do
     printf 'header = "Sec-WebSocket-Protocol: oc.bearer, %s"\n' "$TOK" \
       | curl -s --http1.1 -o /dev/null -w "$host: %{http_code}\n" -m 8 --config /dev/stdin \
           -H 'Connection: Upgrade' -H 'Upgrade: websocket' \
           -H "Origin: https://$host" \
           -H "Sec-WebSocket-Key: $(head -c16 /dev/urandom | base64)" -H 'Sec-WebSocket-Version: 13' \
           "https://$host/ws/events"
   done
   unset TOK
   ```

   `101` = the authenticated upgrade crossed the new route end-to-end (curl then idles until the
   -m 8 timeout — that is expected). Anything else fails the gate.

Only after both parts pass on both domains is the phase accepted:

| Check | Old domain | New domain |
|---|---|---|
| Page serves (200, login redirect) | ✅ must stay green | ✅ |
| OIDC discovery 200 / admin paths 404 | ✅ | ✅ |
| Unauthed WS upgrade → 401 (apex + es) | ✅ | ✅ |
| Authenticated login round-trip | ✅ | ✅ (issuer still `auth.fullfunding.nl` — by design) |
| REST `/api/*` from the browser | ✅ | ❌ **known-blocked**: the served page points at absolute `https://fullfunding.nl` API bases and the web backend serves no CORS headers, so cross-origin fetches are browser-blocked until the Phase-2 env flip. Do not "fix" this with CORS — Phase 2 removes the cross-origin condition itself. |

## Phase 2 — cutover (EXECUTED 2026-08-08, a declared auth/UI outage window)

No web image rebuild was needed: the served URLs come from RUNTIME env (`RuntimeProfileConfig`
injects `window.__OPTIONS_EDGE_ENV__` from pod env each request), so the cutover was an env flip
plus rollouts. Jenkins URL matrices are build-time image defaults (they matter for runs outside
k8s) and were flipped in the same change-set.

What shipped (deploy PR #746), in this order:

1. Keycloak `KC_HOSTNAME` → `https://auth.bleadingoptions.com` (+ config-nonce). This is the TOKEN
   ISSUER and is single-valued, so every consumer had to move in the same window; all sessions
   were invalidated by design.
2. Issuer/JWKS consumers: prod feed-gateway, spx-mission-control (base **and** the production
   per-service slice — the per-service job applies the slice, not `k8s/base`), es4 es-feed-gateway.
3. Web runtime URLs on BOTH web deployments: `VITE_AUTH_ISSUER`, `VITE_WS_URL`,
   `APP_FEED_GATEWAY_WS_URL`, `VITE_API_BASE_URL`, `VITE_MISSION_CONTROL_URL`,
   `VITE_REPLAY_ORCHESTRATOR_URL`, plus `APP_ES_OPTIONS_URL` (WebNav's compiled default still
   names the old es host, so the env override is mandatory).
4. Deploy order actually run: `common-infra-deploy ENVIRONMENT=production` (Keycloak; it PAUSES on
   an "Apply to production?" input — approve it or the build ABORTS on timeout, as build #32 did)
   → `service-deploy` feed-gateway → spx-mission-control → web (all `BUILD_IMAGES=false`) →
   `es4-deploy deploy-service` es-feed-gateway → es-web.
5. Acceptance: `timeout 600 scripts/ops/verify-prod-tunnel.sh --phase redirect --precheck` passed
   (running-pod issuer/URL contracts on both clusters, live realm + configmap set equality).

## Phase 3 — RETIREMENT (no redirect: the old domain is freed for other applications)

⚠️ **Operator decision (2026-08-08): `fullfunding.nl` is NOT redirected.** It is being reused for
unrelated applications, so our platform must stop claiming it entirely. Consequences that make
this stricter than a redirect-based retirement:

- Re-adding any `*.fullfunding.nl` ingress rule to this tunnel would HIJACK traffic belonging to
  whatever now owns that domain. The canonical tunnel file says so at the top of its ingress list.
- The acceptance gate's `retired` phase (now the DEFAULT) asserts absence in the configs we own —
  the live tunnel, the repo canonical copy, the Keycloak Ingress, both gateway allow-lists, the
  live realm client, the realm import file and the `req` client. It deliberately does NOT probe
  the retired hostnames over HTTP: whatever answers them is no longer ours to judge.
- Old links simply break (there is no forwarding). That is the accepted trade-off for freeing the
  domain; communicate the new URLs to anyone who has the old ones bookmarked.

What Phase 3 removes (this change-set):

| Surface | Change |
|---|---|
| Canonical tunnel + live `/etc/cloudflared/options-edge-stable.yml` | all three `*.fullfunding.nl` ingress blocks deleted |
| `k8s/keycloak/keycloak-ingress.yaml` | `auth.fullfunding.nl` host rule deleted |
| prod feed-gateway + es4 es-feed-gateway | `WS_ALLOWED_ORIGINS` drops the old origins (es4 keeps its intentional LAN origin `http://192.168.100.4:30080`) |
| Realm import + LIVE realm client `options-edge-web` | old redirectUris / webOrigins / post-logout entries removed |
| Realm `req` client `bugzilla-web` | `req.fullfunding.nl` → `req.bleadingoptions.com` |
| Comments/docs across `k8s/`, `docs/` | old hostnames swept |

### Apply order (each step verifiable; the new domain never breaks)

1. **Tunnel** — install the new canonical file per the fail-safe procedure in
   `docs/keycloak-prod.md` (stage → validate → route-table preflight → backup → atomic install →
   restart `options-edge-cloudflared-stable.service` → journal check). The retired hostnames now
   fall through to the terminal `http_status:404`.
2. **Workloads** — `common-infra-deploy ENVIRONMENT=production` (Ingress host removal + realm
   import file; it PAUSES on an "Apply to production?" input — approve it, or the build ABORTS on
   timeout), then `service-deploy SERVICE=feed-gateway ENVIRONMENT=production BUILD_IMAGES=false`,
   then `es4-deploy deploy-service SERVICE=es-feed-gateway`.
3. **LIVE realm** (the import file never mutates an existing realm — this is the only path that
   changes what login actually enforces). Read first, patch by client UUID, read back:

   ```sh
   POD=$(kubectl -n options-edge get pod -l app.kubernetes.io/name=oe-keycloak -o name | head -1)
   ADMIN_PW=$(kubectl -n options-edge get secret oe-keycloak-secrets -o jsonpath='{.data.KC_BOOTSTRAP_ADMIN_PASSWORD}' | base64 -d)
   printf '%s\n' "$ADMIN_PW" | kubectl -n options-edge exec -i "$POD" -- sh -c '
     set -eu; IFS= read -r P
     /opt/keycloak/bin/kcadm.sh config credentials --server http://localhost:8080 --realm master --user abhinav --password "$P"
     ID=$(/opt/keycloak/bin/kcadm.sh get clients -r optionsedge -q clientId=options-edge-web --fields id --format csv --noquotes)
     # BOTH lists are REPLACED wholesale — keep every new-domain + LAN entry, drop only the retired ones
     /opt/keycloak/bin/kcadm.sh update "clients/$ID" -r optionsedge \
       -s "redirectUris=[\"https://bleadingoptions.com/*\",\"https://es.bleadingoptions.com/*\",\"http://192.168.100.4:30080/*\",\"http://192.168.100.252:8094/*\",\"http://192.168.100.103:8094/*\"]" \
       -s "webOrigins=[\"https://bleadingoptions.com\",\"https://es.bleadingoptions.com\",\"http://192.168.100.4:30080\",\"http://192.168.100.252:8094\",\"http://192.168.100.103:8094\"]" \
       -s "attributes.\"post.logout.redirect.uris\"=https://bleadingoptions.com/*##https://es.bleadingoptions.com/*"
     RID=$(/opt/keycloak/bin/kcadm.sh get clients -r req -q clientId=bugzilla-web --fields id --format csv --noquotes)
     /opt/keycloak/bin/kcadm.sh update "clients/$RID" -r req \
       -s "redirectUris=[\"https://req.bleadingoptions.com/oidc-callback\"]" \
       -s "webOrigins=[\"https://req.bleadingoptions.com\"]"
     # read back EVERYTHING mutated: both clients, all three fields
     /opt/keycloak/bin/kcadm.sh get "clients/$ID" -r optionsedge --fields redirectUris,webOrigins,attributes
     /opt/keycloak/bin/kcadm.sh get "clients/$RID" -r req --fields redirectUris,webOrigins'
   ```

   The acceptance gate compares these sets exactly, so a slip is caught immediately — but re-read
   the output above before moving on, because a wrong replacement here breaks login on the NEW
   domain too.
4. **Accept BEFORE the one-way step** — `timeout 600 scripts/ops/verify-prod-tunnel.sh` (defaults
   to `--phase retired`) must be fully green FIRST, and the FULL authenticated smoke must pass on
   BOTH sites (`https://bleadingoptions.com` and `https://es.bleadingoptions.com`): log in, boards
   render with live data, and the WS request reaches `101 Switching Protocols` in the network tab.
   Both sites matter because Phase 3 changes the ES allow-list and the shared realm. Nothing in
   the scripted gate depends on old-domain DNS, so everything it can catch — a stale live Ingress
   rule, a leftover origin, a wrong realm set, a hostless/wildcard/defaultBackend catch-all — must
   be caught while ordinary rollback is still available.
5. **Cloudflare — the DNS handoff that actually frees the domain** (operator, dashboard):
   in zone `fullfunding.nl`, DELETE the `@`, `es` and `auth` records that point at
   `976f76d2-e3c8-4887-a11d-21c27f5e8bed.cfargotunnel.com` (or repoint them at whatever new
   application takes the domain). Removing our ingress rules only makes the tunnel answer 404 —
   until these records are gone, the OptionsEdge tunnel is still the DNS target for that domain.
   Evidence for the record must be CONFIGURATION-level, not a resolver answer: `dig` cannot show
   which tunnel a proxied record targets (and a repointed record still answers from Cloudflare
   IPs). It must also be COMPLETE — the DNS-records API is paginated (20 per page by default), so
   one page proves nothing. Use an exact content filter and assert zero matches:

   ```sh
   set -euo pipefail                      # a failed query must STOP this, never read as clean
   CF=(curl --fail-with-body -sS -H "Authorization: Bearer $CF_TOKEN")
   API="https://api.cloudflare.com/client/v4"
   # Bind the evidence to the RETIRED zone by name — a mistyped/stale ZONE_ID would otherwise
   # produce a clean-looking report about somebody else's zone.
   "${CF[@]}" "$API/zones/$ZONE_ID" | python3 -c "
import json,sys
d=json.load(sys.stdin); assert d['success'], d
name=d['result']['name']
print('zone under audit:', name)
sys.exit(0 if name == 'fullfunding.nl' else 'STOP: ZONE_ID is not the retired zone')"
   Z="$API/zones/$ZONE_ID/dns_records"
   # content.exact is the documented exact filter; the legacy contains-style ?content= is asked as
   # a cross-check. BOTH must return zero — and a nonzero count EXITS nonzero, it does not just print.
   for q in "content.exact=976f76d2-e3c8-4887-a11d-21c27f5e8bed.cfargotunnel.com" \
            "content=976f76d2-e3c8-4887-a11d-21c27f5e8bed.cfargotunnel.com"; do
     "${CF[@]}" "$Z?$q&per_page=100" | python3 -c "
import json,sys
d=json.load(sys.stdin); assert d['success'], d
n=(d.get('result_info') or {}).get('total_count')
if n is None: sys.exit('no total_count in response — cannot assert zero')
print('$q ->', n, 'match(es)')
sys.exit(0 if n == 0 else 'STOP: the retired zone still targets our tunnel')"
   done
   # …and list EVERY record (all pages) so a filter that silently matches nothing cannot pass as
   # proof: the record list is the evidence, the filter is only a convenience.
   pg=1
   while :; do
     body="$("${CF[@]}" "$Z?per_page=100&page=$pg")"
     # exit 0 = last page, 7 = more pages, anything else = a real error that must stop us
     set +e
     printf '%s' "$body" | python3 -c "
import json,sys
d=json.load(sys.stdin); assert d['success'], d
for r in d['result']: print(r['type'], r['name'], '->', r['content'])
ri=d.get('result_info') or {}
tp, pg = ri.get('total_pages'), ri.get('page') or 1
if tp is None: sys.exit('no total_pages in response — cannot prove the listing is complete')
sys.exit(0 if tp <= pg else 7)"
     rc=$?; set -e
     case "$rc" in 0) break ;; 7) pg=$((pg+1)) ;; *) echo "STOP: record listing incomplete (rc=$rc)"; exit 1 ;; esac
   done
   ```

   Acceptance evidence: `success: true`, **zero matches on both filter forms**, and a printed
   record list in which `@`, `es` and `auth` no longer point at
   `976f76d2-…cfargotunnel.com` (keep the output). If you use the dashboard instead, screenshot
   the zone's full DNS tab, not a filtered page.

   **What actually makes this safe — and what does not.** It is tempting to try to *prove absence*
   by enumerating every Cloudflare object that could still route the retired zone to us: rulesets,
   page rules, worker routes, worker custom domains, bulk redirects, snippets — each paginated,
   each able to reach us through a binding or a secret that no text search can see. That proof can
   never be completed, and treating an incomplete sweep as an acceptance gate is worse than not
   running it, because it manufactures false confidence at a one-way boundary.

   The invariant we actually rely on is the opposite direction, and it IS machine-provable:

   > **Our platform refuses the retired hostnames.** The tunnel resolves every retired URL to
   > `http_status:404` (cloudflared's own first-match answer, asserted by the gate), no Ingress of
   > ours names them, no gateway trusts their origins, and the realm issues nothing for them.

   That holds no matter what the retired zone contains or who owns it next — which is precisely
   why retirement is safer than a redirect here. `scripts/ops/verify-prod-tunnel.sh` proves it, and
   its `retired` phase fails closed on wildcards, hostless rules, `defaultBackend` catch-alls,
   shadowed routes and unprobed paths.

   **Still do a best-effort zone sweep — for the domain's NEXT owner, not as our gate.** A stale
   redirect or worker left in that zone would confuse whatever you host there next:

   ```sh
   set -euo pipefail
   Z="https://api.cloudflare.com/client/v4/zones/$ZONE_ID"
   A="https://api.cloudflare.com/client/v4/accounts/$ACCOUNT_ID"
   cf() { curl --fail-with-body -sS -H "Authorization: Bearer $CF_TOKEN" "$@"; }
   for u in "$Z/dns_records?per_page=100" "$Z/rulesets?per_page=50" "$Z/pagerules" \
            "$Z/workers/routes" "$Z/snippets" "$A/workers/domains?per_page=100" \
            "$A/rules/lists?per_page=100"; do
     echo "=== $u ==="; cf "$u"; echo
   done | tee /tmp/cf-retired-zone-sweep.txt
   grep -niE "976f76d2-e3c8-4887-a11d-21c27f5e8bed|cfargotunnel|bleadingoptions\.com" \
     /tmp/cf-retired-zone-sweep.txt || echo "(no obvious reference to us — note that paginated
     results, worker bindings and secrets are NOT covered; this is a courtesy sweep, not a proof)"
   ```

   Then do the one thing that is decisive and needs no enumeration: **delete (or repoint) the
   `@`, `es` and `auth` records in the `fullfunding.nl` zone**, and record the zone's DNS tab
   showing they are gone. `bleadingoptions.com` keeps its three proxied CNAMEs.

### Rollback boundary (one-way after the DNS handoff)

Before the handoff (steps 1-4) rollback is revert-and-redeploy for the tunnel (restore the step-1
backup) and the workloads — **but NOT for the realm**: `--import-realm` never updates an existing
realm, so a git revert cannot put the old client entries back. The realm inverse is an explicit
kcadm run (same shape as step 3, old values restored, with a readback):

```sh
POD=$(kubectl -n options-edge get pod -l app.kubernetes.io/name=oe-keycloak -o name | head -1)
ADMIN_PW=$(kubectl -n options-edge get secret oe-keycloak-secrets -o jsonpath='{.data.KC_BOOTSTRAP_ADMIN_PASSWORD}' | base64 -d)
printf '%s\n' "$ADMIN_PW" | kubectl -n options-edge exec -i "$POD" -- sh -c '
  set -eu; IFS= read -r P
  /opt/keycloak/bin/kcadm.sh config credentials --server http://localhost:8080 --realm master --user abhinav --password "$P"
  ID=$(/opt/keycloak/bin/kcadm.sh get clients -r optionsedge -q clientId=options-edge-web --fields id --format csv --noquotes)
  /opt/keycloak/bin/kcadm.sh update "clients/$ID" -r optionsedge \
    -s "redirectUris=[\"https://fullfunding.nl/*\",\"https://es.fullfunding.nl/*\",\"https://bleadingoptions.com/*\",\"https://es.bleadingoptions.com/*\",\"http://192.168.100.4:30080/*\",\"http://192.168.100.252:8094/*\",\"http://192.168.100.103:8094/*\"]" \
    -s "webOrigins=[\"https://fullfunding.nl\",\"https://es.fullfunding.nl\",\"https://bleadingoptions.com\",\"https://es.bleadingoptions.com\",\"http://192.168.100.4:30080\",\"http://192.168.100.252:8094\",\"http://192.168.100.103:8094\"]" \
    -s "attributes.\"post.logout.redirect.uris\"=https://fullfunding.nl/*##https://es.fullfunding.nl/*##https://bleadingoptions.com/*##https://es.bleadingoptions.com/*"
  RID=$(/opt/keycloak/bin/kcadm.sh get clients -r req -q clientId=bugzilla-web --fields id --format csv --noquotes)
  /opt/keycloak/bin/kcadm.sh update "clients/$RID" -r req \
    -s "redirectUris=[\"https://req.fullfunding.nl/oidc-callback\"]" -s "webOrigins=[\"https://req.fullfunding.nl\"]"
  # read back EVERYTHING restored: both clients, all three fields
  /opt/keycloak/bin/kcadm.sh get "clients/$ID" -r optionsedge --fields redirectUris,webOrigins,attributes
  /opt/keycloak/bin/kcadm.sh get "clients/$RID" -r req --fields redirectUris,webOrigins'
```

Verify the restore with `timeout 600 scripts/ops/verify-prod-tunnel.sh --phase rollback` — that
mode describes exactly what a Phase-3 revert produces: both domains serving and trusted again while
the issuer and served URLs stay on the new domain (Phase-2 state). `--phase dual` would wrongly
demand the OLD issuer and `--phase retired` would wrongly demand absence, so neither can pass here.

⚠️ **Revert the DESIRED STATE, never this whole commit.** `--phase rollback` is introduced BY the
Phase-3 change, so a blanket `git revert` of it takes the verifier away with it and leaves a script
that rejects `--phase rollback`. Roll back selectively — the tunnel file, the manifests' env/host
values, the realm via the kcadm inverse above — and keep `scripts/ops/verify-prod-tunnel.sh` at its
Phase-3 version (if you must work from a reverted tree, run the gate from this commit explicitly:
`git show <phase3-sha>:scripts/ops/verify-prod-tunnel.sh > /tmp/gate.sh && REPO_ROOT=$(pwd) bash /tmp/gate.sh --phase rollback` —
`REPO_ROOT` is required because a script outside the tree would otherwise resolve the repo paths
against `/tmp`). ⚠️ This inverse is valid ONLY before the DNS handoff — after it, re-admitting
those entries is exactly the hijack risk described below, and the correct move is fix-forward.

**After the handoff there is no rollback that re-admits `fullfunding.nl` — in ANY surface.** That
domain may already serve someone else's application, so re-adding it would not merely be untidy:
- restoring the tunnel hostnames would hijack their HTTP traffic;
- restoring the old Keycloak redirect URIs / web origins would let a page on THEIR domain start an
  OIDC flow against our realm and receive tokens;
- restoring the old gateway `WS_ALLOWED_ORIGINS` would let their page open our authenticated
  WebSocket;
- restoring the old Ingress host would route their Host header into Keycloak.

So post-handoff recovery is **fix-forward with new-domain-only configuration**: revert a workload
to a previous IMAGE if needed, but never to a config revision that names the retired domain. Delete
the pre-Phase-3 tunnel backup once the post-handoff gate (step 6) is green so it cannot be restored by reflex,
and note that the `retired` gate fails loudly if any surface re-acquires that domain.
