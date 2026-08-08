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
| `keycloak-ingress.yaml` | traefik Ingress (LEGACY fallback — live public path bypasses traefik) | Routes both auth hostnames' `/realms/optionsedge` + `/resources` → `oe-keycloak:8080` (admin paths get no route); old host rule removed at migration Phase 3 |

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
set -euo pipefail   # any failed validation below MUST stop the install
cloudflared tunnel --config ~/options-edge-stable.yml.new ingress validate
# authoritative preflight: cloudflared itself resolves EVERY public route against the STAGED file,
# and each resolution is asserted against the expected backend (grep -q fails the pipeline).
while read -r u expect; do
  got="$(cloudflared tunnel --config ~/options-edge-stable.yml.new ingress rule "$u" \
         | sed -n 's/^[[:space:]]*service:[[:space:]]*//p' | head -1)"
  [ "$got" = "$expect" ] \
    || { echo "PREFLIGHT FAIL: $u resolved to '$got', expected '$expect'" >&2; exit 1; }
done <<'ROUTES'
https://fullfunding.nl/ws/events http://192.168.100.252:30097
https://fullfunding.nl/ http://192.168.100.252:8094
https://bleadingoptions.com/ws/events http://192.168.100.252:30097
https://bleadingoptions.com/ http://192.168.100.252:8094
https://auth.fullfunding.nl/admin http_status:404
https://auth.fullfunding.nl/realms/master http_status:404
https://auth.fullfunding.nl/realms/optionsedge http://10.43.127.26:8080
https://auth.bleadingoptions.com/admin http_status:404
https://auth.bleadingoptions.com/realms/master http_status:404
https://auth.bleadingoptions.com/realms/optionsedge http://10.43.127.26:8080
https://es.fullfunding.nl/ws/events http://192.168.100.4:30091
https://es.fullfunding.nl/ http://192.168.100.4:30080
https://es.bleadingoptions.com/ws/events http://192.168.100.4:30091
https://es.bleadingoptions.com/ http://192.168.100.4:30080
ROUTES
sudo cp /etc/cloudflared/options-edge-stable.yml /etc/cloudflared/options-edge-stable.yml.bak-$(date +%Y%m%d-%H%M%S)
sudo cp ~/options-edge-stable.yml.new /etc/cloudflared/options-edge-stable.yml.tmp   && sudo mv /etc/cloudflared/options-edge-stable.yml.tmp /etc/cloudflared/options-edge-stable.yml
sudo systemctl restart options-edge-cloudflared-stable.service   # the unit name — NOT bare "cloudflared"
systemctl is-active options-edge-cloudflared-stable.service
journalctl -u options-edge-cloudflared-stable.service -n 20 --no-pager   # no ERR lines
timeout 600 scripts/ops/verify-prod-tunnel.sh --phase <current-phase>    # bounded, phase-appropriate gate
# rollback (exact, using the backup taken above):
#   sudo cp /etc/cloudflared/options-edge-stable.yml.bak-<timestamp> /etc/cloudflared/options-edge-stable.yml
#   sudo systemctl restart options-edge-cloudflared-stable.service
```

(An earlier revision of this doc inlined a
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

## Admin login + create a user (in-cluster, no public /admin)

Use `kcadm.sh` inside the pod — this targets the local server and does not need the public admin
console. **Current credential contract (since 2026-07-18):** the bootstrap `admin` account is
DISABLED; the permanent master-realm admin is **`abhinav`**, and by contract `abhinav`'s password
EQUALS the `KC_BOOTSTRAP_ADMIN_PASSWORD` key in `oe-keycloak-secrets` — the Deployment still reads
that key at boot and `scripts/ops/verify-prod-tunnel.sh` authenticates with it, so the key must
never be removed. Rotation is ONE fail-fast sequence (single captured value, fresh kcadm login, Secret patched only
after the password change succeeded; the verifier re-run catches any residue):

```sh
set -uo pipefail   # deliberately NOT -e: every remote write has an ambiguous-outcome branch that
                   # is inspected explicitly — the reconciler below decides, never a blind exit.
KT="--request-timeout=20s"                                # never hang mid-rotation
NEW_PW="$(openssl rand -base64 24)"                       # captured ONCE — no manual retyping drift
OLD_PW="$(kubectl $KT -n options-edge get secret oe-keycloak-secrets -o jsonpath='{.data.KC_BOOTSTRAP_ADMIN_PASSWORD}' | base64 -d)"
# Refuse to mutate anything until both values provably exist — an empty OLD_PW (failed Secret
# read) or NEW_PW (missing openssl) would corrupt the rotation from the first write.
[ -n "$NEW_PW" ] && [ -n "$OLD_PW" ] || { echo "ABORT: could not initialize both password values — nothing was changed" >&2; exit 2; }
# CONCURRENCY FENCE: exactly one rotation at a time. `kubectl create` is atomic — a second
# operator's create fails and aborts before touching anything. A crashed run leaves the lock;
# takeover = verify the other session is truly dead, then delete the configmap by hand.
kubectl $KT -n options-edge create configmap kc-rotation-lock \
  --from-literal=holder="$(whoami)@$(hostname) $$ $(date -u +%Y%m%dT%H%M%SZ)" \
  || { echo "ABORT: another rotation holds kc-rotation-lock (kubectl -n options-edge get configmap kc-rotation-lock -o yaml to see who) — nothing was changed" >&2; exit 7; }
release_lock() {  # bounded AND verified — a silently surviving lock would block the next rotation.
  # The verification must itself be trustworthy: `get` failing does NOT mean the lock is gone —
  # only an explicit NotFound does; an API error means deletion is UNVERIFIED, which is a failure.
  kubectl $KT -n options-edge delete configmap kc-rotation-lock --ignore-not-found >/dev/null 2>&1
  local out
  if out="$(kubectl $KT -n options-edge get configmap kc-rotation-lock 2>&1)"; then
    echo "WARNING: kc-rotation-lock could NOT be deleted — remove it by hand before the next rotation" >&2
    return 1
  elif printf '%s' "$out" | grep -q 'NotFound'; then
    return 0
  else
    echo "WARNING: could not VERIFY kc-rotation-lock deletion (API error: $out) — check and remove it by hand" >&2
    return 1
  fi
}
# Persist BOTH candidates (0600, verified) BEFORE the first mutation: an interrupt or crash after
# Keycloak commits but before the Secret reconciles must never orphan the only usable credential.
# The file outlives every failure path and is shredded ONLY after the end-state proof.
# /var/tmp, NOT /tmp: /tmp is legitimately cleared at boot, and this file's whole job is to
# survive a crash/reboot between the two mutations.
RESCUE="$(mktemp /var/tmp/kc-rotation-rescue.XXXXXX)" \
  && chmod 600 "$RESCUE" \
  && printf 'OLD_PW=%s\nNEW_PW=%s\n' "$OLD_PW" "$NEW_PW" > "$RESCUE" \
  && [ -s "$RESCUE" ] \
  && [ "$(stat -c %a "$RESCUE" 2>/dev/null || stat -f %Lp "$RESCUE")" = "600" ] \
  || { echo "ABORT: could not persist the rescue file — nothing was changed" >&2
       # A partially-written file may still hold the OLD password: destroy it, and say so if
       # even that fails (exit 6 = manual cleanup required, still nothing mutated).
       release_lock   # nothing was mutated — safe to free the fence
       if [ -n "${RESCUE:-}" ] && [ -e "$RESCUE" ]; then
         rm -f "$RESCUE" 2>/dev/null
         [ -e "$RESCUE" ] && { echo "  AND the partial rescue file could not be removed — delete $RESCUE by hand" >&2; exit 6; }
       fi
       exit 2; }
# The traps EXIT — with no -e, a print-only trap would let an interrupted `sleep` fall straight
# through into reconciliation INSIDE the quiesce window, exactly the delayed-commit race the
# fence exists to prevent. Distinct codes: 130 = SIGINT, 143 = SIGTERM.
# NO unconditional EXIT release: on interrupt/indeterminate outcomes the state may still need
# manual reconciliation, and the lock is what stops a second operator from racing it — those
# paths deliberately RETAIN the lock (delete kc-rotation-lock yourself once reconciled). The
# lock is released explicitly only where the state is proven consistent, or where nothing was
# ever mutated.
trap 'echo "INTERRUPTED (SIGINT) mid-rotation — candidates preserved at $RESCUE (mode 0600). WAIT >=40s (the pod-side write fence) before inspecting or changing EITHER state, then reconcile by hand. kc-rotation-lock is RETAINED until you delete it" >&2; exit 130' INT
trap 'echo "TERMINATED (SIGTERM) mid-rotation — candidates preserved at $RESCUE (mode 0600). WAIT >=40s (the pod-side write fence) before inspecting or changing EITHER state, then reconcile by hand. kc-rotation-lock is RETAINED until you delete it" >&2; exit 143' TERM
# Client-side `timeout 60` is the real bound: --request-timeout limits one API request and
# --pod-running-timeout only the wait-for-running — neither bounds the remote command itself. An
# expiry here is exactly the "ambiguous transport" case the reconciler already handles.
kc_exec() { timeout --kill-after=5s 60s kubectl $KT --pod-running-timeout=20s -n options-edge exec -i deploy/oe-keycloak -- sh -c "$1"; }
kc_login_ok() {  # does Keycloak accept this password RIGHT NOW? (the only trustworthy state)
  printf '%s\n' "$1" | kc_exec 'IFS= read -r P; /opt/keycloak/bin/kcadm.sh config credentials --server http://localhost:8080 --realm master --user abhinav --password "$P"' >/dev/null 2>&1
}
kc_setpw() {  # $1=auth-pw $2=new-pw, both via stdin. ONE pod-side timeout fences the WHOLE
  # login+write sequence; --kill-after guarantees an uncatchable KILL 5s after TERM (a TERM-
  # resistant JVM child would otherwise outlive the fence). After the 30s+5s envelope no pod-side
  # write can still be in flight — so after QUIESCE below, what Keycloak accepts is final.
  printf '%s\n%s\n' "$1" "$2" | kc_exec 'timeout --kill-after=5s 30s sh -c '\''set -eu; IFS= read -r AUTHP; IFS= read -r NEWP
    /opt/keycloak/bin/kcadm.sh config credentials --server http://localhost:8080 --realm master --user abhinav --password "$AUTHP"
    /opt/keycloak/bin/kcadm.sh set-password -r master --username abhinav --new-password "$NEWP"'\'''
}
secret_now() { kubectl $KT -n options-edge get secret oe-keycloak-secrets -o jsonpath='{.data.KC_BOOTSTRAP_ADMIN_PASSWORD}' | base64 -d; }
patch_secret() {  # via stdin (no argv exposure), 3 bounded attempts
  for _ in 1 2 3; do
    printf '{"stringData":{"KC_BOOTSTRAP_ADMIN_PASSWORD":"%s"}}' "$1" \
      | kubectl $KT -n options-edge patch secret oe-keycloak-secrets --patch-file /dev/stdin && return 0
    sleep 2
  done
  return 1
}
indeterminate() {  # the rescue file was verified-written BEFORE the first mutation — point at it
  echo "ROTATION INDETERMINATE: $1" >&2
  echo "  both candidate values remain at $RESCUE (mode 0600) — reconcile by hand, then shred it" >&2
  echo "  kc-rotation-lock is RETAINED so nobody races your manual reconciliation — delete it when done" >&2
  exit 3
}
shred_rescue() {  # only after an end-state proof; VERIFIED destruction
  if command -v shred >/dev/null; then shred -u "$RESCUE"
  else dd if=/dev/urandom of="$RESCUE" bs=1k count=1 conv=notrunc 2>/dev/null; rm -f "$RESCUE"; fi
  if [ -e "$RESCUE" ]; then
    echo "WARNING: rescue file could NOT be destroyed — remove $RESCUE by hand before leaving" >&2
    return 1
  fi
}
post_consistency_traps() {  # the mid-rotation messages would now LIE (state is consistent; the
  # remaining risk is only incomplete cleanup) — swap them the moment consistency is proven.
  trap 'echo "signal during CLEANUP: rotation state is already CONSISTENT — verify by hand that $RESCUE is destroyed and kc-rotation-lock is deleted" >&2; exit 130' INT
  trap 'echo "signal during CLEANUP: rotation state is already CONSISTENT — verify by hand that $RESCUE is destroyed and kc-rotation-lock is deleted" >&2; exit 143' TERM
}

if ! kc_setpw "$OLD_PW" "$NEW_PW"; then
  # Ambiguous transport outcome: the pod-side write may or may not have committed. QUIESCE past
  # the FULL 30s+5s TERM->KILL envelope so no delayed commit can land AFTER we observe the state
  # — an observation taken inside the fence window would not be final.
  echo "set-password transport ambiguous — quiescing 40s past the 30s+5s TERM->KILL envelope" >&2
  sleep 40
fi

# RECONCILE: ask Keycloak which password it accepts NOW (final, thanks to the fence), then drive
# the Secret to that value.
if kc_login_ok "$NEW_PW"; then TARGET="$NEW_PW"
elif kc_login_ok "$OLD_PW"; then TARGET="$OLD_PW"; echo "set-password did not take effect — converging on OLD" >&2
else indeterminate "Keycloak accepts neither value (connectivity?)"; fi
if [ "$(secret_now)" != "$TARGET" ]; then
  patch_secret "$TARGET" || indeterminate "Keycloak holds its value but the Secret patch keeps failing"
fi
# END-STATE PROOF: the Secret's value must authenticate. Only then is the state consistent.
kc_login_ok "$(secret_now)" || indeterminate "Secret and Keycloak still disagree after reconcile"
if [ "$TARGET" = "$OLD_PW" ]; then
  # Consistent, but NOT rotated — the write never took. End state is proven, so the rescue file
  # (which holds the unused NEW value) is shredded; exit nonzero so nothing upstream treats this
  # safe-rollback state as a completed rotation; retry the sequence.
  post_consistency_traps
  cleanup_rc=0
  release_lock || cleanup_rc=8   # exit 8: consistent, but the lock survived — clear it by hand
  shred_rescue || cleanup_rc=5   # exit 5: consistent, but credential material left on disk
  echo "rotation DID NOT complete: converged back on the OLD password (consistent; retry needed)" >&2
  [ "$cleanup_rc" != "0" ] && exit "$cleanup_rc"
  exit 4
fi
post_consistency_traps
cleanup_rc=0
release_lock || cleanup_rc=8   # exit 8: rotated, but the lock survived — clear it by hand
shred_rescue || cleanup_rc=5   # exit 5: rotated, but credential material left on disk
echo "rotation converged on the NEW password"
[ "$cleanup_rc" != "0" ] && exit "$cleanup_rc"
# final gate — pass the CURRENT migration phase explicitly (dual | redirect | retired):
timeout 600 scripts/ops/verify-prod-tunnel.sh --phase <current-phase>
```

Credentials travel via `kubectl exec -i` stdin — they never appear in LOCAL argv or shell
history. Honest scope of that guarantee: `kcadm.sh` only accepts `--password`/`--new-password` as
arguments, so inside the pod the value is briefly visible in the pod-local process list; that
exposure is accepted deliberately (the pod is single-purpose, and reading its /proc requires the
same `exec` privilege this procedure already needs — the same trade-off the rotation sequence and
the verifier make).

```sh
ADMIN_PW=$(kubectl -n options-edge get secret oe-keycloak-secrets -o jsonpath='{.data.KC_BOOTSTRAP_ADMIN_PASSWORD}' | base64 -d)
POD=$(kubectl -n options-edge get pod -l app.kubernetes.io/name=oe-keycloak -o name | head -1)
read -rs -p "new user's password: " USER_PW; echo
printf '%s\n%s\n' "$ADMIN_PW" "$USER_PW" | kubectl -n options-edge exec -i "$POD" -- sh -c '
  set -eu; IFS= read -r ADMINP; IFS= read -r USERP
  /opt/keycloak/bin/kcadm.sh config credentials --server http://localhost:8080 --realm master --user abhinav --password "$ADMINP"
  /opt/keycloak/bin/kcadm.sh create users -r optionsedge -s username=<user> -s enabled=true
  /opt/keycloak/bin/kcadm.sh set-password -r optionsedge --username <user> --new-password "$USERP"'
unset USER_PW
```

Replay roles are a SEPARATE, explicit grant — `replay-admin` means "full replay administration
(any operation)" and must never ride along silently with routine user creation:

```sh
printf '%s\n' "$ADMIN_PW" | kubectl -n options-edge exec -i "$POD" -- sh -c '
  set -eu; IFS= read -r ADMINP
  /opt/keycloak/bin/kcadm.sh config credentials --server http://localhost:8080 --realm master --user abhinav --password "$ADMINP"
  /opt/keycloak/bin/kcadm.sh add-roles -r optionsedge --username <user> --rolename replay-admin'
```

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
WS_ALLOWED_ORIGINS=https://fullfunding.nl,https://bleadingoptions.com   # explicit allow-list required once auth is on (both domains during the migration)
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
- **Admin console:** never public. It IS reachable on the trusted LAN via the `oe-keycloak-lan`
  LoadBalancer (`http://192.168.100.252:8089/admin/`, `KC_HOSTNAME_ADMIN`); kcadm-in-pod remains the
  scripted path. The public `/admin` stays edge-404ed — front it with Cloudflare Access if a public
  console is ever genuinely needed, never by unblocking it openly.
