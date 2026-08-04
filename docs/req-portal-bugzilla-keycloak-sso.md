# Requirement Intake Portal — dedicated Bugzilla at req.fullfunding.nl with Keycloak SSO (realm-isolated)

**Status: DESIGN / PROPOSED — rev 11 (Codex 3-bar rounds 1–10, all REQUEST_CHANGES; round 2 accepted
the architecture; rounds 7–9 narrowed to the email-claim contract, corrected below). Not yet
implemented.** "Current state (as-is)", the REQ-5c source-behavior block, the REQ-5d authorization
table, the readiness-probe semantics, and the free-port check are verified against the running
systems / the pinned source commit `276673ab6` on 2026-08-03; everything else is intended future
behavior.

**Date:** 2026-08-03  **Owner:** Abhinav
**Repos:** options-edge-deploy (realm bootstrap, `bugzilla-req` images + compose, Jenkins job)
**Hosts:** 192.168.100.252 (prod)

> ⚠️ **HOSTING MODEL SUPERSEDED IN PART (2026-08-04) — read
> [`fullfunding-namespace-gate1.md`](./fullfunding-namespace-gate1.md) first.** The user has
> since required this portal to run **inside the prod k3s cluster, in a dedicated `fullfunding`
> namespace**, not as a host Docker-Compose stack. That Gate-1 document carries a normative
> requirement-by-requirement disposition table (its §4): REQ-3, REQ-5a (mechanism only), REQ-8,
> REQ-9, REQ-10a and REQ-10b are **superseded** there; every other requirement below — the realm
> design, the OIDC client, the identity/claim contract pinned to `276673ab6`, the authorization
> matrix, the isolation proofs and the risk register — remains authoritative and unchanged. Where
> the two documents differ on **where containers run**, the Gate-1 document governs; where they
> differ on **what the portal must enforce**, this document governs.

**rev 11 changes (from Codex rounds 7–10):** (1) **the email-claim contract no longer
over-promises.** mod_auth_openidc can match a MEMBER of a claim array, so `Require claim "email~…"`
alone does not establish scalar type. REQ-5a now states an explicit two-layer contract: absent /
empty / non-matching is denied at Apache; a MULTI-MEMBER array that slips past is fail-closed one layer
down (joined value fails `validate_email_syntax` ⇒ hard `AUTH_ERROR auth_invalid_email`, no account,
no CGI fall-through, `Verify.pm:76-80` @ `276673ab6`), while a SINGLETON array is explicitly
accepted as equivalent to the scalar claim (identical resulting identity, keyed on `sub`) — no
"all arrays rejected" claim is made. Structurally, Keycloak's `email` is single-valued with
self-registration off, so neither shape arises from provisioning. `claims_expr` with
`type(email)=="string"` is documented as optional strengthening (JQ-dependent, never assumed), and
V4d now tests both array shapes with their exact expected results — and rev 10 states plainly that a
SINGLETON valid array is ACCEPTED (it flattens to the identical scalar identity, keyed on `sub`);
no "all arrays rejected" guarantee is made anywhere in the document. (2) **the attachment cells were
corrected to match stock behavior** — an attachment's submitter may edit/obsolete THEIR attachment
even on another user's ticket (OTHER = ALLOW), with a new separate row denying edits to attachments
submitted by someone else; the text also notes that attachment permissions are governed separately
from `bug_check_can_change_field`, so the hook fallback does not cover attachment cells.

**Earlier revisions (condensed):** rev 2 pivoted to a dedicated empty Bugzilla instance
(isolation by construction); rev 3 added fail-closed ingress-first rollback, honest V-pre scoping,
session/offboarding bounds, kcadm live-state reconciliation, immutable-`sub` identity, token-level
isolation tests; rev 4 pinned Bugzilla auth behavior from source, split the health model, widened
backups; rev 5 closed the missing-email fail-open path, separated the admin listener structurally,
made V10 satisfiable, resolved the launch-gate contradiction; rev 6 fixed the 8094 port collision
(admin listener → 8095/container 81), excluded secrets from backups, and gave V-restore an
executable loopback-HTTP callback topology; rev 7 corrected the `Require claim` grammar and
reconciled the REQ-5d matrix to the stock `Bugzilla/Bug.pm` authorization code.

---

## 1. Goal

External stakeholders submit project requirements as Bugzilla tickets. They must:

1. log in at **https://req.fullfunding.nl** through **Keycloak** (`auth.fullfunding.nl`),
2. belong to a **separate Keycloak realm** (`req`) whose users **cannot** authenticate to the
   OptionsEdge trading UI/API (`fullfunding.nl`, realm `optionsedge`), and
3. be **structurally unable** to see internal OptionsEdge bug data (no internal data is present or
   mounted in the external stack).

The internal Bugzilla (`http://192.168.100.252:8092/`) is not modified: no file, param, container,
image, or compose change (evidence: image digests, mount list, compose config hash, params
checksum, functional checks — REQ-6).

## 2. Architecture (accepted in round 2): dedicated instance

A second, dedicated Bugzilla stack (`bugzilla-req`: own web+MariaDB containers, empty database)
serves external intake. Application-level isolation is by construction; host-level risks are R-12.
The internal instance carries no application-level regression risk (untouched); shared-host
resource contention is accepted as R-12. `bugzilla-req` is a **distinct service identity** from the
internal `bugzilla` stack (One Service One Identity: two different services, not two versions).

## 3. Current state (as-is, verified 2026-08-03)

| Fact | Source |
|---|---|
| Internal Bugzilla 5.2 on `.252`: compose project `bugzilla` (`options-edge-bugzilla-web` 8092→80, `options-edge-bugzilla-db`), compose `/home/options-edge/deploy/bugzilla/docker-compose.yml`, env `/home/options-edge/config/bugzilla.env`, data `/home/options-edge/data/bugzilla/` | `docker ps`, compose labels, file reads |
| Web image builds ON `.252` from `/home/options-edge/data/bugzilla/runtime` — upstream `github.com/bugzilla/bugzilla`, branch `5.2`, commit `276673ab6`, clean worktree | inspection on `.252` |
| Internal `urlbase` = `http://192.168.100.252:8092/`, native CGI login; `Bugzilla/Auth/Login/Env.pm` present | params + tree |
| Keycloak 26 in k3s ns `options-edge` (`oe-keycloak`, ClusterIP `10.43.127.26:8080` pinned, LAN admin `:8089`, single replica), Postgres-backed; realm `optionsedge` imported from ConfigMap `oe-keycloak-realm` via `start --import-realm` (creation-only: existing realms skipped) | `k8s/keycloak/*` @ origin/main 2a9165b |
| Keycloak deploys via `k8s/infra/overlays/production` → `k8s/overlays/production` (Jenkins, main-only) | `k8s/infra/overlays/production/kustomization.yaml:11` |
| Cloudflare tunnel `options-edge-option-chain` (`/etc/cloudflared/options-edge-stable.yml`): `fullfunding.nl`→`.252:8094` (+`/ws/events`→`:30097`), `auth.fullfunding.nl`→`10.43.127.26:8080` (`/admin`,`/realms/master`→404), `es.fullfunding.nl`→`.4:30080`, catch-all 404 | config read |
| No `cert.pem` in `/etc/cloudflared` → DNS managed in Abhinav's Cloudflare dashboard | dir listing; keycloak-prod runbook |

## 4. Requirements

Stable ids REQ-1…REQ-13 (REQ-5 split 5a–5d). Initial state for all: **TRACKED-PENDING** (§9).
"Onboarding gate" = the §6 step-8 gate: every matrix row green (V7-int included) AND the 24 h
observation window closed clean. Step 7.5 is a provisional technical gate only.

### REQ-1 — Keycloak realm `req`: bootstrap-import + live-state reconciliation + recoverable state
- **Bootstrap:** `req-realm.json` in ConfigMap `oe-keycloak-realm`; `start --import-realm` creates
  the new realm on the next rollout (creation-only; it never reconciles).
- **Reconciliation:** the repo JSON is desired state. After every deploy touching it, a kcadm step
  compares LIVE security-relevant realm/client fields against the JSON; drift is corrected via
  kcadm AND the JSON updated in the same change. Acceptance always validates live state.
- **Recoverable state (two distinct artifacts):** a kcadm realm export is the **semantic
  comparison / inverse-change input** (it omits sessions/tokens/events and is NOT a restore
  artifact). Realm-scoped mistakes are corrected by **inverse kcadm operations**. Disaster recovery
  is the **Keycloak Postgres dump** (REQ-10a): `pg_dump` (MVCC-consistent single transaction) taken
  pre-change in the quiet pre-market window, owner Abhinav; a full DB restore rolls back ALL realms
  including any unrelated `optionsedge` changes since the dump, so it may be executed only on
  Abhinav's explicit authorization with that consequence acknowledged.
- Realm settings: `registrationAllowed: false`, `bruteForceProtected: true`,
  `resetPasswordAllowed: false` (no SMTP; resets by Abhinav via LAN admin), `sslRequired: external`,
  `loginWithEmailAllowed: true`; password policy length ≥ 12; MFA not required at launch (R-8; KC
  OTP can be enabled per-user later). No change to the `optionsedge` realm object.
- **Lifecycle:** provision/disable/remove ONLY by Abhinav (idempotent kcadm: check-then-apply).
  Every user is created WITH an email (required — REQ-5c contract). Offboarding procedure (always):
  disable user + revoke KC sessions + restart `bugzilla-req-web` (clears all portal sessions,
  REQ-5b) — exposure window ≈ minutes. Provisioning policy: never an internal operator's email
  (REQ-5c collision semantics).
**Acceptance:** realm discovery correct; live-state reconciliation green; `optionsedge` no-change
proof per REQ-7; onboarding gate.

### REQ-2 — Confidential OIDC client `bugzilla-web` in realm `req`
`publicClient: false`, `standardFlowEnabled: true`, `directAccessGrantsEnabled: false`,
`serviceAccountsEnabled: false`, PKCE S256, **`redirectUris:
["https://req.fullfunding.nl/oidc-callback"]` — exact URI, no wildcard**, `webOrigins:
["https://req.fullfunding.nl"]`, no post-logout redirect URIs. **Claim issuance is part of this
requirement:** the client keeps Keycloak's default `email` and `profile` client scopes, EXPECTED to carry
`sub`/`email`/`name`; the authoritative check is live — kcadm scope/mapper inspection plus a decoded
ID token during V4 (declarative defaults are not treated as proof). Secret: generated by Keycloak, transferred per REQ-9, never committed.
**Acceptance:** kcadm live inspection; V2b (foreign `redirect_uri` rejected); ID-token claim check
in V4.

### REQ-3 — Public hostname: published LAST, closed FIRST
- Ingress rule (before catch-all): `hostname: req.fullfunding.nl` → `service: http://127.0.0.1:8093`
  (loopback; cloudflared runs on `.252`). Config backed up (`.bak-<ts>`); restart in REQ-10 window.
- **Pre-check:** before enabling the ingress rule, verify NO `req.fullfunding.nl` DNS record
  already exists (dashboard check) — a stale record would defeat "DNS last".
- **Ordering (hard):** ingress + DNS only after V-pre green (REQ-10c). DNS publish (proxied CNAME →
  `<tunnel-id>.cfargotunnel.com`) is the final step; end-to-end V4/V6t/V11 run immediately after,
  BEFORE any stakeholder user is provisioned (V4 itself uses a disposable test user).
- **Rollback — two branches:**
  - *Pre-public* (no ingress/DNS yet): no public exposure exists; fix or tear down privately
    (compose down / LKG redeploy per REQ-8).
  - *Public fail-closed invariant (identical wording governs §6):* (1) point the
    `req.fullfunding.nl` ingress rule at `http_status:404` (or remove it); (2) restart cloudflared;
    (3) PROVE the hostname returns the closed response; (4) remove the DNS record; (5) only then
    modify or stop the application.
**Acceptance:** V3/V8 (incl. WebSocket) after every restart; at no point is an unprotected Bugzilla
internet-reachable; V-rollback rehearses the public branch.

### REQ-4 — Portal images: traceable, immutable-after-build, digest-deployed
(Renamed from "reproducible" — an apt-based build is not input-locked; what IS guaranteed:
traceability of inputs and immutability after build.)
- Web `Dockerfile` (context `bugzilla-req/` in options-edge-deploy): clone upstream at **pinned
  commit `276673ab6`** (a commit selected from branch 5.2; bumps are reviewed PRs, REQ-12), upstream
  build + overlay (`libapache2-mod-auth-openidc`, module enabled, vhost/OIDC config per REQ-5a).
  Base `ubuntu:22.04` **by digest** (recorded in the Dockerfile).
- DB `Dockerfile`: derived from the pinned checkout's `Dockerfile.mariadb`, with its base image
  digest **pinned in OUR Dockerfile as a design input** (not inherited at build time).
- Evidence (not input-locks): `dpkg -l` snapshot, source-archive commit hash, both image digests.
- Identities: `options-edge/bugzilla-req-web`, `options-edge/bugzilla-req-db`, tags
  `prod-<build>-<sha>`; compose references **by digest**.
**Acceptance:** Jenkins build from pushed main commit; dual digest evidence; running digests match.

### REQ-5a — Exposure model, vhosts, and the concrete header trust boundary
- Compose at `/home/options-edge/deploy/bugzilla-req/docker-compose.yml`; data
  `/home/options-edge/data/bugzilla-req/{data,mysql}`; env `/home/options-edge/config/bugzilla-req.env`
  (0600); secrets as file mounts (REQ-9); `restart: unless-stopped`; healthchecks REQ-10b.
  Port binding **`127.0.0.1:8093:80`** — loopback only, PROVEN by `ss -ltn` + LAN-refused test
  (V-lan), not inferred.
- **Two separate listeners (structural, distinct container ports — not ServerName-based):**
  - **Public: host `127.0.0.1:8093` → container `80`**, Apache `Listen 80`, and that listener's
    vhost set contains ONLY the OIDC-protected vhost — there is no native-login vhost on it at all.
    No Host value, absolute-form target, duplicate Host, HTTP/1.0 no-Host, or HTTP/2 authority
    trick can reach native Bugzilla through it (V3 tests all of these, including
    `Host: bugzilla-req-admin.local`).
  - **Admin: host `127.0.0.1:8095` → container `81`**, Apache `Listen 81`, vhost set contains ONLY
    the admin (native-login) vhost. Reachable solely through an `ssh -L` tunnel to `.252`.
  Host port 8095 verified free on `.252` (2026-08-03 `ss -ltn`; re-checked at §6 step 0). **Host
  port 8094 must NOT be used — the existing tunnel routes `fullfunding.nl` → `.252:8094`** (§3);
  V8 explicitly regression-tests that route. **No tunnel ingress rule references the admin
  endpoint** (asserted by config inspection, not by port number alone).
  Separation is proven by `apachectl -S` (each listener's vhost set) plus V-lan (both ports:
  loopback-only sockets, LAN refused) — `ServerName` is documentation here, never access control.
- **Public vhost OIDC directives:** `OIDCProviderMetadataURL
  https://auth.fullfunding.nl/realms/req/.well-known/openid-configuration`, `OIDCClientID
  bugzilla-web`, `OIDCRedirectURI https://req.fullfunding.nl/oidc-callback`, **`OIDCScope "openid
  email profile"`**, **`OIDCRemoteUserClaim email`**, PKCE S256, `OIDCPassClaimsAs environment`
  (claims as env only — never headers), `OIDCPassAccessToken Off`, `OIDCPassRefreshToken Off`,
  claim propagation restricted to `sub`/`email`/`name` via `OIDCWhiteListedClaims` if the installed
  package supports it (verified at step 3; JQ-dependent filtering is NOT assumed — `OIDCPassClaimsAs
  environment` alone does not filter), `OIDCCacheType shm`,
  `OIDCSessionCacheFallbackToCookie Off` (pins the REQ-5b offboarding model against package/config
  drift), `OIDCStateMaxNumberOfCookies 7 true` (per-browser state bound — NOT an origin-wide rate
  limiter; multi-tab login-loop behavior tested in V3).
- **Authorization is claim-based, not merely `valid-user` — the expression is pinned:**
  `Require claim "email~^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$"` — the `claim`
  authorization provider with the `~` regex separator INSIDE the quoted expression (upstream form:
  `Require claim "name~\w+ Jones$"`); the dot is escaped once (`\.`) because this is an Apache
  config value, not a string literal. This — NOT `OIDCRemoteUserClaim`, which only names the claim
  used for `REMOTE_USER` — is the enforcement point. Without it, `Require valid-user` would admit a
  subject with no email and Bugzilla's Env login would return `AUTH_NODATA` (`Login/Env.pm:33`) and
  fall through to CGI — the fail-open path this closes.
- **Exact scope of that rule, stated honestly (two-layer contract):**
  - *Denied at Apache, before Bugzilla:* email claim absent, empty, or present with no member
    matching the regex.
  - *Array-valued email — two distinct cases, stated exactly:*
    - **Multi-member array** (e.g. `["ok@x.com","junk"]`): may satisfy the Apache rule via one
      member, but is fail-closed one layer down — the joined env value fails
      `validate_email_syntax` and returns a hard `AUTH_ERROR auth_invalid_email`
      (`Verify.pm:76-80` @ `276673ab6`): no account created, no CGI fall-through (an error, not
      `AUTH_NODATA`).
    - **Singleton array** (`["ok@x.com"]`): joins to exactly the same scalar string as the plain
      claim, so it DOES authenticate and can auto-provision. **This is accepted, not a defect:** the
      resulting identity is byte-identical to the scalar case, and identity is keyed on `sub`
      (extern_id), not on the claim's JSON container. No design claim of "all arrays are rejected"
      is made.
    Structurally, Keycloak's `email` is a single-valued user attribute and the realm forbids
    self-registration (REQ-1), so neither array shape can arise from provisioning in the first
    place.
  - *Optional strengthening:* if the installed package provides `claims_expr` with JQ support
    (verified at §6 step 3), the normative rule additionally asserts `type(email)=="string"`,
    denying arrays at Apache instead of at Bugzilla. JQ support is NOT assumed; the two-layer
    contract above stands without it.
  **Launch prerequisite (verified at §6 step 3, before any public exposure):** the installed package
  supports this regex form, verified by `apachectl -t` plus positive and negative claim vectors. If
  it does not, the ordered fallbacks are (a) `claims_expr` (JQ-dependent, verified), or (b) a
  mandatory pre-Bugzilla deny gate in the vhost that rejects any request whose claim env value is
  absent, empty, or not a single regex-matching address. **V4d is the binding acceptance test —
  with the array vector's expected result being "denied at Apache OR hard-failed at Bugzilla with
  `auth_invalid_email` and no account created"; if no available mechanism passes V4d, launch is
  blocked** (it is not waived).
  `email_verified` is NOT required (accounts are admin-provisioned in a realm with no
  self-registration; explicitly accepted, R-8) — if self-registration is ever enabled this becomes
  a required claim condition.
  Only unauthenticated surface: the OIDC handshake.
- **Header strip (enumerated, both vhosts, before auth/CGI):** `RequestHeader unset` (early mode if
  the shipped Apache supports it) for: `X-Remote-User`, `Remote-User`, `X-Forwarded-User`,
  `REMOTE_USER`, `OIDC-CLAIM-sub`, `OIDC-CLAIM-email`, `OIDC-CLAIM-name`, and every header whose
  CGI mapping could collide with the configured claim env names (hyphen and underscore variants).
  The deployed mod_auth_openidc version and its built-in suspicious-header scrubbing are recorded
  in the as-built update. Note the structural backstop: Bugzilla Env auth reads process env vars by
  their EXACT param-configured names (`Bugzilla/Auth/Login/Env.pm:29-31` @ `276673ab6`), and CGI maps request
  headers only to `HTTP_*`-prefixed names — the strip list is defense in depth on top of that.
- **CGI-env observation test (V-env):** during V-pre, a temporary diagnostic CGI dumps the env
  Bugzilla actually sees for the spoof battery (V6b); it is removed afterwards and its absence
  verified at the onboarding gate.
- **Admin vhost** (break-glass + setup) on the separate admin listener above: identity headers
  stripped here too; no OIDC module scope → Env login gets no data → clean CGI fallback
  (`Env.pm:33` AUTH_NODATA) to native login. Accepted risk R-7.
**Acceptance:** V-lan, V3 (incl. HEAD/unexpected methods, malformed/oversized/encoded requests),
V6b, V-env; onboarding gate.

### REQ-5b — Session semantics & offboarding (stated truthfully)
- `OIDCSessionType server-cache` (module default; shm cache). Sessions live in Apache memory:
  **restarting `bugzilla-req-web` invalidates ALL portal sessions** — this, not passphrase
  abstraction, is the revocation mechanism. `OIDCCryptoPassphrase` rotation additionally
  invalidates all state/session cookies (it encrypts them) and is the compromise-response step.
- Bounds: `OIDCSessionInactivityTimeout 1800` (30 min after last activity),
  `OIDCSessionMaxDuration 28800` (hard cap 8 h from session creation regardless of activity).
  Realm `req`: SSO idle 30 min, SSO max 10 h, access token 5 min. No token refresh is configured —
  after authentication the local session is independent of Keycloak until it expires; that is WHY
  the bounds and the restart procedure exist. No back-channel logout at launch.
- **Offboarding (the procedure, REQ-1):** disable + revoke in KC + web-container restart ⇒ exposure
  window ≈ minutes, not "until token expiry".
**Acceptance:** V-off tests BOTH: (a) an active session's behavior after KC disable alone
(documented expectation: survives until restart/expiry), and (b) the full procedure killing the
session within minutes; passphrase rotation rehearsed (REQ-9).

### REQ-5c — Identity & claim contract — PINNED FROM SOURCE (commit `276673ab6`)
Binding: `auth_env_id` = sub-claim env var (`OIDC_CLAIM_sub`) → Bugzilla `extern_id`;
`auth_env_email` = `OIDC_CLAIM_email`; `auth_env_realname` = `OIDC_CLAIM_name` (exact env spellings
confirmed from the deployed module at implementation; the CONTRACT is fixed now).

Source-verified behavior (all `Bugzilla/Auth/`, commit `276673ab6`):
- Env login reads process env vars named by the params (`Login/Env.pm:29-31`); **missing email ⇒
  `AUTH_NODATA`** (`Login/Env.pm:33`) ⇒ fallthrough to CGI. On the public vhost this is
  unreachable: the pinned `Require claim "email~..."` authorization expression (REQ-5a) — not the
  scopes and not `OIDCRemoteUserClaim` — denies inside Apache before Bugzilla whenever the email
  claim is absent, empty, or malformed. Array-valued email is NOT uniformly denied at Apache: a
  multi-member array is fail-closed here instead (joined value fails `validate_email_syntax` ⇒ hard
  `AUTH_ERROR auth_invalid_email`, no account, no CGI fall-through, `Verify.pm:76-80`), while a
  singleton array is accepted as equivalent to the scalar claim (identical resulting identity, keyed
  on `sub`). See REQ-5a's two-layer contract; V4d tests both shapes. Every `req` user is provisioned
  with an email.
- **extern_id conflict** (email→account A, sub→account B) ⇒ hard `AUTH_ERROR extern_id_conflict`,
  no account capture (`Verify.pm:60-70`).
- **New sub + existing email ⇒ binds** that account to the sub (`Verify.pm:97-101`). On this
  dedicated instance only operator accounts pre-exist, and provisioning policy forbids operator
  emails for external users (REQ-1) — tested V4c.
- **New sub + new email ⇒ auto-creates** the account (`Verify.pm:74-93`); this path does NOT
  consult `createemailregexp` (no reference anywhere under `Bugzilla/Auth/`), so CGI self-signup can
  be fully disabled while Env auto-provisioning works.
- **Email or name change in KC ⇒ in-place update** of the same account, keyed by extern_id
  (`Verify.pm:126-140`: `set_login` / `set_name` then `update`); a re-assigned old email cannot
  capture another account (conflict rule above) — tested V4b.
**Acceptance:** V4 (ID-token claims verified), V4b, V4c, V4d, V6b; onboarding gate.

### REQ-5d — Application state & authorization model (versioned runbook + postconditions)
Runbook (in repo, next to compose) applies and its postconditions verify:
- Params: `urlbase=https://req.fullfunding.nl/`, `user_info_class=Env,CGI`, `auth_env_*` per 5c,
  `createemailregexp` empty (CGI self-signup off), `requirelogin` on, `maxlocalattachment=0`
  (**attachments stored in the DB** — makes REQ-10a's dump cover them; verified in V-restore),
  `maxattachmentsize` per REQ-11 size chain, `usevisibilitygroups` on with external users in no
  visibility group (cannot enumerate other users).
- Products/groups: default `TestProduct` removed; single product **"Requirements"**; external users
  hold NO groups (no `editbugs`, `canconfirm`, `creategroups`, `editcomponents`, `admin`,
  `editclassifications`, `bz_sudoers`); sole admin = Abhinav's account; postcondition enumerates
  every group and its members. Note Bugzilla grants some rights by ROLE (reporter/assignee/CC), not
  only by group — hence the explicit matrix below rather than inference from group names.
- **Authorization contract — CLOSED matrix, pinned to VERIFIED stock behavior (no custom extension).**
  Enforcement mechanism = stock `Bugzilla::Bug::check_can_change_field` (`Bugzilla/Bug.pm:4514-4650`
  @ `276673ab6`) + the named params + group membership; the design was reconciled TO that code
  rather than declaring cells the stock controls cannot deliver. Verified source facts driving the
  table: a user without `editbugs` who is the REPORTER may change most fields of their own ticket,
  but never `assigned_to`, `qa_contact`, `target_milestone`, time-tracking/`deadline`, `priority`
  (with `letsubmitterchoosepriority` OFF), or anything requiring `canconfirm`; comments and flags
  are open to anyone who can see the bug (`Bug.pm:4567-4570`) — so flags are closed by using NO
  flag types on this instance; an attachment's submitter may edit/obsolete their own attachment regardless of who
  reported the ticket (attachment permissions are governed separately from
  `bug_check_can_change_field`, so the hook fallback does NOT cover attachment cells — an
  attachment-policy change would require its own specified mechanism).
  Actors: `OWN` = external user on their own ticket, `OTHER` = external user on another external
  user's ticket, `ANON` = unauthenticated.

  | Operation | OWN | OTHER | ANON |
  |---|---|---|---|
  | Create ticket (product/component fixed to Requirements/General) | ALLOW | ALLOW | DENY |
  | View a Requirements ticket | ALLOW | ALLOW | DENY |
  | Add comment | ALLOW | ALLOW | DENY |
  | Add attachment | ALLOW | ALLOW | DENY |
  | Edit/obsolete an attachment THEY submitted (incl. one they attached to another user's ticket) | ALLOW | ALLOW | DENY |
  | Edit/obsolete an attachment submitted by SOMEONE ELSE (any ticket) | DENY | DENY | DENY |
  | Edit summary / description-level fields / severity / platform / version / custom fields | ALLOW | DENY | DENY |
  | Change `assigned_to`, `qa_contact`, `target_milestone` | DENY | DENY | DENY |
  | Change `priority` (`letsubmitterchoosepriority` OFF) | DENY | DENY | DENY |
  | Time-tracking fields / `deadline` (no time-tracking group membership) | DENY | DENY | DENY |
  | Confirm a bug / any `everconfirmed` transition (no `canconfirm`) | DENY | DENY | DENY |
  | Other status/resolution transitions on own ticket (stock reporter right) | ALLOW | DENY | DENY |
  | Flags | DENY (no flag types defined) | DENY | DENY |
  | CC changes (self) | ALLOW | ALLOW | DENY |
  | Dependencies / duplicates / alias / See-Also on own ticket | ALLOW | DENY | DENY |
  | Private comments / attachments (create or read; `insidergroup` empty ⇒ feature unused) | DENY | DENY | DENY |
  | Mass-change endpoints (require `editbugs` on the targets) | DENY | DENY | DENY |
  | Move / clone to another product (only Requirements exists) | DENY | DENY | DENY |
  | Product/component/version/milestone discovery beyond Requirements | DENY | DENY | DENY |
  | User search / autocomplete / REST user enumeration (`usevisibilitygroups` ON, no shared group) | DENY | DENY | DENY |
  | CSV / XML / REST export of tickets they can view | ALLOW | ALLOW | DENY |
  | Saved-search sharing, charts/report config | DENY | DENY | DENY |
  | Any surface outside the Requirements product | DENY | DENY | DENY |
  | Any admin surface (params, users, groups, products, sudo) | DENY | DENY | DENY |

  **This table is the contract, not a starting point.** Every cell is claimed to follow from stock
  code + the named params (`letsubmitterchoosepriority` OFF, `usevisibilitygroups` ON, no flag
  types, empty `insidergroup`, `requirelogin` ON, single product, external users in no group). If
  V-authz shows ANY cell deviating, the resolution is an approved design revision — a change to the
  params/config, an accepted change to the expected value, or (last resort, and then fully
  specified: pinned in the image with digest evidence, fail-closed on load failure, applied to UI
  and REST, unit + V-authz covered, included in backup/LKG/schema rules) a `bug_check_can_change_field`
  hook extension, which `Bug.pm:4548-4565` is designed to accept. The as-built record documents
  EVIDENCE per cell; it may not silently change an expected value.
- **Rebuild-from-empty rehearsal** once before launch: fresh stack → runbook → postconditions green.
**Acceptance:** rehearsal + live postconditions + **V-authz** executing every cell of the matrix
above (UI and REST) with recorded per-cell evidence; any deviation blocks launch.

### REQ-6 — Internal Bugzilla untouched (no-op requirement)
Evidence at every declared transition (§6 steps 1,3,6,7): image digests, mount list, compose config
hash, params checksum identical; login + one REST API-key call working. Any restart of the internal
stack during the project is a reportable deviation.
**Acceptance:** V7 — "configuration evidence identical and functions working".

### REQ-7 — Cross-system isolation — token-level proof
- Trading side accepts ONLY issuer `…/realms/optionsedge` (read-only inspection of deployed web env
  + feed-gateway JWT config as evidence).
- **V6t:** disposable test user + TEMPORARY kcadm test client (`isolation-test`, direct-grant,
  minimal scopes) → real `req` access token → trading API ⇒ 401/403; `req` browser session at
  `fullfunding.nl` ⇒ no session; symmetric negative for `optionsedge` users at the portal. Client
  deletion verified in a finally-style cleanup step (postcondition: client absent).
- **`optionsedge` no-change proof:** normalized kcadm export diff of security-relevant fields
  (semantic, not byte).
- **V6b spoof battery:** canonical/lower/upper/hyphen/underscore variants, duplicates,
  `Authorization`, `Remote-User`, `X-Remote-User`, claim-name candidates; authenticated user A
  submitting user B's identity header still acts as A; both vhosts via loopback; verified against
  the CGI env actually observed (V-env).
**Acceptance (never skipped):** V6, V6b, V6t, semantic no-change proof; onboarding gate.

### REQ-8 — Jenkins-only build & deploy path (fail-closed deploys)
- New Jenkins job (options-edge-deploy, main-only SCM): builds both images on `local-mac` (amd64
  cross-build), pushes to `.252:5000`, delivers compose, runs `docker compose up -d` remotely
  (Jenkins executes; never runs on `.252`). Concurrency lock on deploy stage.
- **Deploy protocol:** capture pre-deploy digests/config → apply → **bounded health polling**
  (REQ-10b gates, e.g. 12×10 s) → on failure classify state (any service not at target digest AND
  healthy ⇒ deploy FAILED) → automatic redeploy of recorded **last-known-good** digests → if LKG
  also fails, job fails loudly and the public fail-closed rollback (REQ-3) applies.
- **Schema-compatibility rule:** LKG image rollback is valid only within the same Bugzilla schema
  (checksetup migrates forward only); a rollback across a schema-migrating bump requires DB restore
  from REQ-10a backups — stated in the runbook.
- Internal stack's pre-existing out-of-band build: unchanged, not widened.
**Acceptance:** Jenkins evidence; V-rollback (LKG rehearsal) green.

### REQ-9 — Secrets: scoped claim, file-mounted, atomic rotation
- **Scope (stated precisely):** the client secret necessarily also exists in Keycloak's database;
  the env/secret files on `.252` are the only OPERATOR-MANAGED plaintext persistence outside
  Keycloak. Secrets are delivered to Apache via compose **file mounts** (0600 host → 0400
  in-container, root-owned) — **never via container environment** (nothing in `docker inspect` env
  output) and never in healthcheck command text. Delivery mechanism: `OIDCClientSecret
  exec:/usr/local/bin/read-secret` (read-once helper) — **the installed module version's support
  for `exec:` is a LAUNCH PREREQUISITE verified in step 3**; if the packaged version lacks it, the
  fallback is a root-0400 conf fragment rendered at container start, which becomes an additional
  **authorized secret location** (below). Docker-admin access = R-12.
- **Authorized-location model (makes V10 satisfiable):** secret material may exist ONLY at an
  explicit allowlist — the host secret files, their in-container mounts, and (fallback only) the
  named root-0400 conf fragment — each with verified ownership/permissions. V10 asserts: present at
  every allowlisted path with correct mode, and ABSENT everywhere else (repo, Jenkins console +
  archived artifacts, `docker inspect` output incl. env and healthcheck, container logs, backup
  listings, process args, shell history).
- **Two secrets, two lifecycles:** the **client secret** is generated by Keycloak (rotation =
  regenerate in KC → replace file → restart); the **`OIDCCryptoPassphrase`** is generated locally
  (`openssl rand`) and never known to Keycloak (rotation = regenerate locally → replace file →
  restart, which also invalidates all sessions — the REQ-5b emergency control). Both are rehearsed
  once during rollout. Neither is backed up (REQ-10a); both are regenerated during recovery.
- Transfer: single server-side pipe (kcadm → file), `set +x`, umask 077, no echo; atomic
  replacement (write-tmp + `mv`). **Client-secret rotation:** regenerate in KC → atomically replace file →
  restart web (one Jenkins-driven op). If the restart/deploy fails after KC regeneration, LKG
  redeploy reads the SAME updated file (already valid at KC) — states converge; the brief
  login-fail window is taken in the change window.
- **V10:** the authorized-location assertion above — present-and-correctly-permissioned at the
  allowlist, absent at every other searched surface — always WITHOUT printing values
  (length-prefix/canary match).
**Acceptance:** V10 + rotation rehearsal recorded.

### REQ-10 — Windows, backups, health, verification, observation

#### REQ-10a — Windows & recoverable state
- Window: KC rollout + cloudflared restarts outside Mon–Fri 09:30–16:15 America/New_York (rule.md
  calendar); emergency exceptions = Abhinav's explicit go. Steps serialized. Pre-step-0: disk/memory
  headroom, port-8093-free, DNS-record-absent checks.
- Keycloak: `pg_dump` pre-change (consistent single-transaction dump, taken in the quiet window,
  owner Abhinav) → `/home/options-edge/backups/keycloak/`; restore validated once into a throwaway
  Postgres container. Use per REQ-1 (DR only, all-realms consequence acknowledged).
- Portal: **nightly backup generation** (cron on `.252`), written as ONE atomic generation:
  `mysqldump --single-transaction` (covers tickets AND attachments — in-DB mode per REQ-5d) + tar
  of `/home/options-edge/data/bugzilla-req/data` **excluding any secret material** + image digests
  + runbook version, all into a timestamped **temp directory on the same filesystem and under the
  same parent** as the final location (so the rename is atomic), plus a `manifest.json` (start/end
  time, DB schema version, image digests, source revision, per-artifact checksums) written LAST as
  the **completion marker**, then **atomic `mv`** into `/home/options-edge/backups/bugzilla-req/`.
  **Secrets are deliberately NOT backed up** (they would otherwise become unlisted plaintext
  secret locations, contradicting REQ-9's allowlist): the OIDC client secret is regenerated in
  Keycloak and `OIDCCryptoPassphrase` is regenerated during any recovery — a documented restore
  step, and harmless since passphrase regeneration only invalidates sessions.
  Only completed, checksum-valid generations are restorable; retention (14 days) and the existing
  `.252` daily archive job skip in-progress temp directories by construction. **Monitoring:** the job writes a status line to the
  existing `.252` cron-log/archive path that the daily ops check reads (this is backup-job
  alerting, a separate control from service uptime monitoring — R-6 covers the latter), and
  observation checks `newest generation age < 26 h` and
  `backup filesystem free > 20 GB / 10%`. **RPO 24 h, RTO hours (manual restore)** — R-9. Backups
  are unencrypted on the host (same custody as all prod data) — accepted in R-9.
- **V-restore (content-verified, executable topology):** restore a completed generation into a
  fresh stack on a **separate loopback port**, never touching the live stack or its callback. OIDC
  is exercised through a **temporary** kcadm-created client `bugzilla-restore-test` whose exact
  redirect URI is the **loopback HTTP callback** `http://127.0.0.1:18093/oidc-callback` — no TLS
  endpoint, certificate, or proxy is needed (the realm's `sslRequired: external` permits plain HTTP
  on private/loopback addresses; the browser reaches it through `ssh -L 18093:127.0.0.1:<restore
  container port>` to `.252`, so local port 18093 and the restore stack's own loopback port are
  both pinned). Production ingress and the production client are never involved; the temporary
  client's redirect URI is asserted before the test runs. the test asserts the authenticated request hit the RESTORED
  container digest + database (distinct ticket id / digest check), then verifies a known ticket,
  attachment bytes checksum, identity mapping (extern_id), and product/group postconditions.
  Finally-style cleanup deletes the temporary client and tunnel and VERIFIES their absence.
  Cadence: rehearsed before launch and re-run per release that changes images or schema
  (`restore validation = per-release`, not merely one-time).

#### REQ-10b — Health model (liveness ≠ readiness ≠ security gate)
- Web liveness: Apache process up, config valid (`apachectl -t` at build + start).
- Web readiness: an **unauthenticated, non-mutating** admin-listener probe that nonetheless
  initializes Bugzilla and touches the DB. With `requirelogin` on, Bugzilla forces LOGIN_REQUIRED
  for REST (`Bugzilla.pm:333` @ `276673ab6`), so the probe asserts the SPECIFIC login-required REST
  error JSON from `GET /rest/parameters` — reaching that error already proves Perl, CGI, Bugzilla
  bootstrap and DB connectivity. The exact endpoint/expected body is confirmed as a **step-3
  readiness prerequisite**; if that response proves not DB-backed, another non-mutating DB-touching
  admin-listener endpoint is substituted. **No API key is introduced** (an API key in healthcheck text would
  land in `docker inspect` and contradict V10); if a future probe ever needs credentials, it must
  be a formally scoped REQ-9 secret with file delivery. The probe creates no audit noise and
  mutates nothing.
- DB readiness: schema/application-level query against the bugs DB (not just `mysqladmin ping`,
  which stays as the container-level check).
- Security gate: public listener returns the expected OIDC redirect (never Bugzilla content).
- Recovery: V-restart — forced restart of db then web; web returns READY after DB restart without
  manual action; data persists.
- **Failure classification during deploy polling (REQ-8):** distinguish *not-yet-ready* (retry
  within the bound) from *permanently failed* (config/auth error — stop polling early); in both
  terminal cases capture logs + probe output as diagnostics BEFORE the automatic LKG replacement
  overwrites the state.

#### REQ-10c — Verification scope & observation
- **V-pre (local; what it CAN prove):** vhost selection for arbitrary Hosts; anonymous
  denial/redirect with correct realm/client/callback/state/nonce/PKCE parameters; zero Bugzilla
  body pre-auth; V6b spoof battery (with V-env); admin-vhost behavior; V-lan. **End-to-end OIDC
  login (V4 family) is a post-DNS, pre-onboarding gate** — it cannot execute before public
  reachability exists.
- Isolation rows: never skippable. V7-int (internal trading-UI live cards): not skippable —
  deferrable ONLY to the next market-hours window (owner Abhinav, deadline next trading session).
- Observation: 24 h post-publish (owner Abhinav): cloudflared origin errors, portal Apache error
  log, KC failed-login storms, restart counts, disk. **Numeric thresholds:** alert/act if `/home`
  free < 20 GB or < 10%; any container restart count > 0 investigated. Step 8 incomplete until the
  window closes clean.
- **Logs as covered data (REQ-13):** container logs via docker json-file rotation (`max-size 50m`,
  `max-file 3` ⇒ bounded retention); access restricted to host admins; tokens never appear in URLs
  (code flow; opaque `state` only) and claims are not logged at default `OIDCLogLevel`; Apache logs
  hold emails at most — accepted within R-9/R-12 custody.
- Evidence: tracking bug in the INTERNAL Bugzilla (id assigned at §6 step 0; carries R-6 follow-up).

### REQ-11 — Login-surface & edge hardening (layered, plan-aware)
- **Layer 1 (authoritative, plan-independent):** Keycloak realm-`req` brute-force protection
  (account lockout) — the actual credential-attack control.
- **Layer 2 (module):** `OIDCStateMaxNumberOfCookies 7 true` caps state-cookie creation at the
  initial unauthenticated redirect surface (the real state-flood vector; callback-only limiting
  would miss it).
- **Layer 3 (edge, best-effort within plan):** at implementation, RECORD the current Cloudflare
  plan and its rate-limiting capabilities, then define the exact expressions (host + normalized
  path + method), threshold, period, counting key, mitigation duration, and action for: (a)
  `auth.fullfunding.nl/realms/req/*` login POSTs — path-scoped so realm `optionsedge` is untouched
  (V11b runs IMMEDIATELY when this rule lands, §6 step 6); (b) `req.fullfunding.nl` handshake
  paths. NAT-shared stakeholder traffic acceptance is decided when thresholds are set. Rule
  rollback = delete rule in dashboard (verified by V11b rerun).
- **Size-limit chain (consistent):** Bugzilla `maxattachmentsize` (10 MB) < Apache
  `LimitRequestBody` (25 MB) < Cloudflare upload cap (plan default ~100 MB) — recorded and tested
  with an attachment at the Bugzilla limit.
**Acceptance:** V11 functional trip test + normal-login pass on both surfaces; V11b; chain test.

### REQ-12 — Patch & vulnerability posture
- Web image pins an exact commit **selected from branch 5.2**; monthly (or on-CVE) a reviewed PR
  bumps the pin to the branch head and redeploys via REQ-8; the monthly review records branch
  activity, upstream Bugzilla security advisories, and Ubuntu package security status (R-13
  evidence criteria). OS/mod_auth_openidc updates ride the
  same rebuild.
- Socket posture verified by `ss -ltn` evidence (V-lan); `.252` firewall unchanged.
**Acceptance:** first build + one bump rehearsal (may coincide with a real upstream bump).

### REQ-13 — Privacy & data handling
External tickets are business-confidential. Covered data: tickets, attachments, exports, **logs**,
**backups**. Storage `/home/options-edge/data/bugzilla-req/`; retention indefinite (R-9); deletion
only by Abhinav. Shared visibility among external users of the single "Requirements" product;
**before EVERY new organization is onboarded, Abhinav confirms it may see existing parties'
submissions**; mutual confidentiality ⇒ new requirement (per-org products/groups) BEFORE that
onboarding. Onboarding checklist encodes this gate.
**Acceptance:** acknowledged at approval; checklist exists at launch.

## 5. Explicit non-goals

- No internal-Bugzilla migration (REQ-6); no trading-service change (read-only evidence only).
- No Bugzilla major-version upgrade (branch 5.2 pin+bump, REQ-12); no self-service signup/reset
  (REQ-1); no SMTP/notifications (nothing user-facing depends on mail; later = new requirement,
  R-11); no per-external-user separation inside "Requirements" (REQ-13 gate).

## 6. Rollout sequence (every matrix row assigned; fail-closed)

| Step | Change / gate | Matrix rows |
|---|---|---|
| 0 | Capacity/port/DNS-absence checks; KC pg_dump + throwaway restore validation; cloudflared backup; open tracking bug | V-restore (KC half) |
| 1 | PR-A realm+client → Codex 3-bar → merge → Jenkins infra deploy → KC rollout (pre-market) | V1, V2, V2b, V9, V7 |
| 2 | Verify live client THEN secret transfer + both rotation rehearsals (client secret, crypto passphrase) | V10 (transfer/storage precheck only) |
| 3 | PR-B images/compose/job → Codex 3-bar → merge → Jenkins build+deploy (loopback-only). **Module prerequisites verified here:** `Require claim "<name>~<regex>"` form (else fallback chain), `exec:` secret retrieval, `OIDCWhiteListedClaims`, readiness-probe response | REQ-10b gates, V-lan (both ports + `apachectl -S`), **binding V10**, V7 |
| 4 | Runbook app-state + rebuild-from-empty rehearsal | REQ-5d postconditions |
| 5 | **V-pre battery** (local) | V3, V6b, V-env, V-authz, V-restart |
| 5.5 | Backup/restore + rollback rehearsals (portal half) | V-restore, V-rollback |
| 6 | Cloudflare edge rules (V11b immediately) → ingress + cloudflared restart (pre-market) | V8, V11b, V7 |
| 7 | **DNS publish (final)** → immediately, before any stakeholder is provisioned | V4, V4b, V4c, V4d, V-off, V5, V6, V6t, V11, V-smoke |
| 7.5 | **Technical gate (provisional):** every matrix row green EXCEPT V7-int if the market is closed | all but V7-int |
| 8 | 24 h observation closes clean **AND V7-int green** (next market-hours window) → **ONBOARDING GATE**; only then the first stakeholder is provisioned, via the REQ-13 checklist | V7-int + observation |

**Rollback:** pre-public branch and public fail-closed invariant exactly as REQ-3 (the REQ-3
wording governs). KC realm-only corrections via inverse kcadm ops; KC DB restore only per REQ-1.

## 7. Verification matrix

| # | Check | Expected | Skippable? |
|---|---|---|---|
| V1 | realm `req` discovery | 200, issuer `…/realms/req` | no |
| V2 | `optionsedge` semantic no-change (normalized export diff) | no security-relevant drift | no |
| V2b | authz request with non-registered `redirect_uri` | rejected by Keycloak | no |
| V3 | PUBLIC listener only: arbitrary Hosts incl. `Host: bugzilla-req-admin.local`, absolute-form targets, duplicate Host, HTTP/1.0 no-Host, HTTP/2 authority variants, GET/HEAD/POST/unexpected methods, malformed/oversized/encoded requests, multi-tab login-loop check | OIDC redirect w/ correct params or clean 4xx; native login unreachable; zero Bugzilla body; no login loop | no |
| V-lan | `ss -ltn` + connect from dev Mac | loopback-only; LAN refused | no |
| V4 | end-to-end login, disposable test user (post-DNS, pre-onboarding); decode ID token | working account; `sub`/`email`/`name` present; sees only "Requirements" | no |
| V4b | email+name change in KC → re-login | same account (extern_id), updated in place, no duplicate | no |
| V4c | new sub + pre-existing email, using a disposable fixture account+user created for the test | binds per source semantics; fixture deleted and absence verified; policy row: no operator-email externals exist | no |
| V4d | email claim absent, empty, malformed, null; array-valued in BOTH shapes (multi-member mixed valid/invalid, and singleton valid); plus `email_verified=false` | absent/empty/malformed/null: denied by the pinned `Require claim "email~…"` inside Apache, no Bugzilla/native-login content in the response. Multi-member array: denied at Apache OR hard-failed at Bugzilla with `auth_invalid_email` and NO account created. Singleton array: accepted, producing an identity byte-identical to the scalar case (documented, not a defect). Per REQ-5a's two-layer contract. `email_verified=false` accepted by design (R-8) | no |
| V5 | portal content audit | no internal data present | no |
| V6 | cross-realm login attempts both directions | rejected | no |
| V6b | spoof battery incl. user-A-sends-user-B, both vhosts, validated against V-env CGI env dump | never authenticates / never switches identity | no |
| V6t | disposable `req` token → trading API; `req` session → `fullfunding.nl`; cleanup verified | 401/403; no session; test client deleted | no |
| V-env | diagnostic CGI env observation during V6b, then removed | expected env only; CGI absent at onboarding gate | no |
| V-authz | every cell of the REQ-5d closed matrix, via UI and REST, actors OWN/OTHER/ANON | each cell matches its declared ALLOW/DENY; any deviation blocks launch | no |
| V-off | (a) KC disable alone; (b) full offboarding procedure | (a) documented survival until restart/expiry; (b) session dead in minutes | no |
| V7 | internal evidence set at steps 1/3/6/7 | configuration evidence identical AND functions working | no |
| V7-int | internal trading-UI live cards | working | deferrable to next market-hours window only |
| V8 | all hostnames after each cloudflared restart, explicitly incl. `fullfunding.nl` → `.252:8094` (the route adjacent to the portal's port choice) and `fullfunding.nl/ws/events` WebSocket | normal | no |
| V9 | KC pod post-rollout | Ready, no loop, `:8089` reachable | no |
| V10 | authorized-location assertion across the REQ-9 surface list, values never printed | found ONLY at the allowlist with correct owner/mode; absent everywhere else (incl. backups) | no |
| V11 | edge-rule trip test + normal login passes + attachment size-chain tested just below / exactly at / just above each effective limit (units normalized across Bugzilla/Apache/Cloudflare; the above-Cloudflare-cap point is executed only if the current plan allows a deterministic test without cost/disruption, else recorded as not-executed with the reason) | block observed; login unaffected; each limit rejects at the expected layer with a clear error | no |
| V11b | `optionsedge` login immediately after auth-hostname rule (and after any rule rollback) | unaffected | no |
| V-smoke | bounded concurrency smoke (≈10 parallel sessions filing tickets) | pass limits pinned BEFORE the run: zero errors, zero container restarts, no request exceeding a 30 s timeout; p50/p95/max latency and CPU/mem headroom recorded as capacity evidence (no absolute latency target for this scale) | no |
| V-restart | forced db-then-web restarts | ready again unaided; data persists | no |
| V-restore | content-verified restore (REQ-10a: ticket, attachment checksum, identities, postconditions, OIDC login) + KC throwaway restore | restores verified | no |
| V-rollback | LKG redeploy rehearsal + public fail-closed rehearsal (hostname provably closed first) | works as specified | no |

## 8. Accepted-risk register (single-operator scope calibration, approved with this design)

| Id | Risk | Rationale / mitigation |
|---|---|---|
| R-1 | No staging env | V-pre honestly scoped; DNS-last; V-rollback rehearsal |
| R-2 | No formal load/soak | <10 users; V-smoke row is the executed check; 24 h observation |
| R-3 | KC single-replica blip during pre-market rollout | existing posture; V9 + validated pg_dump restore |
| R-4 | No SBOM/signing | environment practice; digest pinning = immutability, NOT provenance — stated plainly |
| R-5 | cloudflared restarts blip all hostnames seconds | pre-market only; V8 incl. WebSocket every time |
| R-6 | No automated uptime probe at launch | 24 h manual observation; follow-up carried in the tracking bug (id at step 0), owner Abhinav, target first week post-launch |
| R-7 | Break-glass native admin vhost exists | loopback+ssh only; header strip; V-lan/V6b negatives |
| R-8 | No MFA for externals at launch | admin-provisioned, password policy, brute force, bounded sessions; KC OTP later |
| R-9 | Indefinite retention; RPO 24 h/RTO hours; backups unencrypted on host | intake tracker; nightly content-verified-restorable pair; host custody = R-12 |
| R-10 | Shared visibility among external orgs | REQ-13 per-onboarding gate |
| R-11 | No notification email | nothing depends on mail; users check the portal |
| R-12 | Single-host colocation (shared fate; host admin sees everything incl. secrets) | tunnel-only exposure, loopback bind, OIDC front; host hardening unchanged |
| R-13 | ubuntu 22.04 archive + Bugzilla 5.2 lifetime | commit selected from branch 5.2; monthly review records branch activity AND upstream security advisories AND Ubuntu package security status — branch activity alone is not a support guarantee (REQ-12) |

## 9. Requirement reconciliation (authoritative state; all gated on the §6 step-8 onboarding gate)

**As-built note (2026-08-04):** the design merged as `1fb2dd6`. PR-A implements REQ-1 + REQ-2 only
(`k8s/keycloak/keycloak-realm-configmap.yaml` adds the `req-realm.json` key; the KC Deployment gains
an `oe/config-nonce` so the plain ConfigMap change actually rolls the pod, since `--import-realm`
runs only at container start). Deviation recorded per the Implemented-Code Documentation Accuracy
Rule: the client does NOT declare `defaultClientScopes` — it inherits the realm defaults exactly as
`options-edge-web` does, because `openid` is not a Keycloak client scope and naming a non-existent
scope would be a guess; REQ-2's live kcadm + decoded-ID-token check remains the authority for the
`email`/`profile` claims.

| Id | State | Evidence / gate |
|---|---|---|
| REQ-1 | IMPLEMENTING | PR-A (realm JSON + nonce) open; becomes IMPLEMENTED only after merge + KC rollout + V1/V2/V9 + live reconciliation + onboarding gate |
| REQ-2 | IMPLEMENTING | PR-A (client JSON) open; becomes IMPLEMENTED only after merge + kcadm live inspection + V2b + V4 claim check + onboarding gate |
| REQ-3 | TRACKED-PENDING | ordering + V3/V8 + V-rollback + onboarding gate |
| REQ-4 | TRACKED-PENDING | PR-B + dual digests + onboarding gate |
| REQ-5a | TRACKED-PENDING | V-lan/V3/V6b/V-env + onboarding gate |
| REQ-5b | TRACKED-PENDING | V-off (both halves) + onboarding gate |
| REQ-5c | TRACKED-PENDING | V4/V4b/V4c/V4d + onboarding gate |
| REQ-5d | TRACKED-PENDING | rehearsal + postconditions + V-authz + onboarding gate |
| REQ-6 | TRACKED-PENDING | V7 at steps 1/3/6/7 + onboarding gate |
| REQ-7 | TRACKED-PENDING | V6/V6b/V6t + semantic proof + onboarding gate |
| REQ-8 | TRACKED-PENDING | Jenkins evidence + V-rollback + onboarding gate |
| REQ-9 | TRACKED-PENDING | V10 (widened) + rotation rehearsal + onboarding gate |
| REQ-10 | TRACKED-PENDING | backups/restore + health gates + observation + onboarding gate |
| REQ-11 | TRACKED-PENDING | V11/V11b + chain test + onboarding gate |
| REQ-12 | TRACKED-PENDING | build + bump rehearsal + onboarding gate |
| REQ-13 | TRACKED-PENDING | acknowledgment + onboarding checklist at onboarding gate |

## 10. Open decisions (none blocking)

- Intake product name: **"Requirements"** (rename trivial before onboarding).
- Uptime-probe placement (prod-mac monitor vs market-sentinel) — R-6, in the tracking bug.
