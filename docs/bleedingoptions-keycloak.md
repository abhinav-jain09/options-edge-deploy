# Public Keycloak (`bo-keycloak`) — operator runbook

The identity provider for **bleedingoptions.com**, the public Gamma Lab. Realm `bleedingoptions`,
namespace `bleedingoptions`, entirely separate from the internal `oe-keycloak` in `options-edge`.

Design and requirement ids: `bleedingoptions-public-gamma-lab.md` in the `options-edge` repo.

> **Not public yet.** Gate 4a builds the instance only. There is no DNS record, no tunnel route and no
> public web workload. Keycloak answers on the LAN (`http://192.168.100.252:8189`) and in-cluster,
> nowhere else. Publishing is PGL-032/PGL-036; the web tier is PGL-072-gated.

---

## 1. Before the first deploy — the Secret

`bo-keycloak-secrets` **never enters git** (PGL-030), and it is **not created by hand**. An earlier
version of this section printed a `kubectl create secret` command — that is a hand-deploy, which the
Absolute Jenkins-Only Deployment Rule forbids for Secrets by name. The sanctioned path is the
`bleedingoptions-secrets-deploy` job (`Jenkinsfile.bleedingoptions-secrets`). Three keys:

| Key | What it is |
|---|---|
| `POSTGRES_PASSWORD` | Read by BOTH Postgres and Keycloak's JDBC URL, so they cannot drift into a mismatch |
| `KC_BOOTSTRAP_ADMIN_PASSWORD` | Bootstrap admin only — disabled after §2 |
| `KC_SMTP_PASSWORD` | The Gmail app password for `info@bleedingoptions.com` |

**Step 1 — store the values in Jenkins once.** Generate the two passwords rather than reusing one
(`openssl rand -base64 30`), and add them as *Secret text* credentials with these exact ids:
`bo-keycloak-postgres-password`, `bo-keycloak-admin-password`, `bo-keycloak-smtp-password`. The job
reads them from the credential store, so no value ever appears in this repo, in a manifest, in a job
parameter or in a build log.

**Step 2 — run the jobs in this order.** The namespace and the workloads are owned by `common-infra`;
the Secret job requires the namespace and refuses to create it, so that one object has one owner.

| # | Job | Parameters | Why here |
|---|---|---|---|
| 1 | `common-infra` | `ENVIRONMENT=production`, **`DEPLOY_DRY_RUN=false`**, **`RECONCILE_REALM=false`** | Creates the namespace, the NetworkPolicies and the workloads. Keycloak will not start yet — the Secret is missing — and that is expected. |
| 2 | `bleedingoptions-secrets-deploy` | `ENVIRONMENT=production`, **`DEPLOY_DRY_RUN=false`** | Writes the Secret. Keycloak and Postgres then boot and the realm is imported. |
| 3 | — | — | Retire the bootstrap admin (§2), then create the reconciler client (§3). |
| 4 | `common-infra` | `ENVIRONMENT=production`, **`DEPLOY_DRY_RUN=false`**, `RECONCILE_REALM=true` | Now the reconciler exists, so the realm can be reconciled. |

> **Both parameters default to the safe value, and both must be overridden.** `DEPLOY_DRY_RUN`
> defaults to `true` on *every* job here, so a run left at the defaults renders and diffs and creates
> nothing — step 2 would then fail on a namespace that step 1 never made. `RECONCILE_REALM` defaults
> to `true`, and its stage binds the `bo-keycloak-reconciler-secret` credential — a client that cannot
> exist until Keycloak has booted and imported the realm — so leaving it on for the first run applies
> every resource and *then* fails the build, which reads like a broken deploy when it is only
> ordering.

> **The app password currently sits in the git-tracked `url.md`.** It has not been committed, but a
> credential in a tracked file is one `git add -A` from being in GitHub history permanently, where
> deleting the file does not remove it. Revoke it at
> [Google account security](https://myaccount.google.com/apppasswords), issue a fresh one, and put
> the new one only in the Jenkins credential store.

### 1a. Rotating `POSTGRES_PASSWORD` — not a one-field change

The other two keys rotate by editing the Jenkins credential and re-running the job: the bootstrap
admin password is read only at first boot, and Keycloak picks up a new SMTP password on restart.

`POSTGRES_PASSWORD` is different, and getting it wrong takes the realm down. The value is read by
*both* sides, but only one of them is authoritative: `POSTGRES_PASSWORD` initialises the database role
**on an empty data directory only**. On an existing volume Postgres ignores it entirely and keeps the
role password it already has — so changing the Secret changes only what Keycloak *presents*, and
Keycloak then fails authentication and CrashLoops. Rotate in this order:

> **This is a short planned outage, not a seamless rotation.** Postgres has no dual-password support:
> `ALTER ROLE ... PASSWORD` invalidates the old password for every NEW connection the instant it
> runs. Keycloak's already-established pool connections survive, but any reconnect between step 1 and
> step 4 fails. Do this in a maintenance window and expect Keycloak to be unavailable from step 1
> until the restart in step 4 completes.

1. Change the role inside the running database using psql's **`\password`** meta-command, at an
   interactive prompt:

   ```bash
   kubectl -n bleedingoptions exec -it sts/bo-keycloak-postgres -- psql -U keycloak -d keycloak
   ```
   ```text
   keycloak=# \password keycloak
   Enter new password:            <-- not echoed
   Enter it again:
   keycloak=# \q
   ```

   Use `\password`, not `ALTER ROLE ... PASSWORD '<new>'`, and do not try to script it by feeding
   SQL on stdin. `\password` reads the value from the terminal and sends it **already hashed**, so
   the plaintext never appears in argv on either side, in the server log, in `pg_stat_activity`, or in
   this document. Because it is typed at a prompt rather than substituted into SQL, no shell or psql
   escaping applies and every character is safe.

   An earlier version of this runbook piped `\set pw '<value>'` on stdin and claimed it quoted
   correctly. It does not: tested against psql 16.14, a password containing `'` is rejected
   (`unterminated quoted string`) and one containing `\` is silently stored with the backslash
   **dropped** — a rotation that reports success and leaves a password nobody can reproduce.

   Note `sts/` — `bo-keycloak-postgres` is a **StatefulSet**, so `deploy/` fails with "no matches".
2. Update the `bo-keycloak-postgres-password` credential in Jenkins to exactly that value.
3. Run `bleedingoptions-secrets-deploy` with `DEPLOY_DRY_RUN=false`.
4. Restart Keycloak so it re-reads the Secret:
   `kubectl -n bleedingoptions rollout restart deploy/bo-keycloak`. Until this completes Keycloak is
   still presenting the old password, which is why step 1 comes first and why the window exists.

**Rollback:** re-run step 1 with the old password, restore the Jenkins credential, re-run the job and
restart again. Postgres keeps serving its data throughout — only authentication changes — so there is
no data-loss window, only an availability one.

---

## 2. First boot — retire the bootstrap admin (PGL-031C)

`KC_BOOTSTRAP_ADMIN_USERNAME=admin` exists to create the first real administrator and nothing else.
Leaving it enabled leaves a well-known username on an internet-facing identity provider.

```bash
# Admin console, LAN only:  http://192.168.100.252:8189
# 1. Sign in to the master realm as `admin`.
# 2. Create your own master-realm admin user, set a password, grant it `admin`.
# 3. Sign out, sign back in as that user, and DISABLE the `admin` account.
```

Then create the reconciler's service account (§3). Keep `KC_BOOTSTRAP_ADMIN_PASSWORD` in the Secret
even after disabling the account — Keycloak reads it at every boot, and a missing key fails startup.

---

## 3. The reconciler's credential — least privilege (PGL-031A1)

`--import-realm` **skips a realm that already exists**, so the manifest is desired state and
`scripts/ops/bleedingoptions-realm-reconcile.sh` is what actually applies it. It authenticates as a
service-account client in the `bleedingoptions` realm.

Create client `realm-reconciler`: confidential, service accounts on, standard flow **off**, direct
access grants **off**. Then grant its service-account user exactly these `realm-management` roles:

```text
view-realm  manage-realm  view-clients  manage-clients  query-groups  manage-authorization
view-users  view-events   manage-events
```

**It must NOT hold `manage-users`, `impersonation`, or `realm-admin`.** The reconciler's contract is
that it never *writes* users — approval state lives in group membership, and a reconciler able to
edit memberships could un-approve everyone.

> **Why `view-users` is in the list** (changed 2026-08-22, was originally forbidden): Keycloak 26
> hides groups entirely from a service account without it — `GET /groups` returns `[]` with HTTP
> 200, `group-by-path` returns 403 — verified against the live realm. Under the original six-role
> set the reconcile script therefore saw every group as MISSING, applied the realm settings, and
> died on a 409 creating a group that existed all along: a half-reconciled realm on every run.
> `view-users` is read-only; the write roles stay forbidden, so the un-approve-everyone failure
> mode remains impossible. `view-events`/`manage-events` are needed because the event-auditing
> config (PGL-062) lives on its own endpoint with its own permission.
>
> `view-users` buys READS only. Group **writes** — creating a group, adding or removing a group's
> role mapping — are gated behind `manage-users` (verified live 2026-08-22: `POST /groups` and the
> role-mapping POST both return 403 with the full nine-role set), and fine-grained admin permissions
> are a disabled PREVIEW feature on Keycloak 26.0.8. So the reconciler *detects* group drift and
> *blocks the deploy before writing anything*; the correction itself is always a console action.

Verify the negative — the token itself is the authorization, so read the claim rather than probing
endpoints (a probe that "proves" writability would BE a write):

```bash
# Expect the nine roles above PLUS query-clients and query-users — the token carries the EFFECTIVE
# set, and view-clients/view-users are composites that expand to those two (Keycloak 26.0.8).
# manage-users / impersonation / realm-admin must not appear.
TOKEN=$(curl -fsS -X POST http://192.168.100.252:8189/realms/bleedingoptions/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id=realm-reconciler -d client_secret="$KC_ADMIN_SECRET" \
  | jq -r .access_token)
cut -d. -f2 <<<"$TOKEN" | tr '_-' '/+' | { p=$(cat); while [ $(( ${#p} % 4 )) -ne 0 ]; do p="$p="; done; \
  printf '%s' "$p"; } | base64 -d | jq -r '.resource_access["realm-management"].roles | sort[]'
```

The reconcile script asserts the same thing in its preflight on every run and refuses to write
anything if the set is not exact, so a drifted grant fails the deploy rather than riding along.

Store its secret alongside the others and pass it as `KC_ADMIN_SECRET`.

---

### 3a. If the reconciler cannot build the browser flow

Every other admin call the reconciler makes uses a stable, obvious endpoint. The one exception is
creating the OTP subflow: Keycloak's `executions/flow` endpoint takes a `provider` field whose
accepted value for a plain basic-flow subflow is a long-standing quirk of the admin API, and it could
not be verified without a running server. **If `--apply` fails there on the first run**, build the
flow once by hand — it is a five-minute job and then the reconciler only ever verifies it:

```text
Authentication > Flows > Create flow
  Name: bleedingoptions-browser        Type: Basic flow
Add step      : Cookie                          -> Alternative
Add sub-flow  : bleedingoptions-browser-forms   -> Alternative
  inside it, Add step: Username Password Form   -> Required
  inside it, Add step: OTP Form                 -> Required
Then: Action > Bind flow > Browser flow
```

Then re-run `--check`: it verifies the binding independently of how the flow was created, so it will
tell you whether OTP is genuinely enforced either way.

**Why REQUIRED and not the stock Conditional OTP:** the built-in flow makes OTP conditional on the
user having configured an authenticator, so an account with no authenticator signs in with a password
alone. `CONFIGURE_TOTP` as a default required action covers new users, but not an account whose OTP
credential was later deleted — §6 does exactly that for a lost phone.

### 3b. SMTP is NOT configured by hand — the reconciler owns it

`bleedingoptions.com` is a Workspace **user alias domain** (added 2026-08-15: Admin → Domains →
Manage domains → Add a domain → *User alias domain*, chosen over a secondary domain because an alias
is free and reuses existing users while a secondary bills per user). Google added the verification
TXT through its Cloudflare integration, Gmail activated free, and the MX (`@` → `smtp.google.com`,
priority 1, proxy OFF) was added by hand.

**Do not type the SMTP settings into the admin console.** `bleedingoptions-realm-reconcile.sh --apply`
writes them from the manifest, taking the password from `KC_SMTP_PASSWORD`, and it is authoritative —
anything typed by hand is either overwritten on the next run or becomes drift the run then reports.
The settings it applies:

| Field | Value | Why |
|---|---|---|
| host / port | `smtp.gmail.com` / `587`, StartTLS | Standard Gmail submission |
| auth username | **`info@amskel.nl`** | Gmail authenticates as the ACCOUNT. The app password belongs to it |
| from / replyTo | **`info@bleedingoptions.com`** | The alias is what recipients see — the point of having one |

⚠️ The username and the From address are DIFFERENT on purpose. Authenticating as the alias fails SMTP
auth outright: no mail at all, discovered when the first person registers.

⚠️ A user alias domain mirrors EXISTING users — `info@amskel.nl` becomes `info@bleedingoptions.com`.
An address whose local part has no matching user (`admin@`, say) does not exist unless
`admin@amskel.nl` does. Set `from` to an alias that resolves, or Gmail rejects the send.

## 4. Reconcile the realm

```bash
export KC_ADMIN_SECRET='<realm-reconciler secret>'
export KC_SMTP_PASSWORD='<gmail app password>'

scripts/ops/bleedingoptions-realm-reconcile.sh --check    # report drift, change nothing
scripts/ops/bleedingoptions-realm-reconcile.sh --apply    # correct it
```

`--check` exits non-zero on any drift so a pipeline fails on it rather than logging it (PGL-031B).
For the realm settings, roles, flows, required actions, events and the client it is **authoritative,
not add-only**: drift is corrected, extras are removed. **Group drift is the one exception —
detect-and-block**: Keycloak 26 gates every group write (create, mapping add, mapping remove) behind
`manage-users`, which the reconciler must never hold, so an unexpected role on `/gamma-lab-approved`
cannot be removed by the script. Instead `--apply` refuses **before performing any write**, listing
the exact console actions; the grant is live for every member of that group until an operator
removes it, which is why the run fails loudly rather than logging.

Concurrency is handled by the Jenkins job's `disableConcurrentBuilds()`, so the script takes no lock
and needs no Kubernetes API access.

---

## 5. Approving a user — the day-to-day operation

Anyone may register. A new account lands in `/pending-approval`, which maps **no roles**, so they can
sign in and see the page but are served **no market data at all** — the board endpoints answer
`403 NOT_APPROVED` and the page says the account is awaiting approval.

**To see who is waiting:** admin console → Groups → `pending-approval` → Members.

**To approve:**

```text
Users > <the person> > Groups > Join `gamma-lab-approved`
                              > Leave `pending-approval`
```

That grants the `gamma-lab` realm role, which is what the board endpoints require. Data appears on
their next token refresh — within 300 seconds, no deploy, no restart.

**To revoke:** remove them from `gamma-lab-approved`. Access ends when their current access token
expires — **up to 300 seconds later, not instantly** (PGL-026). Their existing token keeps working
until then; that is the accepted contract, not a bug. Also end their sessions
(Users → Sessions → Sign out) so the refresh path cannot mint a new token.

---

## 6. Lost phone / lost authenticator (PGL-028B)

Login requires a TOTP code at every sign-in, so a lost or wiped phone locks the person out
permanently unless you act. **Assume every one of these reaches you**, and see the note below.

**Operator reset — the reliable path, and today the only one.** Users → *the person* → Credentials →
delete the **otp** credential. Their next login fires `CONFIGURE_TOTP` and they re-enrol with a fresh
QR code.

> **Recovery codes are NOT currently in play, despite what the design's PGL-028B assumes.** Enabling
> `CONFIGURE_TOTP` does not by itself generate recovery codes — Keycloak's Recovery Authentication
> Codes are a separate required action and flow step that this realm does not configure, and the
> browser flow built in §3a has no recovery-code execution. So every lost phone currently lands on
> you. If self-service recovery matters before launch, that is a distinct piece of work: enable the
> recovery-codes required action, add its execution to the browser flow as an alternative to the OTP
> form, and verify a real user can actually use one.

> **Confirm identity out of band before resetting anyone's second factor.** "I lost my phone" is also exactly how an attacker
> asks you to remove someone's second factor. Use a channel you already trust for that person — not
> the email address on the account, which is what an attacker with mailbox access would be using.

---

## 7. Verify the NetworkPolicies are actually enforced (PGL-035)

**Do not assume they work.** k3s ships flannel, which does not enforce NetworkPolicy unless the
embedded kube-router controller is running. An unenforced policy is a comment, and citing it as a
control would be worse than having none.

```bash
# Should SUCCEED — Keycloak may reach its own Postgres.
kubectl -n bleedingoptions exec deploy/bo-keycloak -- \
  sh -c 'timeout 5 bash -c "</dev/tcp/bo-keycloak-postgres-db/5432" && echo ALLOWED || echo BLOCKED'

# Should be BLOCKED — Keycloak has no business reaching the internal broker.
kubectl -n bleedingoptions exec deploy/bo-keycloak -- \
  sh -c 'timeout 5 bash -c "</dev/tcp/192.168.100.252/9092" && echo ALLOWED || echo BLOCKED'
```

If the second prints `ALLOWED`, policies are **not** being enforced. Fix that before the public tier
exists, and until then treat the namespace as having no network boundary.

---

## 8. Backups (PGL-059)

The PVC protects against pod loss, not node loss and not deletion. `local-path` is node-affine.

```bash
kubectl -n bleedingoptions exec bo-keycloak-postgres-0 -- \
  pg_dump -U keycloak keycloak | gzip > "bo-keycloak-$(date +%F).sql.gz"
```

Daily 02:00 ET, encrypted, 30-day retention, stored off the node holding the PVC, RPO 24 h / RTO 4 h.
A logical `pg_dump`, not a volume copy of a running database — a torn copy restores to nothing.
**Rehearse a restore before public launch**, and annually after; an untested backup is not a backup.

---

## 9. What is deliberately absent in Gate 4a

| Not here | Where it lives |
|---|---|
| Public DNS + tunnel route | PGL-032 / PGL-036, with the near-identical-hostname guard PGL-036A |
| The public web Deployment | PGL-072 — forbidden above zero replicas until PGL-050/051/052 pass on the deployed digest |
| Pinned `clusterIP` for `bo-keycloak` | Pinned in the same change as the tunnel rule that targets it; see the comment in `keycloak-deployment.yaml` |
| SPF / DKIM / DMARC on the zone | PGL-024A — signup and reset mail lands in spam until these exist |

## 10. Related

- `k8s/bleedingoptions/` — every manifest for this tenant
- `scripts/ops/bleedingoptions-realm-reconcile.sh` — the reconciler
- `docs/keycloak-prod.md` — the INTERNAL instance; do not confuse the two
- `docs/domain-migration-bleadingoptions.md` — why the internal domain is spelled differently
