# Public Keycloak (`bo-keycloak`) — operator runbook

The identity provider for **bleedingoptions.com**, the public Gamma Lab. Realm `bleedingoptions`,
namespace `bleedingoptions`, entirely separate from the internal `oe-keycloak` in `options-edge`.

Design and requirement ids: `bleedingoptions-public-gamma-lab.md` in the `options-edge` repo.

> **Not public yet.** Gate 4a builds the instance only. There is no DNS record, no tunnel route and no
> public web workload. Keycloak answers on the LAN (`http://192.168.100.252:8189`) and in-cluster,
> nowhere else. Publishing is PGL-032/PGL-036; the web tier is PGL-072-gated.

---

## 1. Before the first deploy — the Secret

`bo-keycloak-secrets` is created **out of band and never enters git** (PGL-030). Three keys:

| Key | What it is |
|---|---|
| `POSTGRES_PASSWORD` | Read by BOTH Postgres and Keycloak's JDBC URL, so they cannot drift into a mismatch |
| `KC_BOOTSTRAP_ADMIN_PASSWORD` | Bootstrap admin only — disabled after §2 |
| `KC_SMTP_PASSWORD` | The Gmail app password for `info@bleedingoptions.com` |

```bash
kubectl create namespace bleedingoptions --dry-run=client -o yaml | kubectl apply -f -

kubectl -n bleedingoptions create secret generic bo-keycloak-secrets \
  --from-literal=POSTGRES_PASSWORD='<generate>' \
  --from-literal=KC_BOOTSTRAP_ADMIN_PASSWORD='<generate>' \
  --from-literal=KC_SMTP_PASSWORD='<the app password>'
```

Generate the two passwords rather than reusing one: `openssl rand -base64 30`.

> **The app password currently sits in the git-tracked `url.md`.** It has not been committed, but a
> credential in a tracked file is one `git add -A` from being in GitHub history permanently, where
> deleting the file does not remove it. Revoke it at
> [Google account security](https://myaccount.google.com/apppasswords), issue a fresh one, and put
> the new one only here.

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
```

**It must NOT hold `manage-users`, `view-users` or `impersonation`.** The blanket phrase
"realm-management" would include them, and the reconciler's whole contract is that it never touches
users — approval state lives in group membership, and a reconciler able to edit memberships could
un-approve everyone. Verify the negative:

```bash
# Expect 403. If this returns a user list, the service account is over-privileged — fix before use.
TOKEN=$(curl -fsS -X POST http://192.168.100.252:8189/realms/bleedingoptions/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id=realm-reconciler -d client_secret="$KC_ADMIN_SECRET" \
  | jq -r .access_token)
curl -s -o /dev/null -w '%{http_code}\n' \
  -H "Authorization: Bearer $TOKEN" \
  http://192.168.100.252:8189/admin/realms/bleedingoptions/users
```

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

`--check` exits non-zero on any drift so a pipeline fails on it rather than logging it (PGL-031B). It
is **authoritative, not add-only**: an unexpected role on `/gamma-lab-approved` is removed, not
merely reported, because reporting leaves the grant live for every member of that group.

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
