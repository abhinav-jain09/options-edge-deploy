# Portal application state — versioned runbook (REQ-5d)

**Version: 1** — bump on every change, and record the version in each backup generation's manifest
(REQ-10a) so a restored database can be matched to the runbook that produced it.

The compose artifact is reproducible; the resulting *service* is not, unless the params, product and
permission state are applied deterministically and their postconditions checked. That is what this
file is for. Every step below is applied through the **admin listener** (`ssh -L` tunnel, REQ-5a) —
never through the public listener, which is OIDC-protected and never carries native login.

```bash
# open the admin tunnel from your workstation (nothing here is reachable from the LAN)
ssh -L 8095:127.0.0.1:8095 abhinav@192.168.100.252
# then browse http://127.0.0.1:8095/  (native login, break-glass path R-7)
```

## 1. Parameters

| Param | Value | Why it is load-bearing |
|---|---|---|
| `urlbase` | `https://req.fullfunding.nl/` | canonical public base for generated links |
| `user_info_class` | `Env,CGI` | env-SSO on the public listener; native CGI on the admin listener |
| `auth_env_id` | the `sub` claim env var (`OIDC_CLAIM_sub`) | identity key → Bugzilla `extern_id`; email is mutable and must never be the key (REQ-5c) |
| `auth_env_email` | `OIDC_CLAIM_email` | account login name |
| `auth_env_realname` | `OIDC_CLAIM_name` | display name |
| `createemailregexp` | *(empty)* | kills anonymous CGI self-signup. Verified from source: this param does **not** gate Env auto-provisioning — there is no reference to it anywhere under `Bugzilla/Auth/` @ `276673ab6` — so SSO users still auto-provision |
| `requirelogin` | on | no anonymous surface at all |
| `maxlocalattachment` | `0` | attachments stored **in the database**, which is what makes the nightly `mysqldump` actually cover them (REQ-10a / V-restore) |
| `maxattachmentsize` | `10240` (10 MB) | bottom of the REQ-11 size chain: Bugzilla 10 MB < Apache 25 MB < Cloudflare plan cap |
| `usevisibilitygroups` | on | external users cannot enumerate other users; with no shared visibility group, the user directory is closed |
| `insidergroup` | *(empty)* | private comments/attachments feature unused, so the matrix's DENY rows for it are structural |
| `useqacontact` | off | removes a field the authorization matrix would otherwise have to govern |
| `letsubmitterchoosepriority` | off | makes `priority` a DENY cell for reporters (`Bug.pm:4650` checks exactly this param) |

Exact env-var spellings for `auth_env_*` are confirmed against the deployed `mod_auth_openidc`
version at apply time and recorded in the design's as-built note — the *contract* is fixed, the
spelling is verified, never guessed.

## 2. Products and groups

1. Delete (or disable) the default `TestProduct`.
2. Create product **Requirements**, one component **General**, default assignee = Abhinav.
3. Groups: external users hold **no** groups — not `editbugs`, `canconfirm`, `creategroups`,
   `editcomponents`, `editclassifications`, `admin`, or `bz_sudoers`. The sole admin account is
   Abhinav's.
4. Define **no flag types** on this instance. Bugzilla lets any user who can see a bug set flags
   (`Bug.pm:4567-4570`), so the matrix's flag DENY row is only true if no flag types exist.

## 3. Authorization matrix

The binding table lives in the design document (REQ-5d) and is reconciled to the stock
`Bugzilla::Bug::check_can_change_field` behaviour (`Bug.pm:4514-4650` @ `276673ab6`), not to a
wished-for policy. Two consequences worth restating here, because they surprise people:

- a non-`editbugs` **reporter may change most fields of their own ticket** — that is stock Bugzilla,
  and the matrix says ALLOW rather than pretending otherwise;
- an **attachment's submitter may edit/obsolete their own attachment even on someone else's
  ticket**, so that cell is ALLOW for both actors, while attachments submitted by others are DENY.

If a declared cell cannot be enforced with the params above, the fix is an approved design revision
— never a quiet edit of the expected value in the as-built record.

## 4. Postconditions (run after applying, and after any restore)

```bash
# from the admin tunnel; expects the exact values in section 1
curl -s http://127.0.0.1:8095/rest/parameters | python3 -m json.tool | head -40
# products: exactly one, named Requirements
curl -s http://127.0.0.1:8095/rest/product_enterable
# group membership: external users must appear in no group
```

Record the output in the tracking bug. A `rebuild-from-empty rehearsal` — fresh stack → this runbook
→ postconditions green — is required once before launch (REQ-5d) and proves the runbook is
sufficient on its own.

## 5. What this runbook deliberately does NOT do

- It does not create external users. Those are provisioned in Keycloak realm `req` by Abhinav
  (REQ-1); their Bugzilla accounts auto-provision on first SSO login (REQ-5c).
- It does not touch the internal Bugzilla on port 8092 in any way (REQ-6).
