# Portal application state — versioned runbook (REQ-5d)

> **Hosting model:** REQ-5d is UNCHANGED under `fullfunding-namespace-gate1.md` §4, so the *state*
> this runbook describes is mechanism-independent. Reaching the admin listener is NOT: under the
> namespace model it is a `kubectl port-forward` to the portal pod's port 81, and the host-Compose
> `ssh -L` form is superseded. Commands below therefore name the listener, not the transport.

**Version: 1** — bump on every change, and record the version in each backup generation's manifest
(REQ-10a) so a restored database can be matched to the runbook that produced it.

The deployment artifact is reproducible; the resulting *service* is not, unless the params, product
and permission state are applied deterministically and their postconditions checked. That is what
this file is for. Every step below is applied through the **admin listener** (container port 81,
REQ-5a) — never through the public listener, which is OIDC-protected and never carries native login.

Open a forward to the portal's **admin listener (container port 81)** using whatever the deployed
mechanism provides, then browse it: native login, break-glass path R-7.

## 1. Parameters — applied automatically, NOT by hand

**Every parameter in the REQ-5d table is baked into `oidc/checksetup-answers.tmpl` and applied by
`checksetup.pl` on every container start.** `checksetup` is idempotent, so the container — not a
human following a checklist — is what makes the portal's parameter state deterministic, and it is
why a rebuild-from-empty rehearsal converges to the same state.

Do not set these through the admin UI: a UI change would drift from the template and be silently
reverted on the next boot. To change one, edit the template, open a PR, and redeploy.

The template documents each value's purpose inline. The load-bearing ones: `user_info_class=Env,CGI`,
`auth_env_id=OIDC_CLAIM_sub` (identity keyed on the immutable `sub`, never on mutable email),
`requirelogin=1`, `createemailregexp=''`, `maxlocalattachment=0` (attachments in the DB, so the
nightly `mysqldump` really covers them), `usevisibilitygroups=1`, `insidergroup=''`,
`letsubmitterchoosepriority=0`, `useqacontact=0`, `usetargetmilestone=0`.

Exact `auth_env_*` env-var spellings are confirmed against the deployed `mod_auth_openidc` version
by the deploy job's compatibility gate before the stack is ever exposed — the contract is fixed
here, the spelling is verified, never guessed.

## 2. Products and groups — the only manual steps

These are not expressible in a checksetup answers file, so they are applied once through the admin
listener and then asserted by the postcondition script in section 4.

1. **Delete** the default `TestProduct` (Administration → Products → TestProduct → Delete). Deleting,
   not disabling: a disabled product still exists as a surface the authorization matrix would have to
   govern, and the postcondition asserts exactly one product.
2. Create product **Requirements**, one component **General**, default assignee the admin account
   `BZ_ADMIN_EMAIL` (the account checksetup created, from the non-secret setting the deployment supplies).
3. Groups: external users hold **no** groups. Do not add anyone to `editbugs`, `canconfirm`,
   `creategroups`, `editcomponents`, `editclassifications`, `admin` or `bz_sudoers`. The sole admin
   is the checksetup-created account.
4. Define **no flag types**. Bugzilla lets any user who can see a bug set flags
   (`Bug.pm:4567-4570`), so the matrix's flag DENY row is only true while no flag type exists.

## 3. Authorization matrix

The binding table lives in the design document (REQ-5d), reconciled to the stock
`Bugzilla::Bug::check_can_change_field` behaviour (`Bug.pm:4514-4650` @ `276673ab6`) rather than to a
wished-for policy. Two consequences worth restating, because they surprise people:

- a non-`editbugs` **reporter may change most fields of their own ticket** — stock Bugzilla, and the
  matrix says ALLOW rather than pretending otherwise;
- an **attachment's submitter may edit/obsolete their own attachment even on someone else's
  ticket**, so that cell is ALLOW for both actors, while attachments submitted by others are DENY.

If a declared cell cannot be enforced with the parameters above, the fix is an approved design
revision — never a quiet edit of the expected value in the as-built record.

## 4. Postconditions — assert, do not eyeball

The postcondition CHECKS are fixed here; their transport is not, so the script that runs them ships
with the deployment mechanism (superseded for host-Compose; to be written against the namespace
model). Whatever runs them must **exit non-zero** on any mismatch rather than pretty-print for a
human to squint at, and must assert:

- every REQ-5d parameter equals its expected value (read from the live `params.json`, since
  `requirelogin=1` means an unauthenticated REST caller cannot enumerate them);
- exactly one product exists, named `Requirements`, with exactly one component `General`;
- zero flag types are defined;
- the admin account holds `admin` AND is the only member of any privileged group — asserted
  positively, because an empty query result means a missing admin, not a clean one.

Record the output in the tracking bug. A **rebuild-from-empty rehearsal** — fresh stack → this
runbook → postconditions exit 0 — is required once before launch (REQ-5d) and is what proves the
runbook is sufficient on its own.

## 5. What this runbook deliberately does NOT do

- It does not create external users. Those are provisioned in Keycloak realm `req` by Abhinav
  (REQ-1); their Bugzilla accounts auto-provision on first SSO login (REQ-5c).
- It does not touch the internal Bugzilla on port 8092 in any way (REQ-6).
