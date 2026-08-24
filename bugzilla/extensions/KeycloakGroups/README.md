# KeycloakGroups

Makes **Keycloak the single place where Bugzilla access is granted**. Tick a user into
`/optionsedge/internal` in Keycloak; at their next login Bugzilla puts them in the
`optionsedge-internal` group. Untick it; at their next login they are removed.

## Why it has to exist

Bugzilla's `Env` login reads exactly three things from the environment:

```
auth_env_id  auth_env_email  auth_env_realname
```

There is **no `auth_env_group`** — group membership cannot arrive that way. That is a limit of the
product, not a setting someone forgot. This extension bridges the gap using the `groups` claim that
mod_auth_openidc already places in the environment (`OIDC_CLAIM_groups`).

## Why it hooks the *Verify* class, not the Login class

`Bugzilla::Auth::Login::Env` declares `requires_verification => 0`, so `Bugzilla::Auth::login()`
takes the else-branch and calls `create_or_update_user()` on the **verifier**. That is the first
point at which the `Bugzilla::User` object exists — created or found — and group membership has to
be applied to a user that exists. The Login class never sees one.

The `auth_verify_methods` hook also forbids *adding* keys to the module map, only overriding
existing ones. `user_verify_class` is `DB`, so `DB` is what is overridden. Two consequences worth
knowing: no Bugzilla parameter has to change, and removing this extension restores stock behaviour
with no other edit.

## Name mapping

```
/optionsedge/internal  ->  optionsedge-internal
/fullfunding/external  ->  fullfunding-external
```

Leading `/` dropped, remaining `/` become `-`. This is why the Keycloak mapper must be configured
with `full.path=true`: the bare child names would be `internal`/`external` for every application
and would collide.

## The safety boundary

The extension may only touch groups matching:

```
^[A-Za-z0-9_]+-(?:internal|external)$
```

Bugzilla's privilege groups — `admin`, `editbugs`, `tweakparams`, `creategroups` — cannot match.
So a missing, empty or malformed claim can never strip administrative rights; the worst case is
losing access to bug products, which is repaired by fixing the claim and logging in again.

Two further deliberate choices:

- A claim naming a group that does not exist in Bugzilla is **ignored**, not auto-created. Creating
  an access group is an administrative act and must not be triggered by a token.
- A sync failure never blocks a login: the user still gets in with the groups they already had, and
  the reason is written to the Apache error log.

## Verified behaviour

Exercised against a disposable Bugzilla user that also held `editbugs`:

| `OIDC_CLAIM_groups` | resulting groups |
|---|---|
| `/optionsedge/internal` | editbugs, optionsedge-internal |
| `/optionsedge/internal,/fullfunding/external` | editbugs, fullfunding-external, optionsedge-internal |
| `/fullfunding/external` | editbugs, fullfunding-external |
| *(empty)* | editbugs |
| `/nonexistent/group` | editbugs |
| `/optionsedge/internal, /optionsedge/external` | editbugs, optionsedge-external, optionsedge-internal |

`editbugs` survives every case, including the empty claim. Revocation works, unknown groups are
ignored, and whitespace after the delimiter is handled.

## Limitation to be aware of

Sync happens **at login**. Removing someone from a Keycloak group does not end a session that is
already established — that runs until it expires (idle 30 min, hard cap 8 h). To cut access
immediately: disable the user in Keycloak **and** restart `options-edge-bugzilla-web`, which
invalidates every mod_auth_openidc session.
