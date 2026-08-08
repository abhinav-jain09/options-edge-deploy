# Bugzilla customisations

The internal Bugzilla on `.252` is **built on the host**, from an upstream checkout at
`/home/options-edge/data/bugzilla/runtime` (branch 5.2, pinned commit `276673ab6`), with its
compose files under `/home/options-edge/deploy/bugzilla/`. That build path predates this repo and
is a known exception to the "everything deploys through Jenkins from main" rule — this directory
does **not** change that.

What this directory is for: the files we wrote ourselves live here under version control, so they
are reviewable and recoverable. They are still *installed* by copying them onto the host, and the
authoritative copy at runtime is the one on `.252`.

## Install path — both locations matter

| Location | Why |
|---|---|
| `/home/options-edge/data/bugzilla/runtime/extensions/<Name>/` | the **build context**; without this, the next image rebuild silently deletes the customisation |
| `/var/www/html/extensions/<Name>/` (inside `options-edge-bugzilla-web`) | the **running** container; needed until the next rebuild |

This two-place rule is not optional. The logout-link template override hit exactly this trap once
already: fixed in the container, lost on rebuild.

After installing, restart the container — under mod_perl, extensions are loaded once at startup.

## Contents

- `extensions/KeycloakGroups/` — makes Keycloak group membership the single source of truth for
  Bugzilla access. See the comments in `Extension.pm` for why it hooks the Verify class rather
  than the Login class.
