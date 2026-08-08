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
- `templates/en/custom/global/common-links.html.tmpl` — the logout link. Bugzilla hides its own
  under Env auth (`can_logout = 0`), so this points at mod_auth_openidc instead. The return URL is
  derived from the current request; the comment in the file explains why neither a hard-coded host
  nor a relative path works.

## Dropping the compiled-template cache

Template changes need `/var/www/html/data/template` cleared to take effect. **Do not `rm -rf` its
contents** — the directories get recreated as `root:root` by the next root-run process, and Apache
(`www-data`) can then no longer write there, which breaks every page with

```
cache failed to write index.html.tmpl ... Could not create temp file ... Permission denied
```

Clear it and restore the permissions its parent uses, or simply let Bugzilla fix them:

```bash
docker exec -u root options-edge-bugzilla-web sh -c \
  "chown -R root:www-data /var/www/html/data/template && \
   find /var/www/html/data/template -type d -exec chmod 770 {} \; && \
   cd /var/www/html && ./checksetup.pl --no-templates"
```
