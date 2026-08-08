# Internal Bugzilla — project / lifecycle configuration

Design: [`docs/bugzilla-project-lifecycle.md`](../docs/bugzilla-project-lifecycle.md).
Declared state: [`configuration/expected-state.json`](configuration/expected-state.json).

```
configuration/
  expected-state.json                    the SSOT: products, components, fields, statuses,
                                         workflow matrix, resolutions
  setup-projects.pl                      idempotent provisioner (dry-run by default)
  verify-projects.py                     structural + behavioural verification
  extensions/IssueTypeWorkflow/          keeps the two lifecycles apart
```

The Bugzilla runtime checkout on `.252` (`/home/options-edge/data/bugzilla/runtime`) is **not**
version controlled — a pre-existing exception. Nothing here is copied into it: the extension and the
scripts are bind-mounted read-only from this repository, so what runs is what was reviewed.

## Compose mounts

Add to `/home/options-edge/deploy/bugzilla/docker-compose.yml`, service `bugzilla-web`:

```yaml
    volumes:
      - /home/options-edge/deploy/bugzilla/configuration/extensions/IssueTypeWorkflow:/var/www/html/extensions/IssueTypeWorkflow:ro
      - /home/options-edge/deploy/bugzilla/configuration/setup-projects.pl:/var/www/html/local/setup-projects.pl:ro
      - /home/options-edge/deploy/bugzilla/configuration/expected-state.json:/var/www/html/local/expected-state.json:ro
```

Mount the extension **directory**, never the whole `extensions/` tree — that would shadow the
extensions Bugzilla ships with.

## Runbook

Everything below runs on `.252`. Steps 3–5 are the only ones that mutate anything.

**1. Preflight (read-only)**

```bash
docker exec options-edge-bugzilla-web perl -c /var/www/html/extensions/IssueTypeWorkflow/Extension.pm
```

**2. Dry run — read the whole plan before continuing**

```bash
docker exec options-edge-bugzilla-web perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login <admin> --default-assignee <triage-login>
```

**3. Back up, with the web tier stopped so nothing writes underneath the dump**

```bash
docker stop options-edge-bugzilla-web && docker exec options-edge-bugzilla-db sh -c 'mariadb-dump --single-transaction --routines --triggers --events --hex-blob -u root -p"$MARIADB_ROOT_PASSWORD" bugs' | gzip > ~/bugzilla-pre-projects-$(date +%Y%m%dT%H%M%S).sql.gz
```

Then verify the dump before trusting it: `gzip -t <file>` and record its `sha256sum`. Also copy
`/home/options-edge/data/bugzilla/data/params.json`.

**4. Apply**

```bash
docker start options-edge-bugzilla-web && docker exec options-edge-bugzilla-web perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login <admin> --default-assignee <triage-login> --apply
```

**5. Restart the web tier** so no request keeps a pre-change field/status cache:

```bash
docker restart options-edge-bugzilla-web
```

**6. Verify** (needs an API key — this instance has `requirelogin` on):

```bash
BZ_API_KEY=<key> python3 configuration/verify-projects.py --base-url http://192.168.100.252:8092 --state configuration/expected-state.json
```

**7. Re-run the provisioner and confirm it reports zero actions** — that is the idempotence proof:

```bash
docker exec options-edge-bugzilla-web perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login <admin>
```

**8. Only after verification passes**, retire `TestProduct` by adding `--remove-decommissioned` to
the apply command. The script refuses to delete a product that still holds bugs and deactivates it
instead.

## Rollback

`Bugzilla::Field->create` issues `ALTER TABLE bugs`, and DDL implicitly commits on MariaDB — so a
partial run cannot be rolled back with a transaction. Rollback is: stop both containers, drop and
recreate the `bugs` database, import the dump from step 3, restore `params.json`, start MariaDB then
the web tier, and re-run step 6 with `--no-smoke`. Do not try to unpick it with ad-hoc `DROP COLUMN`.

## Notes

- The provisioner is safe to re-run at any time; it only ever converges towards
  `expected-state.json`.
- `verify-projects.py` files real smoke-test bugs (summary prefix `[SMOKE]`). Run it during a change
  window, or with `--no-smoke` for structural assertions only.
- Adding a third project is just another entry under `products` in `expected-state.json`. A third
  *issue type* additionally needs a `cf_issue_type` value, its controlled category values, a prefixed
  set of open statuses, and the matching maps in `Extension.pm`.
