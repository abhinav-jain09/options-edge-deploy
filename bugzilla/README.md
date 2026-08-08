# Internal Bugzilla — project / lifecycle configuration

Design: [`docs/bugzilla-project-lifecycle.md`](../docs/bugzilla-project-lifecycle.md).
Declared state: [`configuration/expected-state.json`](configuration/expected-state.json).

```
configuration/
  expected-state.json                    the SSOT: products, components, fields, statuses,
                                         workflow matrix, resolutions, enforcement policy
  setup-projects.pl                      idempotent provisioner (dry-run by default)
  verify-projects.py                     structural + behavioural verification
  backup.sh / restore-backup.sh          verified backup and guarded restore
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

Everything runs on `.252`. Set these once per session:

```bash
BZ=options-edge-bugzilla-web; ADMIN=<admin-login>; TRIAGE=<triage-login>
```

Steps that mutate anything: **4** (configuration) and **6** (files and closes smoke bugs). Steps 1,
2 and 7 are read-only; step 3 backs up; step 5 restarts the web tier.

Every step is chained with `&&`: if one fails, the next does not run — in particular Apache is not
restarted over a partially mutated database.

**1. Load and syntax checks** — the extension must parse and Bugzilla must still boot with it
mounted:

```bash
docker exec $BZ perl -c /var/www/html/extensions/IssueTypeWorkflow/Extension.pm
```

**2. Dry run — read the whole plan before continuing.** This is safe against the live site: it
writes nothing, and also cross-checks that the extension's policy constants match the JSON.

```bash
docker exec $BZ perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login "$ADMIN" --default-assignee "$TRIAGE"
```

**3. Stop Apache and back up.** The container's PID 1 is a sleep loop, so stopping Apache takes the
site offline without stopping the container — which matters, because `localconfig` lives inside it.

```bash
docker exec $BZ apachectl stop && configuration/backup.sh
```

`backup.sh` dumps the database, checks the dump's own `-- Dump completed` footer, records the schema
charset and a checksum, and copies `params.json`. The footer check is the point: a failed
`mariadb-dump` still produces a perfectly valid *empty* gzip that passes `gzip -t`. It also keeps the
root password out of process arguments by writing a mode-0600 client option file inside the database
container.

**If it does not print `BACKUP OK`, stop — restart Apache with `docker exec $BZ apachectl start` and
do not apply.**

**4. Apply.** The script refuses to run while Apache is answering on `localhost:80`; that check is
what guarantees enforcement is never observably half-installed.

```bash
docker exec $BZ perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login "$ADMIN" --default-assignee "$TRIAGE" --apply
```

**5. Restart the web tier** so no process keeps a pre-change field/status cache. Chained to step 4,
so a failed apply leaves the site down rather than serving a half-provisioned installation:

```bash
docker restart $BZ
```

**6. Verify.** Needs an admin API key (`requirelogin` is on, and the parameter assertions need
`tweakparams`). This step **files smoke-test bugs** prefixed `[SMOKE]` and closes them again;
run it in a change window. `--no-smoke` gives structural assertions only, but then the extension is
not proven to be enforcing.

```bash
read -rs BZ_API_KEY && export BZ_API_KEY && python3 configuration/verify-projects.py --state configuration/expected-state.json
```

It defaults to `http://localhost:8092` and **refuses** a non-loopback plain-HTTP URL unless you pass
`--allow-remote-http`, because the endpoint speaks plain HTTP and the key travels in a header. Run it
on `.252` itself and revoke the key afterwards.

`read -rs` keeps the key out of shell history and out of the command's argv. It is still an
environment variable of the verifier process, so root — and on a default `hidepid=0` system, any
process of the same user — can read it from `/proc`; treat it as short-lived, not secret-at-rest.

**7. Re-run the dry run and confirm it reports zero actions** — that is the idempotence proof:

```bash
docker exec $BZ perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login "$ADMIN"
```

**8. Only after step 6 passes**, retire `TestProduct`. This is opt-in and never happens during a
normal apply; the script aborts rather than removing a product that still holds bugs.

```bash
docker exec $BZ apachectl stop && docker exec $BZ perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login "$ADMIN" --apply --remove-decommissioned && docker restart $BZ
```

If that fails, Apache stays stopped on purpose. Fix the cause, then re-run the same command.

## Rollback

`Bugzilla::Field->create` issues `ALTER TABLE bugs`, and DDL implicitly commits on MariaDB — so a
partial run cannot be undone with a transaction. Rollback is a restore:

```bash
configuration/restore-backup.sh ~/bugzilla-pre-projects-<TS>.sql.gz
```

The script verifies the checksum, the gzip and the dump footer **before** dropping anything,
recreates the schema with the charset and collation recorded at backup time, checks the table count
after importing, restores `params.json`, and only then restarts Apache. If any step fails it leaves
Apache stopped and says so — a half-restored database never serves traffic.

Do not try to unpick a partial run with ad-hoc `DROP COLUMN`.

## Notes

- The provisioner only ever converges towards `expected-state.json`. In **preflight** — before any
  mutation, and in dry runs too — it aborts on anything undeclared: an extra status, resolution,
  field value, component, version or milestone. Declare it in the SSOT, or delete it. Deactivating
  is *not* enough: Bugzilla's REST serialisation of field values carries no active flag, so the
  verifier cannot tell a deactivated extra from a live one and would fail on it.
- The last thing a successful apply does is set the `issue_type_workflow_enforced` parameter. That
  marker is what makes a later removal of any part of the model **fail closed**: with it set, a
  missing field, issue type or status blocks every guarded change until the provisioner is re-run,
  instead of silently switching enforcement off.
- It also aborts if an existing item already violates the model (wrong-branch status, wrong
  resolution, mismatched or missing category, no issue type). The extension validates fields as they
  change, so it can never repair an item that was already inconsistent.
- Adding a third project is just another entry under `products`. A third *issue type* additionally
  needs a `cf_issue_type` value, its controlled category values, a prefixed set of open statuses,
  the workflow edges, the `enforcement` maps, and the matching constants in `Extension.pm` — the
  provisioner refuses to run if those two disagree.
