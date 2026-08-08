# Internal Bugzilla — project / lifecycle configuration

Design: [`docs/bugzilla-project-lifecycle.md`](../docs/bugzilla-project-lifecycle.md).
Declared state: [`configuration/expected-state.json`](configuration/expected-state.json).

```
configuration/
  expected-state.json                    the SSOT: products, components, fields, statuses,
                                         workflow matrix, resolutions, enforcement policy
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

Everything runs on `.252`. Set these once per session:

```bash
BZ=options-edge-bugzilla-web; ADMIN=<admin-login>; TRIAGE=<triage-login>
```

Steps that mutate anything: **4** (config) and **6** (files smoke bugs). Steps 1, 2 and 7 are
read-only; step 3 backs up; step 5 restarts.

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
docker exec $BZ apachectl stop
```

```bash
set -o pipefail; TS=$(date +%Y%m%dT%H%M%S); OUT=~/bugzilla-pre-projects-$TS.sql.gz; docker exec $BZ-db sh -c 'mariadb-dump --single-transaction --routines --triggers --events --hex-blob -u root -p"$MARIADB_ROOT_PASSWORD" "$BZ_DB_NAME"' | gzip > "$OUT" && zgrep -q -- "-- Dump completed" "$OUT" && sha256sum "$OUT" | tee "$OUT.sha256" && echo "BACKUP OK: $OUT"
```

`pipefail` plus the `-- Dump completed` footer check is the point: without both, a failed
`mariadb-dump` still produces a perfectly valid *empty* gzip that passes `gzip -t`. **If it does not
print `BACKUP OK`, stop and restart Apache (`docker exec $BZ apachectl start`) — do not apply.**

Also copy the params file (it is rewritten in step 4):

```bash
docker cp $BZ:/var/www/html/data/params.json ~/bugzilla-params-pre-projects-$TS.json
```

**4. Apply.** The script refuses to run while Apache is answering on `localhost:80`; that check is
what guarantees enforcement is never observably half-installed.

```bash
docker exec $BZ perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login "$ADMIN" --default-assignee "$TRIAGE" --apply
```

**5. Restart the web tier** so no process keeps a pre-change field/status cache:

```bash
docker restart $BZ
```

**6. Verify.** Needs an admin API key (`requirelogin` is on, and the parameter assertions need
`tweakparams`). This step **files smoke-test bugs** prefixed `[SMOKE]` and closes them again;
run it in a change window. `--no-smoke` gives structural assertions only, but then the extension is
not proven to be enforcing.

```bash
BZ_API_KEY=<key> python3 configuration/verify-projects.py --base-url http://192.168.100.252:8092 --state configuration/expected-state.json
```

The key is read from the environment only, never a flag, so it stays out of `ps` and shell history.
Note the endpoint is plain HTTP on the LAN — the key is exposed to anyone who can sniff that
segment. Prefer running the verifier **on `.252` itself** against `http://localhost:8092`, and
revoke the key afterwards.

**7. Re-run the dry run and confirm it reports zero actions** — that is the idempotence proof:

```bash
docker exec $BZ perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login "$ADMIN"
```

**8. Only after step 6 passes**, retire `TestProduct`. This is opt-in and never happens during a
normal apply; the script aborts rather than removing a product that still holds bugs.

```bash
docker exec $BZ apachectl stop && docker exec $BZ perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login "$ADMIN" --apply --remove-decommissioned; docker restart $BZ
```

## Rollback

`Bugzilla::Field->create` issues `ALTER TABLE bugs`, and DDL implicitly commits on MariaDB — so a
partial run cannot be undone with a transaction. Rollback is a restore:

```bash
docker exec $BZ apachectl stop
docker exec -i $BZ-db sh -c 'mysql -u root -p"$MARIADB_ROOT_PASSWORD" -e "DROP DATABASE \`$BZ_DB_NAME\`; CREATE DATABASE \`$BZ_DB_NAME\`;"'
zcat ~/bugzilla-pre-projects-<TS>.sql.gz | docker exec -i $BZ-db sh -c 'mysql -u root -p"$MARIADB_ROOT_PASSWORD" "$BZ_DB_NAME"'
docker cp ~/bugzilla-params-pre-projects-<TS>.json $BZ:/var/www/html/data/params.json
docker exec $BZ chown www-data:www-data /var/www/html/data/params.json
docker restart $BZ
```

Then check the *pre-change* shape, not the target shape — `verify-projects.py` asserts the new
model and is expected to fail against a restored database:

```bash
curl -s -H "X-BUGZILLA-API-KEY: $BZ_API_KEY" http://localhost:8092/rest/product_selectable
curl -s -H "X-BUGZILLA-API-KEY: $BZ_API_KEY" "http://localhost:8092/rest/field/bug" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sorted(v["name"] for v in next(f for f in d["fields"] if f["name"]=="bug_status")["values"]))'
```

That should show the stock statuses and no `cf_issue_type`. Do not try to unpick a partial run with
ad-hoc `DROP COLUMN`.

## Notes

- The provisioner only ever converges towards `expected-state.json`. It deliberately **aborts** on
  anything undeclared — an extra active status, resolution, field value, component, version or
  milestone — rather than silently tolerating it, so the script and the verifier can never disagree
  about what "converged" means. Add the new thing to the SSOT, or deactivate it.
- It also aborts if an existing item already violates the model (wrong-branch status, wrong
  resolution, mismatched or missing category, no issue type). The extension validates fields as they
  change, so it can never repair an item that was already inconsistent.
- Adding a third project is just another entry under `products`. A third *issue type* additionally
  needs a `cf_issue_type` value, its controlled category values, a prefixed set of open statuses,
  the workflow edges, the `enforcement` maps, and the matching constants in `Extension.pm` — the
  provisioner refuses to run if those two disagree.
