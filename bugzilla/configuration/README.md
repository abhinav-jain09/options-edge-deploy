# Internal Bugzilla — project / lifecycle configuration

Design: [`docs/bugzilla-project-lifecycle.md`](../../docs/bugzilla-project-lifecycle.md).
Declared state: [`expected-state.json`](expected-state.json).

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
version controlled — a pre-existing exception. Nothing here is copied into it: the extension, the
provisioner and the SSOT are bind-mounted read-only from this repository, so what runs is what was
reviewed. The verifier and the backup/restore scripts run from the host.

> **This installation is live.** It holds 177 bugs in `OptionsEdge` and four products created by
> separate work. No status is renamed, no field is made mandatory, nothing is migrated.
>
> One thing does reach the existing rows, and it is unavoidable: a Bugzilla custom select is
> `varchar(64) NOT NULL DEFAULT '---'`, so adding the Category and Environment fields writes that
> unset sentinel into all 177 rows. No NULL-able custom field exists in Bugzilla. The provisioner
> **proves** that is all that happened — every pre-existing column digested before and after, and
> each new column asserted to be exactly `---` on every pre-existing row.
>
> The issue type is **derived from the product** — `OptionsEdge`/`Fullfunding` are BUG,
> `* Requirements` are REQUIREMENT — so there is no per-bug type field to backfill.

## Compose mounts

Add to `/home/options-edge/deploy/bugzilla/docker-compose.yml`, service `bugzilla-web`:

```yaml
    volumes:
      - /home/options-edge/deploy/bugzilla/configuration/extensions/IssueTypeWorkflow:/var/www/html/extensions/IssueTypeWorkflow:ro
      - /home/options-edge/deploy/bugzilla/configuration/setup-projects.pl:/var/www/html/local/setup-projects.pl:ro
      - /home/options-edge/deploy/bugzilla/expected-state.json:/var/www/html/local/expected-state.json:ro
```

Mount the extension **directory**, never the whole `extensions/` tree — that would shadow the
extensions Bugzilla ships with.

## Runbook

Everything runs on `.252`. Set these once per session:

```bash
BZ=options-edge-bugzilla-web; ADMIN=<admin-login>
```

**0. Prerequisite for the FIRST apply: an unprivileged reporter account.** This installation has a
single profile today, and verification cannot prove the path external stakeholders actually use
without a second one — a user in neither `editbugs` nor `canconfirm`, who also cannot edit other
people's bugs through any product group grant. **Create that account yourself** (account creation is
not something this runbook automates), give it an API key, and only then run step 5. Until it
exists, the first apply is deliberately incomplete: `--strict` will fail rather than report success
over an unproven path.

Step **5** is the only one that changes anything: it applies the configuration and then files and
closes smoke bugs. Steps 1, 2, 4 and 6 are read-only; step 3 backs up. Step 5 is deliberately a
single `&&` chain, so a failure at any point stops the rest and leaves the site down.

**1. Load check** — this compiles the extension *and* loads it the way Bugzilla does, which is the
part that matters: a broken extension otherwise takes the whole site down at step 4.

```bash
docker exec $BZ perl -e 'use lib qw(/var/www/html /var/www/html/lib /var/www/html/local/lib/perl5); use Bugzilla; my @e = @{Bugzilla->extensions}; die "IssueTypeWorkflow not loaded\n" unless grep { $_->NAME eq "IssueTypeWorkflow" } @e; print "loaded: ", join(", ", map { $_->NAME } @e), "\n"'
```

**2. Dry run — read the whole plan before continuing.** This is safe against the live site: it
writes nothing, and also cross-checks that the extension's policy constants match the JSON.

```bash
docker exec $BZ perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login "$ADMIN"
```

**3. Stop Apache and back up.** The container's PID 1 is a sleep loop, so stopping Apache takes the
site offline without stopping the container — which matters, because `localconfig` lives inside it.

```bash
docker exec $BZ apachectl stop && ./backup.sh
```

`backup.sh` dumps the database, checks the dump's own `-- Dump completed` footer, records the schema
charset and a checksum, and copies `params.json`. The footer check is the point: a failed
`mariadb-dump` still produces a perfectly valid *empty* gzip that passes `gzip -t`. It also keeps the
root password out of process arguments by writing a mode-0600 client option file inside the database
container.

**If it does not print `BACKUP OK`, stop — restart Apache with `docker exec $BZ apachectl start` and
do not apply.**

**4. Get two API keys into the environment** — step 5 needs both. `BZ_API_KEY` must belong to an
admin (`requirelogin` is on, and the parameter assertions need `tweakparams`). `BZ_REPORTER_KEY`
must belong to a user WITHOUT `editbugs`/`canconfirm` — the verifier checks that before believing
it — because that is the only way to prove how an external stakeholder's filing actually behaves.

```bash
read -rs BZ_API_KEY && export BZ_API_KEY && read -rs BZ_REPORTER_KEY && export BZ_REPORTER_KEY
```

`read -rs` keeps the key out of shell history and out of any command's argv. It is still an
environment variable of the verifier process, so root — and on a default `hidepid=0` system, any
process of the same user — can read it from `/proc`; treat it as short-lived, not secret-at-rest.
Revoke it when you are done.

**5. Apply, restart, verify — one operation.** A failed apply never restarts; a failed verification
takes Apache down again by itself, rather than leaving traffic flowing through unproven enforcement.

The provisioner refuses to run while Apache is answering on `localhost:80`, which is what guarantees
enforcement is never observably half-installed. The restart clears any pre-change field/status
cache. The apply ends with a self-test that deletes the bug-creation rows inside a transaction,
checks that the extension goes fail-closed, and rolls back. Verification then **files smoke-test
bugs** prefixed `[SMOKE]` and closes them again — so run this in a change window.

```bash
docker exec $BZ perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login "$ADMIN" --apply \
  && docker restart $BZ \
  && { python3 ./verify-projects.py --state expected-state.json --reporter-api-key-env BZ_REPORTER_KEY --strict \
       || { echo "VERIFICATION FAILED - taking the site down again"; docker exec $BZ apachectl stop; false; }; }
```

`--strict` and the reporter key are **mandatory for the first apply**: without them the run reports
success while the path external stakeholders actually use — an unprivileged reporter filing a
REQUIREMENT — is only a warning.

The verifier defaults to `http://localhost:8092` and **refuses** a non-loopback plain-HTTP URL unless
you pass `--allow-remote-http`: the endpoint speaks plain HTTP and the key travels in a header. Run
it on `.252` itself. `--no-smoke` gives structural assertions only, but then the extension is not
proven to be enforcing.

Nothing is scheduled for decommissioning, so no product warning is expected.

**6. Re-run the dry run and confirm it reports zero actions.** That is the idempotence proof for the
configuration; a dry run reads only, so it is safe against the live site:

```bash
docker exec $BZ perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login "$ADMIN"
```

Pass the same `--default-assignee` as step 4: it defaults to `--admin-login`, so omitting it would
make the script plan a reassignment of every component and the run would not be a no-op.

**7. Nothing to decommission.** `decommission.products` is empty: `TestProduct` was renamed to
`OptionsEdge` by earlier work and holds the 177 live bugs. The command below is kept only for the day
something genuinely needs retiring; it aborts rather than removing a product that still holds bugs.

```bash
docker exec $BZ apachectl stop && docker exec $BZ perl /var/www/html/local/setup-projects.pl --state /var/www/html/local/expected-state.json --admin-login "$ADMIN" --apply --remove-decommissioned && docker restart $BZ
```

If that fails, Apache stays stopped on purpose. Fix the cause, then re-run the same command. Then
re-run step 5 with `--require-decommissioned` to assert it is really gone.

## Rollback

`Bugzilla::Field->create` issues `ALTER TABLE bugs`, and DDL implicitly commits on MariaDB — so a
partial run cannot be undone with a transaction. Rollback is a restore:

```bash
./restore-backup.sh ~/bugzilla-pre-projects-<TS>.sql.gz
```

The script verifies the checksums, the gzip, the dump footer, the charset sidecar and that the dump
really came from this database **before** touching anything; then recreates the schema with the
recorded charset and collation, diffs the restored tables against the manifest taken at backup time,
restores `params.json`, and only then restarts Apache. A failure *before* it stops Apache leaves the
instance exactly as it was; a failure *after* leaves Apache stopped and says so — either way a
half-restored database never serves traffic.

Do not try to unpick a partial run with ad-hoc `DROP COLUMN`.

## Notes

- The provisioner only ever converges towards `expected-state.json`. In **preflight** — before any
  mutation, and in dry runs too — it aborts on anything undeclared: an extra status, resolution,
  field value, component, version or milestone. Declare it in the SSOT, or delete it. Deactivating
  is *not* enough: Bugzilla's REST serialisation of field values carries no active flag, so the
  verifier cannot tell a deactivated extra from a live one and would fail on it.
- There is **no** "enforcement enabled" switch, deliberately: anyone with `tweakparams` could turn
  one off. The extension anchors on `cf_category` instead — if it exists, the declared model
  (the field's shape, all four products by **name and id**, the category vocabulary, the
  resolutions, the statuses with their open flags, and the full workflow matrix) must match, or
  every guarded change is refused until the provisioner is re-run. Deleting the field is not a way
  out: from a web or API request that is *broken*, not *bootstrap*. "Never provisioned" is judged from the
  database — no category field *and* no `REQ_*` status — so mounting the extension on a fresh
  installation is safe, while removing the field afterwards is not a way out.
- It also aborts if an existing item already violates the model — a status or resolution that does
  not belong to its product's type, or a category belonging to the other type. An **empty** category
  is always fine; that is exactly what leaves the 177 existing bugs editable. The extension validates
  fields as they change, so it can never repair an item that was already inconsistent.
- Adding a project means a new product plus its entry in `product_type` **and** `product_type_id`,
  mirrored in `Extension.pm`. A third *issue type* additionally needs its category values, a
  prefixed set of open statuses, the workflow edges and the `enforcement` maps — the provisioner
  refuses to run if the JSON and the extension's constants disagree.
