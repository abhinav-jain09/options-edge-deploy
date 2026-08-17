# Rotating `APP_POSTGRES_PASSWORD` (bo-app-postgres)

## Why this is a runbook and not a job stage

An earlier draft of `Jenkinsfile.bleedingoptions-secrets` did this automatically: it detected that
the database rejected the credential and ran `ALTER ROLE` to make them agree. That was removed, and
the reasons are worth stating, because "automate it" is the obvious instinct and it is wrong here.

1. **`ALTER ROLE` invalidates every running pod immediately.** Secret-backed environment variables
   do not update inside a running container. The moment the role changes, the live web tier is
   authenticating with a password that no longer exists — and `WatchlistStore` degrades to an empty
   list rather than erroring, so the public site keeps serving and simply loses the feature. A
   secrets job has no rollout of its own, so it cannot close that window.
2. **The cleartext password ends up in an SQL statement.** `ALTER ROLE ... PASSWORD 'literal'`
   sends the password to the server in the statement text, and PostgreSQL can write it to the log
   under `log_statement = ddl | mod | all`. The documentation warns about exactly this.
3. **Rotation is rare and consequential.** The cost of a runbook is a few minutes, once. The cost of
   an automated live-credential change that fires on a misconfiguration is an outage on a public
   site.

So the job now **refuses to write a Secret the database will reject**, and stops. It fails before
the write, so a wrong credential costs nothing.

## The order that works

The only safe ordering changes the database first and the running pods last, with the Secret written
in between.

**1. Change the role, from a session that will not be logged.**

Statement logging is the leak in step 2 of the reasoning above, so turn it off for this session
before the `ALTER` and confirm what it was first.

```bash
kubectl -n bleedingoptions exec -it sts/bo-app-postgres -c postgres -- psql -U bleedingoptions -d bleedingoptions
```

```sql
SHOW log_statement;                      -- expect 'none'; if not, this session must set it
SET log_statement = 'none';
SET log_min_error_statement = 'panic';   -- so a failed ALTER does not log the statement either
\password bleedingoptions
```

`\password` is the point of doing this interactively: psql hashes the password **client-side** and
sends a SCRAM verifier, so the cleartext never reaches the server, never enters the statement text,
and cannot be logged. Type the new value at the two prompts. Do not paste it into an `ALTER ROLE`
statement — that is the thing being avoided.

**2. Store the same value in Jenkins**, in the `bo-app-postgres-password` credential.

**3. Write the Secret**, with the preflight deliberately overridden:

- Job: `bleedingoptions-secrets`
- `DEPLOY_DRY_RUN = false`
- `ALLOW_APP_DB_ROLE_MISMATCH = true`

The override is needed here and *only* here. The preflight connects from inside the database pod
and will now succeed anyway if step 1 was done correctly — the flag exists for the case where it
cannot confirm, not as a routine setting. If you find yourself setting it on a normal run,
something is wrong and the flag is not the answer.

**4. Roll the web tier** so the new value is actually picked up:

```bash
kubectl -n bleedingoptions rollout restart deploy/bleedingoptions-gamma-lab
kubectl -n bleedingoptions rollout status  deploy/bleedingoptions-gamma-lab --timeout=180s
```

The deployment uses `Recreate`, so expect a gap of tens of seconds on a page that polls every 15 s.

**5. Verify the feature, not just the pod.** A green rollout only proves the container started;
the watchlist degrades quietly, so it will look fine either way. Sign in at
<https://bleedingoptions.com/gamma-lab>, star a symbol, reload, and confirm it is still there.

## What "wrong" looks like

If the role and the Secret disagree, nothing crashes. The board renders, the page works, and the
watchlist is simply always empty — for everyone, permanently. That is the whole reason the preflight
exists, and the reason step 5 checks the feature rather than the rollout.

## Related

- `Jenkinsfile.bleedingoptions-secrets` — the preflight and the `ALLOW_APP_DB_ROLE_MISMATCH` flag
- `k8s/bleedingoptions/app-postgres.yaml` — where `POSTGRES_PASSWORD` is consumed, at initdb only
- `docs/bleedingoptions-keycloak.md` — the equivalent concern for the *Keycloak* database, which
  fails loudly instead (Keycloak will not boot), and so is a different problem with a different fix
