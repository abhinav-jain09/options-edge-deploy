# Prod Cloudflare tunnel — `options-edge-stable.yml`

This directory holds the reviewed source of truth for the Cloudflare tunnel that makes the trading
platform publicly reachable from the prod host `192.168.100.252`. Live path:
`/etc/cloudflared/options-edge-stable.yml`, systemd unit `options-edge-cloudflared-stable.service`.

| Hostname                   | Serves                                            |
| -------------------------- | ------------------------------------------------- |
| `bleadingoptions.com`      | SPX board — `/ws/events` → feed-gateway NodePort   |
| `auth.bleadingoptions.com` | Keycloak (`/admin` and `/realms/master` → 404)     |
| `es.bleadingoptions.com`   | ES board on es4 (`192.168.100.4`)                  |
| `req.bleadingoptions.com`  | Bugzilla (host container on `127.0.0.1:8092`)      |

Any hostname not listed above falls through to the terminal `http_status:404` rule. Unmatched *paths*
on a listed hostname do not — they hit that hostname's own catch-all rule.

Read the header comment in the YAML before touching the `/ws/events` line. It records the 2026-07-31
incident where the live copy pointed at the k3s ServiceLB instead of the gateway NodePort and the
board went dark for ~2h of market hours while every component tested healthy in isolation.

### About `fullfunding.nl` — two different retirements, one day apart

The YAML carries two notes that look contradictory. Both are true:

- **2026-08-08** — the *OptionsEdge* hostnames on that domain (`fullfunding.nl`, `es.`, `auth.`) were
  retired by migration Phase 3, with no redirect, so the domain could be reused. Never re-add them:
  they would hijack traffic from whatever now owns the domain.
- **2026-08-09** — the bare apex, by then serving the unrelated FullFunding jaarrekening app, was
  moved off this tunnel onto its **own** tunnel `5c86fa1d-8307-43d8-9ccf-aa240b7156e1`
  (`/etc/cloudflared/fullfunding.yml`), so that restarting the trading platform tunnel no longer
  interrupts it and vice versa.

Its origin was `http://127.0.0.1:80` — traefik, matched by `Host`, not a NodePort or ClusterIP that a
Service recreation would invalidate. That topology now lives only in the new tunnel's config, and
**`/etc/cloudflared/fullfunding.yml` is not tracked in this repo** — the same single-copy-on-one-host
problem this directory exists to solve. See `k8s/services/fullfunding-app/overlays/production/`.

## Nothing deploys this file

There is no Jenkins job and no automation. This repo copy is what gets reviewed; the live copy is what
routes traffic, and they stay in step only because someone copies one onto the other.

`scripts/ops/verify-prod-tunnel.sh` is the drift check — but know what it actually asserts. It
compares live against this copy **after `strip()`ing full-line comments, blank lines, inline comments
and trailing whitespace** (its `strip()` helper). That is *normalized-text*
equality, not YAML-semantic equality: it will report a match while the two files differ by many bytes
of comment, and because the `#` stripping is not YAML-aware it would also cut a `#` inside a quoted
scalar. It only catches drift when somebody runs it — nothing runs it on a schedule. If you want byte
identity, check it yourself:

```bash
ssh abhinav@192.168.100.252 'cat /etc/cloudflared/options-edge-stable.yml' \
  | cmp - infra/prod/cloudflared/options-edge-stable.yml && echo "byte-identical"
```

## Applying a change

**Preconditions.** Run every local step from the Step 1 worktree (`/tmp/tunnel-deploy`), not from your
day-to-day checkout — Step 5 executes the verifier out of that same tree. You need: `cloudflared` on
the workstation
(2026.6.0 matches the host), key-based SSH to `192.168.100.252` and `192.168.100.4`, an **interactive
TTY** for the prod host (`sudo -n` is not available there — it requires a password), and, for the
verifier, `kubectl` reach into both clusters plus `bash`, `curl`, `python3`/PyYAML and `timeout`.

**Step 1 — merge here first, then run from a clean `origin/main` worktree.** This file is reviewed;
the box is not. Merging the PR does not update your clone, and a stale clone will happily redeploy an
old route that still passes syntax validation. Sync **before** the Step 2 comparison — comparing a
stale clone against production is exactly how a required deploy gets skipped.

Sync the whole tree, not just the YAML: Step 5's verifier also reads `scripts/ops/verify-prod-tunnel.sh`
itself and `k8s/keycloak/keycloak-ingress.yaml` / `keycloak-realm-configmap.yaml`
(its `ING_FILE` and `REALM_CM` inputs). A stale or locally-edited copy of any of those can print
`VERIFIED` against stale expectations. A dedicated worktree pins all of them to the reviewed commit at
once:

```bash
set -euo pipefail
git fetch origin
git worktree add /tmp/tunnel-deploy origin/main    # all verifier inputs, at the reviewed commit
cd /tmp/tunnel-deploy
[ -z "$(git status --porcelain)" ] || { echo "ABORT: worktree not clean" >&2; exit 1; }
MERGE_SHA=<the merge commit from the PR>           # assert it, don't eyeball it
git merge-base --is-ancestor "$MERGE_SHA" HEAD \
  || { echo "ABORT: $MERGE_SHA is not in HEAD — this tree predates the reviewed merge" >&2; exit 1; }
```

Run every remaining local step from `/tmp/tunnel-deploy`. Remove it with
`git worktree remove /tmp/tunnel-deploy` when you are done.

**Step 2 — is a deploy even needed?** A merge that only reconciles the repo to what the box already
runs needs **no** install and **no** restart. With the tree now synced, compare. Handle the three
outcomes separately — an unreadable file or a broken `diff` must abort, never read as "different, so
deploy":

```bash
set -euo pipefail
WORK=$(mktemp -d)            # private workspace: fixed /tmp names can be pre-created or symlinked
ssh abhinav@192.168.100.252 'cat /etc/cloudflared/options-edge-stable.yml' > "$WORK/live.yml" \
  || { echo "ABORT: could not read the live config — this is not drift" >&2; exit 1; }
[ -s "$WORK/live.yml" ] || { echo "ABORT: live config read back empty" >&2; exit 1; }
diff -u "$WORK/live.yml" infra/prod/cloudflared/options-edge-stable.yml && rc=0 || rc=$?
case "$rc" in
  0) echo "IN STEP — do not install, do not restart. Skip Steps 3-4; go straight to Step 5." ;;
  1) echo "DRIFT — continue with Step 3." ;;
  *) echo "ABORT: diff failed (exit $rc) — this is not drift" >&2; exit 1 ;;
esac
LIVE_SUM=$(shasum -a 256 "$WORK/live.yml" | cut -d' ' -f1); echo "LIVE_SUM: $LIVE_SUM"
```

Note the `LIVE_SUM` — Step 4 re-checks it under the lock. Everything between this comparison and the
install is unlocked, so another operator can complete a newer deploy in that window; without the
recheck you would silently overwrite their work with your older pinned tree.

**Step 3 — validate locally, then stage to the host.** The staged filename carries the content hash,
so a second operator staging a different file cannot silently replace what you validated:

```bash
set -euo pipefail   # a failed check MUST stop the scp
# The worktree is not immutable — re-assert it is still the reviewed tree before shipping from it.
[ -z "$(git status --porcelain)" ] || { echo "ABORT: worktree modified since Step 1" >&2; exit 1; }
# Digest FIRST, then check, then confirm the bytes did not move underneath the checks — otherwise
# FULLSUM could end up certifying a file edited after the guard read it.
cp infra/prod/cloudflared/options-edge-stable.yml "$WORK/candidate.yml"
FULLSUM=$(shasum -a 256 "$WORK/candidate.yml" | cut -d' ' -f1); SUM=${FULLSUM:0:12}
cloudflared tunnel --config "$WORK/candidate.yml" ingress validate
python3 scripts/ops/assert-no-retired-hostname.py "$WORK/candidate.yml"   # structural guard, see below
[ "$(shasum -a 256 "$WORK/candidate.yml" | cut -d' ' -f1)" = "$FULLSUM" ] \
  || { echo "ABORT: candidate changed while it was being checked" >&2; exit 1; }
scp "$WORK/candidate.yml" "abhinav@192.168.100.252:~/options-edge-stable.yml.$SUM"
# copy these three assignments into the Step 4 shell verbatim:
echo "SUM=$SUM"; echo "FULLSUM=$FULLSUM"; echo "LIVE_SUM=$LIVE_SUM"
```

Steps 2 and 3 must run in **one** local shell — Step 3 reuses `LIVE_SUM` from Step 2 and does not
recompute it, so in a fresh shell `set -u` aborts at the final `echo`. If that happens, re-run Step 2
rather than inventing a value.

`assert-no-retired-hostname.py` is the guard that keeps `fullfunding.nl` off this tunnel, and it must
run **here**, locally: it needs PyYAML, which the workstation has and **the prod host does not**.
That is safe because Step 4 installs only bytes matching `FULLSUM` — the digest carries the guarantee
across. It refuses to be fooled the way a `grep` or a handful of URL probes can be: a rule like
`- {hostname: fullfunding.nl, path: /secret, service: http://evil:8080}` passes
`cloudflared ingress validate` and still answers 404 on `/`, so only real YAML parsing settles it. It
exits 0 clean, 1 on a violation, and **2 if it could not run at all** — treat that as a failure, never
as a pass.

> ⚠️ **`--config` must come before `ingress validate`.** It is a flag of the `tunnel` command, so both
> `cloudflared --config "$FILE" tunnel ingress validate` and
> `cloudflared tunnel --config "$FILE" ingress validate` work. Putting it **after** the subcommand —
> `cloudflared tunnel ingress validate --config "$FILE"` — prints
> `Incorrect Usage: flag provided but not defined: -config`, dumps the help text, and **exits 0**
> without validating anything. A `&&` chain or CI step reads that as success. The tell is the absence
> of the `Validating rules from <path>` / `OK` lines. (Confirmed on cloudflared 2026.6.0, 2026-08-09.)

**Step 4 — install on the host.** ⚠️ **This restarts the tunnel — read "There is no in-place reload"
below before you run it.** The shape — validate, route-preflight, back up, install atomically, restart
the exact unit, verify — comes from [`docs/keycloak-prod.md`](../../../docs/keycloak-prod.md), but
**this file is the current procedure for tunnel changes**; that one is a Keycloak-rollout narrative
whose tunnel snippet predates the lock, the digest checks and two of the preflight routes. Where they
differ, follow this file. Run it in an interactive **bash** shell on the prod host —
`ssh -t abhinav@192.168.100.252 bash -l`, since the block relies on `pipefail`, `[[ … =~ … ]]` and the
`EXIT` trap, and `ssh -t` alone just starts whatever login shell the account has. These commands do not
run over a pasted local block, and `sudo` will prompt. Copy in the three assignments printed by Step 3 (`SUM`, `FULLSUM`, `LIVE_SUM`). The whole
sequence is held under one `flock`, so a second operator cannot install between your preflight and
your restart:

```bash
set -euo pipefail   # any failed check below MUST stop the install
SUM=…; FULLSUM=…; LIVE_SUM=…        # the three values Step 3 printed
STAGED=~/options-edge-stable.yml.$SUM
CAND=/etc/cloudflared/options-edge-stable.yml.tmp
[ -s "$STAGED" ] || { echo "ABORT: staged file $STAGED missing or empty" >&2; exit 1; }
exec 9>~/.oe-tunnel-deploy.lock
flock -n 9 || { echo "ABORT: another tunnel deploy holds the lock" >&2; exit 1; }
trap 'exec 9>&-' EXIT      # releases only if the shell exits early; see the explicit close below
# the filename is a label, not proof — verify the staged bytes are what you validated locally.
# ingress checks alone would not catch a swapped `tunnel:` or `credentials-file:` line.
echo "$FULLSUM  $STAGED" | sha256sum -c - \
  || { echo "ABORT: staged file does not match the Step 3 digest" >&2; exit 1; }
# SEAL the verified bytes into a root-owned file straight away. $STAGED stays user-writable, so
# validating it and then copying it later would leave a window to swap it after the checks pass.
# Everything below validates and installs THIS file, never $STAGED again.
sudo install -m 0644 -o root -g root "$STAGED" "$CAND"
echo "$FULLSUM  $CAND" | sha256sum -c - \
  || { echo "ABORT: sealed candidate does not match the Step 3 digest" >&2; exit 1; }
# and confirm nobody deployed while you were staging
NOW_SUM=$(sha256sum /etc/cloudflared/options-edge-stable.yml | cut -d' ' -f1)
[ "$NOW_SUM" = "$LIVE_SUM" ] \
  || { echo "ABORT: live config changed since Step 2 — re-run from Step 1" >&2; exit 1; }
cloudflared tunnel --config "$CAND" ingress validate
# the retirement guard already ran locally in Step 3, on the exact bytes $FULLSUM pins.
# authoritative preflight: syntax validity is not routing correctness — the 2026-07-31 outage config
# was perfectly valid. Make cloudflared resolve EVERY public route against the SEALED candidate.
while read -r u expect; do
  got="$(cloudflared tunnel --config "$CAND" ingress rule "$u" \
         | sed -n 's/^[[:space:]]*service:[[:space:]]*//p' | head -1)"
  [ "$got" = "$expect" ] \
    || { echo "PREFLIGHT FAIL: $u resolved to '$got', expected '$expect'" >&2; exit 1; }
done <<'ROUTES'
https://bleadingoptions.com/ws/events http://192.168.100.252:30097
https://bleadingoptions.com/ http://192.168.100.252:8094
https://auth.bleadingoptions.com/admin http_status:404
https://auth.bleadingoptions.com/realms/master http_status:404
https://auth.bleadingoptions.com/realms/optionsedge http://10.43.127.26:8080
https://es.bleadingoptions.com/ws/events http://192.168.100.4:30091
https://es.bleadingoptions.com/ http://192.168.100.4:30080
https://req.bleadingoptions.com/ http://127.0.0.1:8092
https://fullfunding.nl/ http_status:404
ROUTES
BAK=/etc/cloudflared/options-edge-stable.yml.bak-$(date +%Y%m%d-%H%M%S); echo "BACKUP: $BAK"
# Never clobber an existing backup: a reused second, a clock rollback or a rerun would destroy the
# only copy of a live-only state. `ln` is atomic and fails with EEXIST — a test-then-cp would not.
# The link shares the live inode, which is exactly right here because the install below replaces the
# NAME with `mv`; the old inode lives on as $BAK. (For that reason, never edit the live file in
# place — an in-place write would rewrite every backup hard-linked to it.)
sudo ln /etc/cloudflared/options-edge-stable.yml "$BAK" \
  || { echo "ABORT: $BAK already exists or could not be linked" >&2; exit 1; }
echo "$LIVE_SUM  $BAK" | sha256sum -c - \
  || { echo "ABORT: backup does not match the live file it should have captured" >&2; exit 1; }
sudo mv "$CAND" /etc/cloudflared/options-edge-stable.yml   # atomic; installs the SEALED bytes
SINCE=$(date '+%Y-%m-%d %H:%M:%S')   # bound the journal read to THIS restart
sudo systemctl restart options-edge-cloudflared-stable.service   # the unit name — NOT bare "cloudflared"
systemctl is-active options-edge-cloudflared-stable.service      # exits non-zero unless active
INV=$(systemctl show -p InvocationID --value options-edge-cloudflared-stable.service)
[[ "$INV" =~ ^[0-9a-f]{32}$ ]] \
  || { echo "ABORT: could not read a valid InvocationID — cannot judge stability" >&2; exit 1; }
sleep 15
systemctl is-active options-edge-cloudflared-stable.service
# is-active alone cannot see a crash-loop: systemd restarts the unit and it reports active again.
# The InvocationID changes on every start, so an unchanged one means the SAME invocation survived
# the window. That is continuity over 15s, not a health proof — a process can sit up and disconnected,
# and a slower loop can outlast the window. The public probes at the end of Step 5 are what actually
# prove the tunnel serves; this just catches the fast, obvious failure before you get there.
[ "$(systemctl show -p InvocationID --value options-edge-cloudflared-stable.service)" = "$INV" ] \
  || { echo "ABORT: a new service invocation started within 15s — it is not staying up; roll back" >&2; exit 1; }
echo "$FULLSUM  /etc/cloudflared/options-edge-stable.yml" | sha256sum -c -   # fail-closed, not eyeballed
# Read the log yourself. NOTE the sudo: this account is not in adm/systemd-journal, so a bare
# `journalctl -u …` prints nothing and exits 1 — it would look quiet no matter what happened.
sudo journalctl -u options-edge-cloudflared-stable.service --since "$SINCE" --no-pager
exec 9>&-; trap - EXIT   # release the deploy lock
```

The `trap` covers an early abort. On a successful run it fires only when the interactive shell itself
exits — far too late — which is why the block ends with an explicit `exec 9>&-`. If you ran the lines
by hand, run that release by hand too.

**On concurrency.** The `flock` covers the install itself — validate through restart — which is the
window where two simultaneous runs would actually corrupt each other. It deliberately does *not* span
Step 5: holding it across a separate workstation step would make the deployment hostage to one SSH
session and would block the rollback a failed Step 5 needs.

Be precise about what the in-lock `LIVE_SUM` check does cover: changes between *your own* Step 2 and
Step 4. It cannot see a deployment that starts after your Step 4 — that one passes its own CAS
legitimately and can land while your Step 5 is mid-run, so the verifier could sample two configs and
still print `VERIFIED`. **Run one deployment lifecycle at a time: do not begin another Step 1 until
the previous Step 5 and its board checks are done.** Step 5 also re-reads the live digest at the end
as a cheap backstop. This is a convention plus a backstop, not a fencing protocol; if this ever
becomes a multi-operator system that gap needs closing properly.

### Rollback — check first whether it is even legal

⚠️ **A backup is illegal to restore if it declares a retired hostname, or resolves one to anything
other than `http_status:404` — whatever its timestamp.** `fullfunding.nl` is now served by its **own**
tunnel, so restoring such a file would make this tunnel start answering for a hostname it must not
own. The verifier enforces the same rule: `--phase retired` fails any config declaring a retired
hostname (`scan_config_for_retired()`), and `--phase rollback` does not apply either — it expects the
full Phase-2 state, and we are past the DNS handoff.

**For the change that introduced this README, rollback is therefore fix-forward only:** its backup
`…bak-20260809-132945-ff-route-removed` declares `fullfunding.nl` and resolves it to
`http://127.0.0.1:80`. There is no valid "restore the old file" here — open a follow-up PR that
corrects the config and deploy it through Steps 1-5.

**Judge by content, never by filename.** The date settles nothing in either direction: the split
happened *during* 2026-08-09, and of the backups on the box today, `…bak-20260808-165234` and
`…bak-20260809-110805` declare no retired hostname and resolve them all to `http_status:404` — legal
on content despite older-looking names — while several others resolve `fullfunding.nl` to a live
backend. The block below asks cloudflared and the file structure instead of guessing.

For a later change whose backup is post-split, a rollback gets **the same treatment as an install** —
same lock, same seal, same preflight, same compare-and-swap. It is not a lesser operation: a backup
that was valid when taken can carry an origin that has since moved, which is precisely the 2026-07-31
outage class.

First, on the **workstation** — the guard needs PyYAML and the prod host does not have it. Note the
live digest too, so the restore can refuse to clobber a deploy that landed meanwhile:

```bash
set -euo pipefail                     # local, from /tmp/tunnel-deploy
WORK=$(mktemp -d)
BAK=<the BACKUP: path printed during Step 4>
scp "abhinav@192.168.100.252:$BAK" "$WORK/rollback.yml"
BAKSUM=$(shasum -a 256 "$WORK/rollback.yml" | cut -d' ' -f1)
cloudflared tunnel --config "$WORK/rollback.yml" ingress validate
python3 scripts/ops/assert-no-retired-hostname.py "$WORK/rollback.yml"   # exit 2 = failure, not a pass
[ "$(shasum -a 256 "$WORK/rollback.yml" | cut -d' ' -f1)" = "$BAKSUM" ] \
  || { echo "ABORT: candidate changed while it was being checked" >&2; exit 1; }
# Roll back only YOUR OWN deployment: LIVE_SUM is the FULLSUM your Step 4 installed, not "whatever
# is live now". If they differ, something changed underneath you — stop and find out what.
LIVE_SUM=<the FULLSUM your Step 4 installed>
NOW=$(ssh abhinav@192.168.100.252 'sha256sum /etc/cloudflared/options-edge-stable.yml' | cut -d' ' -f1)
[ "$NOW" = "$LIVE_SUM" ] \
  || { echo "ABORT: live is $NOW, not your $LIVE_SUM — someone else deployed; do NOT roll back" >&2; exit 1; }
echo "BAKSUM=$BAKSUM"; echo "LIVE_SUM=$LIVE_SUM"; echo "keep $WORK/rollback.yml for Step 5"
```

Then restore, on the host, in an interactive **bash** shell:

```bash
set -euo pipefail
BAK=<same path>; BAKSUM=<printed above>; LIVE_SUM=<printed above>
CAND=/etc/cloudflared/options-edge-stable.yml.tmp
[ -s "$BAK" ] || { echo "ABORT: backup $BAK missing or empty" >&2; exit 1; }
exec 9>~/.oe-tunnel-deploy.lock
flock -n 9 || { echo "ABORT: another tunnel deploy or rollback holds the lock" >&2; exit 1; }
trap 'exec 9>&-' EXIT      # releases only if the shell exits early
echo "$BAKSUM  $BAK" | sha256sum -c - \
  || { echo "ABORT: backup changed since it was checked" >&2; exit 1; }
[ "$(sha256sum /etc/cloudflared/options-edge-stable.yml | cut -d' ' -f1)" = "$LIVE_SUM" ] \
  || { echo "ABORT: a deploy landed while you were checking — re-assess before rolling back" >&2; exit 1; }
sudo install -m 0644 -o root -g root "$BAK" "$CAND"          # seal, then check the sealed copy
echo "$BAKSUM  $CAND" | sha256sum -c - \
  || { echo "ABORT: sealed rollback candidate does not match BAKSUM" >&2; exit 1; }
# same route preflight as an install — a backup's origins can have gone stale since it was taken
while read -r u expect; do
  got="$(cloudflared tunnel --config "$CAND" ingress rule "$u" \
         | sed -n 's/^[[:space:]]*service:[[:space:]]*//p' | head -1)"
  [ "$got" = "$expect" ] \
    || { echo "PREFLIGHT FAIL: $u resolved to '$got', expected '$expect'" >&2; exit 1; }
done <<'ROUTES'
https://bleadingoptions.com/ws/events http://192.168.100.252:30097
https://bleadingoptions.com/ http://192.168.100.252:8094
https://auth.bleadingoptions.com/admin http_status:404
https://auth.bleadingoptions.com/realms/master http_status:404
https://auth.bleadingoptions.com/realms/optionsedge http://10.43.127.26:8080
https://es.bleadingoptions.com/ws/events http://192.168.100.4:30091
https://es.bleadingoptions.com/ http://192.168.100.4:30080
https://req.bleadingoptions.com/ http://127.0.0.1:8092
https://fullfunding.nl/ http_status:404
ROUTES
sudo mv "$CAND" /etc/cloudflared/options-edge-stable.yml   # atomic; installs the SEALED bytes
SINCE=$(date '+%Y-%m-%d %H:%M:%S')
sudo systemctl restart options-edge-cloudflared-stable.service
systemctl is-active options-edge-cloudflared-stable.service
INV=$(systemctl show -p InvocationID --value options-edge-cloudflared-stable.service)
[[ "$INV" =~ ^[0-9a-f]{32}$ ]] || { echo "ABORT: no valid InvocationID" >&2; exit 1; }
sleep 15                                                         # see Step 4 for what this proves
systemctl is-active options-edge-cloudflared-stable.service
[ "$(systemctl show -p InvocationID --value options-edge-cloudflared-stable.service)" = "$INV" ] \
  || { echo "ABORT: a new service invocation started within 15s — it is not staying up" >&2; exit 1; }
echo "$BAKSUM  /etc/cloudflared/options-edge-stable.yml" | sha256sum -c -   # fail-closed
sudo journalctl -u options-edge-cloudflared-stable.service --since "$SINCE" --no-pager   # sudo: see Step 4
exec 9>&-; trap - EXIT   # release the deploy lock
```

The Step 5 gate then **fails whenever the restored and merged configs differ after the verifier's
normalization** — it diffs live against the *merged* repo copy (its "live vs repo" section) and you
have deliberately put the previous config back. (Rolling back a comment-only change can still pass,
since that normalization drops comments.) When it does fail, that is expected, not a second fault.
Point it at what you actually restored instead — `$WORK/rollback.yml` is already the copy you checked
before restoring:

```bash
SSHOPTS="-o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=10 -o ServerAliveCountMax=3"
BAK_LOCAL="$WORK/rollback.yml"
timeout 900 env REPO_COPY="$BAK_LOCAL" PROD_SSH="ssh $SSHOPTS abhinav@192.168.100.252" \
  ES4_SSH="ssh $SSHOPTS abhinav@192.168.100.4" scripts/ops/verify-prod-tunnel.sh --phase retired
```

Either way, finish by making the repo describe what is running again — revert the merge or land the
follow-up. Leaving the two out of step is the exact condition this directory exists to prevent.

**Step 5 — accept.** From `/tmp/tunnel-deploy` on your workstation — the same reviewed tree Step 1
created, so the verifier and its manifest inputs match the config you just installed:

```bash
set -euo pipefail
SSHOPTS="-o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=10 -o ServerAliveCountMax=3"
[ -z "$(git status --porcelain)" ] || { echo "ABORT: worktree modified since Step 1" >&2; exit 1; }
# Which copy the verifier compares live against: the merged file normally, or the restored backup
VERIFY_COPY=infra/prod/cloudflared/options-edge-stable.yml    # after a rollback: "$WORK/rollback.yml"
BEFORE=$(ssh $SSHOPTS abhinav@192.168.100.252 'sha256sum /etc/cloudflared/options-edge-stable.yml') \
  || { echo "ABORT: could not read the live config" >&2; exit 1; }
timeout 900 env PROD_SSH="ssh $SSHOPTS abhinav@192.168.100.252" ES4_SSH="ssh $SSHOPTS abhinav@192.168.100.4" \
  REPO_COPY="$VERIFY_COPY" scripts/ops/verify-prod-tunnel.sh --phase retired
AFTER=$(ssh $SSHOPTS abhinav@192.168.100.252 'sha256sum /etc/cloudflared/options-edge-stable.yml') \
  || { echo "ABORT: could not re-read the live config" >&2; exit 1; }
[ "$AFTER" = "$BEFORE" ] \
  || { echo "ABORT: live config changed mid-verification — the VERIFIED stamp is void, re-run" >&2; exit 1; }
```

Accept only on `prod tunnel VERIFIED (phase=retired)` **and exit 0** — check the code, not just the
tail. A non-zero exit with no `FAIL:` lines usually means the run never got that far: `124` is the
outer `timeout`, and a dropped SSH or a missing dependency can abort mid-check. Keep the full output.

The verifier does not prove a real user can trade. Finish by logging in through Keycloak and
confirming a live board on **both** `bleadingoptions.com` and `es.bleadingoptions.com` — the
authenticated WebSocket reaching `101` and carrying data is the acceptance the 2026-07-31 incident
turned on. Load `req.bleadingoptions.com` too if the Bugzilla route was touched.

If Step 5 or the board test fails, go to **Rollback** (above) — the deploy lock is released at the end
of Step 4 precisely so recovery is never blocked waiting on your own session.

⚠️ **Roll back only your own deployment.** The rollback block asserts that what is live still matches
what your Step 4 installed. If it does not, something changed underneath you — stop and work out what,
rather than restoring a backup over it.

## ⚠️ There is no in-place reload — it is a restart, and it drops all four hostnames

`SIGHUP` does **not** reload this config in place on this host, and `systemctl reload` is unavailable:
the unit declares `Restart=always` with `RestartSec=5` and **no `ExecReload=`**. Signalling the process
makes systemd restart it — the **PID changes**, the tunnel reconnects, and `bleadingoptions.com`,
`auth.`, `es.` and `req.` all go unreachable together.

Normally that is seconds. It is **not guaranteed** to be: if the new process fails to start it retries
every 5s and the outage lasts until someone rolls back. So treat this as a planned outage of the whole
public surface, not a config refresh — and note the blast radius is wider than the trading board.
Picking a window outside market hours is necessary but not sufficient: it also takes down Keycloak
(every session across both boards) and Bugzilla.

## The `.bak-*` files on the host

The `.bak-*` files in `/etc/cloudflared/` are the informal history from before this file was reviewed
here, plus one per deploy from Step 4 onward. They are not authoritative — this directory is, and has
been since PR #675. Their remaining value is narrow: they are the only record of states that existed
**only** on the box and were never committed.

Retention: keep **every backup dated 2026-08-09 or earlier** (the pre-tracking history, including the
named `…bak-20260809-132945-ff-route-removed`) and the **newest five** taken since. Beyond that, do
not delete on the assumption that a backup is "already in Git": Step 4 takes a backup precisely when
Step 2 found drift, so it can hold a live-only state that was never committed, and the verifier
passing tells you about the *current* file, not a historical one. Before removing any backup, prove
its content exists in this repo's history — e.g. `git log --all --format=%H -- <path>` and compare
`git show <sha>:<path>` byte-for-byte with the backup.
