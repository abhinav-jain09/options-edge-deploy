# Bugzilla: two projects, two issue types, two lifecycles

Internal Bugzilla (`http://192.168.100.252:8092`, Bugzilla **5.2**, source checkout on `.252` at
`/home/options-edge/data/bugzilla/runtime` @ `276673ab6`) gets two lifecycles: every item is either
a **BUG** or a **REQUIREMENT**, each type has its own lifecycle, and each has its own category
vocabulary.

## 0. What the installation actually is

Not a fresh install. It holds **177 live bugs** (2026-06-11 .. 2026-08-03) and **four products**
created by separate work:

| Product | Type | Bugs |
|---|---|---|
| `OptionsEdge` | BUG | 177 |
| `Fullfunding` | BUG | 0 |
| `OptionsEdge Requirements` | REQUIREMENT | 0 |
| `Fullfunding Requirements` | REQUIREMENT | 0 |

Two constraints follow, and they decide almost everything below: **the four products are adopted,
not replaced**, and **no existing bug's data may be modified**.

Stated precisely, because the difference matters: applying this configuration changes no *value* of
any existing bug. It does add two nullable columns to `bugs` (`Bugzilla::Field->create` issues
`ALTER TABLE`), which existing rows acquire as empty — and depending on the MariaDB version and DDL
algorithm, that `ALTER` may rebuild the table physically. What it never does is change a status,
resolution, product or any other field of a bug that already exists. The provisioner **proves** that
by digesting every pre-existing row before and after and refusing to finish if anything differs.

The constraint is not a preference, it is arithmetic. A mandatory custom select over a populated
table gives all 177 rows the `---` sentinel. And a status rename is not metadata: Bugzilla
implements it as `UPDATE bugs SET <field> = ?` (`Field/Choice.pm:158`), which would have rewritten
the 80 rows sitting on `CONFIRMED`/`IN_PROGRESS`.

## 1. The constraint that shapes everything

`bug_status`, `status_workflow`, `resolution` and `fielddefs` are **installation-wide** in Bugzilla.
`Bugzilla::Status::can_change_to` reads one global matrix; there is no per-product and no
per-issue-type workflow. So "a bug lifecycle and a requirement lifecycle" cannot be two independent
workflows. They are encoded as **one global superset matrix with disjoint open branches**, and a
small extension is what stops an item crossing from one branch to the other.

Naming convention alone is not enough: without server-side enforcement, a `PUT /rest/bug/<id>` with
`status=REQ_APPROVED` on a bug is perfectly legal to core Bugzilla and would persist.

## 2. Model

| Concern | Decision |
|---|---|
| Item type | **derived from the product** (`product_type` in the SSOT). No per-bug type field. |
| Category | `cf_category` single-select, **optional**, one flat `BUG_*`/`REQ_*` vocabulary |
| Environment | `cf_environment`, controlled at field level by product (the two BUG products) |
| Classifications | stay off |

**The type is the product.** `bugs.product_id` is already the normalised ownership column; a second
per-bug copy could only drift from it, and — decisively — could not be populated for the 177 existing
rows without writing to them. Search ergonomics: "all bugs" is `product IN (OptionsEdge,
Fullfunding)`.

The cost is that moving an item between products can change its type, so **the product is guarded**:
same-type moves are ordinary, cross-type moves are refused. That check cannot live in
`bug_check_can_change_field`, because `Bugzilla::Bug::_set_product` (`Bug.pm:2716`) mutates the
object directly instead of routing `product` through the setter. It runs in `bug_start_of_update`
(`Bug.pm:905`) instead — after the base update but *inside* the enclosing transaction, so throwing
rolls the whole submission back. Note the change is still keyed `product_id` with numeric ids at
that point; `Bug.pm:915-922` rewrites it to `product` with names only afterwards.

**The category is optional on purpose.** An empty category is always valid, which is exactly what
leaves the 177 existing bugs freely editable.

## 3. Lifecycles

```
BUG:          UNCONFIRMED → CONFIRMED → IN_PROGRESS → RESOLVED → VERIFIED
REQUIREMENT:  REQ_DRAFT → REQ_REVIEW → REQ_APPROVED → REQ_IN_PROGRESS → RESOLVED → VERIFIED
```

The BUG branch keeps Bugzilla's **stock names**: renaming them would rewrite the live rows using
them. Only the four `REQ_*` statuses are new, so no bug's status value is touched.

The exact matrix, `is_open` flags and `require_comment` values live in
[`bugzilla/configuration/expected-state.json`](../bugzilla/configuration/expected-state.json), which
is the single source of truth for both the provisioner and the verifier.

**The closed tail (`RESOLVED`/`VERIFIED`) is deliberately shared.** Bugzilla has exactly one
`duplicate_or_move_bug_status` parameter, and `Bugzilla::Status::add_missing_bug_status_transitions()`
forces an edge from every status to it. A disjoint `REQ_CLOSED` tail would therefore break native
duplicate handling (`dupe_of` could only target one of the two) and would be re-broken by Bugzilla
itself on the next status edit. A requirement's "delivered" state is `RESOLVED + IMPLEMENTED`.

Resolutions are global too, so the stock five stay and `IMPLEMENTED`, `REJECTED`, `DEFERRED` are
added; the extension restricts each type to its own subset (`DUPLICATE` is legal for both).

`UNCONFIRMED` stays on the BUG branch — Bugzilla treats it specially (it cannot be deleted, and
`allows_unconfirmed` / `everconfirmed` depend on it).

The 177 existing bugs land correctly without being touched: they are in `OptionsEdge` (BUG), on
`CONFIRMED`/`IN_PROGRESS`/`RESOLVED`, resolved `FIXED` — every one of which is BUG-valid.

## 4. Enforcement — `extensions/IssueTypeWorkflow`

Three hooks, all inside `Bugzilla::Bug`, so the UI, bulk edit, email-in, XML-RPC/JSON-RPC and REST
all get the same policy:

- **`bug_check_can_change_field`** — every update. Denies a status outside the item's own branch, a
  resolution not allowed for its type, and a category not belonging to its type. Denial is by
  pushing `PRIVILEGES_REQUIRED_EMPOWERED` into `priv_results`, **not** by throwing — see §6.
- **`bug_start_of_update`** — cross-type product moves, which the setter hook cannot see.
- **`bug_end_of_create_validators`** — creation. Checks the category against the product's type and
  canonicalises the entry status (BUG → `UNCONFIRMED`, REQUIREMENT → `REQ_DRAFT`).

Enforcement has **three** states:

| `cf_category` | `REQ_*` statuses | model matches | behaviour |
|---|---|---|---|
| absent | absent | — | never provisioned — nothing enforced, so `checksetup.pl`, the first mount and the first apply all work |
| absent | present | — | **broken** — the anchor was removed after provisioning |
| present | — | yes | normal policy |
| present | — | no | **broken** — every guarded change refused until the provisioner is re-run |

There is deliberately **no** "enforcement enabled" parameter. Any such switch is reachable by anyone
with `tweakparams`, which would hand administrators the one thing this design exists to deny them.
`cf_category` is the marker instead, and deleting it is not a way out: provisioning also creates the
`REQ_*` statuses, which cannot be removed while items sit on them, so a missing category field with
those statuses present reads as `broken`, not `bootstrap`.

"Matches" means everything that can change what the policy **permits**, and deliberately not more:
`cf_category`'s type/custom/optional/`enter_bug` and the absence of any controller on it;
`cf_environment` present and visible for exactly the BUG products; every mapped product present,
active, and agreeing on **both name and id**; exactly the declared category vocabulary, all active;
every resolution the policy hands out; every status, active and with the declared `is_open`; and the
**entire workflow matrix**, compared as a multiset. So a re-pointed category, a flipped `is_open`, a
deleted creation edge or an `editworkflow.cgi` shortcut all stop the installation rather than
quietly redefining the lifecycles.

Cosmetics — descriptions, sort keys, `buglist` flags — are not checked here, because they cannot
widen anything and would be paid for on every request; the provisioner's zero-change dry run covers
them instead.

That is also why the policy — statuses, resolutions, categories, `is_open`, the matrix — is compiled
into the extension as well as declared in the SSOT. Reading only the database would make an
`editvalues.cgi` edit *be* the policy; reading only the constants would miss drift. Requiring both to
agree closes each hole with the other, and `setup-projects.pl` refuses to run if the constants and
the JSON disagree.

The provisioner then **proves** the fail-closed state at the end of every apply: it deletes the
bug-creation rows inside a transaction, checks that the extension goes `broken`, and rolls back. A
regression in that check is otherwise invisible until the day it matters.

**Bulk edits cannot half-apply.** `Bugzilla::WebService::Bug::update` wraps its whole loop in one
transaction (`Bug.pm` REST path), and `process_bug.cgi` calls `set_all` on *every* selected bug
(line 340-350) before the first `update()` (line 385) — and our denial happens at set time. So a
single illegal item aborts the entire submission.

## 5. What Bugzilla's own code forced us to change

Four findings from reading the 5.2 source that changed the design:

1. **`priv_results` is a hard deny, not a privilege escalation.** `Bug.pm:4558` takes the first
   entry `> 0` and returns 0 immediately, without consulting the user's real groups — admins
   included. And because `_refine_available_statuses` (`Bug.pm:3917-3937`) calls
   `check_can_change_field` purely to build the status dropdown, *throwing* there would break
   rendering of every bug page. Denying instead both hard-blocks the move and filters the dropdown
   down to the item's own lifecycle for free.
2. **One controller per field value.** `bug_status`, `resolution` and every `cf_*` value table carry
   a single `visibility_value_id INT2` column (`DB/Schema.pm`, `Field/Choice.pm` `DB_COLUMNS`). A
   category value therefore cannot belong to both types, so the two vocabularies are prefixed
   (`BUG_*` / `REQ_*`) rather than sharing names like `DATA`.
3. **Unprivileged reporters get `UNCONFIRMED` forced on them.** `Bug.pm:1526-1536` overrides the
   requested status for anyone without `editbugs`/`canconfirm`, which would drop every externally
   filed REQUIREMENT onto the BUG lifecycle. Canonicalising the status in
   `bug_end_of_create_validators` (which runs after all validators, `Bug.pm:882`) is what fixes it —
   so filing onto the wrong entry point is *corrected*, not rejected.
4. **`everconfirmed` is computed before our hook.** `_check_bug_status` sets it at `Bug.pm:1584` from
   the status it saw. Replacing `bug_status` afterwards without recomputing `everconfirmed` would
   write an inconsistent row, so the extension recomputes it.

Also verified: `bug_status` arrives at the create hook as a `Bugzilla::Status` **object**
(`Bug.pm:1540`) while custom selects arrive as **strings** (`_check_select_field` returns
`$object->name`); `is_open` is settable only at create time (it is absent from
`Bugzilla::Field::Choice::UPDATE_COLUMNS`), so changing it needs direct SQL;
`Bugzilla::Field->create` performs its own `ALTER TABLE bugs`, so no `checksetup.pl` is needed to
add a custom field — but that DDL implicitly commits, which is why the run is **not** one atomic
transaction and rollback means restoring the database.

## 6. Applying it

`Product.create`/`Component.create` exist over REST; statuses, the workflow matrix, custom fields and
value control have **no WebService**. So everything is done by one idempotent Perl program,
`bugzilla/configuration/setup-projects.pl`, run inside the web container. It is dry-run by default
and, before touching anything, it:

- validates the declared state on its own terms (no duplicate statuses or edges, every edge resolves,
  every status can reach the duplicate status, every enforcement entry names something declared);
- asserts the extension's compiled-in policy constants and error codes **match the JSON**, so the two
  halves of the policy cannot drift apart unnoticed;
- refuses to apply while Apache is still answering, because enforcement is genuinely off between the
  two `ALTER TABLE`s and config edits would race the workflow rewrite;
- audits every existing item against the model and aborts if any of them violates it — the extension
  validates fields as they change, so it can never repair an item that was already inconsistent.

Anything present but undeclared — an extra status, resolution, field value, component, version or
milestone — aborts the run. That check lives in **preflight**, so a predictable failure cannot leave
a half-provisioned installation and a dry run cannot report "nothing to do" against something the
verifier would reject. Deactivating an extra is not a valid remedy: REST cannot report it, so the two
tools would disagree.

During the run it takes a `GET_LOCK` and re-asserts ownership before **every** mutation — each one is
announced through a single `plan()` chokepoint, which is where the check lives, because the lock is
connection-scoped and a silent reconnect would drop it. It rewrites `status_workflow` in one
transaction with a multiset read-back comparison and an explicit rollback, and treats a failed cache
flush as fatal. After the mutations it re-audits every item — the DDL for a mandatory select field
gives pre-existing rows the unset sentinel, and declaring the model complete over that would lock
those items out of every guarded edit — and then proves the fail-closed state (below).

Backup and restore are scripts rather than pasted commands (`backup.sh`, `restore-backup.sh`): the
backup verifies the dump's own completion footer, because a failed `mariadb-dump` still yields a
valid *empty* gzip, and the restore verifies everything before it drops anything and leaves Apache
stopped if any step fails.

The script and the extension live in this repository and are bind-mounted read-only into the
container (the verifier runs from the host, against REST) — the Bugzilla runtime checkout itself is not version controlled, and this design
deliberately does not add anything new to it.

Verification is `bugzilla/configuration/verify-projects.py`: structural assertions read back over
REST (through a restarted web process, so a stale cache fails the run) plus a behavioural pass that
files a BUG and a REQUIREMENT and walks both lifecycles end to end. Its negative cases are
**generated** from the SSOT: at each state, the illegal targets are exactly those reachable in the
global matrix but not allowed for that issue type — targets core already blocks are skipped and
reported as such, because they would pass with the extension switched off. It also covers native
duplicate handling and bulk-update atomicity, and closes its smoke bugs on the way out.

Refusals are asserted by **code**, not by "something went wrong": a denied update is
`illegal_change` (115, HTTP 401), and the extension registers its own creation errors
(100004-100008, HTTP 400) through the `webservice_error_codes` hook. Otherwise a bad API key or a
500 would look exactly like successful enforcement. The two `initial_status_*` codes are race defence: a damaged
workflow normally trips `issue_type_workflow_misconfigured` first. The verifier asserts exact codes
for the reachable ones and says plainly which it cannot induce.

## 7. Known limits

- An update refused by the extension surfaces as Bugzilla's generic `illegal_change` error rather
  than a bespoke message. That is the price of denying via `priv_results` instead of throwing (§5.1);
  creation-time errors do have their own messages and codes.
- Because the closed tail is shared, any report that distinguishes "fixed bug" from "implemented
  requirement" must filter on the product and `resolution`, not on status alone.
- The extension duplicates the status/resolution policy as Perl constants (it must not depend on a
  config file at runtime). The provisioner asserts they match the JSON on every run, and
  `verify-projects.py` proves them behaviourally.
- A value *rename* would rewrite bug rows directly — `Bugzilla::Field::Choice::update` issues
  `UPDATE bugs SET <field> = ?` without going through `Bugzilla::Bug` — so it is refused up front in
  `object_before_set` (`Object.pm:445`), which fires before `set` does any work. Renaming or
  deleting a guarded status, resolution, category value or mapped product is blocked, which closes
  the swap-through-temporary-names route. The apply proves both guards by attempting the forbidden
  operations inside a transaction it rolls back.
- An administrator editing `editvalues.cgi`/`editworkflow.cgi` can no longer widen policy — the
  compiled-in vocabulary and the fail-closed marker see to that — but they can still make the
  installation *stop working* (which is the intended direction), and items stored before such an edit
  are not re-audited. Re-running the provisioner is what detects and reports it.
- REST exposes no `is_active` for generic field values and no `sortkey`/`buglist`/`obsolete` for
  fields, so the verifier cannot prove those. They are covered instead by the provisioner's
  zero-action dry run, which compares exactly those attributes — the two together are the
  verification, neither alone.
- Every Bugzilla upgrade must re-run `verify-projects.py` before the instance takes traffic: a
  changed hook signature would silently stop enforcement or block editing.
- The verifier talks plain HTTP on the LAN, so its API key is sniffable; run it on `.252` against
  `localhost` and revoke the key afterwards.
- A dry run makes no persistent change, but it is not literally inert: it opens a database
  connection and takes a server-side advisory lock.
- The low-privilege-reporter path (`Bug.pm:1526-1536`) is only *really* covered if you hand the
  verifier an API key for a user without `editbugs`/`canconfirm`, via `--reporter-api-key-env`. The
  verifier creates no accounts, and without that key it reports the check as **failed/unproven**
  rather than quietly passing: an admin asking for `UNCONFIRMED` exercises the canonicalisation but
  not the privilege-dependent override.
- `REQ_APPROVED` is a state, not an authorisation. Restricting it to named approvers needs a group
  check that is deliberately not built yet.
