# Bugzilla: two projects, two issue types, two lifecycles

Internal Bugzilla (`http://192.168.100.252:8092`, Bugzilla **5.2**, source checkout on `.252` at
`/home/options-edge/data/bugzilla/runtime` @ `276673ab6`) gets two projects — **optionedge** and
**fullfunding** — where every item is either a **BUG** or a **REQUIREMENT**, each type has its own
lifecycle, and each type has its own category vocabulary.

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
| Project | Bugzilla **Product**: `optionedge`, `fullfunding`. Classifications stay off. |
| Item type | `cf_issue_type` single-select, mandatory, **immutable after creation** |
| Category | `cf_category` single-select, mandatory, values controlled by `cf_issue_type` |
| Subsystem | Bugzilla **Component** (not the category) |
| Environment | `cf_environment`, visible for BUG only |

Four products (`optionedge-bugs`, `optionedge-reqs`, …) were rejected: the workflow is global, so
splitting products buys no separate lifecycle at all, and it breaks dependencies, duplicate links
and cross-type search for nothing.

**Type is immutable** because a type change would strand the item on a status and a category
belonging to the other lifecycle. The one exception: an item whose type is missing or `---` may have
its type set once, so a mis-provisioned item stays repairable instead of frozen forever.

## 3. Lifecycles

```
BUG:          UNCONFIRMED → BUG_CONFIRMED → BUG_IN_PROGRESS → RESOLVED → VERIFIED
REQUIREMENT:  REQ_DRAFT → REQ_REVIEW → REQ_APPROVED → REQ_IN_PROGRESS → RESOLVED → VERIFIED
```

The exact matrix, `is_open` flags and `require_comment` values live in
[`bugzilla/configuration/expected-state.json`](../bugzilla/configuration/expected-state.json), which
is the single source of truth for both the provisioner and the verifier.

`CONFIRMED` and `IN_PROGRESS` are **renamed** to `BUG_CONFIRMED` / `BUG_IN_PROGRESS` rather than
deleted and recreated, so status IDs and any history survive.

**The closed tail (`RESOLVED`/`VERIFIED`) is deliberately shared.** Bugzilla has exactly one
`duplicate_or_move_bug_status` parameter, and `Bugzilla::Status::add_missing_bug_status_transitions()`
forces an edge from every status to it. A disjoint `REQ_CLOSED` tail would therefore break native
duplicate handling (`dupe_of` could only target one of the two) and would be re-broken by Bugzilla
itself on the next status edit. A requirement's "delivered" state is `RESOLVED + IMPLEMENTED`.

Resolutions are global too, so the stock five stay and `IMPLEMENTED`, `REJECTED`, `DEFERRED` are
added; the extension restricts each type to its own subset (`DUPLICATE` is legal for both).

`UNCONFIRMED` stays on the BUG branch — Bugzilla treats it specially (it cannot be deleted, and
`allows_unconfirmed` / `everconfirmed` depend on it).

## 4. Enforcement — `extensions/IssueTypeWorkflow`

Two hooks, both inside `Bugzilla::Bug`, so the UI, bulk edit, email-in, XML-RPC/JSON-RPC and REST all
get the same policy:

- **`bug_check_can_change_field`** — every update. Denies: any change of `cf_issue_type`; a status
  outside the item's own branch; a resolution not allowed for its type; a category not owned by its
  type. Denial is by pushing `PRIVILEGES_REQUIRED_EMPOWERED` into `priv_results`, **not** by throwing
  — see §6.
- **`bug_end_of_create_validators`** — creation. Requires a type and a matching category, and
  canonicalises the entry status (BUG → `UNCONFIRMED`, REQUIREMENT → `REQ_DRAFT`).

The category vocabulary is **not** duplicated in the extension: Bugzilla already stores which issue
type each `cf_category` value belongs to (`visibility_value_id`), so the extension reads the
authoritative answer from the configuration it was given.

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
`bugzilla/configuration/setup-projects.pl`, run inside the web container. It is dry-run by default,
takes a `GET_LOCK`, refuses to run against the wrong version or with bugs sitting on statuses outside
the target model, rewrites `status_workflow` in a single transaction with a read-back comparison, and
flushes memcached at the end.

The script, the extension and the verifier live in this repository and are bind-mounted read-only
into the container — the Bugzilla runtime checkout itself is not version controlled, and this design
deliberately does not add anything new to it.

Verification is `bugzilla/configuration/verify-projects.py`: structural assertions read back over
REST (through a restarted web process, so a stale cache fails the run) plus a behavioural pass that
files a BUG and a REQUIREMENT, walks both lifecycles end to end, and asserts every illegal
cross-lifecycle move is refused.

## 7. Known limits

- An update refused by the extension surfaces as Bugzilla's generic `illegal_change` error rather
  than a bespoke message. That is the price of denying via `priv_results` instead of throwing (§5.1);
  creation-time errors do have their own messages.
- Because the closed tail is shared, any report that distinguishes "fixed bug" from "implemented
  requirement" must filter on `cf_issue_type` and `resolution`, not on status alone.
- The extension duplicates the status/resolution policy as Perl constants (it must not depend on a
  config file at runtime). `verify-projects.py` proves the two agree behaviourally rather than by
  inspection.
- Every Bugzilla upgrade must re-run `verify-projects.py` before the instance takes traffic: a
  changed hook signature would silently stop enforcement or block editing.
- `REQ_APPROVED` is a state, not an authorisation. Restricting it to named approvers needs a group
  check that is deliberately not built yet.
