#!/usr/bin/env python3
"""Verify the internal Bugzilla against expected-state.json.

Two layers, because metadata alone proves nothing:

  * structural - every product, component, custom field, field value, status,
    workflow edge and resolution is read back over REST (i.e. through a
    *restarted* web process, so a stale cache shows up as a failure) and
    compared with the declared state;

  * behavioural - a BUG and a REQUIREMENT are filed and walked through their
    complete lifecycles, and every illegal cross-lifecycle move is attempted and
    must be refused. This is the only thing that actually proves the
    IssueTypeWorkflow extension is loaded and enforcing.

Exits non-zero if anything fails. Nothing here mutates configuration.

  ./verify-projects.py --base-url http://192.168.100.252:8092 \
                       --state expected-state.json --api-key "$BZ_API_KEY"
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

FIELD_TYPE_SINGLE_SELECT = 2

failures = []
checks = 0


def check(name, ok, detail=""):
    global checks
    checks += 1
    if ok:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name}" + (f"\n          {detail}" if detail else ""))
        failures.append(name)
    return ok


def eq(name, got, want):
    return check(name, got == want, f"got {got!r}, want {want!r}")


class Bz:
    def __init__(self, base_url, api_key):
        self.base = base_url.rstrip("/")
        self.api_key = api_key

    def _request(self, method, path, payload=None):
        url = f"{self.base}/rest{path}"
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        if self.api_key:
            req.add_header("X-BUGZILLA-API-KEY", self.api_key)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status, json.loads(resp.read().decode() or "{}")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode()
            try:
                return exc.code, json.loads(body or "{}")
            except json.JSONDecodeError:
                return exc.code, {"error": True, "message": body[:500]}

    def get(self, path):
        return self._request("GET", path)

    def post(self, path, payload):
        return self._request("POST", path, payload)

    def put(self, path, payload):
        return self._request("PUT", path, payload)


# --------------------------------------------------------------------------
# structural
# --------------------------------------------------------------------------

def verify_version(bz, state):
    print("\n[1] Bugzilla version")
    _, body = bz.get("/version")
    eq("version", body.get("version"), state["bugzilla_version"])


def verify_products(bz, state):
    print("\n[2] Products and components")
    for spec in state["products"]:
        name = spec["name"]
        status, body = bz.get("/product?names=" + urllib.parse.quote(name))
        products = body.get("products") or []
        if not check(f"product '{name}' exists", len(products) == 1, json.dumps(body)[:300]):
            continue
        product = products[0]

        eq(f"product '{name}' is_active", bool(product.get("is_active")), spec["is_active"])
        eq(f"product '{name}' has_unconfirmed",
           bool(product.get("has_unconfirmed")), spec["allows_unconfirmed"])
        eq(f"product '{name}' default_milestone",
           product.get("default_milestone"), spec["default_milestone"])

        got_components = sorted(c["name"] for c in product.get("components", []))
        eq(f"product '{name}' components", got_components,
           sorted(c["name"] for c in spec["components"]))

        eq(f"product '{name}' versions",
           sorted(v["name"] for v in product.get("versions", [])), sorted(spec["versions"]))
        eq(f"product '{name}' milestones",
           sorted(m["name"] for m in product.get("milestones", [])),
           sorted(spec["milestones"]))

        unassigned = [c["name"] for c in product.get("components", [])
                      if not c.get("default_assigned_to")]
        check(f"product '{name}' every component has a default assignee",
              not unassigned, f"missing on: {unassigned}")


def fields_by_name(bz):
    status, body = bz.get("/field/bug")
    if "fields" not in body:
        print(f"  FATAL cannot read /rest/field/bug (HTTP {status}): "
              f"{json.dumps(body)[:300]}")
        sys.exit(2)
    return {f["name"]: f for f in body["fields"]}


def verify_custom_fields(bz, fields, state):
    print("\n[3] Custom fields")
    for name, spec in state["fields"].items():
        field = fields.get(name)
        if not check(f"field {name} exists", field is not None):
            continue

        eq(f"field {name} is_custom", bool(field.get("is_custom")), True)
        eq(f"field {name} type", field.get("type"), FIELD_TYPE_SINGLE_SELECT)
        eq(f"field {name} display_name", field.get("display_name"), spec["description"])
        eq(f"field {name} is_mandatory", bool(field.get("is_mandatory")),
           spec["is_mandatory"])
        eq(f"field {name} is_on_bug_entry", bool(field.get("is_on_bug_entry")),
           spec["enter_bug"])

        if spec.get("value_field"):
            eq(f"field {name} value_field", field.get("value_field"), spec["value_field"])
        if spec.get("visibility_field"):
            eq(f"field {name} visibility_field",
               field.get("visibility_field"), spec["visibility_field"])
            eq(f"field {name} visibility_values",
               sorted(field.get("visibility_values") or []),
               sorted(spec["visibility_values"]))

        want_values = sorted(v["value"] for v in spec["values"])
        got_values = sorted(v["name"] for v in field.get("values", [])
                            if v["name"] not in ("", "---"))
        eq(f"field {name} values", got_values, want_values)

        # Every value must be owned by exactly the declared issue type. Bugzilla
        # 5.2 stores one controller per value, so this list is 0 or 1 long.
        for v in spec["values"]:
            if "controlled_by" not in v:
                continue
            got = next((x.get("visibility_values") or []
                        for x in field.get("values", []) if x["name"] == v["value"]), None)
            eq(f"field {name} value '{v['value']}' controlled_by",
               got, [v["controlled_by"]])


def verify_statuses_and_workflow(bz, fields, state):
    print("\n[4] Statuses and workflow matrix")
    field = fields.get("bug_status")
    if not check("bug_status field present", field is not None):
        return

    values = {v["name"]: v for v in field.get("values", [])}

    want_names = sorted(s["value"] for s in state["statuses"])
    got_names = sorted(n for n in values if n != "")
    eq("status set", got_names, want_names)

    for spec in state["statuses"]:
        v = values.get(spec["value"])
        if not v:
            continue
        eq(f"status '{spec['value']}' is_open", bool(v.get("is_open")), spec["is_open"])

    # The synthetic '' entry is Bugzilla's bug-creation row.
    want_edges = {}
    for edge in state["workflow"]:
        want_edges.setdefault(edge["from"], {})[edge["to"]] = bool(edge["require_comment"])

    for from_status in sorted(set(list(want_edges) + [n for n in values])):
        v = values.get(from_status)
        if v is None:
            check(f"workflow row for '{from_status}'", False, "status missing")
            continue
        got = {t["name"]: bool(t.get("comment_required"))
               for t in v.get("can_change_to", [])}
        want = want_edges.get(from_status, {})
        eq(f"workflow from '{from_status or '(new bug)'}'",
           dict(sorted(got.items())), dict(sorted(want.items())))


def verify_resolutions(bz, fields, state):
    print("\n[5] Resolutions")
    field = fields.get("resolution")
    if not check("resolution field present", field is not None):
        return
    got = sorted(v["name"] for v in field.get("values", []) if v["name"] != "")
    eq("resolution set", got, sorted(r["value"] for r in state["resolutions"]))


# --------------------------------------------------------------------------
# behavioural
# --------------------------------------------------------------------------

SMOKE_PREFIX = "[SMOKE] issue-type lifecycle verification"


def file_bug(bz, product, component, issue_type, category, extra=None):
    payload = {
        "product": product,
        "component": component,
        "summary": f"{SMOKE_PREFIX} - {issue_type}",
        "version": "unspecified",
        "description": "Filed by verify-projects.py. Safe to close or delete.",
        "cf_issue_type": issue_type,
        "cf_category": category,
    }
    payload.update(extra or {})
    return bz.post("/bug", payload)


def move(bz, bug_id, **fields):
    payload = {"comment": {"body": "verify-projects.py lifecycle step"}}
    payload.update(fields)
    return bz.put(f"/bug/{bug_id}", payload)


def state_of(bz, bug_id):
    _, body = bz.get(f"/bug/{bug_id}")
    bug = (body.get("bugs") or [{}])[0]
    return (bug.get("status"), bug.get("resolution"),
            bug.get("cf_issue_type"), bug.get("cf_category"))


def expect_refused(bz, label, bug_id, before, **fields):
    status, body = move(bz, bug_id, **fields)
    refused = status >= 400 or body.get("error")
    check(f"refused: {label}", refused,
          f"HTTP {status}, body {json.dumps(body)[:200]}")
    after = state_of(bz, bug_id)
    eq(f"unchanged after refusing: {label}", after, before)


def verify_bug_lifecycle(bz, state):
    print("\n[6] BUG lifecycle (optionedge)")
    status, body = file_bug(bz, "optionedge", "Cross-Cutting / Other",
                            "BUG", "BUG_CODE")
    if not check("file a BUG", status < 400 and body.get("id"),
                 json.dumps(body)[:300]):
        return None
    bug_id = body["id"]
    print(f"  ....  bug {bug_id}")

    eq("BUG starts UNCONFIRMED", state_of(bz, bug_id)[0], "UNCONFIRMED")

    for target in ("BUG_CONFIRMED", "BUG_IN_PROGRESS"):
        st, b = move(bz, bug_id, status=target)
        check(f"BUG -> {target}", st < 400 and not b.get("error"),
              json.dumps(b)[:200])
        eq(f"BUG is {target}", state_of(bz, bug_id)[0], target)

    st, b = move(bz, bug_id, status="RESOLVED", resolution="FIXED")
    check("BUG -> RESOLVED/FIXED", st < 400 and not b.get("error"), json.dumps(b)[:200])
    eq("BUG is RESOLVED/FIXED", state_of(bz, bug_id)[:2], ("RESOLVED", "FIXED"))

    st, b = move(bz, bug_id, status="VERIFIED")
    check("BUG -> VERIFIED", st < 400 and not b.get("error"), json.dumps(b)[:200])
    eq("BUG is VERIFIED/FIXED", state_of(bz, bug_id)[:2], ("VERIFIED", "FIXED"))

    before = state_of(bz, bug_id)
    expect_refused(bz, "BUG -> REQ_REVIEW", bug_id, before, status="REQ_REVIEW")
    expect_refused(bz, "BUG issue type change", bug_id, before,
                   cf_issue_type="REQUIREMENT")
    expect_refused(bz, "BUG given a REQ_ category", bug_id, before,
                   cf_category="REQ_LOGIC")
    expect_refused(bz, "BUG resolved IMPLEMENTED", bug_id, before,
                   status="RESOLVED", resolution="IMPLEMENTED")
    return bug_id


def verify_requirement_lifecycle(bz, state):
    print("\n[7] REQUIREMENT lifecycle (fullfunding)")
    status, body = file_bug(bz, "fullfunding", "Cross-Cutting / Other",
                            "REQUIREMENT", "REQ_LOGIC")
    if not check("file a REQUIREMENT", status < 400 and body.get("id"),
                 json.dumps(body)[:300]):
        return None
    bug_id = body["id"]
    print(f"  ....  bug {bug_id}")

    eq("REQUIREMENT starts REQ_DRAFT", state_of(bz, bug_id)[0], "REQ_DRAFT")

    for target in ("REQ_REVIEW", "REQ_APPROVED", "REQ_IN_PROGRESS"):
        st, b = move(bz, bug_id, status=target)
        check(f"REQUIREMENT -> {target}", st < 400 and not b.get("error"),
              json.dumps(b)[:200])
        eq(f"REQUIREMENT is {target}", state_of(bz, bug_id)[0], target)

    before = state_of(bz, bug_id)
    expect_refused(bz, "REQUIREMENT resolved FIXED", bug_id, before,
                   status="RESOLVED", resolution="FIXED")
    expect_refused(bz, "REQUIREMENT -> BUG_CONFIRMED", bug_id, before,
                   status="BUG_CONFIRMED")
    expect_refused(bz, "REQUIREMENT given a BUG_ category", bug_id, before,
                   cf_category="BUG_CODE")

    st, b = move(bz, bug_id, status="RESOLVED", resolution="IMPLEMENTED")
    check("REQUIREMENT -> RESOLVED/IMPLEMENTED", st < 400 and not b.get("error"),
          json.dumps(b)[:200])
    st, b = move(bz, bug_id, status="VERIFIED")
    check("REQUIREMENT -> VERIFIED", st < 400 and not b.get("error"),
          json.dumps(b)[:200])
    eq("REQUIREMENT is VERIFIED/IMPLEMENTED",
       state_of(bz, bug_id)[:2], ("VERIFIED", "IMPLEMENTED"))

    st, b = move(bz, bug_id, status="REQ_REVIEW", resolution="")
    check("REQUIREMENT reopens to REQ_REVIEW", st < 400 and not b.get("error"),
          json.dumps(b)[:200])
    eq("reopened REQUIREMENT has no resolution", state_of(bz, bug_id)[:2],
       ("REQ_REVIEW", ""))
    return bug_id


def verify_creation_guards(bz):
    print("\n[8] Creation-time guards")
    st, b = file_bug(bz, "optionedge", "Cross-Cutting / Other", "BUG", "REQ_LOGIC")
    check("refused: file a BUG with a REQ_ category", st >= 400 or b.get("error"),
          json.dumps(b)[:200])

    st, b = file_bug(bz, "fullfunding", "Cross-Cutting / Other",
                     "REQUIREMENT", "BUG_CODE")
    check("refused: file a REQUIREMENT with a BUG_ category",
          st >= 400 or b.get("error"), json.dumps(b)[:200])

    # An item filed onto the other lifecycle's entry point is canonicalised, not
    # rejected: Bugzilla itself forces UNCONFIRMED for reporters without
    # editbugs/canconfirm (Bug.pm:1526-1536), which would otherwise dump every
    # externally filed REQUIREMENT onto the BUG lifecycle.
    st, b = file_bug(bz, "fullfunding", "Cross-Cutting / Other",
                     "REQUIREMENT", "REQ_COSMETIC", extra={"status": "UNCONFIRMED"})
    if check("file a REQUIREMENT asking for UNCONFIRMED", st < 400 and b.get("id"),
             json.dumps(b)[:200]):
        eq("REQUIREMENT was canonicalised to REQ_DRAFT",
           state_of(bz, b["id"])[0], "REQ_DRAFT")
        return b["id"]
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://192.168.100.252:8092")
    parser.add_argument("--state", default="expected-state.json")
    parser.add_argument("--api-key", default=os.environ.get("BZ_API_KEY", ""))
    parser.add_argument("--no-smoke", action="store_true",
                        help="structural assertions only; files no bugs")
    args = parser.parse_args()

    with open(args.state, encoding="utf-8") as fh:
        state = json.load(fh)

    if not args.api_key and not args.no_smoke:
        sys.exit("An API key is required (--api-key or BZ_API_KEY): this "
                 "installation has requirelogin enabled.")

    bz = Bz(args.base_url, args.api_key)

    verify_version(bz, state)
    fields = fields_by_name(bz)
    verify_products(bz, state)
    verify_custom_fields(bz, fields, state)
    verify_statuses_and_workflow(bz, fields, state)
    verify_resolutions(bz, fields, state)

    smoke_ids = []
    if not args.no_smoke:
        smoke_ids = [i for i in (verify_bug_lifecycle(bz, state),
                                 verify_requirement_lifecycle(bz, state),
                                 verify_creation_guards(bz)) if i]

    print(f"\n{'=' * 60}")
    if smoke_ids:
        print(f"smoke test bugs: {', '.join(str(i) for i in smoke_ids)}")
    if failures:
        print(f"FAILED  {len(failures)}/{checks} checks failed:")
        for name in failures:
            print(f"  - {name}")
        sys.exit(1)
    print(f"OK  all {checks} checks passed")


if __name__ == "__main__":
    main()
