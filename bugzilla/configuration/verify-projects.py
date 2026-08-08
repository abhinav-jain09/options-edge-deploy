#!/usr/bin/env python3
"""Verify the internal Bugzilla against expected-state.json.

Two layers, because metadata alone proves nothing:

  * structural - every product, component, version, milestone, custom field,
    field value, status, workflow edge, resolution and parameter is read back
    over REST (i.e. through a *restarted* web process, so a stale cache shows up
    as a failure) and compared with the declared state;

  * behavioural - a BUG and a REQUIREMENT are filed and walked through their
    complete lifecycles, and every cross-lifecycle move that core Bugzilla would
    otherwise permit is attempted and must be refused with the exact error code
    the policy raises. This is the only thing that proves the IssueTypeWorkflow
    extension is loaded and enforcing.

The negative cases are GENERATED from expected-state.json, not hard-coded: for
each state of each walk, the illegal targets are exactly
(reachable in the global workflow) minus (allowed for this issue type). Targets
the global matrix already blocks are excluded on purpose - core would refuse
those with the extension switched off, so they prove nothing.

Refusals are checked against specific codes (115 for a denied update, the
extension's own 1000xx codes on creation). A 401 from a bad API key, a 404 or a
500 must never be able to masquerade as successful enforcement.

Exits non-zero if anything fails. It changes no configuration, but it does file
smoke-test bugs (summary prefix [SMOKE]) and closes them again on the way out.

Run it ON the Bugzilla host against loopback: the API key is sent as a header,
and this instance speaks plain HTTP, so a remote run exposes an admin key to
anyone who can see that segment. A non-loopback http:// URL needs
--allow-remote-http.

  read -rs BZ_API_KEY; export BZ_API_KEY
  ./verify-projects.py --state expected-state.json
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

FIELD_TYPE_SINGLE_SELECT = 2
HTTP_DENIED = 401   # REST_STATUS_CODE_MAP maps code 115 to STATUS_NOT_AUTHORIZED
HTTP_BAD_REQUEST = 400

failures = []
checks = 0


class BzTransportError(RuntimeError):
    """A request failed in a way that says nothing about Bugzilla's policy."""


def check(name, ok, detail=""):
    global checks
    checks += 1
    if ok:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name}" + (f"\n          {detail}" if detail else ""))
        failures.append(name)
    return bool(ok)


def eq(name, got, want):
    return check(name, got == want, f"got {got!r}, want {want!r}")


def skip(name, why):
    print(f"  ....  {name} - {why}")


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
                status, raw = resp.status, resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            try:
                status, raw = exc.code, exc.read().decode("utf-8")
            except UnicodeDecodeError as decode_exc:
                raise BzTransportError(
                    f"{method} {url}: HTTP {exc.code} with undecodable body"
                ) from decode_exc
        except (urllib.error.URLError, OSError) as exc:
            raise BzTransportError(f"{method} {url}: {exc}") from exc
        except UnicodeDecodeError as exc:
            raise BzTransportError(f"{method} {url}: undecodable body") from exc

        try:
            body = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError as exc:
            raise BzTransportError(
                f"{method} {url}: HTTP {status} with non-JSON body: {raw[:200]!r}"
            ) from exc
        if not isinstance(body, dict):
            raise BzTransportError(f"{method} {url}: expected a JSON object, got {type(body).__name__}")
        return status, body

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


def verify_parameters(bz, state):
    print("\n[2] Parameters")
    status, body = bz.get("/parameters")
    params = body.get("parameters")
    if not check("parameters readable (API key needs tweakparams/admin)",
                 isinstance(params, dict) and params,
                 f"HTTP {status}: {json.dumps(body)[:200]}"):
        return
    for name, want in state["params"].items():
        if name not in params:
            check(f"param {name} visible", False,
                  "not returned; the API key's user is probably not an admin")
            continue
        eq(f"param {name}", params[name], want)


def verify_products(bz, state):
    print("\n[3] Products, components, versions, milestones")
    for spec in state["products"]:
        name = spec["name"]
        _, body = bz.get("/product?names=" + urllib.parse.quote(name))
        products = body.get("products") or []
        if not check(f"product '{name}' exists", len(products) == 1,
                     json.dumps(body)[:300]):
            continue
        product = products[0]

        eq(f"product '{name}' description", product.get("description"),
           spec["description"])
        eq(f"product '{name}' classification", product.get("classification"),
           spec["classification"])
        eq(f"product '{name}' is_active", bool(product.get("is_active")),
           spec["is_active"])
        eq(f"product '{name}' has_unconfirmed",
           bool(product.get("has_unconfirmed")), spec["allows_unconfirmed"])
        eq(f"product '{name}' default_milestone",
           product.get("default_milestone"), spec["default_milestone"])

        components = product.get("components", [])
        eq(f"product '{name}' components", sorted(c["name"] for c in components),
           sorted(c["name"] for c in spec["components"]))

        by_name = {c["name"]: c for c in components}
        for want in spec["components"]:
            got = by_name.get(want["name"])
            if got is None:
                continue
            eq(f"component '{name}/{want['name']}' description",
               got.get("description"), want["description"])
            check(f"component '{name}/{want['name']}' is active",
                  bool(got.get("is_active")))
            check(f"component '{name}/{want['name']}' has a default assignee",
                  bool(got.get("default_assigned_to")),
                  f"default_assigned_to={got.get('default_assigned_to')!r}")

        eq(f"product '{name}' versions",
           sorted(v["name"] for v in product.get("versions", [])
                  if v.get("is_active", True)),
           sorted(spec["versions"]))
        eq(f"product '{name}' milestones",
           sorted(m["name"] for m in product.get("milestones", [])
                  if m.get("is_active", True)),
           sorted(spec["milestones"]))

    for name in state["decommission"]["products"]:
        _, body = bz.get("/product?names=" + urllib.parse.quote(name))
        gone = not (body.get("products") or [])
        inactive = not gone and not body["products"][0].get("is_active")
        check(f"decommissioned product '{name}' is gone or inactive",
              gone or inactive)


def fields_by_name(bz):
    status, body = bz.get("/field/bug")
    if "fields" not in body:
        raise BzTransportError(
            f"cannot read /rest/field/bug (HTTP {status}): {json.dumps(body)[:300]}")
    return {f["name"]: f for f in body["fields"]}


def verify_custom_fields(fields, state):
    print("\n[4] Custom fields")
    for name, spec in state["fields"].items():
        field = fields.get(name)
        if not check(f"field {name} exists", field is not None):
            continue

        eq(f"field {name} is_custom", bool(field.get("is_custom")), True)
        eq(f"field {name} type", field.get("type"), FIELD_TYPE_SINGLE_SELECT)
        eq(f"field {name} display_name", field.get("display_name"),
           spec["description"])
        eq(f"field {name} is_mandatory", bool(field.get("is_mandatory")),
           spec["is_mandatory"])
        eq(f"field {name} is_on_bug_entry", bool(field.get("is_on_bug_entry")),
           spec["enter_bug"])

        eq(f"field {name} value_field", field.get("value_field"),
           spec.get("value_field"))
        eq(f"field {name} visibility_field", field.get("visibility_field"),
           spec.get("visibility_field"))
        eq(f"field {name} visibility_values",
           sorted(field.get("visibility_values") or []),
           sorted(spec.get("visibility_values") or []))

        values = {v["name"]: v for v in field.get("values", [])}
        eq(f"field {name} values",
           sorted(n for n in values if n not in ("", "---")),
           sorted(v["value"] for v in spec["values"]))

        for want in spec["values"]:
            got = values.get(want["value"])
            if got is None:
                continue
            eq(f"field {name} value '{want['value']}' sort_key",
               got.get("sort_key"), want["sortkey"])
            # Bugzilla 5.2 stores one controller per value, so this list is
            # either empty or exactly one issue type long.
            eq(f"field {name} value '{want['value']}' controlled_by",
               got.get("visibility_values") or [],
               [want["controlled_by"]] if "controlled_by" in want else [])


def verify_statuses_and_workflow(fields, state):
    print("\n[5] Statuses and workflow matrix")
    field = fields.get("bug_status")
    if not check("bug_status field present", field is not None):
        return

    values = {v["name"]: v for v in field.get("values", [])}
    eq("status set", sorted(n for n in values if n != ""),
       sorted(s["value"] for s in state["statuses"]))

    for spec in state["statuses"]:
        got = values.get(spec["value"])
        if got is None:
            continue
        eq(f"status '{spec['value']}' is_open", bool(got.get("is_open")),
           spec["is_open"])
        eq(f"status '{spec['value']}' sort_key", got.get("sort_key"),
           spec["sortkey"])

    # Compared as sorted LISTS, not dicts: status_workflow's UNIQUE index does
    # not constrain rows whose old_status is NULL, so the creation row can be
    # duplicated - and a dict would silently collapse the duplicate away.
    want_edges = {}
    for edge in state["workflow"]:
        want_edges.setdefault(edge["from"], []).append(
            (edge["to"], bool(edge["require_comment"])))

    for from_status in sorted(set(list(want_edges) + list(values))):
        got_value = values.get(from_status)
        if got_value is None:
            check(f"workflow row for '{from_status}'", False, "status missing")
            continue
        got = sorted((t["name"], bool(t.get("comment_required")))
                     for t in got_value.get("can_change_to", []))
        eq(f"workflow from '{from_status or '(new bug)'}'", got,
           sorted(want_edges.get(from_status, [])))


def verify_resolutions(fields, state):
    print("\n[6] Resolutions")
    field = fields.get("resolution")
    if not check("resolution field present", field is not None):
        return
    values = {v["name"]: v for v in field.get("values", [])}
    eq("resolution set", sorted(n for n in values if n != ""),
       sorted(r["value"] for r in state["resolutions"]))
    for spec in state["resolutions"]:
        got = values.get(spec["value"])
        if got is not None:
            eq(f"resolution '{spec['value']}' sort_key", got.get("sort_key"),
               spec["sortkey"])


# --------------------------------------------------------------------------
# behavioural
# --------------------------------------------------------------------------

SMOKE_PREFIX = "[SMOKE] issue-type lifecycle verification"


class Walker:
    """Files smoke items and drives them, asserting policy at every step."""

    def __init__(self, bz, state):
        self.bz = bz
        self.state = state
        self.enf = state["enforcement"]
        self.denied_code = self.enf["error_codes"]["core_illegal_change"]
        self.creation_codes = set(self.enf["error_codes"]["extension"].values())
        self.filed = []

        self.reachable = {}
        for edge in state["workflow"]:
            self.reachable.setdefault(edge["from"], set()).add(edge["to"])

        self.categories = {}
        for value in state["fields"]["cf_category"]["values"]:
            self.categories.setdefault(value["controlled_by"], []).append(
                value["value"])

    # -- helpers ---------------------------------------------------------

    def file(self, product, component, issue_type, category, extra=None):
        payload = {
            "product": product,
            "component": component,
            "summary": f"{SMOKE_PREFIX} - {issue_type}",
            "version": self.state["products"][0]["versions"][0],
            "description": "Filed by verify-projects.py. Closed automatically.",
            "cf_issue_type": issue_type,
            "cf_category": category,
        }
        payload.update(extra or {})
        status, body = self.bz.post("/bug", payload)
        if status < 400 and body.get("id"):
            self.filed.append(body["id"])
        return status, body

    def move(self, bug_id, **fields):
        payload = {"comment": {"body": "verify-projects.py lifecycle step"}}
        payload.update(fields)
        return self.bz.put(f"/bug/{bug_id}", payload)

    def state_of(self, bug_id):
        status, body = self.bz.get(f"/bug/{bug_id}")
        bugs = body.get("bugs")
        if status >= 400 or not bugs:
            raise BzTransportError(
                f"cannot read bug {bug_id} (HTTP {status}): {json.dumps(body)[:200]}")
        bug = bugs[0]
        missing = [k for k in ("status", "resolution", "cf_issue_type",
                               "cf_category") if k not in bug]
        if missing:
            raise BzTransportError(
                f"bug {bug_id} is missing expected field(s) {missing}: the "
                "custom fields are probably not provisioned")
        return (bug["status"], bug["resolution"], bug["cf_issue_type"],
                bug["cf_category"])

    def expect_allowed(self, label, bug_id, **fields):
        status, body = self.move(bug_id, **fields)
        check(f"allowed: {label}", status < 400 and not body.get("error"),
              f"HTTP {status}: {json.dumps(body)[:200]}")

    def expect_denied(self, label, bug_id, **fields):
        """The policy - not core, not the transport - must refuse this."""
        before = self.state_of(bug_id)
        status, body = self.move(bug_id, **fields)
        code = body.get("code")
        check(f"denied: {label}",
              status == HTTP_DENIED and code == self.denied_code,
              f"expected HTTP {HTTP_DENIED} code {self.denied_code}, "
              f"got HTTP {status} code {code}: {body.get('message', '')[:160]}")
        eq(f"unchanged after denying: {label}", self.state_of(bug_id), before)

    def expect_creation_refused(self, label, expected_error, **kwargs):
        want = self.enf["error_codes"]["extension"][expected_error]
        status, body = self.file(**kwargs)
        code = body.get("code")
        check(f"refused at creation: {label}",
              status == HTTP_BAD_REQUEST and code == want,
              f"expected HTTP {HTTP_BAD_REQUEST} code {want} "
              f"({expected_error}), got HTTP {status} code {code}: "
              f"{body.get('message', '')[:160]}")

    # -- generated probes -------------------------------------------------

    def probe_illegal_statuses(self, bug_id, issue_type, current):
        """Every target core would allow from here but this type must not use."""
        allowed = set(self.enf["allowed_statuses"][issue_type])
        illegal = sorted(self.reachable.get(current, set()) - allowed)
        if not illegal:
            skip(f"illegal status targets from '{current}' ({issue_type})",
                 "the global matrix already blocks them all; nothing here is "
                 "extension-only")
            return
        for target in illegal:
            self.expect_denied(f"{issue_type} on '{current}' -> '{target}'",
                               bug_id, status=target)

    def probe_illegal_resolutions(self, bug_id, issue_type):
        allowed = set(self.enf["allowed_resolutions"][issue_type])
        for resolution in sorted(
                {r["value"] for r in self.state["resolutions"]} - allowed):
            self.expect_denied(
                f"{issue_type} resolved '{resolution}'", bug_id,
                status="RESOLVED", resolution=resolution)

    def probe_allowed_resolutions(self, bug_id, issue_type, reopen_to):
        """Every resolution the type MAY use must actually be accepted."""
        for resolution in sorted(self.enf["allowed_resolutions"][issue_type]):
            if resolution == "DUPLICATE":
                continue  # set through dupe_of, covered separately
            self.expect_allowed(f"{issue_type} resolved '{resolution}'", bug_id,
                                status="RESOLVED", resolution=resolution)
            eq(f"{issue_type} is RESOLVED/{resolution}",
               self.state_of(bug_id)[:2], ("RESOLVED", resolution))
            self.expect_allowed(f"{issue_type} reopened from '{resolution}'",
                                bug_id, status=reopen_to, resolution="")

    def probe_allowed_categories(self, bug_id, issue_type):
        """Every category the type MAY use must actually be accepted."""
        for category in self.enf["allowed_categories"][issue_type]:
            self.expect_allowed(f"{issue_type} categorised '{category}'",
                                bug_id, cf_category=category)
            eq(f"{issue_type} category is '{category}'",
               self.state_of(bug_id)[3], category)

    def probe_illegal_categories(self, bug_id, issue_type):
        for other, values in self.categories.items():
            if other == issue_type:
                continue
            for value in values:
                self.expect_denied(f"{issue_type} given category '{value}'",
                                   bug_id, cf_category=value)

    def probe_type_immutable(self, bug_id, issue_type):
        for other in self.enf["allowed_statuses"]:
            if other != issue_type:
                self.expect_denied(f"{issue_type} retyped to '{other}'",
                                   bug_id, cf_issue_type=other)


def walk_lifecycle(w, issue_type, product, path, closing_resolution):
    """File one item and walk it along `path`, probing at every state."""
    print(f"\n[7] {issue_type} lifecycle ({product})")
    category = w.categories[issue_type][0]
    status, body = w.file(product, "Cross-Cutting / Other", issue_type, category)
    if not check(f"file a {issue_type}", status < 400 and body.get("id"),
                 f"HTTP {status}: {json.dumps(body)[:300]}"):
        return None
    bug_id = body["id"]
    print(f"  ....  bug {bug_id}")

    initial = w.enf["initial_status"][issue_type]
    eq(f"{issue_type} starts at '{initial}'", w.state_of(bug_id)[0], initial)

    current = initial
    w.probe_illegal_statuses(bug_id, issue_type, current)

    for target in path:
        w.expect_allowed(f"{issue_type} '{current}' -> '{target}'", bug_id,
                         status=target)
        eq(f"{issue_type} is '{target}'", w.state_of(bug_id)[0], target)
        current = target
        w.probe_illegal_statuses(bug_id, issue_type, current)

    reopen_to = "BUG_CONFIRMED" if issue_type == "BUG" else "REQ_REVIEW"
    w.probe_allowed_categories(bug_id, issue_type)
    w.probe_illegal_resolutions(bug_id, issue_type)
    w.probe_allowed_resolutions(bug_id, issue_type, reopen_to)
    w.expect_allowed(f"{issue_type} -> RESOLVED/{closing_resolution}", bug_id,
                     status="RESOLVED", resolution=closing_resolution)
    eq(f"{issue_type} is RESOLVED/{closing_resolution}",
       w.state_of(bug_id)[:2], ("RESOLVED", closing_resolution))

    w.probe_illegal_statuses(bug_id, issue_type, "RESOLVED")

    w.expect_allowed(f"{issue_type} -> VERIFIED", bug_id, status="VERIFIED")
    eq(f"{issue_type} is VERIFIED/{closing_resolution}",
       w.state_of(bug_id)[:2], ("VERIFIED", closing_resolution))

    # VERIFIED is where both reopen branches are reachable, so this is the state
    # that actually exercises the extension rather than the global matrix.
    w.probe_illegal_statuses(bug_id, issue_type, "VERIFIED")
    w.probe_illegal_categories(bug_id, issue_type)
    w.probe_type_immutable(bug_id, issue_type)

    if reopen_to in w.enf["allowed_statuses"][issue_type]:
        w.expect_allowed(f"{issue_type} reopens to '{reopen_to}'", bug_id,
                         status=reopen_to, resolution="")
        eq(f"reopened {issue_type} has no resolution",
           w.state_of(bug_id)[:2], (reopen_to, ""))

    return bug_id


def verify_creation_guards(w):
    print("\n[8] Creation-time guards")
    product = "optionedge"
    component = "Cross-Cutting / Other"

    w.expect_creation_refused(
        "BUG with a REQUIREMENT category", "issue_category_mismatch",
        product=product, component=component, issue_type="BUG",
        category=w.categories["REQUIREMENT"][0])
    w.expect_creation_refused(
        "REQUIREMENT with a BUG category", "issue_category_mismatch",
        product="fullfunding", component=component, issue_type="REQUIREMENT",
        category=w.categories["BUG"][0])
    w.expect_creation_refused(
        "no issue type", "issue_type_required", product=product,
        component=component, issue_type="---", category=w.categories["BUG"][0])
    w.expect_creation_refused(
        "no category", "issue_category_required", product=product,
        component=component, issue_type="BUG", category="---")

    # An unknown-but-syntactically-valid type: core's own select validation
    # rejects a value that is not in the field, so this asserts the FIRST
    # refusal wins rather than our own code - which is why it is checked
    # loosely, as a refusal, not as issue_type_unknown.
    status, body = w.file(product, component, "NOT_A_TYPE",
                          w.categories["BUG"][0])
    check("refused at creation: unknown issue type",
          status >= 400 and body.get("error"),
          f"HTTP {status}: {json.dumps(body)[:200]}")

    print("  ....  issue_type_initial_status_unavailable and "
          "issue_type_initial_status_needs_comment are NOT exercised: both "
          "require deliberately breaking the workflow first")

    # Filing onto the other lifecycle's entry point is corrected, not rejected:
    # Bug.pm:1526-1536 forces UNCONFIRMED on any reporter without
    # editbugs/canconfirm, so rejecting it would break external filing. This
    # also stands in for the low-privilege-reporter case, which needs a second
    # account the verifier does not create.
    status, body = w.file("fullfunding", component, "REQUIREMENT",
                          w.categories["REQUIREMENT"][0],
                          extra={"status": "UNCONFIRMED"})
    if check("file a REQUIREMENT asking for UNCONFIRMED",
             status < 400 and body.get("id"), f"HTTP {status}: {json.dumps(body)[:200]}"):
        eq("REQUIREMENT was canonicalised to REQ_DRAFT",
           w.state_of(body["id"])[0], "REQ_DRAFT")


def verify_low_privilege_reporter(bz, state, w, key_env):
    """The one path a privileged smoke test cannot reach.

    Bug.pm:1526-1536 overrides the requested initial status to UNCONFIRMED for
    any reporter without editbugs/canconfirm - which is exactly how an external
    stakeholder files. The canonicalisation in bug_end_of_create_validators is
    what puts such a REQUIREMENT back on its own lifecycle, so this deserves a
    real unprivileged account rather than an admin pretending.
    """
    print("\n[9] Low-privilege reporter")
    if not key_env:
        check("low-privilege reporter path proven", False,
              "not run: pass --reporter-api-key-env NAME with the API key of a "
              "user lacking editbugs/canconfirm. The admin-side probe in [8] "
              "exercises the canonicalisation but NOT the privilege-dependent "
              "override, so this is unproven.")
        return

    key = os.environ.get(key_env, "")
    if not check(f"${key_env} is set", bool(key)):
        return

    reporter = Bz(bz.base, key)
    payload = {
        "product": "fullfunding",
        "component": "Cross-Cutting / Other",
        "summary": f"{SMOKE_PREFIX} - REQUIREMENT filed unprivileged",
        "version": state["products"][0]["versions"][0],
        "description": "Filed by verify-projects.py as an unprivileged user.",
        "cf_issue_type": "REQUIREMENT",
        "cf_category": w.categories["REQUIREMENT"][0],
    }
    status, body = reporter.post("/bug", payload)
    if not check("unprivileged user can file a REQUIREMENT",
                 status < 400 and body.get("id"),
                 f"HTTP {status}: {json.dumps(body)[:200]}"):
        return
    w.filed.append(body["id"])
    eq("unprivileged REQUIREMENT still starts at REQ_DRAFT",
       w.state_of(body["id"])[0], "REQ_DRAFT")


def verify_duplicate_path(w):
    """RESOLVED+DUPLICATE must keep working for both types - it is the reason
    the closed tail is shared in the first place."""
    print("\n[10] Native duplicate handling")
    for issue_type, product in (("BUG", "optionedge"),
                                ("REQUIREMENT", "fullfunding")):
        category = w.categories[issue_type][0]
        s1, b1 = w.file(product, "Cross-Cutting / Other", issue_type, category)
        s2, b2 = w.file(product, "Cross-Cutting / Other", issue_type, category)
        if not check(f"file two {issue_type}s for the duplicate test",
                     s1 < 400 and s2 < 400 and b1.get("id") and b2.get("id")):
            continue
        status, body = w.move(b2["id"], dupe_of=b1["id"])
        check(f"{issue_type} can be marked a duplicate",
              status < 400 and not body.get("error"),
              f"HTTP {status}: {json.dumps(body)[:200]}")
        eq(f"duplicate {issue_type} is RESOLVED/DUPLICATE",
           w.state_of(b2["id"])[:2], ("RESOLVED", "DUPLICATE"))


def verify_bulk_atomicity(w):
    """One illegal item in a multi-bug update must abort the whole request."""
    print("\n[11] Bulk update atomicity")
    s1, b1 = w.file("optionedge", "Cross-Cutting / Other", "BUG",
                    w.categories["BUG"][0])
    s2, b2 = w.file("fullfunding", "Cross-Cutting / Other", "REQUIREMENT",
                    w.categories["REQUIREMENT"][0])
    if not check("file one item of each type for the bulk test",
                 s1 < 400 and s2 < 400 and b1.get("id") and b2.get("id")):
        return
    bug_id, req_id = b1["id"], b2["id"]

    # Park both on VERIFIED, the one state from which BUG_CONFIRMED is reachable
    # in the global matrix for both - so the refusal below can only come from
    # the extension.
    for target in ("BUG_CONFIRMED", "RESOLVED", "VERIFIED"):
        w.move(bug_id, status=target,
               **({"resolution": "FIXED"} if target == "RESOLVED" else {}))
    for target in ("REQ_REVIEW", "REQ_APPROVED", "RESOLVED", "VERIFIED"):
        w.move(req_id, status=target,
               **({"resolution": "IMPLEMENTED"} if target == "RESOLVED" else {}))

    before_bug, before_req = w.state_of(bug_id), w.state_of(req_id)
    if not check("both bulk-test items reached VERIFIED",
                 before_bug[0] == "VERIFIED" and before_req[0] == "VERIFIED",
                 f"bug={before_bug}, requirement={before_req}"):
        return

    status, body = w.bz.put(f"/bug/{bug_id}", {
        "ids": [bug_id, req_id],
        "status": "BUG_CONFIRMED",
        "resolution": "",
        "comment": {"body": "verify-projects.py bulk atomicity probe"},
    })
    check("bulk update containing one illegal item is refused",
          status == HTTP_DENIED and body.get("code") == w.denied_code,
          f"HTTP {status} code {body.get('code')}: {body.get('message','')[:160]}")
    eq("legal item in the bulk update was NOT changed", w.state_of(bug_id),
       before_bug)
    eq("illegal item in the bulk update was NOT changed", w.state_of(req_id),
       before_req)


def close_smoke_bugs(w):
    """Leave the tracker tidy - REST cannot delete bugs, so close them."""
    if not w.filed:
        return
    print("\n[12] Cleanup")
    closed, stuck = 0, []
    for bug_id in w.filed:
        try:
            status, resolution, issue_type, _ = w.state_of(bug_id)
            if status in ("RESOLVED", "VERIFIED") and resolution:
                closed += 1
                continue
            wanted = "INVALID" if issue_type == "BUG" else "REJECTED"
            code, body = w.move(bug_id, status="RESOLVED", resolution=wanted)
            if code < 400 and not body.get("error"):
                closed += 1
            else:
                stuck.append(bug_id)
        except BzTransportError as exc:
            stuck.append(f"{bug_id} ({exc})")
    check(f"all {len(w.filed)} smoke bug(s) closed", not stuck,
          f"still open, close by hand: {stuck}")
    print(f"  ....  {closed}/{len(w.filed)} smoke bug(s) closed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8092")
    parser.add_argument("--allow-remote-http", action="store_true",
                        help="permit a non-loopback plain-HTTP base URL "
                             "(sends the API key in clear over the network)")
    parser.add_argument("--state", default="expected-state.json")
    parser.add_argument("--no-smoke", action="store_true",
                        help="structural assertions only; files no bugs")
    parser.add_argument("--reporter-api-key-env", default="",
                        help="name of an env var holding the API key of a user "
                             "WITHOUT editbugs/canconfirm. Supplying it proves "
                             "the privilege-dependent status override in "
                             "Bug.pm:1526-1536 really is corrected; without it "
                             "that path is only covered by proxy.")
    args = parser.parse_args()

    # Deliberately env-only: an --api-key argument would show up in the process
    # list and shell history on a shared host.
    api_key = os.environ.get("BZ_API_KEY", "")
    if not api_key:
        sys.exit("BZ_API_KEY is required: this installation has requirelogin "
                 "enabled, so even read-only endpoints need authentication.")

    parsed = urllib.parse.urlparse(args.base_url)
    if (parsed.scheme == "http"
            and parsed.hostname not in ("localhost", "127.0.0.1", "::1")
            and not args.allow_remote_http):
        sys.exit(f"refusing to send the API key in clear to {parsed.hostname}: "
                 "run this on the Bugzilla host against localhost, use https, "
                 "or pass --allow-remote-http if you accept the exposure.")

    try:
        with open(args.state, encoding="utf-8") as fh:
            state = json.load(fh)
    except OSError as exc:
        sys.exit(f"cannot read {args.state}: {exc}")
    except json.JSONDecodeError as exc:
        sys.exit(f"{args.state} is not valid JSON: {exc}")

    bz = Bz(args.base_url, api_key)

    try:
        verify_version(bz, state)
        verify_parameters(bz, state)
        verify_products(bz, state)
        fields = fields_by_name(bz)
        verify_custom_fields(fields, state)
        verify_statuses_and_workflow(fields, state)
        verify_resolutions(fields, state)
    except BzTransportError as exc:
        sys.exit(f"\nABORTED: {exc}")

    structural_failures = len(failures)
    walker = None
    if args.no_smoke:
        print("\n--no-smoke: behavioural verification skipped. The extension is "
              "NOT proven to be enforcing.")
    elif structural_failures:
        print(f"\n{structural_failures} structural failure(s): skipping the "
              "behavioural pass rather than filing bugs into a known-broken "
              "installation.")
    else:
        walker = Walker(bz, state)
        try:
            walk_lifecycle(walker, "BUG", "optionedge",
                           ["BUG_CONFIRMED", "BUG_IN_PROGRESS"], "FIXED")
            walk_lifecycle(walker, "REQUIREMENT", "fullfunding",
                           ["REQ_REVIEW", "REQ_APPROVED", "REQ_IN_PROGRESS"],
                           "IMPLEMENTED")
            verify_creation_guards(walker)
            verify_low_privilege_reporter(bz, state, walker,
                                          args.reporter_api_key_env)
            verify_duplicate_path(walker)
            verify_bulk_atomicity(walker)
        except BzTransportError as exc:
            check("behavioural pass completed", False, str(exc))
        finally:
            try:
                close_smoke_bugs(walker)
            except BzTransportError as exc:
                print(f"  ....  cleanup aborted: {exc}")

    print(f"\n{'=' * 60}")
    if walker and walker.filed:
        print("smoke test bugs: "
              + ", ".join(str(i) for i in walker.filed))
    if failures:
        print(f"FAILED  {len(failures)}/{checks} checks failed:")
        for name in failures:
            print(f"  - {name}")
        sys.exit(1)
    print(f"OK  all {checks} checks passed")


if __name__ == "__main__":
    main()
