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

Run it ON the Bugzilla host against loopback. This instance speaks plain HTTP,
so a remote run exposes an admin key to anyone who can see that segment. A
non-loopback http:// URL needs --allow-remote-http.

HOW THE KEY IS SENT, AND WHY IT ENDS UP IN A LOG
------------------------------------------------
Bugzilla 5.2 has no X-BUGZILLA-API-KEY header. Auth/Login/APIKey.pm reads the
key from `Bugzilla->input_params->{Bugzilla_api_key}` and nothing else, and
WebService/Server/REST.pm parses a JSON body only when the method is NOT GET
(_retrieve_json_params). So on a GET - which is every read this script makes -
the key can only travel in the QUERY STRING, and Apache's combined log format
records the query string.

That means each run writes the admin API key into access.log in clear text.
This is a property of Bugzilla 5.2, not of this script; the header approach
this file used to describe never worked here and silently fell back to
anonymous, unauthenticated requests.

Treat the key as disclosed to anyone who can read that log, and prefer one of:
  * redact query strings for this vhost, e.g. log "%m %U" instead of "%r";
  * or rotate the key after each run (Preferences -> API Keys).

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


warnings = []


def _unset(value):
    """Bugzilla stores an unset select as '---' (varchar(64) NOT NULL DEFAULT
    '---'), so normalise that and the empty string to the same thing."""
    return "" if value in (None, "", "---") else value


def skip(name, why):
    print(f"  ....  {name} - {why}")


def warn(name, why, expected=False):
    """Something this run could not prove.

    `expected=True` marks a warning the runbook says to expect at this point
    (TestProduct before it is retired), so --strict does not turn the
    documented sequence into an impossible one.
    """
    print(f"  WARN  {name}\n          {why}")
    warnings.append((name, expected))


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """urllib copies custom headers across redirects, API key included, so a
    loopback endpoint could bounce the key to a remote host and sail past the
    --allow-remote-http check. Refuse redirects outright."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise BzTransportError(
            f"refusing to follow a {code} redirect to {newurl}: that would "
            "send the API key to another origin")


def require_shape(state):
    """Fail with one clear sentence rather than a KeyError six calls deep."""
    if not isinstance(state, dict):
        raise ValueError("the state file must contain a JSON object")

    def need(container, key, kind, where):
        if key not in container:
            raise ValueError(f"missing {where}{key}")
        if not isinstance(container[key], kind):
            raise ValueError(
                f"{where}{key} must be {kind.__name__}, got "
                f"{type(container[key]).__name__}")
        return container[key]

    for key, kind in (("spec_version", str), ("bugzilla_version", str),
                      ("issue_types", list), ("params", dict),
                      ("fields", dict), ("statuses", list), ("workflow", list),
                      ("resolutions", list), ("products", list),
                      ("enforcement", dict), ("decommission", dict)):
        need(state, key, kind, "")

    enf = state["enforcement"]
    for key in ("initial_status", "initial_statuses", "allowed_statuses",
                "allowed_resolutions", "allowed_categories", "error_codes"):
        need(enf, key, dict, "enforcement.")
    need(enf["error_codes"], "core_illegal_change", int, "enforcement.error_codes.")
    need(enf["error_codes"], "extension", dict, "enforcement.error_codes.")
    need(state["fields"], "cf_category", dict, "fields.")
    need(state["decommission"], "products", list, "decommission.")

    for status in state["statuses"]:
        need(status, "value", str, "statuses[].")
        need(status, "is_open", bool, "statuses[].")
        need(status, "sortkey", int, "statuses[].")
    for edge in state["workflow"]:
        need(edge, "from", str, "workflow[].")
        need(edge, "to", str, "workflow[].")
        need(edge, "require_comment", bool, "workflow[].")
    for resolution in state["resolutions"]:
        need(resolution, "value", str, "resolutions[].")
    for name, field in state["fields"].items():
        for key, kind in (("description", str), ("type", str),
                          ("is_mandatory", bool), ("enter_bug", bool),
                          ("buglist", bool), ("values", list)):
            need(field, key, kind, f"fields.{name}.")
        for value in field["values"]:
            need(value, "value", str, f"fields.{name}.values[].")
            need(value, "sortkey", int, f"fields.{name}.values[].")
        if name == "cf_category":
            for value in field["values"]:
                need(value, "issue_type", str, "fields.cf_category.values[].")

    for product in state["products"]:
        # No issue_type here: the type is a per-BUG field, so every product
        # holds both kinds. An earlier design derived it from the product and
        # this check outlived it.
        for key in ("name", "description", "classification",
                    "default_milestone"):
            need(product, key, str, "products[].")
        for key in ("is_active", "allows_unconfirmed"):
            need(product, key, bool, "products[].")
        for key in ("components", "versions", "milestones"):
            need(product, key, list, f"products[{product.get('name')}].")


class Bz:
    def __init__(self, base_url, api_key):
        self.base = base_url.rstrip("/")
        self.api_key = api_key
        # ProxyHandler({}) disables the environment proxy urllib would
        # otherwise install: a proxy would see the API key even for loopback.
        self.opener = urllib.request.build_opener(
            _NoRedirect, urllib.request.ProxyHandler({}))

    def _request(self, method, path, payload=None):
        # Bugzilla 5.2 reads the key from input_params only (Auth/Login/
        # APIKey.pm), and REST parses a body only for non-GET requests
        # (_retrieve_json_params). So a GET must carry it in the query string;
        # for anything else it goes in the body, where no log will see it.
        # See the module docstring - each GET therefore writes the key into
        # Apache's access log.
        payload_out = payload
        url = f"{self.base}/rest{path}"
        if self.api_key:
            if method == "GET":
                sep = "&" if "?" in url else "?"
                url += sep + urllib.parse.urlencode(
                    {"Bugzilla_api_key": self.api_key})
            else:
                payload_out = dict(payload or {})
                payload_out["Bugzilla_api_key"] = self.api_key

        data = (json.dumps(payload_out).encode()
                if payload_out is not None else None)
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")

        try:
            with self.opener.open(req, timeout=30) as resp:
                status, raw = resp.status, resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            try:
                status, raw = exc.code, exc.read().decode("utf-8")
            except (UnicodeDecodeError, OSError) as read_exc:
                raise BzTransportError(
                    f"{method} {url}: HTTP {exc.code}, but its body could not "
                    f"be read ({read_exc})"
                ) from read_exc
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

def ok_response(name, status, body, key):
    """Structural absence and equality only mean something when the response
    actually succeeded and carries the shape we expect."""
    return check(f"{name}: response is usable",
                 status < 400 and isinstance(body, dict) and key in body,
                 f"HTTP {status}: {json.dumps(body)[:200]}")


def verify_version(bz, state):
    print("\n[1] Bugzilla version")
    status, body = bz.get("/version")
    if ok_response("version", status, body, "version"):
        eq("version", body.get("version"), state["bugzilla_version"])


def verify_parameters(bz, state):
    print("\n[2] Parameters")
    status, body = bz.get("/parameters")
    if not ok_response("parameters", status, body, "parameters"):
        return
    params = body["parameters"]
    if not check("parameters readable (API key needs tweakparams/admin)",
                 isinstance(params, dict) and params,
                 f"got {json.dumps(body)[:200]}"):
        return
    for name, want in state["params"].items():
        if name not in params:
            check(f"param {name} visible", False,
                  "not returned; the API key's user is probably not an admin")
            continue
        # Bugzilla serialises every parameter as a string, but a boolean can
        # come back as 0/1 or ''/1 depending on how it was written. Compare the
        # normalised scalars so a converged installation cannot fail on typing.
        eq(f"param {name}", _param_scalar(params[name]), _param_scalar(want))


def _param_scalar(value):
    if isinstance(value, bool):
        return "1" if value else "0"
    text = str(value).strip()
    return {"": "0", "false": "0", "true": "1"}.get(text.lower(), text)


def verify_products(bz, state, require_decommissioned):
    print("\n[3] Products, components, versions, milestones")
    for spec in state["products"]:
        name = spec["name"]
        status, body = bz.get("/product?names=" + urllib.parse.quote(name))
        if not ok_response(f"product '{name}'", status, body, "products"):
            continue
        products = body["products"]
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
            if want.get("default_assignee"):
                eq(f"component '{name}/{want['name']}' default assignee",
                   got.get("default_assigned_to"), want["default_assignee"])
            else:
                check(f"component '{name}/{want['name']}' has a default assignee",
                      bool(got.get("default_assigned_to")),
                      f"default_assigned_to={got.get('default_assigned_to')!r}")

        # Not filtered by is_active: preflight rejects an undeclared version or
        # milestone whether or not it is active, so the two must agree.
        eq(f"product '{name}' versions",
           sorted(v["name"] for v in product.get("versions", [])),
           sorted(spec["versions"]))
        eq(f"product '{name}' milestones",
           sorted(m["name"] for m in product.get("milestones", [])),
           sorted(spec["milestones"]))

    for name in state["decommission"]["products"]:
        status, body = bz.get("/product?names=" + urllib.parse.quote(name))
        if not check(f"decommission check for '{name}' got a real answer",
                     status < 400 and "products" in body,
                     f"HTTP {status}: {json.dumps(body)[:200]}"):
            continue
        gone = not body["products"]
        if require_decommissioned:
            # Deletion, not deactivation: preflight rejects an undeclared
            # product whether or not it is active.
            check(f"decommissioned product '{name}' is gone", gone)
        elif not gone:
            warn(f"'{name}' is still present",
                 "expected until it is retired; re-run with "
                 "--require-decommissioned afterwards to assert it is gone",
                 expected=True)


def verify_nothing_undeclared(bz, state, fields):
    """The provisioner refuses to run against undeclared objects; the verifier
    has to agree, or an administrator could add a product or a custom field and
    still be told the installation is converged."""
    print("\n[3b] Nothing undeclared")

    declared = {p["name"] for p in state["products"]}
    declared |= set(state["decommission"]["products"])
    status, body = bz.get("/product?type=accessible&include_fields=name")
    # "No undeclared products" may only be concluded from a response that
    # actually listed them; a 401/404/500 would otherwise read as an empty set.
    if not check("product list is readable",
                 status < 400 and isinstance(body.get("products"), list),
                 f"HTTP {status}: {json.dumps(body)[:200]}"):
        return
    live = {p["name"] for p in body["products"]}
    eq("no undeclared products", sorted(live - declared), [])

    declared_fields = set(state["fields"])
    live_custom = {name for name, f in fields.items() if f.get("is_custom")}
    eq("no undeclared custom fields", sorted(live_custom - declared_fields), [])


def fields_by_name(bz):
    status, body = bz.get("/field/bug")
    if status >= 400 or not isinstance(body.get("fields"), list):
        raise BzTransportError(
            f"cannot read /rest/field/bug (HTTP {status}): {json.dumps(body)[:300]}")
    fields = {}
    for f in body["fields"]:
        if not isinstance(f, dict) or "name" not in f:
            raise BzTransportError(
                f"/rest/field/bug returned an entry without a name: {str(f)[:120]}")
        fields[f["name"]] = f
    return fields


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
            # either empty or exactly one entry long.
            #
            # A cf_category value is controlled by the issue type it belongs
            # to, so the form only offers categories of the item's own type.
            # Everything else in this model is uncontrolled. This used to
            # assert that NOTHING carried a controller, which was true of an
            # earlier design and is now the opposite of the requirement.
            controllers = got.get("visibility_values") or []
            want_controller = ([want["issue_type"]]
                               if want.get("issue_type") else [])
            eq(f"field {name} value '{want['value']}' value controller",
               controllers, want_controller)


def verify_statuses_and_workflow(fields, state):
    print("\n[5] Statuses and workflow matrix")
    field = fields.get("bug_status")
    if not check("bug_status field present", field is not None):
        return

    # REST returns the "no status" placeholder - the one carrying the
    # bug-creation edges - with a null name, while the state file spells that
    # same thing "". Normalise to "" so the two line up: keeping None made
    # sorting raise TypeError and aborted the run, and dropping the entry
    # instead left the creation edges unverifiable.
    values = {(v.get("name") or ""): v for v in field.get("values", [])}
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

        # Every product can hold either type, so a walk just needs one product.
        self.products = sorted(p["name"] for p in state["products"])
        self.product_of = {t: self.products for t in state["issue_types"]}

        self.reachable = {}
        for edge in state["workflow"]:
            self.reachable.setdefault(edge["from"], set()).add(edge["to"])

        self.categories = {}
        for value in state["fields"]["cf_category"]["values"]:
            self.categories.setdefault(value["issue_type"], []).append(
                value["value"])

    # -- helpers ---------------------------------------------------------

    def version_of(self, product):
        for spec in self.state["products"]:
            if spec["name"] == product:
                return spec["versions"][0]
        raise BzTransportError(f"product '{product}' is not declared")

    def file(self, product, component, issue_type, category, extra=None):
        """The type is a per-bug field again; the product does not imply it."""
        payload = {
            "product": product,
            "component": component,
            "summary": f"{SMOKE_PREFIX} - {issue_type}",
            "version": self.version_of(product),
            "description": "Filed by verify-projects.py. Closed automatically.",
            "cf_issue_type": issue_type,
            "cf_category": category,
            # This installation leaves defaultplatform/defaultopsys empty, so
            # Bugzilla refuses a filing that omits them ("You must select/enter
            # a OS"). The web form fills them in from the browser; a REST
            # client has to say so itself.
            "op_sys": "Other",
            "rep_platform": "Other",
            # Sent explicitly rather than relying on defaultpriority. That
            # param is verified separately, and a check that silently depends
            # on the thing it is checking proves nothing.
            "priority": "---",
            "severity": "normal",
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
        if (status >= 400 or not isinstance(bugs, list) or not bugs
                or not isinstance(bugs[0], dict)):
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

    def product_of_bug(self, bug_id):
        """Deliberately NOT part of state_of(): that tuple is the lifecycle
        state compared before and after every refusal, and the product is not
        part of it. Indexing state_of() for the product is how the old
        product-move probe came to compare a product name against an issue
        type."""
        status, body = self.bz.get(f"/bug/{bug_id}")
        bugs = body.get("bugs")
        if (status >= 400 or not isinstance(bugs, list) or not bugs
                or "product" not in bugs[0]):
            raise BzTransportError(
                f"cannot read the product of bug {bug_id} (HTTP {status})")
        return bugs[0]["product"]

    def expect_allowed(self, label, bug_id, **fields):
        status, body = self.move(bug_id, **fields)
        check(f"allowed: {label}", status < 400 and not body.get("error"),
              f"HTTP {status}: {json.dumps(body)[:200]}")

    def expect_denied_with(self, label, error, bug_id, **fields):
        """Refused by an error the extension raises itself, rather than by
        core's illegal_change - the product guard throws from
        bug_start_of_update, so it reports its own code."""
        want = self.enf["error_codes"]["extension"][error]
        before = self.state_of(bug_id)
        status, body = self.move(bug_id, **fields)
        code = body.get("code")
        check(f"denied: {label}",
              status == HTTP_BAD_REQUEST and code == want,
              f"expected HTTP {HTTP_BAD_REQUEST} code {want} ({error}), got "
              f"HTTP {status} code {code}: {body.get('message','')[:160]}")
        eq(f"unchanged after denying: {label}", self.state_of(bug_id), before)

    def expect_denied_status(self, label, bug_id, **fields):
        """A cross-lifecycle STATUS change, refused as an impossible
        transition. Pinned to that one code so a genuine privilege problem, a
        404 or a 500 cannot pass as enforcement."""
        want = self.enf["error_codes"]["core_illegal_bug_status_transition"]
        before = self.state_of(bug_id)
        status, body = self.move(bug_id, **fields)
        code = body.get("code")
        check(f"denied: {label}",
              status == HTTP_BAD_REQUEST and code == want,
              f"expected HTTP {HTTP_BAD_REQUEST} code {want}, got HTTP {status} "
              f"code {code}: {body.get('message','')[:160]}")
        eq(f"unchanged after denying: {label}", self.state_of(bug_id), before)

    def expect_denied_any(self, label, bug_id, **fields):
        """Refused, by any of THIS policy's refusals - core's illegal_change or
        one of the extension's own codes - and nothing moved.

        For submissions where several guards apply at once and the winner is
        decided by set_all's field ordering rather than by policy. A stray 404,
        500 or auth failure still fails: the code has to be one we recognise.
        """
        want = {self.enf["error_codes"]["core_illegal_change"],
                self.enf["error_codes"]["core_illegal_bug_status_transition"]}
        want |= set(self.enf["error_codes"]["extension"].values())
        before = self.state_of(bug_id)
        status, body = self.move(bug_id, **fields)
        code = body.get("code")
        check(f"denied: {label}",
              status in (HTTP_DENIED, HTTP_BAD_REQUEST) and code in want,
              f"expected HTTP {HTTP_DENIED}/{HTTP_BAD_REQUEST} with one of "
              f"{sorted(want)}, got HTTP {status} code {code}: "
              f"{body.get('message','')[:160]}")
        eq(f"unchanged after denying: {label}", self.state_of(bug_id), before)

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
            # A refused STATUS surfaces as illegal_bug_status_transition, not
            # illegal_change. The extension denies through priv_results, which
            # makes _refine_available_statuses drop the target from the
            # available list, so core rejects it as an impossible transition
            # before it ever weighs privileges. That filtering is deliberate -
            # it is what keeps the dropdown showing one lifecycle.
            self.expect_denied_status(
                f"{issue_type} on '{current}' -> '{target}'",
                bug_id, status=target)

    def probe_illegal_resolutions(self, bug_id, issue_type):
        # Its own code, not core's illegal_change: refusing a resolution
        # through priv_results made Bugzilla claim a permissions problem, which
        # was untrue and unactionable, so the extension throws instead.
        allowed = set(self.enf["allowed_resolutions"][issue_type])
        for resolution in sorted(
                {r["value"] for r in self.state["resolutions"]} - allowed):
            self.expect_denied_with(
                f"{issue_type} resolved '{resolution}'",
                "issue_type_resolution_mismatch", bug_id,
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
                self.expect_denied_with(
                    f"{issue_type} given category '{value}'",
                    "issue_category_mismatch", bug_id, cf_category=value)

    def probe_combined_illegal(self, bug_id, issue_type):
        """Several guarded fields changing at once, which is where a
        field-ordering bug inside set_all would show up."""
        other = next(t for t in self.enf["allowed_statuses"] if t != issue_type)
        other_status = next(
            (s for s in sorted(self.reachable.get("VERIFIED", set()))
             if s in self.enf["allowed_statuses"][other]
             and s not in self.enf["allowed_statuses"][issue_type]), None)
        if other_status:
            # Deliberately NOT pinned to one code. Several guards apply at
            # once and which fires first is set_all's field ordering, not
            # policy; asserting a particular code here would be asserting that
            # ordering. What must hold is that it is refused and nothing moved.
            self.expect_denied_any(
                f"{issue_type}: status+resolution+category all flipped to "
                f"{other}'s", bug_id, status=other_status, resolution="",
                cf_category=self.categories[other][0])
        self.expect_denied_with(
            f"{issue_type}: given {other}'s category",
            "issue_category_mismatch", bug_id,
            cf_category=self.categories[other][0])

        # Clearing the category must be ALLOWED: an empty category is what
        # leaves the pre-existing bugs untouched.
        self.expect_allowed(f"{issue_type}: category cleared", bug_id,
                            cf_category="---")
        eq(f"{issue_type}: category really is unset",
           _unset(self.state_of(bug_id)[3]), "")

    def probe_product_moves(self, bug_id, issue_type):
        """A move between products is ordinary, and must not disturb the type.

        The type is a per-bug field, so every product holds both kinds and no
        move is cross-lifecycle. This once asserted the opposite - that moving
        to another product was refused - because an earlier design derived the
        type FROM the product. What matters now is the other half: that the
        item keeps its type, status and category across the move, since those
        are what the lifecycle is enforced against.
        """
        home = self.product_of_bug(bug_id)
        elsewhere = [p for p in self.products if p != home]
        if not elsewhere:
            skip(f"{issue_type} product move", "only one product is declared")
            return

        for target in elsewhere:
            before = self.state_of(bug_id)
            self.expect_allowed(f"{issue_type} moved to '{target}'", bug_id,
                                product=target, component="General",
                                version=self.version_of(target))
            eq(f"{issue_type} really is in '{target}' now",
               self.product_of_bug(bug_id), target)
            # The whole lifecycle state - status, resolution, type, category -
            # must survive a move untouched. Only the product may differ.
            eq(f"{issue_type} kept its lifecycle across the move to '{target}'",
               self.state_of(bug_id), before)

        self.expect_allowed(f"{issue_type} moved back to '{home}'", bug_id,
                            product=home, component="General",
                            version=self.version_of(home))
        eq(f"{issue_type} is back in '{home}'",
           self.product_of_bug(bug_id), home)


def walk_lifecycle(w, issue_type, product, path, closing_resolution):
    """File one item and walk it along `path`, probing at every state."""
    print(f"\n[7] {issue_type} lifecycle ({product})")
    category = w.categories[issue_type][0]
    status, body = w.file(product, "General", issue_type, category)
    if not check(f"file a {issue_type}", status < 400 and body.get("id"),
                 f"HTTP {status}: {json.dumps(body)[:300]}"):
        return None
    bug_id = body["id"]
    print(f"  ....  bug {bug_id}")

    # Any DECLARED entry point, not just the default. A BUG filed by someone
    # with editbugs lands on CONFIRMED, because core picks the first available
    # status that is not UNCONFIRMED - and CONFIRMED is a legal entry point
    # here, which is how 78 of the 178 pre-existing bugs were filed.
    entry_points = w.enf["initial_statuses"][issue_type]
    initial = w.state_of(bug_id)[0]
    check(f"{issue_type} starts at a declared entry point",
          initial in entry_points,
          f"got '{initial}', want one of {entry_points}")

    current = initial
    w.probe_illegal_statuses(bug_id, issue_type, current)

    for target in path:
        w.expect_allowed(f"{issue_type} '{current}' -> '{target}'", bug_id,
                         status=target)
        eq(f"{issue_type} is '{target}'", w.state_of(bug_id)[0], target)
        current = target
        w.probe_illegal_statuses(bug_id, issue_type, current)

    reopen_to = "CONFIRMED" if issue_type == "BUG" else "REQ_REVIEW"
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
    w.expect_denied(f"{issue_type} retyped", bug_id,
                    cf_issue_type=('REQUIREMENT' if issue_type == 'BUG' else 'BUG'))
    w.probe_combined_illegal(bug_id, issue_type)

    if reopen_to in w.enf["allowed_statuses"][issue_type]:
        w.expect_allowed(f"{issue_type} reopens to '{reopen_to}'", bug_id,
                         status=reopen_to, resolution="")
        eq(f"reopened {issue_type} has no resolution",
           w.state_of(bug_id)[:2], (reopen_to, ""))

    return bug_id


def verify_every_product(w):
    """Every product must accept BOTH types - that is the whole point of the
    Internal/External split not being the type."""
    print("\n[8] Every product accepts both types")
    for product in w.products:
      for issue_type in w.state["issue_types"]:
        category = w.categories[issue_type][0]
        status, body = w.file(product, "General", issue_type, category)
        if not check(f"file a {issue_type} into '{product}'",
                     status < 400 and body.get("id"),
                     f"HTTP {status}: {json.dumps(body)[:200]}"):
            continue
        entry_points = w.enf["initial_statuses"][issue_type]
        got = w.state_of(body["id"])[0]
        check(f"'{product}' {issue_type} starts at a declared entry point",
              got in entry_points, f"got '{got}', want one of {entry_points}")
        other = next(t for t in w.enf["allowed_statuses"] if t != issue_type)
        w.expect_denied_with(
            f"'{product}' {issue_type} given {other}'s category",
            "issue_category_mismatch", body["id"],
            cf_category=w.categories[other][0])


def verify_creation_guards(w):
    print("\n[9] Creation-time guards")
    product = w.product_of["BUG"][0]
    component = "General"

    w.expect_creation_refused(
        "BUG with a REQUIREMENT category", "issue_category_mismatch",
        product=product, component=component, issue_type="BUG",
        category=w.categories["REQUIREMENT"][0])
    w.expect_creation_refused(
        "REQUIREMENT with a BUG category", "issue_category_mismatch",
        product=w.product_of["REQUIREMENT"][0], component=component,
        issue_type="REQUIREMENT", category=w.categories["BUG"][0])
    # An empty category must be ACCEPTED - that is the property that leaves the
    # pre-existing bugs alone.
    st, b = w.file(product, component, "BUG", "---")
    if check("file a BUG with no category", st < 400 and b.get("id"),
             f"HTTP {st}: {json.dumps(b)[:200]}"):
        # '---' is Bugzilla's unset sentinel for a select field, and it is what
        # REST returns - not an empty string.
        eq("BUG filed with no category is unset",
           _unset(w.state_of(b["id"])[3]), "")

    print("  ....  issue_type_initial_status_unavailable, "
          "issue_type_initial_status_needs_comment and "
          "issue_type_workflow_misconfigured are NOT exercised here: each "
          "needs the workflow deliberately damaged first. setup-projects.pl's "
          "self-test covers the fail-closed path transactionally.")

    # Filing onto the other lifecycle's entry point is corrected, not rejected:
    # Bug.pm:1526-1536 forces UNCONFIRMED on any reporter without
    # editbugs/canconfirm, so rejecting it would break external filing. This
    # also stands in for the low-privilege-reporter case, which needs a second
    # account the verifier does not create.
    status, body = w.file(w.product_of["REQUIREMENT"][0], component,
                          "REQUIREMENT",
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
    print("\n[10] Low-privilege reporter")
    if not key_env:
        skip("low-privilege reporter probe",
             "not needed: setup-projects.pl proves this transactionally by "
             "dropping its own privileges, filing a REQUIREMENT and rolling "
             "back. Pass --reporter-api-key-env NAME to also prove it "
             "end-to-end with a real Keycloak-provisioned account.")
        return

    key = os.environ.get(key_env, "")
    if not check(f"${key_env} is set", bool(key)):
        return

    reporter = Bz(bz.base, key)

    # Trusting the operator's label would let an admin key pass this probe and
    # prove nothing, so establish who the key belongs to and what they can do.
    status, body = reporter.get("/whoami")
    login = body.get("name")
    if not check("reporter key identifies a user", status < 400 and login,
                 f"HTTP {status}: {json.dumps(body)[:200]}"):
        return
    _, body = bz.get("/user?names=" + urllib.parse.quote(login)
                     + "&include_fields=name,groups")
    users = body.get("users") or []
    if not check(f"admin key can read '{login}' group membership", bool(users),
                 json.dumps(body)[:200]):
        return
    groups = {g["name"] for g in users[0].get("groups", [])}
    if not check(f"'{login}' is in neither editbugs nor canconfirm",
                 not (groups & {"editbugs", "canconfirm"}),
                 f"groups: {sorted(groups)} - this key is NOT low privilege, so "
                 "the probe would prove nothing"):
        return

    # Group names are not the whole story: a product's group controls can grant
    # the same effective permission through another group entirely. So probe the
    # EFFECT - editing a field on someone else's bug is exactly what editbugs
    # buys - rather than trusting membership.
    probe_product = w.product_of["REQUIREMENT"][0]
    st, probe = w.file(probe_product, "General", "REQUIREMENT",
                       w.categories["REQUIREMENT"][0])
    if not check("filed an admin-owned item for the privilege probe",
                 st < 400 and probe.get("id")):
        return
    st, body = reporter.put(f"/bug/{probe['id']}", {"priority": "Low"})
    if not check(f"'{login}' cannot edit someone else's bug (so it really is "
                 "low privilege)", st >= 400 and body.get("error"),
                 f"HTTP {st}: this account CAN edit other people's bugs, so it "
                 "does not take core's low-privilege path and proves nothing"):
        return
    payload = {
        "product": w.product_of["REQUIREMENT"][0],
        "component": "General",
        "summary": f"{SMOKE_PREFIX} - REQUIREMENT filed unprivileged",
        "version": w.version_of(w.product_of["REQUIREMENT"][0]),
        "description": "Filed by verify-projects.py as an unprivileged user.",
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
    print("\n[11] Native duplicate handling")
    for issue_type, product in (("BUG", w.product_of["BUG"][0]),
                                ("REQUIREMENT", w.product_of["REQUIREMENT"][0])):
        category = w.categories[issue_type][0]
        s1, b1 = w.file(product, "General", issue_type, category)
        s2, b2 = w.file(product, "General", issue_type, category)
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
    print("\n[12] Bulk update atomicity")
    s1, b1 = w.file(w.product_of["BUG"][0], "General", "BUG",
                    w.categories["BUG"][0])
    s2, b2 = w.file(w.product_of["REQUIREMENT"][0], "General", "REQUIREMENT",
                    w.categories["REQUIREMENT"][0])
    if not check("file one item of each type for the bulk test",
                 s1 < 400 and s2 < 400 and b1.get("id") and b2.get("id")):
        return
    bug_id, req_id = b1["id"], b2["id"]

    # Park both on VERIFIED, the one state from which CONFIRMED is reachable
    # in the global matrix for both - so the refusal below can only come from
    # the extension.
    for target in ("CONFIRMED", "RESOLVED", "VERIFIED"):
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
        "status": "CONFIRMED",
        "resolution": "",
        "comment": {"body": "verify-projects.py bulk atomicity probe"},
    })
    # The illegal half here is a cross-lifecycle STATUS, so it surfaces as
    # illegal_bug_status_transition rather than illegal_change - see the
    # error-code note in expected-state.json. What this actually proves is the
    # two assertions below: neither bug moved.
    check("bulk update containing one illegal item is refused",
          status == HTTP_BAD_REQUEST
          and body.get("code")
              == w.enf["error_codes"]["core_illegal_bug_status_transition"],
          f"HTTP {status} code {body.get('code')}: {body.get('message','')[:160]}")
    eq("legal item in the bulk update was NOT changed", w.state_of(bug_id),
       before_bug)
    eq("illegal item in the bulk update was NOT changed", w.state_of(req_id),
       before_req)


def close_smoke_bugs(w):
    """Leave the tracker tidy - REST cannot delete bugs, so close them."""
    if not w.filed:
        return
    print("\n[13] Cleanup")
    closed, stuck = 0, []
    for bug_id in w.filed:
        try:
            status, resolution, product, category = w.state_of(bug_id)
            issue_type = _unset(product) or w.state["enforcement"]["legacy_untyped_is"]

            # Closed is not enough: if a negative probe unexpectedly SUCCEEDED,
            # the malformed bug it produced would look tidy and be left behind.
            valid = (
                issue_type is not None
                and status in w.enf["allowed_statuses"][issue_type]
                and (not resolution
                     or resolution in w.enf["allowed_resolutions"][issue_type])
                and (not _unset(category)
                     or category in w.enf["allowed_categories"][issue_type]))
            if not valid:
                stuck.append(f"{bug_id} (policy-invalid: {product}/{status}/"
                             f"{resolution}/{category})")
                check(f"smoke bug {bug_id} is policy-valid", False,
                      "a negative probe appears to have succeeded")
                continue

            if status in ("RESOLVED", "VERIFIED") and resolution:
                closed += 1
                continue
            wanted = "INVALID" if issue_type == "BUG" else "REJECTED"
            code, body = w.move(bug_id, status="RESOLVED", resolution=wanted)
            if code >= 400 or body.get("error"):
                stuck.append(bug_id)
                continue
            # A 200 is not proof: read the state back.
            final_status, final_resolution = w.state_of(bug_id)[:2]
            if final_status == "RESOLVED" and final_resolution == wanted:
                closed += 1
            else:
                stuck.append(f"{bug_id} (still {final_status}/{final_resolution})")
        except BzTransportError as exc:
            stuck.append(f"{bug_id} ({exc})")
            check(f"smoke bug {bug_id} final state is known", False, str(exc))
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
    parser.add_argument("--require-decommissioned", action="store_true",
                        help="assert the decommissioned products are gone "
                             "(use after runbook step 8)")
    parser.add_argument("--strict", action="store_true",
                        help="treat warnings (things this run could not prove) "
                             "as failures")
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
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        sys.exit(f"--base-url must be an http(s) URL, got {args.base_url!r}")
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

    try:
        require_shape(state)
    except ValueError as exc:
        sys.exit(f"{args.state}: {exc}")

    bz = Bz(args.base_url, api_key)

    # Prove the key actually authenticates before anything else runs.
    #
    # Bugzilla answers some endpoints anonymously and refuses others, so a key
    # that is not being applied at all does not announce itself - it shows up
    # as a scatter of unrelated-looking failures much further down. That is
    # exactly what happened when this script sent an X-BUGZILLA-API-KEY header
    # that Bugzilla 5.2 does not read: version and parameters "passed" while
    # every product read failed.
    status, body = bz.get("/user?match=&include_fields=id,name")
    if status >= 400 or not isinstance(body.get("users"), list):
        sys.exit(
            f"\nABORTED: the API key was not accepted (HTTP {status}): "
            f"{str(body.get('message', body))[:200]}\n"
            "Check the key is not revoked, and that it belongs to an account "
            "that still exists.")
    print(f"authenticated (key accepted by {args.base_url})\n")

    try:
        verify_version(bz, state)
        verify_parameters(bz, state)
        verify_products(bz, state, args.require_decommissioned)
        fields = fields_by_name(bz)
        verify_nothing_undeclared(bz, state, fields)
        verify_custom_fields(fields, state)
        verify_statuses_and_workflow(fields, state)
        verify_resolutions(fields, state)
    except BzTransportError as exc:
        sys.exit(f"\nABORTED: {exc}")

    structural_failures = len(failures)
    walker = None
    if args.no_smoke:
        warn("behavioural enforcement is NOT proven",
             "--no-smoke was given, so no item was filed and no illegal "
             "transition was attempted. Only the configuration was checked.")
    elif structural_failures:
        print(f"\n{structural_failures} structural failure(s): skipping the "
              "behavioural pass rather than filing bugs into a known-broken "
              "installation.")
    else:
        walker = Walker(bz, state)
        try:
            walk_lifecycle(walker, "BUG", walker.product_of["BUG"][0],
                           ["CONFIRMED", "IN_PROGRESS"], "FIXED")
            walk_lifecycle(walker, "REQUIREMENT",
                           walker.product_of["REQUIREMENT"][0],
                           ["REQ_REVIEW", "REQ_APPROVED", "REQ_IN_PROGRESS"],
                           "IMPLEMENTED")
            verify_every_product(walker)
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
                check("smoke-bug cleanup completed", False, str(exc))

    print(f"\n{'=' * 60}")
    if walker and walker.filed:
        print("smoke test bugs: "
              + ", ".join(str(i) for i in walker.filed))
    unexpected = [name for name, expected in warnings if not expected]
    if warnings:
        print(f"{len(warnings)} thing(s) this run could NOT prove:")
        for name, expected in warnings:
            print(f"  ? {name}" + ("  (expected at this stage)" if expected else ""))
    if failures:
        print(f"FAILED  {len(failures)}/{checks} checks failed:")
        for name in failures:
            print(f"  - {name}")
        sys.exit(1)
    if unexpected and args.strict:
        sys.exit(f"FAILED  --strict: {len(unexpected)} unproven check(s) above "
                 "are treated as failures")
    print(f"OK  all {checks} checks passed"
          + (f" ({len(warnings)} unproven)" if warnings else ""))


if __name__ == "__main__":
    main()
