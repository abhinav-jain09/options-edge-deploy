# CI-REQUIRED
"""The dev prefix proxy must not mistake the journal's data path for a page prefix.

THE BUG THIS EXISTS FOR (2026-11-14, live on dev for a day). The proxy carries this rule:

    location ~ ^/[^/]+/(api/.*)$ { proxy_pass http://host.docker.internal:8090/$1...; }

It reads the first segment of ``/<page>/api/...`` as a reverse-proxy prefix and strips it, which
is right for a page that treats its own path that way (``/option-chain/api/quote`` really is
``/api/quote``). The journal page does not: it is served at ``/journal`` by the web app and asks
that SAME app for ``/journal/api/...``, a real path with a real handler.

So ``/journal/api/day-list`` arrived at the web app as ``/api/day-list``. No handler maps it, and
the web app's security chain is deliberately fail-closed, so it answered an EMPTY 403 — and the
page, reading that 403 honestly, said "This journal belongs to one account" to the account that
owns it. A routing fault wearing an authorization fault's face.

Nothing in either application could have caught it: the web app's own tests reach its controller
directly, and the page was correct. The fault lived in the one hop neither repository tests.
"""
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

CONF = Path(__file__).resolve().parents[1] / "infra/dev/public-prefix-proxy/nginx.conf"


class DevPrefixProxyJournalData(unittest.TestCase):

    def setUp(self):
        self.raw = CONF.read_text()
        # Comments stripped before anything is searched for. The first draft of the test below
        # failed on its own explanation: the comment in the config says what a plain
        # `location /journal/api/` would do wrong, and a search over the raw file found those
        # words and called them configuration. Prose is not code.
        self.text = "\n".join(
            line.split("#", 1)[0] for line in self.raw.splitlines())

    def test_the_journal_data_path_is_exempt_from_the_page_prefix_rule(self):
        self.assertIn("location ^~ /journal/api/", self.text,
                      "the journal's data path must be routed before the page-prefix regex")
        self.assertIn("location = /journal/api {", self.text,
                      "and the bare prefix answered rather than redirected")

    def test_the_exemption_uses_the_only_form_that_actually_wins(self):
        # ^~ is load-bearing, not decoration. Nginx prefers a MATCHING REGEX location over a plain
        # prefix one regardless of the order they appear in, so `location /journal/api/` would
        # lose to the page-prefix rule and the bug would be back with the fix sitting right above
        # it, looking applied.
        self.assertNotIn("location /journal/api/", self.text,
                         "a plain prefix location loses to the regex; it must be ^~")

    def test_the_page_prefix_rule_itself_is_still_there(self):
        # The other half. Removing the rule would also "fix" the journal, and break every page
        # that genuinely does treat its path as a proxy prefix.
        self.assertIn("location ~ ^/[^/]+/(api/.*)$", self.text,
                      "pages that DO prefix their api calls still need the strip")

    def test_the_journal_route_carries_what_a_journal_request_needs(self):
        block = self.text.split("location ^~ /journal/api/", 1)[1].split("}", 1)[0]
        self.assertIn("proxy_pass http://host.docker.internal:8090;", block,
                      "no URI on proxy_pass, so the path reaches the app unchanged")
        self.assertIn("Authorization", block,
                      "the journal authorizes on the caller's bearer token")
        self.assertIn("client_max_body_size", block,
                      "a board screenshot is PUT through here and would 413 at the 1m default")

    @unittest.skipUnless(shutil.which("docker") and not os.environ.get("OE_NO_DOCKER"),
                         "docker is needed to run the real nginx")
    def test_a_real_nginx_routes_the_journal_untouched_and_still_strips_a_page_prefix(self):
        """Ask nginx what it DOES, which no amount of reading the file can tell you.

        The real config is run with one addition: a second server on 8090 that echoes the path it
        was asked for. host.docker.internal is pointed at the container itself, so every
        proxy_pass in the real rules lands on that echo and the answer IS the path the web app
        would have received. No stub of the rules, no second container.
        """
        # The echo reports the METHOD, the full request URI (query included) and the declared
        # body length, so one probe can show whether a request was rewritten, whether its query
        # survived, and whether its body arrived.
        echo_server = (
            "  server {\n"
            "    listen 8090;\n"
            "    client_max_body_size 64m;\n"
            "    location / { default_type text/plain;\n"
            "      return 200 \"$request_method $request_uri len=$content_length\"; }\n"
            "  }\n"
        )
        # Injected before the final closing brace of the http{} block.
        at = self.raw.rstrip().rfind("}")
        at = self.raw.rstrip()[:at].rfind("}") + 1
        probed = self.raw[:at] + "\n" + echo_server + self.raw[at:]

        with tempfile.TemporaryDirectory() as tmp:
            conf = Path(tmp) / "nginx.conf"
            conf.write_text(probed)
            os.chmod(tmp, 0o755)
            os.chmod(conf, 0o644)
            script = (
                "nginx -g 'daemon on;' && sleep 1 && "
                # 2 MiB: over nginx's 1m default, so it also proves client_max_body_size here.
                "dd if=/dev/zero of=/tmp/body bs=1024 count=2048 2>/dev/null && "
                "for p in /journal/api/day-list /journal/api/snapshot/2026-11-14/0930/image "
                "'/journal/api/tape?side=call&strike=7590' /journal/api/note/2026-11-14 "
                "/journal/api /option-chain/api/quote /es-seller-activity/api/x "
                "/journal /journal.js; do "
                "echo \"$p => $(wget -qO- \"http://127.0.0.1:8098$p\" 2>&1 | tail -1)\"; done; "
                # A real PUT with a real body, spoken over nc, because busybox wget cannot PUT
                # and its --post-file sent Content-Length: 0 - a fixture that would have "proved"
                # body forwarding while sending nothing.
                "{ printf 'PUT /journal/api/snapshot/2026-11-14/0930/image HTTP/1.1\\r\\n"
                "Host: dev.example\\r\\nContent-Type: image/jpeg\\r\\n"
                "Content-Length: 2097152\\r\\nConnection: close\\r\\n\\r\\n'; cat /tmp/body; sleep 4; } "
                "| nc 127.0.0.1 8098 > /tmp/put.out 2>&1; "
                "echo \"BODY => $(tail -1 /tmp/put.out)\""
            )
            out = subprocess.run(
                ["docker", "run", "--rm", "--add-host", "host.docker.internal:127.0.0.1",
                 "-v", f"{conf}:/etc/nginx/nginx.conf:ro", "nginx:1.27-alpine",
                 "sh", "-c", script],
                capture_output=True, text=True, timeout=240)
        self.assertEqual(0, out.returncode, f"nginx would not run:\n{out.stderr}")
        seen = dict(
            line.split(" => ", 1) for line in out.stdout.strip().splitlines()
            if " => " in line)

        # The journal's data reaches the app as itself. This is the whole fix.
        self.assertEqual("GET /journal/api/day-list len=", seen.get("/journal/api/day-list"),
                         f"the journal path was rewritten: {out.stdout}")
        self.assertEqual("GET /journal/api/snapshot/2026-11-14/0930/image len=",
                         seen.get("/journal/api/snapshot/2026-11-14/0930/image"),
                         "the board's screenshot path must arrive unrewritten too")
        self.assertEqual("GET /journal/api/note/2026-11-14 len=",
                         seen.get("/journal/api/note/2026-11-14"))

        # The query survives. proxy_pass without a URI is the form that preserves it, but "is the
        # correct form" and "does the right thing here" are different claims.
        self.assertEqual("GET /journal/api/tape?side=call&strike=7590 len=",
                         seen.get("/journal/api/tape?side=call&strike=7590"),
                         f"the query string must reach the app: {out.stdout}")

        # The bare prefix is ANSWERED, not redirected. A prefix location with a trailing slash
        # served by proxy_pass makes nginx 301 to the slashed form, building the Location from
        # the host it was reached on - which behind the tunnel named the origin.
        self.assertEqual("GET /journal/api len=", seen.get("/journal/api"),
                         f"the bare prefix must not be redirected: {out.stdout}")

        # A body of 2 MiB arrives whole: over nginx's 1m default, which would have refused every
        # board screenshot with a 413 that never reached the app.
        self.assertEqual("PUT /journal/api/snapshot/2026-11-14/0930/image len=2097152",
                         seen.get("BODY"),
                         f"a screenshot-sized PUT must pass through whole: {out.stdout}")

        # And a page that DOES prefix its api calls is still stripped, or this fix broke them.
        self.assertEqual("GET /api/quote len=", seen.get("/option-chain/api/quote"),
                         f"the page-prefix convention must survive: {out.stdout}")
        self.assertEqual("GET /api/x len=", seen.get("/es-seller-activity/api/x"))
        # The page and its assets were never affected; pinned so a future rule cannot change it.
        self.assertEqual("GET /journal len=", seen.get("/journal"))
        self.assertEqual("GET /journal.js len=", seen.get("/journal.js"))


if __name__ == "__main__":
    unittest.main()
