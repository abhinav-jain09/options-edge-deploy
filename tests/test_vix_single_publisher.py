import pathlib
import shutil
import subprocess
import tempfile
import textwrap
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ci" / "validate-vix-single-publisher.sh"

VIX_INSTRUMENTS = (
    '[{"symbol":"VIX","trading_class":"VIX","min_strike":12,"max_strike":45,'
    '"strike_step":0.5,"expiry_mode":"AUTO_VIX_MONTHLY"}]'
)


def spx_deployment(vix_enabled: bool) -> str:
    """Minimal rendered SPX-feed Deployment; vix_enabled toggles the VIX block."""
    env_lines = [
        '            - name: DATABENTO_PUBLISH_INTERVAL_MS',
        '              value: "2000"',
    ]
    if vix_enabled:
        env_lines += [
            "            - name: DATABENTO_EXTRA_INSTRUMENTS",
            f"              value: '{VIX_INSTRUMENTS}'",
            "            - name: DATABENTO_VIX_PRICE_ENABLED",
            '              value: "true"',
        ]
    return textwrap.dedent(
        """\
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: options-edge-databento-feed
        spec:
          replicas: 1
          template:
            spec:
              containers:
                - name: databento-feed
                  image: 192.168.100.252:5000/options-edge-databento-feed:prod
                  env:
        """
    ) + "\n".join(env_lines) + "\n"


def vix_deployment(replicas, topic) -> str:
    """Minimal rendered databento-vix-feed Deployment.

    replicas: int, or None to omit spec.replicas (unreadable -> fail closed).
    topic: str, or None to omit KAFKA_VIX_PRICE_TOPIC (missing = real topic default).
    """
    replicas_line = f"  replicas: {replicas}\n" if replicas is not None else ""
    env_lines = [
        "            - name: METRICS_PORT",
        '              value: "8013"',
    ]
    if topic is not None:
        env_lines += [
            "            - name: KAFKA_VIX_PRICE_TOPIC",
            f'              value: "{topic}"',
        ]
    return (
        textwrap.dedent(
            """\
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: databento-vix-feed
            spec:
            """
        )
        + replicas_line
        + "  template:\n"
        + "    spec:\n"
        + "      containers:\n"
        + "        - name: databento-vix-feed\n"
        + "          image: 192.168.100.252:5000/options-edge-databento-feed:prod\n"
        + "          env:\n"
        + "\n".join(env_lines) + "\n"
    )


class VixSinglePublisherTest(unittest.TestCase):
    """At-most-one VIX publisher assertion (VIX feed separation design rev 4 §7).

    Every planned rollout state must PASS; the both-owners state and the fail-closed
    states (duplicate env, malformed JSON, unreadable replicas) must FAIL. The five
    planned states are explicit design test cases (design §7, at-most-one section).
    """

    def _run(self, *docs: str) -> subprocess.CompletedProcess:
        if shutil.which("yq") is None or shutil.which("python3") is None:
            self.skipTest("yq/python3 not available on this agent")
        with tempfile.NamedTemporaryFile(
            "w", suffix=".yaml", delete=False
        ) as handle:
            handle.write("---\n".join(docs))
            fixture = handle.name
        try:
            return subprocess.run(
                ["bash", str(SCRIPT), fixture],
                capture_output=True,
                text=True,
            )
        finally:
            pathlib.Path(fixture).unlink(missing_ok=True)

    def assert_pass(self, result):
        self.assertEqual(
            result.returncode, 0,
            f"expected PASS:\n{result.stdout}\n{result.stderr}",
        )

    def assert_fail(self, result):
        self.assertEqual(
            result.returncode, 1,
            f"expected FAIL:\n{result.stdout}\n{result.stderr}",
        )

    # --- the five planned states (design §7) --------------------------------------

    def test_state_pr2_default_passes(self):
        # SPX enabled; standalone committed at replicas 0 with the shadow topic.
        self.assert_pass(self._run(
            spx_deployment(vix_enabled=True),
            vix_deployment(replicas=0, topic="underlying.vix.price.shadow"),
        ))

    def test_state_shadow_passes(self):
        # SPX still enabled; standalone at 1 replica publishing the SHADOW topic only.
        self.assert_pass(self._run(
            spx_deployment(vix_enabled=True),
            vix_deployment(replicas=1, topic="underlying.vix.price.shadow"),
        ))

    def test_state_cutover_intermediate_passes(self):
        # Step A applied: SPX block removed, standalone not yet raised (0 -> 1).
        self.assert_pass(self._run(
            spx_deployment(vix_enabled=False),
            vix_deployment(replicas=0, topic="underlying.vix.price"),
        ))

    def test_state_steady_passes(self):
        # Steady state: SPX disabled; standalone owns the real topic (env omitted =
        # the code-default real topic).
        self.assert_pass(self._run(
            spx_deployment(vix_enabled=False),
            vix_deployment(replicas=1, topic=None),
        ))

    def test_state_rollback_passes(self):
        # Rollback: standalone parked at 0 (real topic still configured); SPX re-enabled.
        self.assert_pass(self._run(
            spx_deployment(vix_enabled=True),
            vix_deployment(replicas=0, topic="underlying.vix.price"),
        ))

    # --- the violation the assertion exists for -----------------------------------

    def test_both_owners_fails(self):
        self.assert_fail(self._run(
            spx_deployment(vix_enabled=True),
            vix_deployment(replicas=1, topic="underlying.vix.price"),
        ))

    def test_both_owners_via_default_topic_fails(self):
        self.assert_fail(self._run(
            spx_deployment(vix_enabled=True),
            vix_deployment(replicas=1, topic=None),
        ))

    # --- fail-closed states --------------------------------------------------------

    def test_unreadable_replicas_fails_closed(self):
        self.assert_fail(self._run(
            spx_deployment(vix_enabled=False),
            vix_deployment(replicas=None, topic="underlying.vix.price.shadow"),
        ))

    def test_malformed_extra_instruments_json_fails_closed(self):
        broken = spx_deployment(vix_enabled=True).replace(
            VIX_INSTRUMENTS, '[{"symbol":"VIX", NOT-JSON'
        )
        self.assert_fail(self._run(
            broken,
            vix_deployment(replicas=0, topic="underlying.vix.price.shadow"),
        ))

    def test_duplicate_env_entry_fails_closed(self):
        doc = vix_deployment(replicas=0, topic="underlying.vix.price.shadow")
        doc += (
            "            - name: KAFKA_VIX_PRICE_TOPIC\n"
            '              value: "underlying.vix.price"\n'
        )
        self.assert_fail(self._run(spx_deployment(vix_enabled=True), doc))

    # --- the real committed render (PR-2 state) ------------------------------------

    def test_real_production_render_passes(self):
        if shutil.which("kubectl") is None or shutil.which("yq") is None:
            self.skipTest("kubectl/yq not available on this agent")
        result = subprocess.run(
            ["bash", str(SCRIPT)],
            capture_output=True,
            text=True,
            cwd=ROOT,
        )
        self.assertEqual(
            result.returncode, 0,
            f"validate-vix-single-publisher.sh failed on the real render:\n"
            f"{result.stdout}\n{result.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
