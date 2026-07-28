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
VIX_INSTRUMENTS_LOWERCASE = VIX_INSTRUMENTS.replace(
    "AUTO_VIX_MONTHLY", "auto_vix_monthly"
)


def spx_deployment(enable_flag=None, instruments=None) -> str:
    """Minimal rendered SPX-feed Deployment.

    enable_flag: literal string value for DATABENTO_VIX_PRICE_ENABLED, or None to omit.
    instruments: literal string value for DATABENTO_EXTRA_INSTRUMENTS, or None to omit.
    The two arms are independent so each ownership predicate is tested on its own
    (Codex round-1 finding 5).
    """
    env_lines = [
        "            - name: DATABENTO_PUBLISH_INTERVAL_MS",
        '              value: "2000"',
    ]
    if instruments is not None:
        env_lines += [
            "            - name: DATABENTO_EXTRA_INSTRUMENTS",
            f"              value: '{instruments}'",
        ]
    if enable_flag is not None:
        env_lines += [
            "            - name: DATABENTO_VIX_PRICE_ENABLED",
            f'              value: "{enable_flag}"',
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


def spx_on() -> str:
    """The real prod shape: enable flag AND the AUTO_VIX_MONTHLY instrument."""
    return spx_deployment(enable_flag="true", instruments=VIX_INSTRUMENTS)


def spx_off() -> str:
    """SPX feed with the whole VIX block removed (cutover/steady state)."""
    return spx_deployment()


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

    Every planned rollout state must PASS; the both-owners states, the fail-closed
    states (missing Deployment, duplicate env, malformed JSON, unreadable replicas),
    and the runtime-equivalent adversarial spellings ("y", lowercase expiry mode —
    exactly what the real feed parser accepts) must FAIL.
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
        # PR-2 committed state (approved phase model): SPX enabled; standalone at
        # replicas 0 with the REAL topic explicitly set. One owner (SPX).
        self.assert_pass(self._run(
            spx_on(),
            vix_deployment(replicas=0, topic="underlying.vix.price"),
        ))

    def test_state_shadow_passes(self):
        # Shadow commit: SPX still enabled; standalone at 1 replica on the SHADOW topic.
        self.assert_pass(self._run(
            spx_on(),
            vix_deployment(replicas=1, topic="underlying.vix.price.shadow"),
        ))

    def test_state_cutover_intermediate_passes(self):
        # Step A applied: SPX block removed, standalone not yet raised (0 -> 1).
        self.assert_pass(self._run(
            spx_off(),
            vix_deployment(replicas=0, topic="underlying.vix.price"),
        ))

    def test_state_steady_passes(self):
        # Steady state: SPX disabled; standalone owns the real topic (env omitted =
        # the code-default real topic).
        self.assert_pass(self._run(
            spx_off(),
            vix_deployment(replicas=1, topic=None),
        ))

    def test_state_rollback_passes(self):
        # Rollback: standalone parked at 0 (real topic still configured); SPX re-enabled.
        self.assert_pass(self._run(
            spx_on(),
            vix_deployment(replicas=0, topic="underlying.vix.price"),
        ))

    # --- the violation the assertion exists for -----------------------------------

    def test_both_owners_fails(self):
        self.assert_fail(self._run(
            spx_on(),
            vix_deployment(replicas=1, topic="underlying.vix.price"),
        ))

    def test_both_owners_via_default_topic_fails(self):
        self.assert_fail(self._run(
            spx_on(),
            vix_deployment(replicas=1, topic=None),
        ))

    # --- independent SPX arms + runtime-equivalent spellings (finding 5) -----------
    # The runtime truthy set is {"1","true","yes","on","y"} case-insensitive
    # (feed config.py:49) and expiry_mode is normalized strip().upper()
    # (config.py:143) — each arm ALONE must activate SPX ownership.

    def test_spx_enable_flag_alone_owns(self):
        # enable flag only, no extra-instruments entry at all.
        self.assert_fail(self._run(
            spx_deployment(enable_flag="true", instruments=None),
            vix_deployment(replicas=1, topic="underlying.vix.price"),
        ))

    def test_spx_enable_flag_y_spelling_owns(self):
        # "y" is truthy to the real parser — the assertion must agree.
        self.assert_fail(self._run(
            spx_deployment(enable_flag="y", instruments=None),
            vix_deployment(replicas=1, topic="underlying.vix.price"),
        ))

    def test_spx_instrument_json_alone_owns(self):
        # AUTO_VIX_MONTHLY instrument only, no enable flag.
        self.assert_fail(self._run(
            spx_deployment(enable_flag=None, instruments=VIX_INSTRUMENTS),
            vix_deployment(replicas=1, topic="underlying.vix.price"),
        ))

    def test_spx_instrument_lowercase_spelling_owns(self):
        # lowercase auto_vix_monthly is upper()d by the runtime — must still own.
        self.assert_fail(self._run(
            spx_deployment(enable_flag=None, instruments=VIX_INSTRUMENTS_LOWERCASE),
            vix_deployment(replicas=1, topic="underlying.vix.price"),
        ))

    def test_spx_enable_flag_false_alone_does_not_own(self):
        # An explicit false flag with no instrument is NOT ownership: standalone may
        # own the real topic (this is the steady-state shape with a leftover flag).
        self.assert_pass(self._run(
            spx_deployment(enable_flag="false", instruments=None),
            vix_deployment(replicas=1, topic="underlying.vix.price"),
        ))

    # --- fail-closed states --------------------------------------------------------

    def test_missing_spx_deployment_fails_closed(self):
        self.assert_fail(self._run(
            vix_deployment(replicas=0, topic="underlying.vix.price"),
        ))

    def test_missing_standalone_deployment_fails_closed(self):
        self.assert_fail(self._run(
            spx_on(),
        ))

    def test_unreadable_replicas_fails_closed(self):
        self.assert_fail(self._run(
            spx_off(),
            vix_deployment(replicas=None, topic="underlying.vix.price"),
        ))

    def test_malformed_extra_instruments_json_fails_closed(self):
        self.assert_fail(self._run(
            spx_deployment(enable_flag="true", instruments='[{"symbol":"VIX", NOT-JSON'),
            vix_deployment(replicas=0, topic="underlying.vix.price"),
        ))

    def test_duplicate_env_entry_fails_closed(self):
        doc = vix_deployment(replicas=0, topic="underlying.vix.price")
        doc += (
            "            - name: KAFKA_VIX_PRICE_TOPIC\n"
            '              value: "underlying.vix.price.shadow"\n'
        )
        self.assert_fail(self._run(spx_on(), doc))

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
