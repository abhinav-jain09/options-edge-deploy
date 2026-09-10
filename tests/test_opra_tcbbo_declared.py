from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOPICS_ENV = ROOT / "scripts" / "kafka" / "topics.env"
TAPE = "options.opra.tcbbo"
SHAPE = 32  # prod's live partition count for the tape, and dev's


def _declared(loader: str) -> dict[str, str]:
    """OPTIONS_EDGE_TOPICS as a real consumer of topics.env sees it — evaluated by bash, not regexed.

    `eval` of the assignment lines is what dev-cleanup's ensure_topics does; `source` is the whole-file
    load. Both must see the declaration, including `VAR="$VAR ..."` appends.
    """
    if loader == "eval":
        script = 'eval "$(grep -E "^OPTIONS_EDGE_TOPICS=" "$1")"; printf "%s" "$OPTIONS_EDGE_TOPICS"'
    else:
        script = '. "$1" >/dev/null 2>&1; printf "%s" "$OPTIONS_EDGE_TOPICS"'
    out = subprocess.run(["bash", "-c", script, "bash", str(TOPICS_ENV)],
                         capture_output=True, text=True, check=True).stdout
    return dict(spec.split(":", 1) if ":" in spec else (spec, "") for spec in out.split())


def _consumers(env: str) -> list[str]:
    """Services whose generated manifest for `env` wires the tape in as an env value."""
    pattern = re.compile(rf'^\s*value:\s*"?{re.escape(TAPE)}"?\s*$', re.M)
    return sorted(p.parts[-4] for p in (ROOT / "k8s" / "services").glob(f"*/overlays/{env}/manifest.yaml")
                  if pattern.search(p.read_text()))


class OpraTcbboDeclaredTest(unittest.TestCase):
    """options.opra.tcbbo must be pre-created at its real shape, never left to whichever client touches
    it first after a wipe.

    2026-09-10 (dev): a clean deleted the undeclared tape, broker auto-create (num.partitions=1 on dev
    AND prod) recreated it at 1 partition, a service widened it to 32 seconds later, and
    strike-flow-classifier's Kafka Streams had already created its repartition/changelog topics from the
    1-partition metadata — then failed every rebalance ("expected: 32; actual: 1") with 0 records
    processed while the pod stayed READY.
    """

    def test_the_tape_still_has_streams_consumers_in_dev_and_prod(self) -> None:
        # Guards the premise: if the consumers were rewired away, the declaration below would be
        # protecting nothing and this file would pass vacuously.
        for env in ("dev", "production"):
            with self.subTest(env=env):
                self.assertIn("strike-flow-classifier", _consumers(env))

    def test_tape_declared_at_its_real_shape_for_every_loader(self) -> None:
        for loader in ("eval", "source"):
            with self.subTest(loader=loader):
                topics = _declared(loader)
                # The parse must see a known sibling tape, or an empty/broken load would read as "absent"
                # for the wrong reason.
                self.assertEqual(topics.get("options.databento.raw"), "32")
                self.assertIn(TAPE, topics, f"{TAPE} is not in OPTIONS_EDGE_TOPICS ({loader})")
                self.assertEqual(topics[TAPE], str(SHAPE),
                                 f"{TAPE} declared :{topics[TAPE]} — must be the real {SHAPE}-partition shape")

    def test_tape_is_not_given_a_non_default_policy_or_retention(self) -> None:
        # Prod's live tape is cleanup.policy=delete with retention.ms=-1 (the configmap default), so the
        # declaration is a no-op there. A compaction or retention override would rewrite the raw tape.
        text = TOPICS_ENV.read_text()
        for var in ("OPTIONS_EDGE_COMPACTED_TOPICS", "OPTIONS_EDGE_PURE_COMPACT_TOPICS",
                    "OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES", "OPTIONS_EDGE_EXACT_PARTITION_TOPICS"):
            for m in re.finditer(rf'^{var}="([^"]*)"', text, re.M):
                tokens = {t.split("=", 1)[0].split(":", 1)[0] for t in m.group(1).split()}
                with self.subTest(var=var):
                    self.assertNotIn(TAPE, tokens)


if __name__ == "__main__":
    unittest.main()
