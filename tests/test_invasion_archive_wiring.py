"""The wiring of the strike-invasion events Avro archive reader into oe-archive-kafka.sh and its deploy unit.
OE_INVASION_AVRO_TOPICS ships EMPTY (the topic is JSON until the cutover, so the console consumer keeps archiving it);
the documented switch routes the topic to StrikeInvasionEventsArchiveReader.java. Behavioural checks run the real shell
block with stub reader functions; the rest pins the capture order and the unit lists."""
from __future__ import annotations

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = (ROOT / "scripts/ops/archive/oe-archive-kafka.sh").read_text()
TOPIC = "options.spx.strike-invasion.events"


def block() -> str:
    a = ARCHIVE.index('OE_INVASION_AVRO_TOPICS="${OE_INVASION_AVRO_TOPICS:-}"')
    b = ARCHIVE.index("# THE RECORD LAYOUT OF A COMMITTED-READ FILE.")
    return ARCHIVE[a:b]


def sh(script: str, **env) -> subprocess.CompletedProcess:
    e = {k: v for k, v in os.environ.items() if not k.startswith("OE_") and not k.endswith("_READER")}
    e.update(env)
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=e)


PRELUDE = '''is_indicator_avro_topic() { case " options.indicators.bars " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
run_indicator_reader() { echo "INDICATOR-READER src=$INDICATOR_READER_SRC args=$*"; }
OE_SPOT_AVRO_TOPICS="${OE_SPOT_AVRO_TOPICS:-}"
'''


class InvasionArchiveWiring(unittest.TestCase):
    def test_default_is_empty_so_the_json_topic_keeps_archiving_by_console(self):
        r = sh(PRELUDE + block() + f'\nis_invasion_avro_topic {TOPIC} && echo ON || echo OFF')
        self.assertEqual(r.stdout.strip(), "OFF", r.stderr)
        for f in ["oe-topics.env", "oe-archive-dev.sh", "oe-archive-daily.sh", "oe-archive.crontab"]:
            self.assertNotIn("OE_INVASION_AVRO_TOPICS=", (ROOT / "scripts/ops/archive" / f).read_text(),
                             f"{f}: the switch is flipped by the cutover step, not shipped")

    def test_the_switch_routes_only_the_listed_topic(self):
        r = sh(PRELUDE + block() + f'\nfor t in {TOPIC} options.spx.strike-invasion.current underlying.spx.price; do '
               'is_invasion_avro_topic $t && echo "$t ON" || echo "$t OFF"; done', OE_INVASION_AVRO_TOPICS=TOPIC)
        self.assertEqual(r.stdout.split("\n")[:3], [f"{TOPIC} ON", "options.spx.strike-invasion.current OFF", "underlying.spx.price OFF"])

    def test_a_topic_in_two_avro_lists_is_refused(self):
        r = sh(PRELUDE + block(), OE_INVASION_AVRO_TOPICS="options.indicators.bars")
        self.assertEqual(r.returncode, 2)
        self.assertIn("both OE_INVASION_AVRO_TOPICS and OE_INDICATOR_AVRO_TOPICS", r.stderr)
        r = sh(PRELUDE + block(), OE_INVASION_AVRO_TOPICS="underlying.spx.price", OE_SPOT_AVRO_TOPICS="underlying.spx.price")
        self.assertEqual(r.returncode, 2)
        self.assertIn("both OE_INVASION_AVRO_TOPICS and OE_SPOT_AVRO_TOPICS", r.stderr)

    def test_the_reader_runs_its_source_through_the_shared_jdk_runner_and_the_test_seam_wins(self):
        r = sh(PRELUDE + block() + '\nrun_invasion_reader 5 --topic t', INVASION_READER_SRC="/x/StrikeInvasionEventsArchiveReader.java", INDICATOR_READER="/should/not/run")
        self.assertEqual(r.stdout.strip(), "INDICATOR-READER src=/x/StrikeInvasionEventsArchiveReader.java args=5 --topic t", r.stderr)
        with tempfile.TemporaryDirectory() as d:
            stub = Path(d) / "stub"
            stub.write_text('#!/bin/sh\necho "STUB $*"\n')
            stub.chmod(0o755)
            r = sh(PRELUDE + block() + '\nrun_invasion_reader 5 --topic t', INVASION_READER=str(stub))
            self.assertEqual(r.stdout.strip(), "STUB --topic t", r.stderr)

    def test_capture_order_spot_indicator_invasion_then_console_with_its_own_registry_override(self):
        spot = ARCHIVE.index('if is_spot_avro_topic "$topic"; then')
        ind = ARCHIVE.index('elif is_indicator_avro_topic "$topic"; then')
        inv = ARCHIVE.index('elif is_invasion_avro_topic "$topic"; then')
        console = ARCHIVE.index('kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \\\n           --topic "$topic" --partition "$part" --offset "$from"')
        self.assertLess(spot, inv)
        self.assertLess(inv, ind)
        self.assertLess(ind, console)
        self.assertIn('--registry "$(archive_registry_url "${OE_INVASION_REGISTRY_URL:-}")"', ARCHIVE[inv:ind])
        self.assertEqual(ARCHIVE.count("is_invasion_avro_topic"), 2, "one definition, one capture use")

    def test_the_topic_is_archived_and_not_a_committed_read_topic(self):
        topics = (ROOT / "scripts/ops/archive/oe-topics.env").read_text()
        self.assertIn(TOPIC, re.search(r'OE_HEAVY_TOPICS_prod="([^"]*)"', topics).group(1).split())
        m = re.search(r'OE_VOL_PREMIUM_TRANSACTIONAL_TOPICS="?\$?\{?[^\n]*', ARCHIVE)
        self.assertNotIn(TOPIC, (m.group(0) if m else "") + re.search(r'OE_COMMITTED_READ_TOPICS="[^\n]*', ARCHIVE).group(0),
                         "the capture branch the reader is wired into is the non-committed-read one")

    def test_script_syntax(self):
        self.assertEqual(subprocess.run(["bash", "-n", str(ROOT / "scripts/ops/archive/oe-archive-kafka.sh")], capture_output=True).returncode, 0)

    def test_deploy_unit_ships_builds_and_tests_the_reader(self):
        t = (ROOT / "Jenkinsfile.archive-scripts-deploy").read_text()
        self.assertIn("StrikeInvasionEventsArchiveReader.java", re.search(r'UNIT\s*=\s*"([^"]*)"', t).group(1).split())
        self.assertIn("scripts/ops/archive/StrikeInvasionEventsArchiveReader.java", t)
        self.assertIn("com/optionsedge/contracts/strikeinvasion/StrikeInvasionAvro.class", t, "the built jar must carry the invasion serde")
        self.assertIn("tests.test_invasion_archive_reader", t)
        self.assertIn("tests.test_invasion_archive_wiring", t)
        self.assertTrue((ROOT / "scripts/ops/archive/StrikeInvasionEventsArchiveReader.java").exists())

    def test_cutover_doc_states_the_switch(self):
        d = (ROOT / "docs/STRIKE-INVASION-AVRO-CUTOVER.md").read_text()
        self.assertIn("OE_INVASION_AVRO_TOPICS", d)
        self.assertIn("StrikeInvasionEventsArchiveReader", d)


if __name__ == "__main__":
    unittest.main()
