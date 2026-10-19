"""The wiring of the underlying.spx.price Avro archive reader into oe-archive-kafka.sh and its deploy unit.
OE_SPOT_AVRO_TOPICS ships EMPTY (prod's topic is JSON until the production cutover, so the console consumer keeps
archiving it); the documented switch routes the topic to SpotArchiveReader.java. Behavioural checks run the real
shell block with stub reader functions; the rest pins the capture order and the unit lists."""
from __future__ import annotations

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = (ROOT / "scripts/ops/archive/oe-archive-kafka.sh").read_text()


def block() -> str:
    a = ARCHIVE.index('OE_SPOT_AVRO_TOPICS="${OE_SPOT_AVRO_TOPICS:-}"')
    b = ARCHIVE.index("# THE RECORD LAYOUT OF A COMMITTED-READ FILE.")
    return ARCHIVE[a:b]


def sh(script: str, **env) -> subprocess.CompletedProcess:
    e = {k: v for k, v in os.environ.items() if not k.startswith("OE_") and not k.endswith("_READER")}
    e.update(env)
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=e)


PRELUDE = '''is_indicator_avro_topic() { case " options.indicators.bars " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
run_indicator_reader() { echo "INDICATOR-READER src=$INDICATOR_READER_SRC args=$*"; }
'''


class SpotArchiveWiring(unittest.TestCase):
    def test_default_is_empty_so_prod_json_keeps_archiving_by_console(self):
        self.assertIn('OE_SPOT_AVRO_TOPICS="${OE_SPOT_AVRO_TOPICS:-}"', ARCHIVE)
        r = sh(PRELUDE + block() + '\nis_spot_avro_topic underlying.spx.price && echo ON || echo OFF')
        self.assertEqual(r.stdout.strip(), "OFF", r.stderr)
        for f in ["oe-topics.env", "oe-archive-dev.sh", "oe-archive-daily.sh"]:
            self.assertNotIn("OE_SPOT_AVRO_TOPICS=", (ROOT / "scripts/ops/archive" / f).read_text(), f"{f}: only the prod spot cron enables it")
        # the prod spot cron enables it for exactly the converted topic (prod cutover 2026-10-19), nowhere else in the crontab
        cron = (ROOT / "scripts/ops/archive/oe-archive.crontab").read_text()
        self.assertEqual(cron.count('OE_SPOT_AVRO_TOPICS="underlying.spx.price"'), 1)
        self.assertEqual(cron.count("OE_SPOT_AVRO_TOPICS="), 1)

    def test_the_switch_routes_only_the_listed_topic(self):
        r = sh(PRELUDE + block() + '\nfor t in underlying.spx.price underlying.es.price options.indicators.bars; do '
               'is_spot_avro_topic $t && echo "$t ON" || echo "$t OFF"; done', OE_SPOT_AVRO_TOPICS="underlying.spx.price")
        self.assertEqual(r.stdout.split("\n")[:3], ["underlying.spx.price ON", "underlying.es.price OFF", "options.indicators.bars OFF"])

    def test_a_topic_in_both_avro_lists_is_refused(self):
        r = sh(PRELUDE + block(), OE_SPOT_AVRO_TOPICS="options.indicators.bars")
        self.assertEqual(r.returncode, 2)
        self.assertIn("both OE_SPOT_AVRO_TOPICS and OE_INDICATOR_AVRO_TOPICS", r.stderr)

    def test_the_reader_runs_the_spot_source_through_the_shared_jdk_runner_and_the_test_seam_wins(self):
        r = sh(PRELUDE + block() + '\nrun_spot_reader 5 --topic t', SPOT_READER_SRC="/x/SpotArchiveReader.java", INDICATOR_READER="/should/not/run")
        self.assertEqual(r.stdout.strip(), "INDICATOR-READER src=/x/SpotArchiveReader.java args=5 --topic t", r.stderr)
        with tempfile.TemporaryDirectory() as d:
            stub = Path(d) / "stub"
            stub.write_text('#!/bin/sh\necho "STUB $*"\n')
            stub.chmod(0o755)
            r = sh(PRELUDE + block() + '\nrun_spot_reader 5 --topic t', SPOT_READER=str(stub))
            self.assertEqual(r.stdout.strip(), "STUB --topic t", r.stderr)

    def test_capture_order_spot_then_indicator_then_console_with_its_own_registry_override(self):
        spot = ARCHIVE.index('if is_spot_avro_topic "$topic"; then')
        ind = ARCHIVE.index('elif is_indicator_avro_topic "$topic"; then')
        console = ARCHIVE.index('kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \\\n           --topic "$topic" --partition "$part" --offset "$from"')
        self.assertLess(spot, ind)
        self.assertLess(ind, console)
        self.assertIn('--registry "$(archive_registry_url "${OE_SPOT_REGISTRY_URL:-}")"', ARCHIVE[spot:ind])
        self.assertEqual(ARCHIVE.count("is_spot_avro_topic"), 2, "one definition, one capture use")

    def test_script_syntax(self):
        self.assertEqual(subprocess.run(["bash", "-n", str(ROOT / "scripts/ops/archive/oe-archive-kafka.sh")], capture_output=True).returncode, 0)

    def test_deploy_unit_ships_builds_and_tests_the_spot_reader(self):
        t = (ROOT / "Jenkinsfile.archive-scripts-deploy").read_text()
        self.assertIn("SpotArchiveReader.java", re.search(r'UNIT\s*=\s*"([^"]*)"', t).group(1).split())
        self.assertIn("scripts/ops/archive/SpotArchiveReader.java", t)
        self.assertIn("com/optionsedge/contracts/hpsf/UnderlyingPriceAvro.class", t, "the built jar must carry the spot serde")
        self.assertIn("tests.test_spot_archive_reader", t)
        self.assertTrue((ROOT / "scripts/ops/archive/SpotArchiveReader.java").exists())

    def test_cutover_doc_states_the_switch(self):
        d = (ROOT / "docs/UNDERLYING-SPX-PRICE-AVRO-CUTOVER.md").read_text()
        self.assertIn("OE_SPOT_AVRO_TOPICS", d)
        self.assertIn("SpotArchiveReader", d)


if __name__ == "__main__":
    unittest.main()
