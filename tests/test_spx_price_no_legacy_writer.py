"""assert-no-legacy-writer.sh: a running MM1/bridge process that still names the topic, or a target that advances with no mirror of ours installed, fails."""
import json, os, pathlib, subprocess, tempfile, unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/kafka/spx-price-mirror/assert-no-legacy-writer.sh"
GO = '''#!/usr/bin/env python3
import os, sys
d = os.path.dirname(os.path.abspath(sys.argv[0])); st = os.path.join(d, "n")
n = int(open(st).read()) if os.path.exists(st) else 0
open(st, "w").write(str(n + 1))
ends = os.environ["ENDS"].split(",")
print("tgt:0:" + ends[min(n, len(ends) - 1)])
'''


def run(ps, ends="5,5"):
    d = pathlib.Path(tempfile.mkdtemp())
    (d / "kafka-get-offsets").write_text(GO); os.chmod(d / "kafka-get-offsets", 0o755)
    (d / "sleep").write_text("#!/bin/bash\nexit 0\n"); os.chmod(d / "sleep", 0o755)
    (d / "ps.sh").write_text("#!/bin/bash\ncat <<'EOT'\n" + ps + "\nEOT\n"); os.chmod(d / "ps.sh", 0o755)
    env = dict(os.environ, PATH=f"{d}:{os.environ['PATH']}", PS_CMD=str(d / "ps.sh"), ENDS=ends)
    r = subprocess.run(["bash", str(SCRIPT), "b:9092", "tgt", str(d)], capture_output=True, text=True, env=env)
    return r.returncode, r.stdout + r.stderr


QUIET = "/usr/bin/java -cp x kafka.tools.MirrorMaker --whitelist 'underlying\\.spx\\.index\\.price|underlying\\.vix\\.price'\n/bin/zsh"


class NoLegacyWriter(unittest.TestCase):
    def test_clean_host_and_steady_target_pass(self):
        self.assertEqual(run(QUIET)[0], 0)

    def test_a_running_mirrormaker_with_the_topic_in_its_whitelist_fails_even_if_the_target_is_steady(self):
        rc, out = run("java kafka.tools.MirrorMaker --whitelist 'a|underlying\\.spx\\.price|b'")
        self.assertEqual(rc, 1)
        self.assertIn("RUNNING", out)

    def test_the_es4_bridge_process_fails(self):
        self.assertEqual(run("/bin/bash /Users/abhinav/oe-ops/spx-es4-bridge/run.sh")[0], 1)
        self.assertEqual(run("kafka-console-producer --topic es.underlying.spx.price")[0], 1)

    def test_an_advancing_target_fails_with_a_clean_process_list(self):
        rc, out = run(QUIET, ends="5,9")
        self.assertEqual(rc, 1)
        self.assertIn("legacy writer is live", out)


if __name__ == "__main__":
    unittest.main()
