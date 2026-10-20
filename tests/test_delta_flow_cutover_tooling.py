"""The delta-flow Avro cutover tooling (docs/DELTA-FLOW-AVRO-CUTOVER.md): the Streams-state reset script against stub Kafka CLIs, and the
topic declarations the wipe+recreate relies on (every delta-flow output topic declared at 32 partitions for dev/prod and under the es.
prefix for es4, none of them preserved across a reset). The schema registration script has its own test file
(test_ensure_delta_flow_schemas.py)."""
from __future__ import annotations

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESET = ROOT / "scripts/kafka/reset-delta-flow-state.sh"
APP = "options-edge-delta-flow-service-dev-r2"
STORES = ["latest-quote-store", "cumulative-delta-flow-store", "strike-delta-flow-store", "rolling-delta-flow-store",
          "delta-flow-duplicate-trade-store", "delta-flow-accepted-watermark-store", "delta-flow-trading-date-index-store",
          "positioning-store"]
INTERNAL = [f"{APP}-trades-by-symbol-repartition", f"{APP}-quotes-by-symbol-repartition"] + [f"{APP}-{s}-changelog" for s in STORES]
OUTPUTS = ["delta-flow-by-trade", "delta-flow-by-strike", "delta-flow-10s", "delta-flow-30s", "delta-flow-1m", "delta-flow-5m",
           "delta-flow-15m", "delta-flow-session", "delta-flow-dashboard", "delta-flow-diagnostics", "delta-flow-corrections"]


def reset(state="Empty", topics=None, env=None, list_rc=0, states=None):
    app = (env or {}).get("DELTA_FLOW_APP_ID", APP)
    tmp = Path(tempfile.mkdtemp())
    calls = tmp / "calls"
    calls.touch()
    listing = tmp / "list"
    listing.write_text("\n".join(topics if topics is not None else INTERNAL + OUTPUTS + ["other"]) + "\n")
    seq = tmp / "seq"
    seq.write_text("0")
    arr = " ".join(states or [state])
    kcg = tmp / "kafka-consumer-groups"
    kcg.write_text(
        "#!/bin/bash\n" f'echo "KCG $*" >> "{calls}"\n' 'case " $* " in\n'
        f'  *" --state "*) n=$(cat "{seq}"); set -- {arr}; i=$(( n < $# ? n : $# - 1 )); shift $i; st=$1; echo $((n + 1)) > "{seq}";'
        f' if [ "$st" = absent ]; then echo "Consumer group \'{app}\' does not exist."; exit 1; fi;'
        f' printf "GROUP  COORDINATOR  ASSIGNMENT-STRATEGY  STATE  #MEMBERS\\n{app}  b:1 (1)  range  $st  0\\n"; exit 0 ;;\n'
        '  *" --delete "*) exit 0 ;;\nesac\nexit 1\n')
    kcg.chmod(0o755)
    kt = tmp / "kafka-topics"
    kt.write_text(
        "#!/bin/bash\n" f'echo "KT $*" >> "{calls}"\n' 'case " $* " in\n'
        f'  *" --list "*) [ {list_rc} -ne 0 ] && {{ echo "broker down"; exit {list_rc}; }}; grep -v "^__deleted__$" "{listing}"; exit 0 ;;\n'
        f'  *" --delete --topic "*) t="${{@: -1}}"; sed -i.bak "s/^$t$/__deleted__/" "{listing}"; exit 0 ;;\n'
        "esac\nexit 1\n")
    kt.chmod(0o755)
    e = dict(os.environ, KAFKA_BOOTSTRAP_SERVERS="b:1", DELTA_FLOW_APP_ID=APP, KAFKA_CONSUMER_GROUPS_CMD=str(kcg),
             KAFKA_TOPICS_CMD=str(kt), KAFKA_TOPIC_DELETE_WAIT_SECONDS="3", GROUP_EMPTY_WAIT_S="0")
    e.update(env or {})
    r = subprocess.run(["bash", str(RESET)], capture_output=True, text=True, env=e)
    return r.returncode, r.stdout + r.stderr, calls.read_text().splitlines()


class ResetState(unittest.TestCase):
    def test_empty_group_is_deleted_and_exactly_the_ten_internal_topics_are_deleted(self):
        rc, out, calls = reset("Empty")
        self.assertEqual(rc, 0, out)
        deleted = [c.split("--topic ")[1] for c in calls if c.startswith("KT") and "--delete" in c]
        self.assertEqual(sorted(deleted), sorted(INTERNAL))
        self.assertEqual(len(INTERNAL), 10)
        self.assertTrue(any(c.startswith("KCG") and "--delete --group " + APP in c for c in calls), calls)
        self.assertFalse(any("--reset-offsets" in c for c in calls), "offsets are NOT replayed from 0 (see the script header)")
        for public in OUTPUTS:
            self.assertNotIn(public, deleted, "the public topics are wiped by roll-forward-topics.sh, never by the state reset")
        self.assertIn("=== reset-delta-flow-state: OK ===", out)

    def test_absent_group_and_absent_topics_are_fine(self):
        rc, out, calls = reset("absent", topics=["other"])
        self.assertEqual(rc, 0, out)
        self.assertIn(f"group {APP} absent", out)
        self.assertFalse(any("--delete" in c for c in calls))
        self.assertEqual(out.count("absent options-edge-delta-flow-service-dev-r2-"), 10)

    def test_optional_internal_topics_that_do_not_exist_are_reported_absent_not_failed(self):
        listing = [t for t in INTERNAL if "quotes-by-symbol" not in t and "positioning-store" not in t] + OUTPUTS
        rc, out, calls = reset("Empty", topics=listing)
        self.assertEqual(rc, 0, out)
        self.assertIn(f"absent {APP}-quotes-by-symbol-repartition", out)
        self.assertIn(f"absent {APP}-positioning-store-changelog", out)

    def test_only_an_absent_or_empty_group_is_reset(self):
        for st in ("Stable", "PreparingRebalance", "Dead", "Garbage"):
            rc, out, calls = reset(st)
            self.assertEqual(rc, 1, st)
            self.assertIn("only an absent or Empty group may be reset", out)
            self.assertFalse(any("--delete" in c for c in calls), st)
        rc, out, calls = reset(states=["Stable", "Empty"], env={"GROUP_EMPTY_WAIT_S": "30"})
        self.assertEqual(rc, 0, out)
        self.assertIn("waiting for the last member to leave", out)

    def test_an_unlistable_broker_is_an_unknown_not_an_empty_cluster(self):
        rc, out, calls = reset("Empty", list_rc=1)
        self.assertEqual(rc, 1)
        self.assertIn("cannot list topics", out)
        self.assertFalse(any("--delete" in c for c in calls))

    def test_unknown_application_id_is_refused_before_any_call(self):
        for bad in ("options-edge-delta-flow-service", "options-edge-delta-flow-service-dev", "options-edge-delta-flow-service-dev-r1",
                    "options-edge-strike-invasion-service-dev", "options-edge-delta-flow-service-qa-r2"):
            rc, out, calls = reset("Empty", env={"DELTA_FLOW_APP_ID": bad})
            self.assertEqual(rc, 1, bad)
            self.assertIn("refusing to guess", out)
            self.assertEqual(calls, [], bad)

    def test_prod_and_es4_ids_are_accepted(self):
        for ok in ("options-edge-delta-flow-service-prod-r2", "options-edge-delta-flow-service-es4-r2"):
            rc, out, calls = reset("absent", topics=["other"], env={"DELTA_FLOW_APP_ID": ok})
            self.assertEqual(rc, 0, out)

    def test_dry_run_mutates_nothing(self):
        rc, out, calls = reset("Empty", env={"DEPLOY_DRY_RUN": "true"})
        self.assertEqual(rc, 0, out)
        self.assertIn("would delete group", out)
        self.assertFalse(any("--delete" in c for c in calls))

    def test_the_store_and_repartition_names_match_the_service(self):
        text = RESET.read_text()
        for s in STORES:
            self.assertIn(s, text)
        self.assertIn("trades-by-symbol-repartition", text)
        self.assertIn("quotes-by-symbol-repartition", text)


def var(name: str) -> list[str]:
    out = subprocess.run(["bash", "-c", f'source {ROOT}/scripts/kafka/topics.env; echo "${{{name}:-}}"'], capture_output=True, text=True).stdout
    return out.split()


class Declarations(unittest.TestCase):
    def test_every_output_topic_is_declared_at_32_partitions_for_dev_and_prod(self):
        declared = var("OPTIONS_EDGE_TOPICS")
        for t in OUTPUTS:
            self.assertIn(f"{t}:32", declared, t)

    def test_every_output_topic_is_declared_for_es4_under_the_es_prefix(self):
        declared = var("OPTIONS_EDGE_ES4_TOPICS")
        for t in OUTPUTS:
            self.assertIn(f"es.{t}:32", declared, t)

    def test_no_delta_flow_topic_is_preserved_across_a_reset(self):
        preserved = " ".join(var("OPTIONS_EDGE_RESET_PRESERVED_TOPICS"))
        for t in OUTPUTS:
            self.assertFalse(re.search(rf"(^|\s)(es\.)?{re.escape(t)}(:|\s|$)", preserved), t)


if __name__ == "__main__":
    unittest.main()
