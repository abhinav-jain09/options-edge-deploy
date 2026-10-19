"""The strike-invasion Avro cutover tooling (docs/STRIKE-INVASION-AVRO-CUTOVER.md): the schema registration script against the
stand-in registry, the Streams-state reset script against stub Kafka CLIs, and the topic declarations the wipe+recreate
relies on (public topics declared, all seven repartition topics declared for dev, current classified REBUILDABLE)."""
from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

from stub_schema_registry import Registry, serve

ROOT = Path(__file__).resolve().parents[1]
ENSURE = ROOT / "scripts/kafka/ensure-strike-invasion-schemas.sh"
RESET = ROOT / "scripts/kafka/reset-strike-invasion-state.sh"
NS = "com.optionsedge.contracts.strikeinvasion."
NAMES = ["StrikeInvasionSnapshot"] + ["StrikeInvasionEvent" + t for t in (
    "NonFiniteSpotDropped", "OutOfOrderSpotDropped", "NonFiniteStrikeDropped", "SessionReset", "CandidateInvalidated",
    "Stage1Retired", "State", "Stage1", "Invasion", "Stage2Timeout", "DefenseBroken", "Stage2Verdict")] + [
    "StrikeInvasion" + t for t in ("SpotEvidence", "SrEvidence", "GexEvidence", "FlowEvidence", "BehaviorEvidence",
                                   "PaceEvidence", "PaceFrame", "WallFrame", "WallRow", "WallMeta")]


def schemas_dir(names=NAMES, rename: dict | None = None) -> str:
    d = Path(tempfile.mkdtemp())
    for n in names:
        rec = (rename or {}).get(n, n)
        (d / (n + ".avsc")).write_text(json.dumps({"type": "record", "name": rec, "namespace": NS[:-1],
                                                   "fields": [{"name": "a", "type": "long"}]}))
    return str(d)


def ensure(url, d, dry=False):
    r = subprocess.run(["bash", str(ENSURE)], capture_output=True, text=True,
                       env=dict(os.environ, SCHEMA_REGISTRY_URL=url, SCHEMAS_DIR=d, DEPLOY_DRY_RUN="true" if dry else "false"))
    return r.returncode, r.stdout + r.stderr


class EnsureSchemas(unittest.TestCase):
    def test_fresh_registry_gets_all_23_subjects_under_full_and_rerun_is_idempotent(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            d = schemas_dir()
            rc, out = ensure(url, d)
            self.assertEqual(rc, 0, out)
            self.assertEqual(sorted(reg.subjects), sorted(NS + n for n in NAMES))
            self.assertEqual(len(NAMES), 23)
            self.assertTrue(all(len(v) == 1 for v in reg.subjects.values()))
            self.assertTrue(all(reg.configs[s] == "FULL" for s in reg.subjects))
            rc, out = ensure(url, d)
            self.assertEqual(rc, 0, out)
            self.assertTrue(all(len(v) == 1 for v in reg.subjects.values()), "no second version")
        finally:
            server.shutdown()

    def test_a_drifted_config_is_repaired_and_a_changed_schema_is_refused_as_not_canonical_latest(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            d = schemas_dir()
            self.assertEqual(ensure(url, d)[0], 0)
            reg.configs[NS + "StrikeInvasionSnapshot"] = "BACKWARD"
            self.assertEqual(ensure(url, d)[0], 0)
            self.assertEqual(reg.configs[NS + "StrikeInvasionSnapshot"], "FULL")
            reg.register(NS + "StrikeInvasionEventState", json.dumps({"type": "record", "name": "StrikeInvasionEventState", "namespace": NS[:-1], "fields": []}))
            rc, out = ensure(url, d)
            self.assertNotEqual(rc, 0)
            self.assertIn("not the canonical schema", out)
        finally:
            server.shutdown()

    def test_a_missing_extra_or_misnamed_file_is_refused_before_any_call(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = ensure(url, schemas_dir(NAMES[:-1]))
            self.assertNotEqual(rc, 0)
            self.assertIn("missing=['StrikeInvasionWallMeta.avsc']", out)
            rc, out = ensure(url, schemas_dir(NAMES + ["Extra"]))
            self.assertNotEqual(rc, 0)
            self.assertIn("extra=['Extra.avsc']", out)
            rc, out = ensure(url, schemas_dir(rename={"StrikeInvasionSnapshot": "Other"}))
            self.assertNotEqual(rc, 0)
            self.assertIn("not " + NS + "StrikeInvasionSnapshot", out)
            self.assertEqual(reg.calls, [])
        finally:
            server.shutdown()

    def test_dry_run_mutates_nothing(self):
        reg = Registry()
        server, url = serve(reg)
        try:
            rc, out = ensure(url, schemas_dir(), dry=True)
            self.assertEqual(rc, 0, out)
            self.assertIn("DRY RUN", out)
            self.assertEqual(reg.subjects, {})
            self.assertEqual(reg.configs, {})
        finally:
            server.shutdown()


APP = "options-edge-strike-invasion-service-dev"
INTERNAL = [f"{APP}-repart-{s}" for s in ("spot", "sr", "gex", "flow", "behavior", "pace", "wall")] + \
           [f"{APP}-{s}-store-changelog" for s in ("sr", "gex", "flow", "behavior", "pace")]


def reset(state="Empty", topics=None, env=None, list_rc=0, states=None):
    tmp = Path(tempfile.mkdtemp())
    calls = tmp / "calls"
    calls.touch()
    listing = tmp / "list"
    listing.write_text("\n".join(topics if topics is not None else INTERNAL + ["options.spx.strike-invasion.current", "other"]) + "\n")
    seq = tmp / "seq"
    seq.write_text("0")
    arr = " ".join(states or [state])
    kcg = tmp / "kafka-consumer-groups"
    kcg.write_text(
        "#!/bin/bash\n" f'echo "KCG $*" >> "{calls}"\n' 'case " $* " in\n'
        f'  *" --state "*) n=$(cat "{seq}"); set -- {arr}; i=$(( n < $# ? n : $# - 1 )); shift $i; st=$1; echo $((n + 1)) > "{seq}";'
        f' if [ "$st" = absent ]; then echo "Consumer group \'{APP}\' does not exist."; exit 1; fi;'
        f' printf "GROUP  COORDINATOR  ASSIGNMENT-STRATEGY  STATE  #MEMBERS\\n{APP}  b:1 (1)  range  $st  0\\n"; exit 0 ;;\n'
        '  *" --delete "*) exit 0 ;;\nesac\nexit 1\n')
    kcg.chmod(0o755)
    kt = tmp / "kafka-topics"
    kt.write_text(
        "#!/bin/bash\n" f'echo "KT $*" >> "{calls}"\n' 'case " $* " in\n'
        f'  *" --list "*) [ {list_rc} -ne 0 ] && {{ echo "broker down"; exit {list_rc}; }}; grep -v "^__deleted__$" "{listing}"; exit 0 ;;\n'
        f'  *" --delete --topic "*) t="${{@: -1}}"; sed -i.bak "s/^$t$/__deleted__/" "{listing}"; exit 0 ;;\n'
        "esac\nexit 1\n")
    kt.chmod(0o755)
    e = dict(os.environ, KAFKA_BOOTSTRAP_SERVERS="b:1", STRIKE_INVASION_APP_ID=APP, KAFKA_CONSUMER_GROUPS_CMD=str(kcg),
             KAFKA_TOPICS_CMD=str(kt), KAFKA_TOPIC_DELETE_WAIT_SECONDS="3", GROUP_EMPTY_WAIT_S="0")
    e.update(env or {})
    r = subprocess.run(["bash", str(RESET)], capture_output=True, text=True, env=e)
    return r.returncode, r.stdout + r.stderr, calls.read_text().splitlines()


class ResetState(unittest.TestCase):
    def test_empty_group_is_deleted_and_exactly_the_twelve_internal_topics_are_deleted(self):
        rc, out, calls = reset("Empty")
        self.assertEqual(rc, 0, out)
        deleted = [c.split("--topic ")[1] for c in calls if c.startswith("KT") and "--delete" in c]
        self.assertEqual(sorted(deleted), sorted(INTERNAL))
        self.assertTrue(any(c.startswith("KCG") and "--delete --group " + APP in c for c in calls), calls)
        self.assertFalse(any("--reset-offsets" in c for c in calls), "offsets are NOT replayed from 0 (see the script header)")
        self.assertNotIn("options.spx.strike-invasion.current", deleted)
        self.assertIn("=== reset-strike-invasion-state: OK ===", out)

    def test_absent_group_and_absent_topics_are_fine(self):
        rc, out, calls = reset("absent", topics=["other"])
        self.assertEqual(rc, 0, out)
        self.assertIn(f"group {APP} absent", out)
        self.assertFalse(any("--delete" in c for c in calls))
        self.assertEqual(out.count("absent options-edge-strike-invasion-service-dev-"), 12)

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

    def test_unknown_application_id_is_refused(self):
        rc, out, calls = reset("Empty", env={"STRIKE_INVASION_APP_ID": "options-edge-strike-invasion-service"})
        self.assertEqual(rc, 1)
        self.assertIn("refusing to guess", out)
        self.assertEqual(calls, [])

    def test_dry_run_mutates_nothing(self):
        rc, out, calls = reset("Empty", env={"DEPLOY_DRY_RUN": "true"})
        self.assertEqual(rc, 0, out)
        self.assertIn("would delete group", out)
        self.assertFalse(any("--delete" in c for c in calls))


def var(name: str) -> list[str]:
    out = subprocess.run(["bash", "-c", f'source {ROOT}/scripts/kafka/topics.env; echo "${{{name}:-}}"'], capture_output=True, text=True).stdout
    return out.split()


class Declarations(unittest.TestCase):
    def test_public_topics_are_declared_where_the_recreate_reads_them(self):
        self.assertIn("options.spx.strike-invasion.current:32", var("OPTIONS_EDGE_TOPICS"))
        self.assertIn("options.spx.strike-invasion.current=-1", var("OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"))
        self.assertIn("options.spx.strike-invasion.current", var("OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS"))
        self.assertIn("options.spx.strike-invasion.events:1", var("OPTIONS_EDGE_PARTITION_ONLY_TOPICS"))
        self.assertNotIn("options.spx.strike-invasion.events", " ".join(var("OPTIONS_EDGE_RESET_PRESERVED_TOPICS")))
        self.assertNotIn("options.spx.strike-invasion.current", " ".join(var("OPTIONS_EDGE_RESET_PRESERVED_TOPICS")))

    def test_all_seven_dev_repartition_topics_are_created_up_front_with_one_partition(self):
        po = var("OPTIONS_EDGE_PARTITION_ONLY_TOPICS")
        for s in ("spot", "sr", "gex", "flow", "behavior", "pace", "wall"):
            self.assertIn(f"{APP}-repart-{s}:1", po, s)

    def test_the_repartition_names_match_the_service_and_the_reset_script(self):
        text = RESET.read_text()
        self.assertIn("spot sr gex flow behavior pace wall", text)
        self.assertIn("sr gex flow behavior pace; do", text)


if __name__ == "__main__":
    unittest.main()
