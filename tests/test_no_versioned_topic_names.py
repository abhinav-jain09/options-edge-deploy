"""No topic name in this estate carries a version suffix.

Owner, 2026-10-18: "there is no version, it is always rolled forward -- if you have a version
anywhere, delete the old version, and no version in the system." A topic that must change its
serialization is DELETED AND RECREATED under its own name; it is never migrated to a `<topic>.vN`
sibling, and any sibling that was minted is deleted.

This is a REGRESSION test, and it is the only one that can fail on the whole rule rather than on one
topic. It would have failed before the 2026-10-18 change on five names:
`options.ibkr.gex.status.v2`, `options.databento.gex.oi-status.v2`,
`es.options.databento.gex.oi-status.v2`, `rates.databento.zn.options.raw.v1` and
`rates.databento.zn.options.gex.strike.v1`.

Why it matters more than tidiness: a versioned name is one nothing creates, writes or reads, so a
stale one in a declaration or a ConfigMap points a producer at a topic no consumer reads, or a
consumer at a topic nothing writes. Both surface as an empty panel in the browser rather than as an
error -- the failure that cost ten hours on dev and was live on production for both the pre-open
producer and the gateway on 2026-10-18.

unittest, not bare pytest-style functions: pytest is not installed and CI runs `python3 -m unittest`
(see test_es4_topics_ssot.py's header for the file that silently collected zero tests).
"""
import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ENV = (REPO / "scripts" / "kafka" / "topics.env").read_text()

VERSIONED = re.compile(r"\.v[0-9]+$")

# A topic name is the token before an optional ":<partitions>" or "=<value>" suffix.
TOPIC_LIST_VARS = re.compile(r"^(OPTIONS_EDGE_[A-Z0-9_]*TOPICS)=\"([^\"]*)\"$", re.M)
OVERRIDE_VARS = re.compile(r"^(OPTIONS_EDGE_[A-Z0-9_]*OVERRIDES)=\"([^\"]*)\"$", re.M)


def _declared_topic_names():
    """Every topic NAME topics.env declares, from every list and every per-topic override map.

    Derived from the variable names rather than a hand-kept list of lists: a new
    OPTIONS_EDGE_..._TOPICS introduced later is covered without anyone remembering to add it here.
    """
    names = {}
    for pattern in (TOPIC_LIST_VARS, OVERRIDE_VARS):
        for var, body in pattern.findall(ENV):
            for entry in body.split():
                if entry.startswith("$"):
                    continue
                name = entry.split("=", 1)[0]
                name = name.rsplit(":", 1)[0] if re.search(r":[0-9]+$", name) else name
                names.setdefault(name, set()).add(var)
    return names


class NoVersionedTopicNames(unittest.TestCase):

    def test_no_declared_topic_name_carries_a_version_suffix(self):
        names = _declared_topic_names()
        self.assertGreater(len(names), 100, "the declaration parse found almost nothing — a "
                                            "vacuous pass is the one way this test can lie")
        offenders = {n: sorted(v) for n, v in names.items() if VERSIONED.search(n)}
        self.assertEqual({}, offenders,
                         "topics are rolled forward under their own name; these declarations name a "
                         "versioned sibling: %s" % offenders)

    def test_no_regex_in_topics_env_admits_a_versioned_name(self):
        """The keep-lists and protection regexes must not carry a `.vN` alternative either.

        premarket-reset.sh's PRESERVE_TOPICS_REGEX held `^options\\.ibkr\\.gex\\.status(\\.v2)?$`
        until 2026-10-18 — a surviving versioned code path that normalised keeping the retired
        sibling, and one that no topic-name list would have caught.
        """
        for rel in ("scripts/kafka/topics.env", "scripts/ops/premarket-reset.sh"):
            text = (REPO / rel).read_text()
            for m in re.finditer(r"\\\.v[0-9]+", text):
                line = text[: m.start()].count("\n") + 1
                self.fail("%s:%d escapes a version suffix into a topic regex: %s"
                          % (rel, line, text.splitlines()[line - 1].strip()[:160]))

    def test_no_kubernetes_manifest_sets_a_versioned_topic(self):
        """Env values are the OTHER place a versioned name reaches a client.

        The topic a workload reads is an env value with the declaration only as its default, so a
        manifest or ConfigMap is where a stale `.vN` actually lives. Both generated service slices
        and hand-written bases are scanned.
        """
        offenders = []
        for path in sorted((REPO / "k8s").rglob("*.yaml")):
            for n, line in enumerate(path.read_text().splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                for m in re.finditer(r"[\"']?([a-z0-9][a-z0-9.\-]*\.v[0-9]+)[\"']?\s*$", stripped):
                    name = m.group(1)
                    if "." not in name.rsplit(".v", 1)[0]:
                        continue          # not a dotted topic name (e.g. an image tag)
                    offenders.append("%s:%d %s" % (path.relative_to(REPO), n, stripped[:120]))
        self.assertEqual([], offenders,
                         "a Kubernetes manifest names a versioned topic:\n" + "\n".join(offenders))

    def test_the_preopen_status_topic_is_pure_compact_in_both_environments(self):
        """The one shape change the roll-forward carries, asserted rather than described.

        options.ibkr.gex.status holds the pre-open identity ledger, which the mint reads back, so the
        latest record per key must be non-evictable: pure compact. topic_cleanup_policy() checks
        OPTIONS_EDGE_PURE_COMPACT_TOPICS FIRST and returns "compact", so membership there is what
        decides it — but leaving the name in OPTIONS_EDGE_COMPACTED_TOPICS would make this file say
        two things, and the prod-only subtraction iterates that list and would claim to drop a
        compaction that pure-compact had already won. The topic must be in exactly one of them.
        """
        def names(var):
            out = set()
            for m in re.finditer(rf'^{var}="([^"]*)"$', ENV, re.M):
                for e in m.group(1).split():
                    if not e.startswith("$"):
                        out.add(e.rsplit(":", 1)[0])
            return out

        topic = "options.ibkr.gex.status"
        self.assertIn(topic, names("OPTIONS_EDGE_PURE_COMPACT_TOPICS"))
        self.assertNotIn(topic, names("OPTIONS_EDGE_COMPACTED_TOPICS"))
        self.assertNotIn(topic, names("OPTIONS_EDGE_PROD_ONLY_UNCOMPACTED_TOPICS"))
        # And it keeps every other declaration the ledger depends on.
        self.assertIn(topic, names("OPTIONS_EDGE_RESET_PRESERVED_TOPICS"))
        self.assertIn(topic, names("OPTIONS_EDGE_EXACT_PARTITION_TOPICS"))
        self.assertIn("%s:1" % topic, [e for m in re.finditer(r'^OPTIONS_EDGE_TOPICS="([^"]*)"$', ENV, re.M)
                                       for e in m.group(1).split()],
                      "exactly 1 partition: the mint probe reads partition 0 by assign()")
        self.assertIn("%s=-1" % topic, [e for m in re.finditer(r'^OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="([^"]*)"$', ENV, re.M)
                                        for e in m.group(1).split()])
        self.assertIn("%s=172800000" % topic,
                      [e for m in re.finditer(r'^OPTIONS_EDGE_TOPIC_DELETE_RETENTION_OVERRIDES="([^"]*)"$', ENV, re.M)
                       for e in m.group(1).split()])


if __name__ == "__main__":
    unittest.main()
