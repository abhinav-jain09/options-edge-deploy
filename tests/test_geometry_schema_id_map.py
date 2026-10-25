"""The geometry signal adapter's fixed-id map (scripts/kafka/schema-id-maps/geometry-signal-adapter.map): well formed for ensure-fixed-id-schemas.sh and consistent with the contracts
checkout when one is available (CONTRACTS_DIR=<checkout>), skipped otherwise."""
import json, os, pathlib, unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
MAP = ROOT / "scripts/kafka/schema-id-maps/geometry-signal-adapter.map"


def rows():
    return [l.split("|") for l in MAP.read_text().splitlines() if l.strip() and not l.lstrip().startswith("#")]


class GeometrySchemaIdMap(unittest.TestCase):
    def test_ids_are_the_contiguous_geometry_block_and_unique(self):
        r = rows()
        self.assertEqual([2801, 2802, 2803, 2804, 2805, 2806, 2807], [int(x[1]) for x in r])
        self.assertEqual(len({x[0] for x in r}), len(r))
        for subject, _id, f in r:
            self.assertEqual(3, len(r[0]))
            self.assertEqual("com.optionsedge.contracts.geometry." + f[:-5], subject)

    def test_every_row_matches_the_contracts_schema_file(self):
        d = os.environ.get("CONTRACTS_DIR")
        if not d:
            self.skipTest("CONTRACTS_DIR not set")
        for subject, _id, f in rows():
            schema = json.loads((pathlib.Path(d) / "schemas-registry/geometry" / f).read_text())
            self.assertEqual(subject, schema["namespace"] + "." + schema["name"])


if __name__ == "__main__":
    unittest.main()
