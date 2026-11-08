# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
import json,pathlib,unittest
ROOT=pathlib.Path(__file__).resolve().parents[1]
class MboReservationTest(unittest.TestCase):
 def test_schema_phase_and_runtime_activation_are_consistent(self):
  d=json.loads((ROOT/'schema-manifest/manifest.json').read_text());e=next(e for e in d['entries'] if e.get('subject')=='es.rithmic.mbo-value')
  self.assertEqual(e['id'],3302);self.assertTrue((ROOT/'schema-manifest/schemas'/e['schemaFile']).is_file())
  enabled='RITHMIC_MBO_ENABLED' in (ROOT/'k8s/es4/services/rithmic-depth-bridge.yaml').read_text()
  mirrors=[m for m in d['activeMirrors'] if 'es.rithmic.mbo' in m['topics']]
  if e.get('pending'):
   self.assertEqual(e['environments'],['dev']);self.assertEqual(e['producers'],[]);self.assertEqual(e['consumers'],[])
   self.assertFalse(enabled);self.assertFalse(mirrors)
  else:
   self.assertEqual(set(e['environments']),{'dev','es4'});self.assertTrue(e['producers']);self.assertTrue(enabled)
   self.assertEqual(len(mirrors),1);self.assertEqual((mirrors[0]['from'],mirrors[0]['to']),('es4','dev'))
