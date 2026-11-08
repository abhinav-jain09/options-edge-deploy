# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
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
