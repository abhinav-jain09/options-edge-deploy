# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
import pathlib,unittest,yaml,subprocess
ROOT=pathlib.Path(__file__).resolve().parents[1]
class RithmicMboDeployTest(unittest.TestCase):
 def test_existing_es4_service_owns_mbo_and_dev_mirror_has_no_credentials(self):
  docs=list(yaml.safe_load_all((ROOT/'k8s/es4/services/rithmic-depth-bridge.yaml').read_text()))
  deps={x['metadata']['name']:x for x in docs if x['kind']=='Deployment'}
  c=deps['rithmic-depth-bridge']['spec']['template']['spec']['containers'][0]
  env={x['name']:x.get('value') for x in c['env']}
  self.assertEqual(env['RITHMIC_MBO_ENABLED'],'true');self.assertEqual(c['resources']['limits']['memory'],'1Gi')
  m=deps['rithmic-mbo-mirror-dev']['spec']['template']['spec'];e={x['name']:x.get('value') for x in m['containers'][0]['env']}
  self.assertEqual(e,{'RITHMIC_MIRROR_TARGET':'dev','RITHMIC_MIRROR_TOPIC':'es.rithmic.mbo'})
  self.assertFalse(m['automountServiceAccountToken']);self.assertNotIn('envFrom',m['containers'][0])
  self.assertEqual(m['containers'][0]['command'],['python3','/app/depth-mirror.py'])
  self.assertIn('rithmic-depth-mirror-production',deps);self.assertNotIn('rithmic-mbo-mirror-production',deps)
 def test_topic_is_declared_once_with_existing_single_partition_retention(self):
  script='source scripts/kafka/topics.env; printf "%s\\n%s\\n%s\\n" "$OPTIONS_EDGE_ES4_TOPICS" "$OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES" "$OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"'
  out=subprocess.check_output(['bash','-c',script],cwd=ROOT,text=True).splitlines()
  self.assertEqual(out[0].split().count('es.rithmic.mbo:1'),1)
  self.assertEqual(out[1].split().count('es.rithmic.mbo=43200000'),1)  # ES4 source stays 12 h: the live es4 config is not ours to change
  self.assertIn('es.rithmic.mbo',out[2].split())

 def test_reset_requires_and_restores_mbo_schema_before_any_wipe(self):
  cleanup=(ROOT/'scripts/es4/cleanup-es4.sh').read_text()
  self.assertIn('RithmicMboEvent.avsc',cleanup[:cleanup.index('# ------------------------------------------------------------ 1.')])
  self.assertIn('es.rithmic.mbo-value|3302|RithmicMboEvent.avsc|1',(ROOT/'scripts/kafka/schema-id-maps/rithmic-depth.map').read_text())

 def test_production_fleet_lists_exclude_mbo_but_keep_existing_depth(self):
  import os
  script='source scripts/kafka/topics.env; printf "%s\\n%s\\n%s\\n" "$OPTIONS_EDGE_TOPICS" "$OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES" "$OPTIONS_EDGE_ES4_TOPICS"'
  out=subprocess.check_output(['bash','-c',script],cwd=ROOT,env={**os.environ,'ENVIRONMENT':'production'},text=True).splitlines()
  self.assertNotIn('es.rithmic.mbo:1',out[0].split());self.assertIn('es.rithmic.depth:1',out[0].split())
  self.assertNotIn('es.rithmic.mbo=86400000',out[1].split());self.assertIn('es.rithmic.depth=86400000',out[1].split())
  self.assertIn('es.rithmic.mbo:1',out[2].split(),'ES4 source remains declared independently of fleet target')

 def test_schema_governance_covers_fixed_id_and_dev_only_byte_mirror(self):
  import json
  manifest=json.loads((ROOT/'schema-manifest/manifest.json').read_text())
  e=next(e for e in manifest['entries'] if e.get('subject')=='es.rithmic.mbo-value')
  self.assertEqual(e['id'],3302);self.assertEqual(e['version'],1);self.assertEqual(set(e['environments']),{'dev','es4'});self.assertFalse(e.get('pending',False))
  self.assertEqual(e['source'],{'kind':'FILE','repo':'contracts','path':'schemas/rithmic/RithmicMboEvent.avsc'})
  self.assertTrue((ROOT/'schema-manifest/schemas'/e['schemaFile']).is_file())
  mirrors=[m for m in manifest['activeMirrors'] if 'es.rithmic.mbo' in m['topics']]
  self.assertEqual(len(mirrors),1);self.assertEqual((mirrors[0]['from'],mirrors[0]['to'],mirrors[0]['transport']),('es4','dev','bytes'))

 def test_dev_retention_of_the_three_rithmic_book_topics_is_24_hours(self):
  # dev/default list, including the INDENTED dev-only es.rithmic.mbo line; ES4 source lists stay as they are
  script='source scripts/kafka/topics.env; printf "%s\\n" "$OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES"'
  dev=subprocess.check_output(['bash','-c',script],cwd=ROOT,text=True).split()
  for t in ('es.rithmic.depth','options.rithmic.es-depth-spx','es.rithmic.mbo'):
   self.assertEqual([e for e in dev if e.split('=')[0]==t],[t+'=86400000'])
