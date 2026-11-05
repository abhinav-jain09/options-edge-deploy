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
  self.assertEqual(out[1].split().count('es.rithmic.mbo=43200000'),1)
  self.assertIn('es.rithmic.mbo',out[2].split())
