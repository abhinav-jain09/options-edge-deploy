import unittest,yaml
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
class RithmicEs4Service(unittest.TestCase):
    def test_mirrors_have_no_credentials_and_correct_dev_listener_alias(self):
        docs=list(yaml.safe_load_all((ROOT/'k8s/es4/services/rithmic-depth-bridge.yaml').read_text()))
        deployments={d['metadata']['name']:d for d in docs if d['kind']=='Deployment'}
        self.assertEqual(set(deployments),{'rithmic-depth-bridge','rithmic-depth-mirror-dev','rithmic-depth-mirror-production'})
        for name,d in deployments.items():
            pod=d['spec']['template']['spec'];self.assertFalse(pod['automountServiceAccountToken'])
            if name!='rithmic-depth-bridge':
                env=pod['containers'][0]['env'];self.assertFalse(any('secretKeyRef' in e.get('valueFrom',{}) for e in env))
                self.assertIn({'ip':'192.168.100.102','hostnames':['host.docker.internal']},pod['hostAliases'])
    def test_existing_service_job_owns_rollout_and_bulk_excludes_it(self):
        job=(ROOT/'Jenkinsfile.es4-deploy').read_text()
        self.assertIn("params.SERVICE == 'rithmic-depth-bridge'",job)
        self.assertIn('RITHMIC_PERMITTED_SHA',job)
        self.assertIn('es-feed|rithmic-depth-bridge|spread-skew',job)
        registry=yaml.safe_load((ROOT/'services.yaml').read_text())
        service=next(s for s in registry['services'] if s['name']=='rithmic-depth-bridge')
        self.assertEqual(service['buildJob'],'es4-deploy');self.assertEqual(service['envs'],['es4'])

    def test_reset_restores_schema_before_source_or_mirrors(self):
        reset=(ROOT/'scripts/es4/cleanup-es4.sh').read_text()
        registration=reset.index('schema-id-maps/rithmic-depth.map')
        self.assertLess(registration,reset.index('restoring es4 Deployments to captured replica counts'))
        job=(ROOT/'Jenkinsfile.es4-deploy').read_text()
        self.assertIn('contracts/schemas/rithmic/',job)
        self.assertIn('es.rithmic.depth-value|3301|RithmicDepthFrame.avsc|1',(ROOT/'scripts/kafka/schema-id-maps/rithmic-depth.map').read_text())
    def test_exec_permission_is_provisioned_before_workload_effects(self):
        job=(ROOT/'Jenkinsfile.es4-deploy').read_text();stage=job[job.index("stage('Deploy Rithmic service on ES4')"):]
        self.assertLess(stage.index('auth can-i create pods/exec'),stage.index('python" scripts/deploy-kubernetes.py'))
        role=list(yaml.safe_load_all((ROOT/'k8s/es4/security/jenkins-deployer-rbac.yaml').read_text()))
        self.assertTrue(any('pods/exec' in rule.get('resources',[]) and 'create' in rule['verbs'] for d in role for rule in d.get('rules',[])))
