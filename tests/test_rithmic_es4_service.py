# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
import unittest,yaml
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
class RithmicEs4Service(unittest.TestCase):
    def test_mirrors_have_no_credentials_and_correct_dev_listener_alias(self):
        docs=list(yaml.safe_load_all((ROOT/'k8s/es4/services/rithmic-depth-bridge.yaml').read_text()))
        deployments={d['metadata']['name']:d for d in docs if d['kind']=='Deployment'}
        self.assertEqual(set(deployments),{'rithmic-depth-bridge','rithmic-depth-mirror-dev','rithmic-depth-mirror-production','rithmic-mbo-mirror-dev'})
        for name,d in deployments.items():
            pod=d['spec']['template']['spec'];self.assertFalse(pod['automountServiceAccountToken'])
            if name!='rithmic-depth-bridge':
                env=pod['containers'][0]['env'];self.assertFalse(any('secretKeyRef' in e.get('valueFrom',{}) for e in env))
                self.assertIn({'ip':'192.168.100.102','hostnames':['host.docker.internal']},pod['hostAliases'])
    def test_existing_service_job_owns_rollout_and_bulk_excludes_it(self):
        job=(ROOT/'Jenkinsfile.es4-deploy').read_text()
        self.assertIn("params.SERVICE == 'rithmic-depth-bridge'",job)
        self.assertIn('RITHMIC_PERMITTED_SHA',job)
        self.assertEqual(job.count("stage('Deploy to es4')"),1)
        self.assertNotIn("stage('Deploy Rithmic service on ES4')",job)
        self.assertIn("if (params.ACTION == 'deploy-service' && params.SERVICE == 'rithmic-depth-bridge')",job)
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
        job=(ROOT/'Jenkinsfile.es4-deploy').read_text();stage=job[job.index("stage('Deploy to es4')"):]
        self.assertLess(stage.index('auth can-i create pods/exec'),stage.index('python" scripts/deploy-kubernetes.py'))
        role=list(yaml.safe_load_all((ROOT/'k8s/es4/security/jenkins-deployer-rbac.yaml').read_text()))
        self.assertTrue(any('pods/exec' in rule.get('resources',[]) and 'create' in rule['verbs'] for d in role for rule in d.get('rules',[])))

    def test_dev_clean_keeps_the_externally_mirrored_raw_topic(self):
        import subprocess
        value=subprocess.check_output(['bash','-c','source scripts/kafka/topics.env; printf "%s" "$OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"'],cwd=ROOT,text=True)
        self.assertIn('es.rithmic.depth',value.split())

    def test_unicode_virtualenv_outside_checkout_preserves_strict_source_guard(self):
        import tempfile,subprocess,os
        with tempfile.TemporaryDirectory() as temporary:
            parent=Path(temporary);repo=parent/'checkout';repo.mkdir()
            def git(*args):return subprocess.check_output(['git',*args],cwd=repo,text=True).strip()
            git('init','-q','--initial-branch=main');git('config','core.quotePath','true')
            (repo/'.gitignore').write_text('.jenkins-tmp/\n');(repo/'source.txt').write_text('reviewed\n')
            git('add','.');git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','fixture')
            revision=git('rev-parse','HEAD');venv=repo/'.jenkins-tmp/rithmic-venv';(venv/'bin').mkdir(parents=True)
            (venv/'bin/𝜋thon').write_text('generated test tool\n')
            def guard():return subprocess.run(['bash',str(ROOT/'scripts/jenkins/verify-permitted-tree.sh'),'--dir',str(repo),'--allow-ignored','.jenkins-tmp'],env={**os.environ,'PERMITTED_SHA':revision},capture_output=True,text=True)
            rejected=guard();self.assertNotEqual(rejected.returncode,0);self.assertIn('REFUSED',rejected.stdout+rejected.stderr)
            venv.rename(parent/'jenkins-test-venv')
            accepted=guard();self.assertEqual(accepted.returncode,0,accepted.stdout+accepted.stderr)
            (repo/'source.txt').write_text('unreviewed\n')
            self.assertNotEqual(guard().returncode,0)
