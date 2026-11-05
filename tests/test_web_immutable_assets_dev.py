import pathlib
import subprocess
import unittest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def render(relative):
    text = subprocess.check_output(['kubectl', 'kustomize', str(ROOT / relative)], text=True)
    return [item for item in yaml.safe_load_all(text) if item]


def resource(items, kind, name):
    return next(item for item in items if item['kind'] == kind and item['metadata']['name'] == name)


class WebImmutableAssetsDevTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.web = render('k8s/services/web/overlays/dev')
        cls.mono = render('k8s/overlays/dev')
        cls.infra = render('k8s/infra/overlays/dev')

    def test_web_mirrors_monolith_without_expanding_service_blast_radius(self):
        self.assertEqual({'Deployment', 'Service'}, {item['kind'] for item in self.web})
        for kind in ['Deployment', 'Service']:
            self.assertEqual(resource(self.web, kind, 'options-edge-web'), resource(self.mono, kind, 'options-edge-web'))

    def test_claim_belongs_to_existing_dev_infra(self):
        claim = resource(self.infra, 'PersistentVolumeClaim', 'web-immutable-assets')
        self.assertEqual(claim, resource(self.mono, 'PersistentVolumeClaim', 'web-immutable-assets'))
        self.assertEqual('standard', claim['spec']['storageClassName'])
        self.assertEqual(['ReadWriteOnce'], claim['spec']['accessModes'])
        self.assertEqual('2Gi', claim['spec']['resources']['requests']['storage'])

    def test_preparation_stays_disabled_and_app_volume_is_read_only(self):
        pod = resource(self.web, 'Deployment', 'options-edge-web')['spec']['template']['spec']
        app = next(c for c in pod['containers'] if c['name'] == 'web')
        init = next(c for c in pod['initContainers'] if c['name'] == 'publish-public-assets')
        self.assertEqual(app['image'], init['image'], 'existing digest pinning must resolve one identical image')
        self.assertEqual(['java', '-cp', '@/app/jib-classpath-file', 'app.web.ContextTapeAssetPublisher'], init['command'])
        self.assertEqual('false', next(e['value'] for e in app['env'] if e['name'] == 'CONTEXT_TAPE_COMPILED_ASSETS'))
        self.assertTrue(next(v for v in app['volumeMounts'] if v['name'] == 'public-assets')['readOnly'])
        self.assertEqual('10m', init['resources']['requests']['cpu'])
        self.assertEqual('100m', app['resources']['requests']['cpu'])
        self.assertEqual('web-immutable-assets', next(v for v in pod['volumes'] if v['name'] == 'public-assets')['persistentVolumeClaim']['claimName'])

    def test_production_and_experiment_gain_no_publisher_or_claim(self):
        for env in ['production', 'experiment']:
            items = render('k8s/services/web/overlays/' + env)
            pod = resource(items, 'Deployment', 'options-edge-web')['spec']['template']['spec']
            self.assertFalse(any(c['name'] == 'publish-public-assets' for c in pod.get('initContainers', [])))
            self.assertFalse(any(e['name'].startswith('CONTEXT_TAPE_COMPILED_') for c in pod['containers'] for e in c.get('env', [])))
            self.assertFalse(any(v.get('persistentVolumeClaim', {}).get('claimName') == 'web-immutable-assets' for v in pod.get('volumes', [])))


if __name__ == '__main__':
    unittest.main()
