# CI-COLLECT-ONLY
#
# NOT RUN BY CI TODAY. deploy-validation.yml names its test modules by hand and nothing discovers them, so
# this module is only IMPORTED (by scripts/ci/validate-tests-are-collectable.sh, which catches an import
# error) - none of its assertions execute. 112 of the 138 modules under tests/ were in this position when
# the marker was introduced. The marker is not approval of that: it is the classification that makes the
# dormancy visible and, because every module must carry exactly one marker, stops a NEW module from
# becoming an orphan silently. To promote this module, wire it into a step and change the marker to
# CI-REQUIRED; the guard then enforces the wiring.
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
        cls.gateway = render('k8s/services/feed-gateway/overlays/dev')

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

    def test_activation_enabled_and_app_volume_is_read_only(self):
        pod = resource(self.web, 'Deployment', 'options-edge-web')['spec']['template']['spec']
        app = next(c for c in pod['containers'] if c['name'] == 'web')
        init = next(c for c in pod['initContainers'] if c['name'] == 'publish-public-assets')
        self.assertEqual(app['image'], init['image'], 'existing digest pinning must resolve one identical image')
        self.assertEqual(['java', '-cp', '@/app/jib-classpath-file', 'app.web.ContextTapeAssetPublisher'], init['command'])
        self.assertEqual('true', next(e['value'] for e in app['env'] if e['name'] == 'CONTEXT_TAPE_COMPILED_ASSETS'))
        self.assertTrue(next(v for v in app['volumeMounts'] if v['name'] == 'public-assets')['readOnly'])
        self.assertEqual('10m', init['resources']['requests']['cpu'])
        self.assertEqual('Always', init['imagePullPolicy'])
        self.assertFalse(next(v for v in init['volumeMounts'] if v['name']=='public-assets').get('readOnly',False))
        app_root=next(e['value'] for e in app['env'] if e['name']=='CONTEXT_TAPE_ASSET_ROOT')
        init_root=next(e['value'] for e in init['env'] if e['name']=='CONTEXT_TAPE_ASSET_ROOT')
        self.assertEqual(app_root,init_root)
        self.assertEqual(app_root,next(v['mountPath'] for v in app['volumeMounts'] if v['name']=='public-assets'))
        self.assertEqual(init_root,next(v['mountPath'] for v in init['volumeMounts'] if v['name']=='public-assets'))
        self.assertEqual({'runAsUser':0,'runAsGroup':0},init['securityContext'])
        self.assertEqual('100m', app['resources']['requests']['cpu'])
        self.assertEqual('web-immutable-assets', next(v for v in pod['volumes'] if v['name'] == 'public-assets')['persistentVolumeClaim']['claimName'])

    def test_gateway_authority_and_browser_share_native_scope(self):
        web = resource(self.web, 'Deployment', 'options-edge-web')['spec']['template']['spec']['containers']
        webenv = {e['name']: e.get('value') for c in web if c['name'] == 'web' for e in c.get('env', [])}
        gateway = resource(self.mono, 'Deployment', 'feed-gateway-service')['spec']['template']['spec']['containers']
        gatewayenv = {e['name']: e.get('value') for c in gateway for e in c.get('env', [])}
        self.assertEqual('true', webenv['HEATMAP_NATIVE_HISTORY_ENABLED'])
        self.assertEqual('Rithmic Paper Trading', webenv['HEATMAP_RITHMIC_SYSTEM'])
        self.assertEqual('true', webenv['CONTEXT_TAPE_COMPILED_ASSETS'])
        self.assertEqual('Rithmic Paper Trading', gatewayenv['GATEWAY_RITHMIC_SYSTEM'])
        self.assertEqual('false', gatewayenv['LINE_HEALTH_RELAY_ENABLED'])
        self.assertEqual(resource(self.gateway, 'Deployment', 'feed-gateway-service'), resource(self.mono, 'Deployment', 'feed-gateway-service'))
        self.assertEqual('20000', gatewayenv['ES_LIQUIDITY_WALL_FRESH_MS'])
        self.assertEqual('true', gatewayenv['GATEWAY_AUTH_ENABLED'])
        self.assertEqual('options-edge-web', gatewayenv['GATEWAY_KEYCLOAK_AUDIENCE'])
        self.assertEqual(gatewayenv['GATEWAY_KEYCLOAK_CLIENT_ID'], gatewayenv['GATEWAY_KEYCLOAK_AUDIENCE'])
        self.assertEqual('options-edge-web', gatewayenv['WS_AUTH_AUDIENCE'])

    def test_no_global_build_floor_is_added_for_dev_only_publisher(self):
        self.assertFalse((ROOT / 'k8s/services/web/MIN_IMAGE_BUILD').exists())

    def test_production_and_experiment_gain_no_publisher_or_claim(self):
        for env in ['production', 'experiment']:
            items = render('k8s/overlays/' + env) + render('k8s/infra/overlays/' + env) + render('k8s/services/web/overlays/' + env)
            pod = resource(items, 'Deployment', 'options-edge-web')['spec']['template']['spec']
            self.assertFalse(any(c['name'] == 'publish-public-assets' for c in pod.get('initContainers', [])))
            self.assertFalse(any(e['name'].startswith('CONTEXT_TAPE_COMPILED_') or e['name']=='CONTEXT_TAPE_ASSET_ROOT' for c in pod['containers'] for e in c.get('env', [])))
            self.assertFalse(any(item['kind']=='PersistentVolumeClaim' and item['metadata']['name']=='web-immutable-assets' for item in items))
            self.assertFalse(any(v.get('persistentVolumeClaim', {}).get('claimName') == 'web-immutable-assets' for v in pod.get('volumes', [])))


if __name__ == '__main__':
    unittest.main()
