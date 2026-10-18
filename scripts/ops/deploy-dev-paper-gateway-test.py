#!/usr/bin/env python3
"""Functional configuration and orchestration checks. No container, broker or launchd mutation."""
import contextlib
import importlib.util
import io
import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('gateway',pathlib.Path(__file__).with_name('deploy-dev-paper-gateway.py'))
gateway=importlib.util.module_from_spec(spec);spec.loader.exec_module(gateway)

class GatewayTests(unittest.TestCase):
    def configuration(self,bootstrap=False):
        return json.loads(gateway.run(gateway.compose_args(bootstrap)+['config','--format','json'],env=dict(os.environ,IBKR_PAPER_SECRET_ROOT=str(gateway.SECRETS))))
    def test_rendered_mode_ports_credentials_and_resource_limits(self):
        cfg=self.configuration();g=cfg['services']['gateway'];initial=self.configuration(True)['services']['gateway']
        self.assertEqual(g['image'],gateway.IMAGE);self.assertEqual(g['platform'],'linux/arm64');self.assertEqual(g['environment']['TRADING_MODE'],'paper')
        self.assertEqual({(p['host_ip'],p['published'],p['target']) for p in g['ports']},{('127.0.0.1','4002',4004),('127.0.0.1','5902',5900)})
        self.assertNotIn('TWS_PASSWORD',g['environment']);self.assertNotIn('TWS_USERID',g['environment']);self.assertEqual(g['environment']['TWS_PASSWORD_FILE'],'/credentials/password')
        self.assertEqual(g['restart'],'unless-stopped');self.assertEqual(initial['restart'],'no');self.assertEqual(int(g['mem_limit']),2*1024**3)
        self.assertIn('/dev/tcp/127.0.0.1/4002',g['healthcheck']['test'][-1]);self.assertEqual(g['logging']['options']['max-file'],'3')
        self.assertEqual(cfg['services']['credentials']['network_mode'],'none');self.assertEqual(g['environment']['EXISTING_SESSION_DETECTED_ACTION'],'secondary')
        self.assertTrue(next(v for v in g['volumes'] if v['target']=='/credentials')['read_only'])
    def test_secrets_require_owner_only_regular_files_and_reject_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory);root.chmod(0o700)
            for name in ('username','password','vnc'):
                p=root/name;p.write_text('isolated-fixture\n');p.chmod(0o600)
            with patch.object(gateway,'SECRETS',root):
                gateway.credential_files();(root/'password').chmod(0o644)
                with self.assertRaisesRegex(RuntimeError,'PERMISSIONS_INVALID'):gateway.credential_files()
                (root/'password').chmod(0o600);(root/'vnc').unlink();(root/'vnc').symlink_to(root/'password')
                with self.assertRaisesRegex(RuntimeError,'PERMISSIONS_INVALID'):gateway.credential_files()
    def orchestrate(self,existing=False,running=True,healthy=True,wrong_identity=False):
        effects=[];restart='unless-stopped' if existing else 'no'
        def run(args,**kwargs):
            nonlocal restart
            effects.append(args)
            if args[:3]==['git','rev-parse','HEAD']:return 'a'*40
            if args[:3]==['docker','context','inspect']:return 'unix:///Users/abhinav/.docker/run/docker.sock'
            if 'info' in args:return 'linux/aarch64'
            if 'ps' in args:return gateway.NAME if existing else ''
            if 'inspect' in args:return json.dumps([{'Config':{'Image':'wrong' if wrong_identity else gateway.IMAGE,'Labels':{'com.docker.compose.project':gateway.NAME},'Env':['TRADING_MODE=paper']},'State':{'Running':running,'Health':{'Status':'healthy' if healthy else 'starting'}},'HostConfig':{'RestartPolicy':{'Name':restart}}}])
            if 'update' in args:restart='unless-stopped'
            return ''
        env={'JOB_NAME':gateway.NAME,'NODE_LABELS':'local-mac','BUILD_URL':'http://fixture/job/ibkr-paper-gateway/1/','PERMITTED_SHA':'a'*40}
        return effects,run,env
    def exercise(self,**settings):
        effects,run,env=self.orchestrate(**settings)
        with patch.dict(os.environ,env,clear=True),patch.object(gateway.platform,'system',return_value='Darwin'),patch.object(gateway,'credential_files'),patch.object(gateway,'run',side_effect=run),patch.object(gateway,'verify_tree',side_effect=lambda:effects.append(['VERIFY'])),patch.object(gateway,'install_startup',side_effect=lambda:effects.append(['STARTUP'])),contextlib.redirect_stdout(io.StringIO()):
            gateway.main()
        return effects
    def test_effects_are_fenced_and_restart_enabled_only_after_native_api_health(self):
        effects=self.exercise();mutations=[(i,a) for i,a in enumerate(effects) if any(v in a for v in ('pull','up','update'))]
        self.assertEqual(len(mutations),3)
        for i,args in mutations:self.assertEqual(effects[i-1],['VERIFY'])
        self.assertIn('bootstrap.yaml',' '.join(mutations[1][1]));self.assertLess(next(i for i,a in enumerate(effects) if 'inspect' in a),mutations[2][0]);self.assertEqual(effects[-1],['STARTUP'])
    def test_existing_healthy_gateway_is_not_replaced(self):
        effects=self.exercise(existing=True);self.assertFalse(any(any(v in a for v in ('pull','up','update')) for a in effects));self.assertIn(['STARTUP'],effects)
    def test_wrong_container_or_failed_login_never_enables_restart_or_replaces_existing(self):
        for setting,reason in [({'existing':True,'wrong_identity':True},'IDENTITY_MISMATCH'),({'running':False,'healthy':False},'LOGIN_EXITED')]:
            with self.subTest(setting=setting),self.assertRaisesRegex(RuntimeError,reason):self.exercise(**setting)
    def test_no_jekins_context_cannot_mutate_docker(self):
        with patch.dict(os.environ,{},clear=True),patch.object(gateway,'run') as run:
            with self.assertRaisesRegex(RuntimeError,'JENKINS_DEV_MAC_REQUIRED'):gateway.main()
            run.assert_not_called()
    def test_startup_install_preserves_unknown_existing_files(self):
        with tempfile.TemporaryDirectory() as directory:
            agent=pathlib.Path(directory)/'agent.plist';agent.write_bytes(b'unknown-owner-file')
            with patch.object(gateway,'AGENT',agent),patch.object(gateway,'run') as run:
                with self.assertRaisesRegex(RuntimeError,'EXISTING_STARTUP_AGENT_DIFFERS'):gateway.install_startup()
                run.assert_not_called();self.assertEqual(agent.read_bytes(),b'unknown-owner-file')
    def test_startup_launches_docker_at_login_without_keepalive_or_credentials(self):
        import plistlib
        with tempfile.TemporaryDirectory() as directory:
            agent=pathlib.Path(directory)/'agent.plist';calls=[]
            def run(args,**kwargs):
                calls.append(args)
                if args[1]=='print':raise subprocess.CalledProcessError(113,args)
                return ''
            with patch.object(gateway,'AGENT',agent),patch.object(gateway,'verify_tree',side_effect=lambda:calls.append(['VERIFY'])),patch.object(gateway,'run',side_effect=run):gateway.install_startup()
            payload=plistlib.loads(agent.read_bytes());self.assertEqual(payload['ProgramArguments'],['/usr/bin/open','-g','-a','/Applications/Docker.app']);self.assertTrue(payload['RunAtLoad']);self.assertNotIn('KeepAlive',payload);self.assertEqual(calls[-2],['VERIFY']);self.assertEqual(calls[-1][1],'bootstrap')

if __name__=='__main__':unittest.main()
