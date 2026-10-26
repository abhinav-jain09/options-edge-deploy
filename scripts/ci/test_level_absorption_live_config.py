#!/usr/bin/env python3
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest
import yaml

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('live_config',Path(__file__).with_name('verify-level-absorption-live-config.py'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class LiveConfigTest(unittest.TestCase):
    def setUp(self):
        self.obj=yaml.safe_load((ROOT/'k8s/overlays/dev/level-absorption-config.yaml').read_text())
        self.digest=hashlib.sha256(json.dumps(self.obj['data'],sort_keys=True).encode()).hexdigest()
        self.name=self.obj['metadata']['name']
    def test_actual_three_file_config_passes(self):
        self.assertEqual(self.digest,module.verify(self.obj,self.name,self.digest))
    def test_missing_path_file_is_refused(self):
        del self.obj['data']['path-configuration.json']
        with self.assertRaises(ValueError):module.verify(self.obj,self.name,self.digest)
    def test_extra_file_is_refused(self):
        self.obj['data']['extra']='x'
        with self.assertRaises(ValueError):module.verify(self.obj,self.name,self.digest)
    def test_tampered_configuration_is_refused(self):
        self.obj['data']['path-configuration.json']+=' '
        with self.assertRaises(ValueError):module.verify(self.obj,self.name,self.digest)
    def test_mutable_or_wrong_identity_is_refused(self):
        for field,value in [('immutable',False),('kind','Secret')]:
            obj=copy.deepcopy(self.obj);obj[field]=value
            with self.assertRaises(ValueError):module.verify(obj,self.name,self.digest)
        obj=copy.deepcopy(self.obj);obj['metadata']['name']='wrong'
        with self.assertRaises(ValueError):module.verify(obj,self.name,self.digest)
    def test_pipeline_executes_this_validator(self):
        pipeline=(ROOT/'Jenkinsfile.service-deploy').read_text()
        self.assertIn('get configmap "$name" -o json | python3 scripts/ci/verify-level-absorption-live-config.py "$name" "$want"',pipeline)

if __name__=='__main__':unittest.main()
