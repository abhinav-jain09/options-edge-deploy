import copy
import builtins
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import zipfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("release_download", ROOT / "scripts/jenkins/fetch-canonical-contracts.py")
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class ReleaseDownloadTest(unittest.TestCase):
    def setUp(self):
        self.directory = SimpleNamespace(name=tempfile.mkdtemp(prefix='cvd-contracts-release-test-'))
        self.destination = Path(self.directory.name) / "contracts.jar"
        self.jar = Path(os.environ["CANONICAL_PACKAGED_ARTIFACT"]).read_bytes()
        self.version = os.environ["OPTIONS_EDGE_CONTRACTS_VERSION"]
        self.digest = hashlib.sha256(self.jar).hexdigest()
        self.commit = "a" * 40
        self.metadata = {"number": 83, "building": False, "result": "SUCCESS",
                         "actions": [{"remoteUrls": ["git@github.com:abhinav-jain09/options-edge-contracts.git"],
                                      "lastBuiltRevision": {"SHA1": self.commit, "branch": [{"name": "refs/remotes/origin/main", "SHA1": self.commit}]}}],
                         "artifacts": [{"relativePath": "target/options-edge-contracts-"+self.version+".jar"}]}

    def fetch(self, metadata=None, digest=None):
        with patch.object(release, "read", side_effect=[json.dumps(metadata or self.metadata).encode(), self.jar]) as read:
            release.fetch("http://jenkins.invalid/", "83", self.commit, digest or self.digest, self.version, self.destination)
            return read

    def test_qualified_exact_artifact_is_verified_before_installation(self):
        read = self.fetch()
        self.assertEqual(self.jar, self.destination.read_bytes())
        proof = json.loads(self.destination.with_suffix(".release.json").read_text())
        self.assertEqual(self.digest, proof["sha256"])
        self.assertEqual(self.commit, proof["source"])
        self.assertEqual(2, read.call_count)
        self.assertTrue(read.call_args_list[1].args[0].endswith("/83/artifact/target/options-edge-contracts-"+self.version+".jar"))

    def test_failed_running_wrong_repository_branch_and_commit_cannot_qualify(self):
        cases = []
        for name, value in (("result", "FAILURE"), ("building", True), ("number", 84), ("artifacts", [])):
            case = copy.deepcopy(self.metadata)
            case[name] = value
            cases.append(case)
        for mutate in (lambda item: item.update(remoteUrls=["git@github.com:outside/contracts.git"]),
                       lambda item: item["lastBuiltRevision"].update(SHA1="b" * 40),
                       lambda item: item["lastBuiltRevision"]["branch"][0].update(name="origin/feature")):
            case = copy.deepcopy(self.metadata)
            mutate(case["actions"][0])
            cases.append(case)
        for case in cases:
            with self.assertRaisesRegex(ValueError, "SUCCESS|source|archived"):
                self.fetch(case)
            self.assertFalse(self.destination.exists())

    def test_wrong_jar_digest_never_writes_or_imports_it(self):
        original=builtins.__import__
        def refuse(name,*args,**kwargs):
            if name=='optionsedge_contracts.artifact':raise AssertionError('Unverified artifact import attempted')
            return original(name,*args,**kwargs)
        with patch.object(builtins,'__import__',side_effect=refuse):
            with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                self.fetch(digest="0" * 64)
        self.assertFalse(self.destination.exists())

    def test_exact_parameters_and_credential_free_base_are_mandatory(self):
        with patch.object(release, "read") as read:
            for base, build, commit, digest, version in (
                ("http://jenkins.invalid", "lastSuccessfulBuild", self.commit, self.digest, "0.13.0"),
                ("http://jenkins.invalid", "83", "main", self.digest, "0.13.0"),
                ("http://jenkins.invalid", "83", self.commit, "", "0.13.0"),
                ("http://user:password@jenkins.invalid", "83", self.commit, self.digest, "0.13.0"),
                ("http://jenkins.invalid", "83", self.commit, self.digest, "../0.13.0")):
                with self.assertRaises(ValueError):
                    release.fetch(base, build, commit, digest, version, self.destination)
            read.assert_not_called()

    def test_response_budget_refuses_before_an_unbounded_download(self):
        with patch.object(release.urllib.request, "urlopen", return_value=io.BytesIO(b"x" * 129)) as request:
            with self.assertRaisesRegex(ValueError, "byte budget"):
                release.read("http://jenkins.invalid/test", 128)
            self.assertEqual(15, request.call_args.kwargs["timeout"])

    def latest(self, metadata=None, corrupt=False, extra=None):
        source=Path(self.directory.name)/'main'
        def clone(*args,**kwargs):
            source.mkdir()
            (source/'pom.xml').write_text('<project xmlns="http://maven.apache.org/POM/4.0.0"><version>'+self.version+'</version></project>')
            with zipfile.ZipFile(io.BytesIO(self.jar)) as archive:
                for name in archive.namelist():
                    if name.endswith('.avsc') or name.startswith('optionsedge_contracts/') and name.endswith('.py'):
                        path=source/('schemas' if name.endswith('.avsc') else 'python')/name
                        path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(archive.read(name))
            if corrupt:next((source/'schemas').rglob('*.avsc')).write_text('{}')
        body=self.jar
        if extra:
            buffer=io.BytesIO()
            with zipfile.ZipFile(buffer,'w') as output,zipfile.ZipFile(io.BytesIO(self.jar)) as original:
                for item in original.infolist():output.writestr(item,original.read(item.filename))
                output.writestr(extra,b'raise RuntimeError("must never import")')
            body=buffer.getvalue()
        response=json.dumps(metadata or self.metadata).encode()
        with patch.object(release.subprocess,'run',side_effect=clone) as run, \
             patch.object(release.subprocess,'check_output',return_value=self.commit+'\n') as revision, \
             patch.object(release,'read',side_effect=[response,body,response,body]) as reads:
            release.latest('http://jenkins.invalid',source,self.destination)
            self.assertEqual(['git','clone','--depth','1','--single-branch','--branch','main',
                              'git@github.com:abhinav-jain09/options-edge-contracts.git',str(source)],run.call_args.args[0])
            self.assertEqual((True,120),(run.call_args.kwargs['check'],run.call_args.kwargs['timeout']))
            self.assertEqual('0',run.call_args.kwargs['env']['GIT_TERMINAL_PROMPT'])
            self.assertEqual('ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes',
                             run.call_args.kwargs['env']['GIT_SSH_COMMAND'])
            revision.assert_called_once_with(['git','-C',str(source),'rev-parse','HEAD'],timeout=10,text=True)
            urls=[call.args[0] for call in reads.call_args_list]
            self.assertTrue(urls[0].startswith('http://jenkins.invalid/job/options-edge-contracts/lastBuild/api/json?tree='))
            artifact='http://jenkins.invalid/job/options-edge-contracts/83/artifact/target/options-edge-contracts-'+self.version+'.jar'
            self.assertEqual([artifact,artifact],[urls[1],urls[3]])
            self.assertTrue(urls[2].startswith('http://jenkins.invalid/job/options-edge-contracts/83/api/json?tree='))


    def test_latest_main_resolves_without_any_release_selection(self):
        self.latest()
        proof=json.loads(self.destination.with_suffix('.release.json').read_text())
        self.assertEqual((83,self.commit,self.digest),(proof['build'],proof['source'],proof['sha256']))

    def test_next_compatible_artifact_needs_no_positive_test_version_edit(self):
        properties='META-INF/maven/com.optionsedge/options-edge-contracts/pom.properties'
        major,minor,_=map(int,self.version.split('.'));next_version=f'{major}.{minor+1}.0'
        buffer=io.BytesIO()
        with zipfile.ZipFile(buffer,'w') as output,zipfile.ZipFile(io.BytesIO(self.jar)) as original:
            for item in original.infolist():
                body=original.read(item.filename)
                if item.filename==properties:
                    text=body.decode();old='version='+self.version
                    self.assertEqual(1,text.splitlines().count(old))
                    body=text.replace(old,'version='+next_version).encode()
                output.writestr(item,body)
        self.jar=buffer.getvalue();self.version=next_version;self.digest=hashlib.sha256(self.jar).hexdigest()
        self.metadata['artifacts']=[{'relativePath':'target/options-edge-contracts-'+self.version+'.jar'}]
        self.fetch()
        self.assertEqual(next_version,json.loads(self.destination.with_suffix('.release.json').read_text())['version'])
        self.destination.unlink();self.destination.with_suffix('.release.json').unlink()
        self.latest()
        self.assertEqual(next_version,json.loads(self.destination.with_suffix('.release.json').read_text())['version'])

    def test_latest_never_falls_back_to_an_older_success(self):
        metadata=copy.deepcopy(self.metadata);metadata['result']='FAILURE'
        with self.assertRaisesRegex(ValueError,'Latest.*SUCCESS'):self.latest(metadata)
        self.assertFalse(self.destination.exists())

    def test_latest_rejects_stale_package_despite_successful_build(self):
        with self.assertRaisesRegex(ValueError,'resource differs from main'):self.latest(corrupt=True)
        self.assertFalse(self.destination.exists())

    def test_latest_requires_current_main_source(self):
        metadata=copy.deepcopy(self.metadata);metadata['actions'][0]['lastBuiltRevision']['SHA1']='b'*40
        with self.assertRaisesRegex(ValueError,'current main'):self.latest(metadata)
        self.assertFalse(self.destination.exists())

    def test_latest_rejects_every_unverified_importable_entry(self):
        for entry in ('simplejson.py','sitecustomize.py','optionsedge_contracts/artifact.pyc','optionsedge_contracts/extra.so'):
            with self.subTest(entry=entry):
                # Each case uses a fresh clone/destination like a new CI attempt.
                case=ReleaseDownloadTest();case.setUp()
                try:
                    with self.assertRaisesRegex(ValueError,'Unverified.*entry'):case.latest(extra=entry)
                    self.assertFalse(case.destination.exists())
                finally:case.doCleanups()


if __name__ == "__main__":
    unittest.main()
