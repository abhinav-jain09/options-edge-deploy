"""Source equivalence over real owned Git trees; no builds, broker access or runtime effects."""
import importlib.util,json,os,subprocess,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];spec=importlib.util.spec_from_file_location('component_proof',ROOT/'scripts/jenkins/cvd-component-source-proof.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class SourceProofTest(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='cvd-source-identity-'));self.names=list(m.COMPONENTS)+['unrelated-service'];self.command('init','-b','fixture');self.command('config','user.name','Source Fixture');self.command('config','user.email','source-fixture@example.invalid');self.parent()
        for name in self.names:self.child(name)
        (self.root/'trade-evidence-api').mkdir();(self.root/'trade-evidence-api/app.py').write_text('verified API source\n');self.old=self.commit()
    def command(self,*args):return subprocess.check_output(['git','-c','commit.gpgsign=false','-c','core.hooksPath=/dev/null','-C',str(self.root),*args],text=True,stderr=subprocess.PIPE).strip()
    def parent(self):
        self.write('pom.xml','<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion><groupId>com.optionsedge</groupId><artifactId>options-edge-processing</artifactId><version>0.1.0-SNAPSHOT</version><packaging>pom</packaging><modules>'+''.join('<module>'+n+'</module>' for n in self.names)+'</modules><properties><compiler>21</compiler></properties></project>')
    def write(self,name,text):p=self.root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(text)
    def child(self,name,identity=None):
        self.write(name+'/pom.xml','<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion><parent><groupId>com.optionsedge</groupId><artifactId>options-edge-processing</artifactId><version>0.1.0-SNAPSHOT</version></parent><artifactId>'+(identity or 'options-edge-'+name)+'</artifactId></project>');self.write(name+'/src/main/java/Model.java','class Model {}\n')
    def commit(self):self.command('add','.');self.command('commit','-m','owned source fixture');return self.command('rev-parse','HEAD')
    def test_unrelated_source_update_preserves_exact_component_trees(self):
        self.write('unrelated-service/src/main/java/Model.java','class Other {}\n');new=self.commit();p=m.compare(self.root,self.old,new);self.assertEqual(len(m.COMPONENTS)+1,len(p['componentTrees']));self.assertEqual(self.old,p['qualifiedSource']);self.assertEqual(new,p['currentMainSource'])
    def test_new_unreferenced_simple_module_and_operational_document_are_allowed(self):
        self.names.append('new-service');self.parent();self.child('new-service');self.write('docs/indicator-service/INDICATOR-AVRO-CONVERSION-PLAN.md','operational progress');self.write('docs/market-read/MARKET-READ-REQUIREMENT.md','unrelated market-read design');p=m.compare(self.root,self.old,self.commit());self.assertEqual(['new-service'],p['newUnreferencedModules'])
    def test_new_module_cannot_launder_an_existing_shared_directory(self):
        self.write('scripts/ci/publisher.py','verified publisher\n');old=self.commit()
        self.names.append('scripts');self.parent();self.child('scripts');self.write('scripts/ci/publisher.py','changed publisher\n')
        with self.assertRaisesRegex(ValueError,'existing shared path'):m.compare(self.root,old,self.commit())

    def test_component_common_and_api_source_changes_refuse(self):
        for name in ['context-tape-service/src/main/java/Model.java','processing-common/src/main/java/Model.java','trade-evidence-api/app.py']:
            with self.subTest(path=name):
                self.write(name,'changed implementation\n')
                with self.assertRaises(ValueError):m.compare(self.root,self.old,self.commit())
    def test_existing_closed_pom_external_build_inputs_refuse_equivalence(self):
        p=self.root/'context-tape-service/pom.xml';p.write_text(p.read_text().replace('</project>','<build><sourceDirectory>../unrelated-service/src</sourceDirectory></build></project>'));old=self.commit()
        self.write('unrelated-service/src/main/java/Model.java','class Other {}\n')
        with self.assertRaisesRegex(ValueError,'explicit source/resource'):m.compare(self.root,old,self.commit())

    def test_root_compiler_configuration_change_refuses(self):
        p=self.root/'pom.xml';p.write_text(p.read_text().replace('<compiler>21</compiler>','<compiler>26</compiler>'))
        with self.assertRaises(ValueError):m.compare(self.root,self.old,self.commit())
    def test_existing_unrelated_pom_cannot_shadow_a_dependency(self):
        self.child('unrelated-service','options-edge-processing-common')
        with self.assertRaises(ValueError):m.compare(self.root,self.old,self.commit())
    def test_new_module_cannot_shadow_a_referenced_coordinate_or_add_build_effects(self):
        self.names.append('new-service');self.parent();self.child('new-service','options-edge-processing-common')
        with self.assertRaises(ValueError):m.compare(self.root,self.old,self.commit())
    def test_new_module_build_extension_refuses(self):
        self.names.append('new-service');self.parent();self.child('new-service');p=self.root/'new-service/pom.xml';p.write_text(p.read_text().replace('</project>','<build><extensions/></build></project>'))
        with self.assertRaises(ValueError):m.compare(self.root,self.old,self.commit())
    def test_shared_jenkins_or_unmodelled_input_change_refuses(self):
        self.write('Jenkinsfile','changed build orchestration\n')
        with self.assertRaises(ValueError):m.compare(self.root,self.old,self.commit())
    def test_reordering_modules_refuses(self):
        self.names.reverse();self.parent()
        with self.assertRaises(ValueError):m.compare(self.root,self.old,self.commit())
    def test_runtime_staging_requires_guarded_jenkins_before_git(self):
        with patch.dict(os.environ,{},clear=True),patch.object(m.subprocess,'run') as run:
            with self.assertRaisesRegex(ValueError,'Guarded Jenkins'):m.prove(self.old,self.old)
            run.assert_not_called()
