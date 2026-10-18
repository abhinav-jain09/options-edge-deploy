import importlib.util
from datetime import datetime
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import warnings
import zipfile

spec=importlib.util.spec_from_file_location('packaged',Path(__file__).resolve().parents[1]/'scripts/jenkins/archive-packaged-on-es4.py')
gate=importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)

class PackagedSdkParityTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.sdk=self.root/'sdk.jar';self.fat=self.root/'fat.jar'
        self.write(self.sdk,[('com/optionsedge/Contract.class',b'official'),('cvd/Bar.avsc',b'{"type":"record"}'),('META-INF/MANIFEST.MF',b'sdk')])
        self.write(self.fat,[('com/optionsedge/Contract.class',b'official'),('cvd/Bar.avsc',b'{"type":"record"}'),('META-INF/MANIFEST.MF',b'shaded')])
    def write(self,path,entries):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore',UserWarning)
            with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED) as z:
                for name,data in entries:z.writestr(name,data)
    def test_every_sdk_class_and_schema_matches_with_merged_metadata(self):
        self.assertEqual(gate.sdk_parity(self.sdk,self.fat),2)
    def test_changed_class_or_resource_refuses(self):
        for changed in ['com/optionsedge/Contract.class','cvd/Bar.avsc']:
            with self.subTest(changed=changed):
                self.write(self.fat,[(n,b'changed' if n==changed else d) for n,d in [('com/optionsedge/Contract.class',b'official'),('cvd/Bar.avsc',b'{"type":"record"}')]])
                with self.assertRaisesRegex(ValueError,'bundled SDK member differs: '+changed):gate.sdk_parity(self.sdk,self.fat)
    def test_missing_resource_refuses(self):
        self.write(self.fat,[('com/optionsedge/Contract.class',b'official')])
        with self.assertRaisesRegex(KeyError,'cvd/Bar.avsc'):gate.sdk_parity(self.sdk,self.fat)
    def test_duplicate_members_refuse_in_either_input(self):
        for path in [self.sdk,self.fat]:
            original=path.read_bytes()
            self.write(path,[('com/optionsedge/Contract.class',b'official'),('cvd/Bar.avsc',b'{"type":"record"}'),('com/optionsedge/Contract.class',b'official')])
            with self.assertRaisesRegex(ValueError,'duplicate archive member'):gate.sdk_parity(self.sdk,self.fat)
            path.write_bytes(original)
    def test_empty_sdk_refuses(self):
        self.write(self.sdk,[('META-INF/MANIFEST.MF',b'onlymetadata')])
        with self.assertRaisesRegex(ValueError,'empty SDK parity'):gate.sdk_parity(self.sdk,self.fat)
    def test_package_sizes_refuse_before_zip_parsing(self):
        for path,size in [(self.sdk,32<<20),(self.fat,128<<20)]:
            original=path.read_bytes()
            with path.open('wb') as f:f.truncate(size+1)
            with self.assertRaisesRegex(ValueError,'archive package size bound'):gate.sdk_parity(self.sdk,self.fat)
            path.write_bytes(original)
    def test_large_sdk_member_refuses(self):
        entries=[('oversize',b'x'*((16<<20)+1))]
        self.write(self.sdk,entries);self.write(self.fat,entries)
        with self.assertRaisesRegex(ValueError,'bundled SDK member differs: oversize'):gate.sdk_parity(self.sdk,self.fat)
    def test_cash_window_boundaries_and_weekends(self):
        for instant,refuse in [(datetime(2026,10,19,8,24),False),(datetime(2026,10,19,8,25),True),
                               (datetime(2026,10,19,15,59),True),(datetime(2026,10,19,16,0),False),
                               (datetime(2026,10,18,12,0),False)]:
            with self.subTest(instant=instant),patch.object(gate,'datetime') as clock:
                clock.now.return_value=instant
                if refuse:
                    with self.assertRaisesRegex(ValueError,'cash-window exclusion'):gate.check_window()
                else:gate.check_window()
    def test_zip_expansion_refuses_before_reading_members(self):
        with patch.object(gate.zipfile,'ZipFile') as zipped:
            entry=type('Entry',(),{'file_size':(512<<20)+1,'filename':'too-large'})()
            zipped.return_value.__enter__.return_value.infolist.return_value=[entry]
            with self.assertRaisesRegex(ValueError,'ZIP expansion bound'):gate.sdk_parity(self.sdk,self.fat)
    def test_zip_entry_count_refuses_before_reading_members(self):
        with patch.object(gate.zipfile,'ZipFile') as zipped:
            entry=type('Entry',(),{'file_size':0,'filename':'one'})()
            zipped.return_value.__enter__.return_value.infolist.return_value=[entry]*65537
            with self.assertRaisesRegex(ValueError,'ZIP expansion bound'):gate.sdk_parity(self.sdk,self.fat)
    def test_jenkins_only_refuses_before_any_remote_action(self):
        with patch.dict(gate.os.environ,{},clear=True),patch.object(gate.subprocess,'run') as run:
            with self.assertRaisesRegex(ValueError,'Jenkins only'):gate.main()
            run.assert_not_called()

if __name__=='__main__':unittest.main()
