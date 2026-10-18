"""Pure predicates and command builders; never contact a broker, production host or process."""
import copy
import io
import tempfile
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
from types import SimpleNamespace
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,ROOT/path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
controller=load('archive_controller','scripts/jenkins/cvd-archive-retained-qualification.py')
worker=load('archive_worker','scripts/qualification/archive/history_worker.py')
SOURCE='a'*40;SDK='b'*40

def build(number=39,result='SUCCESS',building=False,tests=False):
 fields={'RUN_TESTS_ONLY':tests,'PERMITTED_SHA':SOURCE,'CONTRACTS_PERMITTED_SHA':SDK,'PROD_HOST':'abhinav@192.168.100.252','INSTALL_DIR':'/home/abhinav/oe-ops'}
 return {'number':number,'result':result,'building':building,'actions':[{'parameters':[{'name':k,'value':v} for k,v in fields.items()]}]}

def inventory():
 return [{'bootstrap':broker,'registry':registry,'topic':topic,'partition':0,'from':10,'until':20} for topic,(broker,registry) in worker.BINDINGS.items()]

class ArchiveQualificationTest(unittest.TestCase):
 def test_latest_install_never_falls_back_or_selects_tests_only(self):
  self.assertEqual(39,controller.latest_installed([build(40,tests=True),build(),build(38)])[0])
  for result,building in [('FAILURE',False),(None,True)]:
   with self.subTest(result=result),self.assertRaisesRegex(ValueError,'no fallback'):controller.latest_installed([build(40,result,building),build()])
  with self.assertRaises(ValueError):controller.latest_installed([build()]*65)
 def test_install_source_sdk_and_location_are_mandatory(self):
  for key,value in [('PROD_HOST','192.168.100.201'),('INSTALL_DIR','/elsewhere'),('CONTRACTS_PERMITTED_SHA','old')]:
   row=build();next(v for v in row['actions'][0]['parameters'] if v['name']==key)['value']=value
   with self.subTest(key=key),self.assertRaises(ValueError):controller.latest_installed([row])
 def test_deployed_jar_proof_requires_qualified_sdk_and_unchanged_component(self):
  proof={'source':SOURCE,'contractsSource':SDK,'codecAssertions':56,'sdkMembersMatched':765,'helperSha256':'c'*64}
  with patch.object(controller,'run',return_value=''):self.assertEqual('c'*64,controller.package_proof(SOURCE,SDK,39,proof))
  for key,value in [('contractsSource','d'*40),('codecAssertions',0),('helperSha256','tag')]:
   bad={**proof,key:value}
   with self.subTest(key=key),patch.object(controller,'run'),self.assertRaises(ValueError):controller.package_proof(SOURCE,SDK,39,bad)
  with patch.object(controller,'run',side_effect=['','','changed.java']),self.assertRaisesRegex(ValueError,'differs from current'):controller.package_proof(SOURCE,SDK,39,proof)
 def test_actual_inventory_requires_all_five_exact_sources_and_bounded_coordinates(self):
  self.assertEqual(inventory(),worker.validate_inventory(inventory()))
  for fault in ['host','duplicate','negative','bool','span','missing']:
   rows=copy.deepcopy(inventory())
   if fault=='host':rows[0]['bootstrap']='192.168.100.201:9092'
   elif fault=='duplicate':rows.append(copy.deepcopy(rows[0]))
   elif fault=='negative':rows[0]['from']=-1
   elif fault=='bool':rows[0]['partition']=True
   elif fault=='span':rows[0]['until']=worker.MAX_SPAN+100
   else:rows.pop()
   with self.subTest(fault=fault),self.assertRaises(ValueError):worker.validate_inventory(rows)
 def test_capture_argv_is_exact_groupless_and_private_output(self):
  row=inventory()[0];command=worker.capture_command(row,Path('/private/0.log'),Path('/private/0.summary'),Path('/private/hosts'))
  self.assertEqual(command,[worker.JAVA,'-Djdk.net.hosts.file=/private/hosts','-Xms32m','-Xmx512m','-XX:MaxDirectMemorySize=64m','-XX:MaxMetaspaceSize=64m','-XX:ReservedCodeCacheSize=32m','-Xss512k','-XX:ActiveProcessorCount=2','-cp','reader.jar','StrikeArchiveReader','--bootstrap',row['bootstrap'],'--canonical-registry',row['registry'],'--topic',row['topic'],'--partition','0','--from','10','--max-end','20','--deadline-ms','900000','--poll-ms','200','--out','/private/0.log','--summary','/private/0.summary'])
  self.assertFalse(any(value.startswith('--mark-') for value in command))
 def test_capture_supervisor_enforces_wall_heap_and_log_limits_and_stops_only_its_child(self):
  class Process:
   pid=123;returncode=0
   def __init__(self):self.polls=0;self.stopped=False;self.effects=[]
   def poll(self):
    self.polls+=1
    return 0 if self.stopped or self.polls>(1 if fault is None else 2) else None
   def terminate(self):self.effects.append('terminate');self.stopped=True
   def wait(self,timeout):self.effects.append('wait');return 0
  for fault in [None,'wall','rss','log']:
   process=Process();clock=iter([0,901 if fault=='wall' else .1,901 if fault=='wall' else .2,901 if fault=='wall' else .3])
   path=SimpleNamespace(open=lambda mode:io.BytesIO(),stat=lambda:SimpleNamespace(st_size=worker.MAX_LOG+1 if fault=='log' else 0))
   with self.subTest(fault=fault),patch.object(worker.subprocess,'Popen',return_value=process),patch.object(worker.time,'monotonic',side_effect=lambda:next(clock)),patch.object(worker.time,'sleep'),patch.object(worker,'usage_kib',return_value=768*1024+1 if fault=='rss' else 100):
    if fault:
     with self.assertRaisesRegex(ValueError,'wall/RSS/log'):worker.supervise(['fixture'],path,1024)
     self.assertEqual(['terminate','wait'],process.effects)
    else:
     self.assertEqual((.2,100),worker.supervise(['fixture'],path,1024));self.assertEqual([],process.effects)
 def test_summary_requires_exact_snapshot_boundary_completed_count_and_deadline(self):
  row=inventory()[0];text='STRIKE_ARCHIVE_READER status=COMPLETE topic='+row['topic']+' partition=0 from=10 lso=22 boundary=20 position=20 records=7 elapsed_ms=100'
  self.assertEqual(7,worker.parse_summary(text,row)['records'])
  for old,new in [('status=COMPLETE','status=TIMEOUT'),('boundary=20','boundary=19'),('position=20','position=19'),('from=10','from=11'),('records=7','records=-1'),('elapsed_ms=100','elapsed_ms=900001')]:
   with self.subTest(old=old),self.assertRaises(ValueError):worker.parse_summary(text.replace(old,new),row)

 def test_cash_window_covers_complete_run_and_margin(self):
  for when in ['2026-10-19T07:52:59','2026-10-19T16:15:00','2026-10-18T09:30:00']:
   controller.check_window(datetime.fromisoformat(when).replace(tzinfo=ZoneInfo('America/New_York')))
  for when in ['2026-10-19T07:53:00','2026-10-19T08:25:00','2026-10-19T16:14:59']:
   with self.subTest(when=when),self.assertRaises(ValueError):controller.check_window(datetime.fromisoformat(when).replace(tzinfo=ZoneInfo('America/New_York')))
 def test_physical_capture_independent_offsets_partition_count_and_line_bound(self):
  row=inventory()[0]
  with tempfile.TemporaryDirectory() as root:
   path=Path(root)/'capture'
   valid=b'CreateTime:1\tPartition:0\tOffset:10\tkey\t{}\nCreateTime:2\tPartition:0\tOffset:19\tkey\t{}\n'
   path.write_bytes(valid)
   self.assertEqual((2,len(valid.splitlines(keepends=True)[0]),hashlib.sha256(valid).hexdigest()),worker.verify_lines(path,row,2))
   self.assertEqual(hashlib.sha256(valid).hexdigest(),worker.digest(path))
   for bad in [valid.replace(b'Partition:0',b'Partition:1'),valid.replace(b'Offset:19',b'Offset:10'),valid.replace(b'Offset:10',b'Offset:9'),valid.replace(b'Offset:19',b'Offset:20'),valid[:-1],b'x'*((16<<20)+8193)]:
    path.write_bytes(bad)
    with self.assertRaises(ValueError):worker.verify_lines(path,row,2)
   path.write_bytes(valid)
   with self.assertRaisesRegex(ValueError,'count'):worker.verify_lines(path,row,3)
 def test_summary_rejects_lso_below_boundary(self):
  row=inventory()[0]
  text='status=COMPLETE topic='+row['topic']+' partition=0 from=10 lso=19 boundary=20 position=20 records=7 elapsed_ms=100'
  with self.assertRaises(ValueError):worker.parse_summary(text,row)

 def test_worker_verifies_all_input_hashes_before_execution(self):
  files={name:'a'*64 for name in ['reader.jar','ArchiveHistoryInventory.java','history_worker.py']}
  with patch.object(worker,'digest',return_value='a'*64) as checksum:worker.verify_inputs({'files':files});self.assertEqual(3,checksum.call_count)
  with patch.object(worker,'digest',return_value='b'*64),self.assertRaisesRegex(ValueError,'bytes differ'):worker.verify_inputs({'files':files})
  with self.assertRaisesRegex(ValueError,'inventory'):worker.verify_inputs({'files':{}})

 def test_cpu_affinity_requires_exactly_two_available_cpus(self):
  self.assertEqual([2,5],worker.validate_cpu_affinity({5,2}))
  for cpus in [set(),{0},{0,1,2},{-1,1},{1,4096}]:
   with self.subTest(cpus=cpus),self.assertRaises(ValueError):worker.validate_cpu_affinity(cpus)

if __name__=='__main__':unittest.main()
