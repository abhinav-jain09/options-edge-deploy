"""Exercise the actual Jenkins topic barrier against deterministic local CLI doubles."""
import os, pathlib, subprocess, tempfile, unittest
SCRIPT=pathlib.Path(__file__).with_name('ensure-absorption-path-topic.sh').resolve()
class PathTopicBarrier(unittest.TestCase):
 def run_barrier(self,**changes):
  with tempfile.TemporaryDirectory() as tmp:
   p=pathlib.Path(tmp)
   (p/'kafka-topics').write_text('''#!/usr/bin/env bash
case " $* " in
 *" --list "*) [ "${LIST_FAIL:-0}" = 0 ] || exit 1; [ "${ABSENT:-0}" = 1 ] || echo futures.level-absorption.path ;;
 *" --create "*) echo created > "$CREATION_MARKER" ;;
 *" --describe "*) echo "Topic: futures.level-absorption.path PartitionCount: ${PARTS:-1} ReplicationFactor: 1" ;;
esac
''')
   (p/'kafka-configs').write_text('''#!/usr/bin/env bash
[ "${CONFIG_FAIL:-0}" = 0 ] || exit 1
echo "cleanup.policy=${POLICY:-compact} retention.ms=${MS:--1} retention.bytes=${BYTES:--1}"
''')
   for name in ['kafka-topics','kafka-configs']:(p/name).chmod(0o755)
   env=dict(os.environ,PATH=tmp+os.pathsep+os.environ['PATH'],KAFKA_BOOTSTRAP_SERVERS='local-test:9092',CREATION_MARKER=str(p/'created'),**changes)
   result=subprocess.run(['bash',str(SCRIPT)],env=env,capture_output=True,text=True)
   return result.returncode,(p/'created').exists(),result.stdout+result.stderr
 def test_existing_valid_is_not_mutated(self):self.assertEqual(self.run_barrier()[:2],(0,False))
 def test_absent_is_created_then_verified(self):self.assertEqual(self.run_barrier(ABSENT='1')[:2],(0,True))
 def test_extra_partitions_fail(self):self.assertNotEqual(self.run_barrier(PARTS='2')[0],0)
 def test_delete_policy_fails(self):self.assertNotEqual(self.run_barrier(POLICY='compact,delete')[0],0)
 def test_finite_retention_fails(self):self.assertNotEqual(self.run_barrier(MS='604800000')[0],0)
 def test_metadata_failure_never_creates(self):self.assertEqual(self.run_barrier(LIST_FAIL='1')[:2],(1,False))
 def test_config_failure_fails(self):self.assertNotEqual(self.run_barrier(CONFIG_FAIL='1')[0],0)
if __name__=='__main__':unittest.main()
