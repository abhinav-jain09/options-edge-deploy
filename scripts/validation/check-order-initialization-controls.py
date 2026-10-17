#!/usr/bin/env python3
"""Deterministic isolated negative controls. Child socket/process routes are denied."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = 'scripts/deploy/initialize-context-tape-orders.py'
TEST = 'tests/test_context_tape_order_initialization.py'
CONTROLS = [
    ('retained offsets', "require(offsets == topic + ':0:0',", 'require(True,',
     'test_retained_data_or_nonzero_floor_refused_before_any_write', 'RuntimeError not raised'),
    ('quiescence', "require(deployment['spec'].get('replicas', 1) == 0,", 'require(True,',
     'test_quiescence_cluster_and_read_failures_refused', 'RuntimeError not raised'),
    ('schema identity', "canonical.get('id') == value['id']", 'True',
     'test_registry_namespace_rendering_uses_read_only_lookup_of_latest_identity', 'RuntimeError not raised'),
    ('source guards', "require(os.environ.get('PERMITTED_SHA_GUARD') == 'PASSED'",
     "require(True or os.environ.get('PERMITTED_SHA_GUARD') == 'PASSED'",
     'test_both_source_guards_and_jenkins_identity_required', 'UNEXPECTED_NETWORK_IN_UNIT_TEST'),
    ('repeat quiescence', 'self.preflight()  # Refuse newly active consumers or newly retained data.', 'pass',
     'test_quiescence_is_rechecked_before_first_write', 'Lists differ'),
    ('actual deployment pod selector', "','.join(key + '=' + value for key, value in sorted(labels.items()))",
     "'app.kubernetes.io/name=' + name",
     'test_actual_selector_includes_terminating_pods_with_zero_status_replicas', 'RuntimeError not raised'),
    ('exact recovery schema id', "value.get('id') == 32", 'True',
     'test_fixture_recovery_is_exact_dev_only_and_never_permanent', 'True is not false')]
CHILD = '''import socket,subprocess,sys,unittest
# This independent outer barrier remains even if the unit-test mocks are removed.
def deny(*args,**kwargs): raise AssertionError("NEGATIVE_CONTROL_EFFECT_ROUTE_DENIED")
socket.socket.connect=deny
socket.create_connection=deny
subprocess.Popen=deny
suite=unittest.defaultTestLoader.discover('tests',pattern='test_context_tape_order_initialization.py')
def selected(node):
 if isinstance(node,unittest.TestSuite): return unittest.TestSuite(x for child in node for x in selected(child))
 return [node] if node.id().endswith('.'+sys.argv[1]) else []
result=unittest.TextTestRunner(verbosity=2).run(selected(suite))
sys.exit(0 if result.wasSuccessful() and result.testsRun==1 else 1)
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source = (ROOT/SCRIPT).read_text()
    tests = (ROOT/TEST).read_bytes()
    results = []
    for name, before, after, test, diagnostic in CONTROLS:
        assert source.count(before) == 1, (name, 'mutation anchor changed')
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory)
            (destination/'scripts/deploy').mkdir(parents=True)
            (destination/'tests').mkdir()
            (destination/TEST).write_bytes(tests)
            for label, candidate in [('positive', source), ('negative', source.replace(before, after, 1))]:
                (destination/SCRIPT).write_text(candidate)
                result = subprocess.run(['python3', '-B', '-c', CHILD, test], cwd=destination,
                                        text=True, capture_output=True, timeout=30)
                observed = result.stdout+result.stderr
                assert 'Ran 1 test' in observed, (name, label, observed)
                if label == 'positive':
                    assert result.returncode == 0, (name, observed)
                else:
                    assert result.returncode != 0 and 'FAIL: '+test+' (' in observed and diagnostic in observed, (name, observed)
                    results.append({'control': name, 'positivePassed': True, 'removedMechanismRejected': True,
                                    'effectRoutes': 'independent child socket/process denial plus unit-test URL/command mocks',
                                    'test': test, 'expectedDiagnostic': diagnostic, 'failureOutput': result.stderr})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({'sourceSha256': hashlib.sha256(source.encode()).hexdigest(),
                                      'testsSha256': hashlib.sha256(tests).hexdigest(), 'controls': results}, indent=2)+'\n')
    print(str(len(results))+' positive/negative control pairs passed with effect routes denied')


if __name__ == '__main__':
    main()
