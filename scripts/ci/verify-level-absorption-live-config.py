#!/usr/bin/env python3
"""Read-only proof of the exact immutable three-file ConfigMap applied by Jenkins."""
import hashlib
import json
import re
import sys

def verify(obj, name, digest):
    if not re.fullmatch(r'[0-9a-f]{64}', digest):
        raise ValueError('expected SHA-256 required')
    if name != 'level-absorption-config-' + digest[:12]:
        raise ValueError('ConfigMap name is not content-addressed')
    if obj.get('kind') != 'ConfigMap' or obj.get('metadata', {}).get('name') != name:
        raise ValueError('live ConfigMap identity differs')
    if obj.get('metadata', {}).get('namespace') != 'options-edge' or obj.get('immutable') is not True:
        raise ValueError('live ConfigMap namespace or immutability differs')
    if obj.get('binaryData'):
        raise ValueError('unexpected binary ConfigMap data')
    data = obj.get('data')
    expected = {'frozen-configuration.json', 'path-configuration.json', 'retention-artifact.json'}
    if not isinstance(data, dict) or set(data) != expected or not all(isinstance(v, str) for v in data.values()):
        raise ValueError('live ConfigMap keys must be exactly the three configuration files')
    actual = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
    if actual != digest:
        raise ValueError('live ConfigMap digest differs from repository')
    return actual

if __name__ == '__main__':
    name, digest = sys.argv[1:]
    print('live ConfigMap', name, 'matches repository digest', verify(json.load(sys.stdin), name, digest))
