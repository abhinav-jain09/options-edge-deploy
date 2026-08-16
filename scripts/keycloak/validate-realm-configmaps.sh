#!/usr/bin/env bash
# Guard for the twice-shipped Keycloak outage class (#835 public realm, oe-keycloak
# CrashLoopBackOff 2026-08-13..16 internal realm): Keycloak's --import-realm parser
# REJECTS unknown fields, so any commentary key ("//", "comment_*", …) inside a realm
# JSON stops the server from booting. Prose belongs in YAML comments above the JSON.
#
# Validates EVERY realm JSON embedded in EVERY keycloak realm configmap in k8s/:
#   1. the JSON parses;
#   2. no object anywhere carries a key starting with "//" or "comment".
set -euo pipefail
cd "$(dirname "$0")/../.."

fail=0
for f in $(grep -rl --include='*.yaml' -- '--import-realm\|realm.json: |' k8s | sort -u); do
  python3 - "$f" <<'PY' || fail=1
import json
import re
import sys

path = sys.argv[1]
s = open(path).read()
found = False
for key in re.findall(r'^  ([\w.-]+\.json): \|$', s, re.M):
    found = True
    block = s.split('  %s: |' % key, 1)[1]
    body = []
    for ln in block.split('\n')[1:]:
        if ln.strip() == '' or ln.startswith('    '):
            body.append(ln[4:])
        else:
            break
    try:
        doc = json.loads('\n'.join(body))
    except json.JSONDecodeError as e:
        print(f'FAIL {path} :: {key}: JSON does not parse: {e}')
        sys.exit(1)

    def walk(o, trail):
        if isinstance(o, dict):
            for k, v in o.items():
                if k.startswith('//') or k.lower().startswith('comment'):
                    print(f'FAIL {path} :: {key}: illegal commentary key "{k}" at {trail} '
                          f'- Keycloak import will refuse to boot')
                    sys.exit(1)
                walk(v, trail + '.' + k)
        elif isinstance(o, list):
            for i, v in enumerate(o):
                walk(v, f'{trail}[{i}]')
    walk(doc, key)
    print(f'OK   {path} :: {key} (realm {doc.get("realm")})')
if not found:
    sys.exit(0)
PY
done
exit "$fail"
