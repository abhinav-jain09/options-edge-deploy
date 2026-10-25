#!/usr/bin/env bash
# validate-level-absorption-config.sh — the Level Absorption ConfigMap (the EYEBALL-only configuration and the retention artifact the
# pod reads at start) must agree with the events topic's byte cap in scripts/kafka/topics.env, and the two input topics the service
# reads that LA-050 requires to be one lineage (the footprint bars and the linearized tape) must be named identically wherever a manifest
# names them for a service that reads both.
#   (no args)                      validate; exit 1 on any disagreement
#   --print-retention-bytes        print the artifact's retentionBytes (after validating) for the Jenkins stages
set -euo pipefail
cd "$(dirname "$0")/../.."
python3 - "$@" <<'PY'
import json, re, sys, pathlib, math
try:
    import yaml
except ImportError:
    sys.exit("FATAL: python3 yaml module required")
cm = yaml.safe_load(pathlib.Path('k8s/services/level-absorption/base/level-absorption-config.yaml').read_text())
assert cm['kind'] == 'ConfigMap' and cm['metadata']['name'] == 'level-absorption-config', 'wrong ConfigMap'
assert set(cm['data']) == {'frozen-configuration.json', 'retention-artifact.json'}, 'ConfigMap keys must be exactly the two files'
art = json.loads(cm['data']['retention-artifact.json'])
cfg = json.loads(cm['data']['frozen-configuration.json'])
err = []
if art.get('kind') != 'level-absorption.retention-artifact' or art.get('formatVersion') != 1: err.append('retention artifact kind/formatVersion')
if art.get('topic') != 'futures.level-absorption.events': err.append('artifact topic is not the events topic')
if art.get('retentionMs') != -1: err.append('artifact retentionMs must be -1')
need = math.ceil(art['measuredBytesPerDay'] * art['retentionDays'] * art['safetyFactor'])
if art.get('retentionBytes') != need: err.append(f"retentionBytes {art.get('retentionBytes')} != ceil(bytes/day x days x factor) = {need}")
if cfg.get('kind') != 'level-absorption.frozen-configuration': err.append('configuration kind')
if 'EYEBALL-ONLY' not in cfg.get('phase0', {}).get('bootstrapGenerator', ''):
    err.append('the shipped configuration is not marked EYEBALL-ONLY PROVISIONAL: a real Phase 0 freeze needs its own reviewed change to this validator')
topics = pathlib.Path('scripts/kafka/topics.env').read_text()
m = re.search(r'^OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES="\$OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES[^"]*\bfutures\.level-absorption\.events=(\d+)', topics, re.M)
if not m: err.append('topics.env does not declare futures.level-absorption.events in OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES')
elif int(m.group(1)) != art['retentionBytes']: err.append(f"topics.env cap {m.group(1)} != artifact {art['retentionBytes']}")
if not re.search(r'^OPTIONS_EDGE_RESET_PRESERVED_TOPICS="[^"]*\bfutures\.level-absorption\.events\b', topics, re.M): err.append('events topic not in OPTIONS_EDGE_RESET_PRESERVED_TOPICS')
if not re.search(r'futures\.level-absorption\.events=-1', topics): err.append('events retention.ms override -1 missing')
# the Deployment must mount the ConfigMap and point the two env vars at keys of it
dep = pathlib.Path('k8s/services/level-absorption/base/level-absorption-deployment.yaml').read_text()
for k in ('frozen-configuration.json', 'retention-artifact.json'):
    if f'/home/options-edge/config/level-absorption/{k}' not in dep: err.append(f'deployment does not point at {k}')
if 'configMap:\n            name: level-absorption-config' not in dep: err.append('deployment does not mount the ConfigMap')
if 'value: EYEBALL' not in dep: err.append('phase is not EYEBALL')
# LA-050: footprint bars and linearized tape names, as every manifest spells them
tape = set(); foot = set()
for p in pathlib.Path('k8s/services').glob('*/base/*-deployment.yaml'):
    t = p.read_text()
    for m2 in re.finditer(r'value:\s*((?:es\.)?(?:underlying\.es\.trades\.linearized|futures\.footprint\.bars))\s*(?:#.*)?$', t, re.M):
        (tape if 'linearized' in m2.group(1) else foot).add(m2.group(1))
if tape - {'underlying.es.trades.linearized'}: err.append(f'linearized tape named inconsistently: {sorted(tape)}')
if foot - {'es.futures.footprint.bars'}: err.append(f'footprint bars named inconsistently: {sorted(foot)}')
if 'es.futures.footprint.bars' not in foot: err.append('LA-050: no manifest names es.futures.footprint.bars')
if 'underlying.es.trades.linearized' not in tape: err.append('LA-050: no manifest names underlying.es.trades.linearized')
if err:
    for e in err: print('FAIL:', e, file=sys.stderr)
    sys.exit(1)
if '--print-retention-bytes' in sys.argv[1:]:
    print(art['retentionBytes'])
else:
    print('ok: level-absorption configuration, retention cap', art['retentionBytes'], 'and LA-050 topic names agree')
PY
