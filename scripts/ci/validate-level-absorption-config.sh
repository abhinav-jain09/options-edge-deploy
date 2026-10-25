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
import json, re, sys, pathlib, math, hashlib
try:
    import yaml
except ImportError:
    sys.exit("FATAL: python3 yaml module required")
cm = yaml.safe_load(pathlib.Path('k8s/overlays/dev/level-absorption-config.yaml').read_text())
assert cm['kind'] == 'ConfigMap', 'not a ConfigMap'
assert set(cm['data']) == {'frozen-configuration.json', 'retention-artifact.json'}, 'ConfigMap keys must be exactly the two files'
art = json.loads(cm['data']['retention-artifact.json'])
cfg = json.loads(cm['data']['frozen-configuration.json'])
err = []
if art.get('kind') != 'level-absorption.retention-artifact' or art.get('formatVersion') != 1: err.append('retention artifact kind/formatVersion')
if art.get('topic') != 'futures.level-absorption.events': err.append('artifact topic is not the events topic')
if art.get('retentionMs') != -1: err.append('artifact retentionMs must be -1')
need = math.ceil(art['measuredBytesPerDay'] * art['retentionDays'] * art['safetyFactor'])
if art.get('retentionBytes') != need: err.append(f"retentionBytes {art.get('retentionBytes')} != ceil(bytes/day x days x factor) = {need}")
if cfg.get('kind') != 'level-absorption.frozen-configuration' or cfg.get('formatVersion') != 2: err.append('configuration kind/formatVersion (the service reads formatVersion 2)')
ph = cfg.get('phase0', {})
for k in ('freezeCommit', 'candidatePercentile', 'bootstrapGenerator', 'bootstrapSeed'):
    if k not in ph: err.append(f'configuration phase0.{k} missing')
pr = cfg.get('parameters', {})
if not isinstance(pr, dict) or not pr: err.append('configuration parameters missing')
else:
    for k, v in pr.items():
        if isinstance(v, bool) or not isinstance(v, (int, float, str)): err.append(f'configuration parameter {k} has an unsupported type')
    for k in ('minimumEffort', 'flatRatioCutoff'):
        if not isinstance(pr.get(k), (int, float)) or pr[k] <= 0: err.append(f'configuration parameter {k} must be a positive number (no code default exists)')
    if not isinstance(pr.get('minimumBarImbalanceBps'), (int, float)) or pr['minimumBarImbalanceBps'] < 0: err.append('configuration parameter minimumBarImbalanceBps must be a non-negative number')
# Verbatim pin: processing docs/level-absorption/phase0/market-structure-{eyeball-configuration,retention-artifact}.json.
# Persistent host mode pins the explicit EYEBALL configuration and one-Friday retention artifact from processing PR #1414.
PIN = {'frozen-configuration.json': '24dfa34790f5f75f09ac82959cc673d56eb9dfe98e76fbaef5e9fb41362fa316', 'retention-artifact.json': 'b53d677e337957e8c17ec5e638a78eb9b28e2325373ea9e00710d8483765f426'}
for k, want in PIN.items():
    got = hashlib.sha256(cm['data'][k].encode()).hexdigest()
    if got != want: err.append(f'{k} differs from the pinned Phase 0 artifact: sha256 {got}')
digest = hashlib.sha256(json.dumps(cm['data'], sort_keys=True).encode()).hexdigest()
cm_name = 'level-absorption-config-' + digest[:12]
if cm['metadata']['name'] != cm_name: err.append(f"the ConfigMap must be named {cm_name} (content-named), it is {cm['metadata']['name']}")
if cm.get('immutable') is not True: err.append('the ConfigMap must be immutable: true')
dep0 = pathlib.Path('k8s/services/level-absorption/base/level-absorption-deployment.yaml').read_text()
if f'configMap:\n            name: {cm_name}\n' not in dep0: err.append(f'the Deployment must mount the ConfigMap {cm_name}')
if 'level-absorption-config-sha256' in dep0: err.append('the obsolete digest annotation is still present')
# production must render no level-absorption ConfigMap; the dev render carries exactly one
import subprocess as _sp
for envname, expect in (('production', 0), ('dev', 1)):
    r = _sp.run(['kubectl', 'kustomize', f'k8s/overlays/{envname}'], capture_output=True, text=True)
    if r.returncode != 0: err.append(f'kustomize {envname} failed: {r.stderr.strip()[:200]}'); continue
    n = sum(1 for d in yaml.safe_load_all(r.stdout) if d and d.get('kind') == 'ConfigMap' and d['metadata']['name'].startswith('level-absorption-config'))
    if n != expect: err.append(f'the {envname} render carries {n} level-absorption ConfigMap(s), expected {expect}')
# no generated slice may carry the ConfigMap (service-deploy.sh refuses ConfigMaps in a render)
for sl in pathlib.Path('k8s/services/level-absorption/overlays').glob('*/manifest.yaml'):
    if re.search(r'^kind:\s*ConfigMap\s*$', sl.read_text(), re.M): err.append(f'{sl} contains a ConfigMap')
if 'EYEBALL-ONLY' not in cfg.get('phase0', {}).get('bootstrapGenerator', ''):
    err.append('the shipped configuration is not marked EYEBALL-ONLY PROVISIONAL: a real Phase 0 freeze needs its own reviewed change to this validator')
import subprocess
ev = 'futures.level-absorption.events'
def effective(var):
    out = subprocess.run(['bash', '-c', 'set -u; OPTIONS_EDGE_TOPICS=""; OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES=""; OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES=""; '
                          'OPTIONS_EDGE_EXACT_PARTITION_TOPICS=""; source scripts/kafka/topics.env; printf "%s" "${' + var + '}"'], capture_output=True, text=True)
    if out.returncode != 0: err.append(f'topics.env does not source cleanly: {out.stderr.strip()}'); return []
    return out.stdout.split()
byt = [w for w in effective('OPTIONS_EDGE_TOPIC_RETENTION_BYTES_OVERRIDES') if w.split('=')[0] == ev]
if byt != [f"{ev}={art['retentionBytes']}"]: err.append(f'topics.env retention.bytes entries for the events topic are {byt}, expected exactly one {ev}={art["retentionBytes"]}')
ms = [w for w in effective('OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES') if w.split('=')[0] == ev]
if ms != [f'{ev}=-1']: err.append(f'topics.env retention.ms entries for the events topic are {ms}, expected exactly one {ev}=-1')
if [w for w in effective('OPTIONS_EDGE_RESET_PRESERVED_TOPICS') if w == ev] != [ev]: err.append('events topic not exactly once in OPTIONS_EDGE_RESET_PRESERVED_TOPICS')
if [w for w in effective('OPTIONS_EDGE_TOPICS') if w.split(':')[0] == ev] != [ev + ':1']: err.append('events topic not exactly once, with 1 partition, in OPTIONS_EDGE_TOPICS')
if [w for w in effective('OPTIONS_EDGE_EXACT_PARTITION_TOPICS') if w == ev] != [ev]: err.append('events topic not exactly once in OPTIONS_EDGE_EXACT_PARTITION_TOPICS')
# the Deployment must mount the ConfigMap and point the two env vars at keys of it
dep = pathlib.Path('k8s/services/level-absorption/base/level-absorption-deployment.yaml').read_text()
for k in ('frozen-configuration.json', 'retention-artifact.json'):
    if f'/home/options-edge/config/level-absorption/{k}' not in dep: err.append(f'deployment does not point at {k}')
if 'value: EYEBALL' not in dep: err.append('phase is not EYEBALL')
# LA-050 (lineage): the footprint bars and the linearized tape must be built from the SAME raw print stream, and level-absorption must read the
# names those two producers publish. Parsed from the manifests (every Deployment doc, env name -> literal value), not scanned as text.
def env_of(path, deployment):
    out = {}
    for doc in yaml.safe_load_all(pathlib.Path(path).read_text()):
        if doc and doc.get('kind') == 'Deployment' and doc['metadata']['name'] == deployment:
            for c in doc['spec']['template']['spec']['containers']:
                for e in c.get('env', []):
                    if 'value' in e: out[e['name']] = e['value']
            return out
    err.append(f'LA-050: {deployment} not found in {path}')
    return out
cvd = env_of('k8s/es4/services/es-cvd.yaml', 'es-cvd-service')
common = yaml.safe_load(pathlib.Path('k8s/es4/es4-common-env.yaml').read_text())
es4_prefix = (common.get('data') or {}).get('TOPIC_PREFIX')
raw = cvd.get('KAFKA_ES_TRADES_TOPIC'); bars = cvd.get('KAFKA_ES_FOOTPRINT_BARS_TOPIC')
if not raw or not bars or not es4_prefix: err.append(f'LA-050: es-cvd raw trades {raw!r}, footprint bars {bars!r} or the es4 TOPIC_PREFIX {es4_prefix!r} unreadable')
for envname in ('dev', 'production'):
    lin = env_of(f'k8s/services/es-trade-linearizer/overlays/{envname}/manifest.yaml', 'es-trade-linearizer-service')
    if lin.get('KAFKA_ES_TRADES_TOPIC') != raw:
        err.append(f"LA-050: {envname} linearizer reads {lin.get('KAFKA_ES_TRADES_TOPIC')!r} but es-cvd (the footprint producer) reads {raw!r}: not one print stream")
    la = env_of(f'k8s/services/level-absorption/overlays/{envname}/manifest.yaml', 'level-absorption-service')
    if la.get('KAFKA_LEVEL_ABSORPTION_FOOTPRINT_TOPIC') != (es4_prefix or '') + (bars or ''):
        err.append(f"LA-050: {envname} level-absorption reads footprint {la.get('KAFKA_LEVEL_ABSORPTION_FOOTPRINT_TOPIC')!r}, the es4 producer publishes {(es4_prefix or '') + (bars or '')!r}")
    if 'KAFKA_LEVEL_ABSORPTION_TRADES_TOPIC' in la:
        err.append('LA-050: level-absorption overrides the trades topic; it must read the linearizer output named by its manifest')
    if lin.get('KAFKA_ES_TRADES_LINEARIZED_TOPIC') != 'underlying.es.trades.linearized':
        err.append(f"LA-050: {envname} linearizer publishes {lin.get('KAFKA_ES_TRADES_LINEARIZED_TOPIC')!r}, level-absorption's manifest reads underlying.es.trades.linearized")
if err:
    for e in err: print('FAIL:', e, file=sys.stderr)
    sys.exit(1)
if '--print-retention-bytes' in sys.argv[1:]:
    print(art['retentionBytes'])
elif '--print-config-name' in sys.argv[1:]:
    print(cm_name)
elif '--print-data-digest' in sys.argv[1:]:
    print(digest)
else:
    print('ok: level-absorption configuration, retention cap', art['retentionBytes'], 'and LA-050 topic names agree')
PY
