#!/usr/bin/env bash
# validate-line-health-config.sh - the Line health configuration ConfigMap and the wiring that turns line health on (requirement LH-E-4, LH-E-5, design 4.6).
#   1. The ConfigMap (the second document of k8s/overlays/dev/level-absorption-config.yaml) is immutable, holds one file, and is NAMED BY ITS CONTENT:
#      level-absorption-line-health-config-<first 12 hex digits of the SHA-256 of the file text>. The service's own configDigest (written into every record) is a
#      different number, the SHA-256 of the RESOLVED configuration (lineHealthKinds=DEFAULT expanded to the kinds the build can read), so it is NOT the name suffix;
#      the running service reports the file's SHA-256 as configurationFileSha256 on /health/line-health, which is what the live check compares with the name.
#   2. The file is in canonical form (keys sorted, one key=value per line, no whitespace around '=', newline-terminated, no key twice) and its key set is
#      EXACTLY the set of design section 4.6a (an unknown or missing key is a failure, as the service itself refuses unknown keys).
#   3. The development enable patch turns line health on, names that ConfigMap, mounts it read-only and points the configuration variable at the mounted file.
#   4. NOTHING ELSE turns it on: no other file under k8s/ or scripts/es4/ names the enable variable except the dev patch and the dev slice generated from it, and the
#      production and experiment renders carry neither the variable nor a line-health ConfigMap. (The flag defaults to false in the service.)
# LINE_HEALTH_ROOT overrides the repository root (the test runs this against mutated copies). Exit 1 on any failure.
set -euo pipefail
cd "${LINE_HEALTH_ROOT:-$(dirname "$0")/../..}"
python3 -I - <<'PY'
import hashlib, pathlib, re, subprocess, sys, os
import yaml

KEYS = """lineHealthKinds depthEnvironment clockUnfit bandTicks crossConfirmSeconds touchExitPoints defendedMin defendedConfirmSeconds givingWayLevel givingWayDrop
givingWayWindowSeconds stateConfirmSeconds wBookMilli wMboMilli wTapeMilli depthReferenceContracts mboWindowSeconds mboMaxLagSeconds minMboFlow tapeWindowSeconds
minTestedContracts pullCutoffMilli followDistancePoints followWindowSeconds minFollowSeconds pushCutoffMilli minPushContracts stallPaceMilliPointsPerMinute stallSeconds
rollbackShareMilli reclaimTicks reclaimSeconds heldOutSeconds beyondTicks absorbTicks vacuumDepthContracts guardTicks guardWindowSeconds guardSecondsRequired
guardClearSeconds fitConfirmSeconds tapeSilentSeconds inputCompletionGraceSeconds displayStaleSeconds retiredDisplayMinutes tickSpacingPixels tickMinHeightPixels
tickMaxHeightPixels tickLabelClearancePixels labelInsetPixels labelOffsetPixels labelRowPixels labelCharWidthPixels labelPaddingPixels labelFontPixels compactFontPixels
legendFontPixels compactRowPixels compactPitchPixels basisBracketMillis basisFreshMillis basisCapacity leaderThresholdPixels markerSizePixels unknownDotTenthsPixels
userLinePublishDebounceMillis snapshotSeconds stateRefreshMinutes labelFloorFontPixels labelFloorRowPixels provisional""".split()
TEXT_KEYS = {'lineHealthKinds', 'depthEnvironment'}
BOOL_KEYS = {'clockUnfit', 'provisional'}
FILE_KEY = 'line-health-configuration.txt'
MOUNT = '/home/options-edge/config/line-health'
PREFIX = 'level-absorption-line-health-config-'
ENABLE = 'LEVEL_ABSORPTION_LINE_HEALTH_ENABLED'
CONF = 'LEVEL_ABSORPTION_LINE_HEALTH_CONFIGURATION'
BOOK = 'LEVEL_ABSORPTION_LINE_HEALTH_BOOK_ENABLED'
USER = 'LINE_HEALTH_USER_LINES_ENABLED'
err = []
def read(p): return pathlib.Path(p).read_text()

docs = [d for d in yaml.safe_load_all(read('k8s/overlays/dev/level-absorption-config.yaml')) if d]
lh = [d for d in docs if d.get('kind') == 'ConfigMap' and d['metadata']['name'].startswith(PREFIX)]
if len(lh) != 1:
    sys.exit(f'FAIL: expected exactly one {PREFIX}* ConfigMap in k8s/overlays/dev/level-absorption-config.yaml, found {len(lh)}')
cm = lh[0]
if cm.get('immutable') is not True: err.append('the line-health ConfigMap must be immutable: true')
if set(cm.get('data', {})) != {FILE_KEY}: err.append(f'the line-health ConfigMap must hold exactly one file, {FILE_KEY}')
text = cm.get('data', {}).get(FILE_KEY, '')
name = PREFIX + hashlib.sha256(text.encode()).hexdigest()[:12]
if cm['metadata']['name'] != name: err.append(f"the line-health ConfigMap must be named {name} (content-named), it is {cm['metadata']['name']}")
if not text.endswith('\n') or text.endswith('\n\n'): err.append('the file must end with exactly one newline')
lines = text.splitlines()
pairs = []
for l in lines:
    if '=' not in l or l != l.strip() or l.split('=', 1)[0] != l.split('=', 1)[0].strip() or l.split('=', 1)[0] == '' or l.split('=', 1)[0].endswith(' '):
        err.append(f'not a canonical key=value line: {l!r}')
    else:
        pairs.append(tuple(l.split('=', 1)))
keys = [k for k, _ in pairs]
if keys != sorted(keys): err.append('keys are not sorted')
if len(set(keys)) != len(keys): err.append('a key appears twice')
if set(keys) - set(KEYS): err.append('unknown key(s): ' + ', '.join(sorted(set(keys) - set(KEYS))))
if set(KEYS) - set(keys): err.append('missing key(s): ' + ', '.join(sorted(set(KEYS) - set(keys))))
for k, v in pairs:
    if k == 'depthEnvironment':
        if not v.strip(): err.append('depthEnvironment is required and may not be empty')
    elif k == 'lineHealthKinds':
        if v != 'DEFAULT' and (not re.fullmatch(r'[A-Z][A-Z0-9_]*(,[A-Z][A-Z0-9_]*)*', v) or v.split(',') != sorted(set(v.split(',')))):
            err.append('lineHealthKinds must be DEFAULT or a sorted comma-separated list of kind names without duplicates')
    elif k in BOOL_KEYS:
        if v not in ('true', 'false'): err.append(f'{k} must be true or false')
    elif not re.fullmatch(r'(0|[1-9][0-9]*)', v):
        err.append(f'{k} must be a non-negative integer, it is {v!r}')

# the development patch
patch = yaml.safe_load(read('k8s/overlays/dev/level-absorption-dev-enable-patch.yaml'))
ctr = patch['spec']['template']['spec']['containers'][0]
env = {e['name']: e.get('value') for e in ctr.get('env', [])}
if env.get(ENABLE) != 'true': err.append(f'the dev patch must set {ENABLE}=true')
if env.get(BOOK) != 'true': err.append(f'the dev patch must set {BOOK}=true (line health M2, shadow book)')
if env.get(USER) != 'true': err.append(f'the dev patch must set {USER}=true (line health M5, user lines)')
if env.get(CONF) != f'{MOUNT}/{FILE_KEY}': err.append(f'the dev patch must set {CONF}={MOUNT}/{FILE_KEY}')
vols = {v['name']: v for v in patch['spec']['template']['spec'].get('volumes', [])}
v = vols.get('level-absorption-line-health-config')
if not v or v.get('configMap', {}).get('name') != name or v['configMap'].get('optional') is not False:
    err.append(f'the dev patch must mount ConfigMap {name} (optional: false)')
mounts = [m for m in ctr.get('volumeMounts', []) if m.get('name') == 'level-absorption-line-health-config']
if len(mounts) != 1 or mounts[0].get('mountPath') != MOUNT or mounts[0].get('readOnly') is not True:
    err.append(f'the dev patch must mount that volume read-only at {MOUNT}')

# nothing else turns it on
allowed = {'k8s/overlays/dev/level-absorption-dev-enable-patch.yaml', 'k8s/services/level-absorption/overlays/dev/manifest.yaml'}
for root in ('k8s', 'scripts/es4'):
    for p in pathlib.Path(root).rglob('*'):
        if p.is_file() and p.as_posix() not in allowed and p.suffix in ('.yaml', '.yml', '.sh', '.py', '.env', '.json'):
            if ENABLE in p.read_text(errors='ignore'): err.append(f'{p} names {ENABLE}; only the dev patch and its generated dev slice may')
            if BOOK in p.read_text(errors='ignore'): err.append(f'{p} names {BOOK}; only the dev patch and its generated dev slice may')
for envname in ('production', 'experiment'):
    r = subprocess.run(['kubectl', 'kustomize', f'k8s/overlays/{envname}'], capture_output=True, text=True)
    if r.returncode != 0:
        err.append(f'kustomize {envname} failed: {r.stderr.strip()[:200]}'); continue
    if ENABLE in r.stdout or BOOK in r.stdout or CONF in r.stdout or USER in r.stdout or 'line-health-config' in r.stdout:
        err.append(f'the {envname} render carries line-health configuration')
if err:
    for e in err: print('FAIL:', e, file=sys.stderr)
    sys.exit(1)
print(f'ok: line-health configuration {name}: {len(keys)} keys, digest {hashlib.sha256(text.encode()).hexdigest()}')
PY
