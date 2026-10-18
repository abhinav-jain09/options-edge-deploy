#!/usr/bin/env python3
"""Strict source-only evidence qualifier. Missing probes/counters are never zero/pass."""
import json
import hashlib
import re
from pathlib import Path
import statistics
import sys

root = Path(sys.argv[1])
errors = []


def require(condition, message):
    if not condition:
        errors.append(message)


def finite(value):
    return type(value) in (int, float) and 0 <= value < float('inf')


require(sys.argv[2] == '0', 'One or more commands failed; later samples cannot erase that failure')
rows = {}
expected_hashes = {}
for variant in ['baseline', 'candidate']:
    try:
        web = root.parent / 'web' / variant / 'src/app/web'
        java = (web / 'ContextTapeAssets.java').read_text().split('static String javascript() {')[1].split('private static String asset')[0]
        names = [name for name in re.findall(r'asset\("([^\"]+)"\)', java) if name != 'context-tape.js']
        names += ['context-tape.css', 'context-tape-market-map.css', 'context-tape-orders.css']
        expected_hashes[variant] = {name: hashlib.sha256((web / 'assets' / name).read_bytes()).hexdigest() for name in names}
    except (OSError, IndexError) as exc:
        require(False, f'{variant}: immutable source hashes unavailable: {exc}')
for variant in ['baseline', 'candidate']:
    for count in [390, 1439]:
        for repeat in range(1, 6):
            name = f'stack-{variant}-{count}-{repeat}'
            try:
                values = [json.loads(line.removeprefix('RESPONSIVE_PROBE '))
                          for line in (root / f'{name}.log').read_text().splitlines()
                          if line.startswith('RESPONSIVE_PROBE ')]
                require(len(values) == 1, f'{name}: exactly one terminal report required')
                row = values[0]
                rows[(variant, count, repeat)] = row
                require(bool(expected_hashes.get(variant)) and row.get('assetHashes') == expected_hashes.get(variant), f'{name}: actual served-source hash mismatch')
                require(row.get('status') == 'MEASURED' and row.get('variant') == variant
                        and row.get('count') == count and row.get('repeat') == repeat, f'{name}: identity/completion')
                require(row.get('bookRecords') == 4096 and row.get('activePositions') == 16
                        and row.get('linkedPlans') == 16 and row.get('geometry') ==
                        {'shades': 16, 'threads': 32, 'labels': 32}, f'{name}: full coupled geometry/book')
                paths = {'full-chart-resize', 'map-snapshot', 'orders-render-health'}
                if variant == 'candidate':
                    paths |= {'floating-move', 'floating-resize', 'panel-dock-detach'}
                    require(row.get('floatingPreparation', {}).get('method') ==
                            'public-layer-controls; existing five-second deadline drained',
                            f'{name}: floating-only fixed-chart preparation required')
                    require(row.get('panelPreparation') == {
                                'method': 'prior document layout forced synchronously before panel clock',
                                'warmup': 50}, f'{name}: panel-only clean-layout preparation required')
                require(set(row.get('paths', {})) == paths, f'{name}: missing/extra paths')
                for key in paths:
                    sample = row.get('paths', {}).get(key, {})
                    data = sample.get('samples', [])
                    require(sample.get('records') == 200 and len(data) == 200 and all(map(finite, data)), f'{name}/{key}: 200 finite postwarm samples')
                    if len(data) == 200 and all(map(finite, data)):
                        require(abs(sample.get('meanMs', -1) - statistics.mean(data)) < 1e-6, f'{name}/{key}: mean recomputation')
                        require(abs(sample.get('p99Ms', -1) - sorted(data)[198]) < 1e-6, f'{name}/{key}: p99 recomputation')
                    if key.startswith('floating-'):
                        require(sample.get('nativeFrames') == 250 and sample.get('distinctFrameTimestamps') is True
                                and sample.get('sameSvg') is True, f'{name}/{key}: native frames/unchanged candles')
                    if key.startswith('floating-') or key == 'panel-dock-detach':
                        require(finite(sample.get('p99Ms')) and sample['p99Ms'] <= 16, f'{name}/{key}: 16ms p99')
                    if key == 'panel-dock-detach':
                        stamps = sample.get('frameTimestamps', [])
                        require(sample.get('nativeFrames') == 250 and sample.get('distinctFrameTimestamps') is True
                                and len(stamps) == 250 and all(map(finite, stamps))
                                and all(b > a for a, b in zip(stamps, stamps[1:])),
                                f'{name}: 250 native panel frames and timestamps required')
                memory = row.get('memory', {})
                retained = memory.get('retainedBytes')
                require(retained == memory.get('loaded', {}).get('heapBytes', -1) - memory.get('empty', {}).get('heapBytes', -1), f'{name}: retained bytes must recompute from actual counters')
                require(finite(retained) and retained <= 32 * 1024 * 1024, f'{name}: unchanged32MiB retained limit')
                for key in ['empty', 'loaded'] + (['beforeCompanion', 'companion', 'returned'] if variant == 'candidate' else []):
                    value = memory.get(key, {})
                    require(finite(value.get('heapBytes')) and value.get('heapBytes', 0) > 0
                            and isinstance(value.get('isolates'), list) and len(value['isolates']) > 0
                            and len(set(value['isolates'])) == len(value['isolates'])
                            and finite(value.get('pss', {}).get('bytes')) and value.get('pss', {}).get('bytes', 0) > 0,
                            f'{name}/{key}: mandatory deduplicated isolate heap and whole-browser PSS')
                if variant == 'candidate':
                    for delta, field in [('companionHeapDelta', 'heapBytes'), ('companionPssDelta', 'pss')]:
                        before = memory.get('beforeCompanion', {}).get(field)
                        after = memory.get('companion', {}).get(field)
                        if field == 'pss':
                            before = (before or {}).get('bytes')
                            after = (after or {}).get('bytes')
                        require(type(before) in (int, float) and type(after) in (int, float)
                                and memory.get(delta) == after - before, f'{name}/{delta}: incremental counter recomputation')
                    for key in ['companionHeapDelta', 'companionPssDelta']:
                        value = memory.get(key)
                        require(type(value) in (int, float) and -float('inf') < value <= 64 * 1024 * 1024,
                                f'{name}/{key}: mandatory64MiB companion increment')
                    require(memory.get('returned', {}).get('heapBytes', float('inf')) - memory.get('empty', {}).get('heapBytes', 0)
                            <= 32 * 1024 * 1024, f'{name}: retained state after repeated companion return')
            except (OSError, ValueError, KeyError, IndexError, TypeError) as exc:
                require(False, f'{name}: missing/invalid report {type(exc).__name__}: {exc}')

comparisons = []
for count in [390, 1439]:
    for key in ['full-chart-resize', 'map-snapshot', 'orders-render-health']:
        try:
            before = statistics.median(rows[('baseline', count, i)]['paths'][key]['meanMs'] for i in range(1, 6))
            after = statistics.median(rows[('candidate', count, i)]['paths'][key]['meanMs'] for i in range(1, 6))
            require(before > 0 and finite(after), f'{count}/{key}: nonzero baseline')
            change = after / before - 1 if before > 0 else None
            comparisons.append({'count': count, 'path': key, 'baselineMedianMs': before, 'candidateMedianMs': after, 'relativeChange': change})
            require(change is not None and change <= .10, f'{count}/{key}: existing path median regressed more than10percent')
        except (KeyError, TypeError):
            require(False, f'{count}/{key}: missing five paired repeats')
    for i in range(1, 6):
        try:
            delta = rows[('candidate', count, i)]['memory']['loaded']['heapBytes'] - rows[('baseline', count, i)]['memory']['loaded']['heapBytes']
            require(delta <= 4 * 1024 * 1024, f'{count}/{i}: in-window layout increment exceeds4MiB')
        except (KeyError, TypeError):
            require(False, f'{count}/{i}: missing incremental memory pair')

legacy = {}
for i in range(1, 6):
    try:
        groups = [json.loads(line.removeprefix('VISUAL_PROBE ')) for line in (root / f'legacy-{i}.log').read_text().splitlines() if line.startswith('VISUAL_PROBE ')]
        require(len(groups) == 3, f'legacy{i}: all three original scenarios')
        for row in groups:
            key = row.get('probe', '') + '-' + row.get('variant', '')
            legacy.setdefault(key, []).append(row)
            require(row.get('withinBudget') is True and row.get('nativeFrames') == 250
                    and row.get('records') == 200 and row.get('budgetMs') == 16 and finite(row.get('sampleP99Ms'))
                    and row['sampleP99Ms'] <= 16 and row.get('distinctFrameTimestamps') is True
                    and row.get('frameScheduler') == 'native-requestAnimationFrame'
                    and row.get('fixedBindings') is True and row.get('sameCandle') is True
                    and row.get('viewportUpdates') == 250 and row.get('shades') == 16
                    and row.get('workload') == 'fixed-trade-viewport-pan-and-latched-stop', f'legacy{i}/{key}: original gate')
            if key == 'linked-overlay-candidate':
                require(row.get('threads') == 32 and row.get('labels') == 32 and row.get('stateTransitions') == 1, f'legacy{i}:32threads/labels and single transition')
    except (OSError, ValueError, TypeError) as exc:
        require(False, f'legacy{i}: missing/invalid {exc}')
require(set(legacy) == {'existing-overlay-baseline', 'existing-overlay-candidate', 'linked-overlay-candidate'}
        and all(len(x) == 5 for x in legacy.values()), 'Every legacy scenario needs all five repeats')
try:
    old = statistics.median(x['elapsedMs'] for x in legacy['existing-overlay-baseline'])
    new = statistics.median(x['elapsedMs'] for x in legacy['existing-overlay-candidate'])
    require(old > 0 and new / old - 1 <= .10, 'Existing overlay five-repeat median regression exceeds10percent')
except (KeyError, TypeError):
    require(False, 'Missing original overlay relative comparison')

summary = {'classification': 'source-test-only; no release or deployment authority', 'status': 'FAIL' if errors else 'PASS',
           'comparisons': comparisons, 'errors': errors, 'reports': len(rows)}
(root / 'responsive-summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
print(json.dumps(summary, allow_nan=False))
sys.exit(bool(errors))
