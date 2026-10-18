#!/usr/bin/env bash
# Only this job's validated temp directory. Failed/partial transfers never delete evidence.
set -euo pipefail
action="${1:-}"
remote="${2:-}"
[[ "$remote" =~ ^/tmp/responsive-validation[.][A-Za-z0-9]{8}$ ]] || exit 1
host=abhinav@192.168.100.4
if [ "$action" = collect ]; then
  ssh -o BatchMode=yes -o ConnectTimeout=10 "$host" bash -s -- "$remote" <<'REMOTE'
set -euo pipefail
cd "$1"
[ ! -L "$1" ] && [ ! -e supervisor-active.json ] || { echo 'Supervisor still active; retain entire owned directory' >&2; exit 1; }
if [ ! -e evidence ]; then
  cp source-identities.json results/source-identities.json
  cp manifest.sha256 results/source-manifest.sha256
  mv results evidence
fi
[ -d evidence ] && [ ! -L evidence ] && [ ! -e results ]
[ -z "$(find -P evidence -type l -print -quit)" ] || { echo 'Evidence symlink refused' >&2; exit 1; }
if [ -f evidence-manifest.sha256 ]; then
  (cd evidence && sha256sum --check ../evidence-manifest.sha256)
else
  (cd evidence && find . -type f -print0 | sort -z | xargs -0 sha256sum) > evidence-manifest.sha256.tmp
  mv evidence-manifest.sha256.tmp evidence-manifest.sha256
fi
REMOTE
  destination="${3:-}"
  [ "$destination" = results ] && [ ! -e "$destination" ] || exit 1
  mkdir "$destination"
  rsync -az "$host:$remote/evidence/" "$destination/"
  rsync -az "$host:$remote/evidence-manifest.sha256" "$destination/evidence-manifest.sha256"
  python3 - "$destination" <<'PY'
import hashlib
from pathlib import Path, PurePosixPath
import re
import sys
root = Path(sys.argv[1])
expected = {}
for line in (root / 'evidence-manifest.sha256').read_text().splitlines():
    value = re.fullmatch(r'([0-9a-f]{64})  \./(.+)', line)
    if not value:
        raise RuntimeError('Invalid evidence manifest')
    name = PurePosixPath(value[2])
    if name.is_absolute() or '..' in name.parts or str(name) in expected:
        raise RuntimeError('Invalid/duplicate evidence path')
    expected[str(name)] = value[1]
actual = {}
for path in root.rglob('*'):
    if path.is_symlink():
        raise RuntimeError('Local evidence symlink refused')
    if path.is_file() and path.name != 'evidence-manifest.sha256':
        actual[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
if not expected or actual != expected:
    raise RuntimeError('Evidence set/hash mismatch; remote originals retained')
print('Every evidence file collected and SHA256 verified')
PY
elif [ "$action" = cleanup ]; then
  verified="${3:-}"
  [[ "$verified" =~ ^[01]$ ]] || exit 1
  ssh -o BatchMode=yes -o ConnectTimeout=10 "$host" python3 - "$remote" "$verified" <<'PY'
import os
from pathlib import Path
import re
import shutil
import sys
root = Path(sys.argv[1])
if not re.fullmatch(r'/tmp/responsive-validation\.[A-Za-z0-9]{8}', str(root)) or root.is_symlink() or root.stat().st_uid != os.getuid():
    raise RuntimeError('Owned cleanup root refused')
if (root / 'supervisor-active.json').exists():
    print('Active supervisor; preserve all files at ' + str(root))
    sys.exit(1)
if sys.argv[2] == '1':
    shutil.rmtree(root)
else:
    for path in root.iterdir():
        if path.name in {'results', 'evidence', 'evidence-manifest.sha256', 'source-identities.json', 'manifest.sha256'}:
            continue
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        else:
            path.unlink()
    print('Remote originals retained at ' + str(root) + '/evidence (or /results if collection never began)')
PY
else
  exit 1
fi
