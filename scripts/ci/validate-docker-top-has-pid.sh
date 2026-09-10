#!/usr/bin/env bash
# `docker top <container> -o <cols>` fails unless <cols> includes pid: docker maps ps output back to the
# container's processes through the PID column ("Couldn't find PID field in ps output"). With stderr
# discarded, a check like `docker top C -o args | grep -q X` is then false on EVERY container, healthy or
# not. It failed the enumeration mirror's es4-docker install on a running MirrorMaker (build #5).
set -uo pipefail
cd "$(dirname "$0")/../.."
python3 - "$@" <<'PY'
import os, re, sys
files = sys.argv[1:]
if not files:
    for root, dirs, names in os.walk("."):
        dirs[:] = [d for d in dirs if d not in (".git", "node_modules", "target")]
        files += [os.path.join(root, n)[2:] for n in names if n.startswith("Jenkinsfile") or n.endswith(".sh")]
bad = 0
for path in sorted(files):
    for no, line in enumerate(open(path, errors="replace"), 1):
        for m in re.finditer(r"docker\s+top\s+\S+\s+-o\s+([^\s'\"|;&)]+)", line):
            cols = m.group(1).split(",")
            if "pid" not in cols:
                print(f"  FAIL {path}:{no}: docker top -o {m.group(1)} has no pid column — docker refuses it")
                bad += 1
print(f"checked {len(files)} file(s)")
sys.exit(1 if bad else 0)
PY
rc=$?
[ "$rc" = 0 ] && echo "=== validate-docker-top-has-pid: OK ===" || echo "=== validate-docker-top-has-pid: FAILED ==="
exit $rc
