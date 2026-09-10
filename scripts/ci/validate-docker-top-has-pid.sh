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
    if path.endswith("validate-docker-top-has-pid.sh"):
        continue                                   # its own explanation quotes the refused form
    for no, line in enumerate(open(path, errors="replace"), 1):
        if line.lstrip().startswith(("#", "//")):
            continue                               # prose about docker top is not a call to it
        # `docker top` and `docker container top`; the column list may be quoted. A column list held in a
        # variable cannot be checked here, so it must be written literally.
        for m in re.finditer(r"docker\s+(?:container\s+)?top\s+\S+\s+(?:-o|--o)\s*=?\s*(['\"]?)([^\s'\"|;&)]+)\1", line):
            spec = m.group(2)
            if "$" in spec:
                print(f"  FAIL {path}:{no}: docker top -o {spec} — write the column list literally, including pid")
                bad += 1
            elif "pid" not in spec.split(","):
                print(f"  FAIL {path}:{no}: docker top -o {spec} has no pid column — docker refuses it")
                bad += 1
print(f"checked {len(files)} file(s)")
sys.exit(1 if bad else 0)
PY
rc=$?
[ "$rc" = 0 ] && echo "=== validate-docker-top-has-pid: OK ===" || echo "=== validate-docker-top-has-pid: FAILED ==="
exit $rc
