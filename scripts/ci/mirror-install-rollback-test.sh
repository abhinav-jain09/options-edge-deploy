#!/usr/bin/env bash
# The install step replaces five files for a unit the stop phase has already brought DOWN. If a move
# fails halfway, the unit is both down and mixed on disk — so the step keeps the previous generation
# and restores it. That restore depends on one easily-lost detail: the ERR trap must be inherited by
# the put() function, which needs `set -E`. Without it the script exited and left exactly the mixed
# generation the trap exists to undo, and nothing failed loudly.
#
# So drive the real task body: a clean install, and a failure at the fourth of five moves.
set -euo pipefail
cd "$(dirname "$0")/../.."
command -v python3 >/dev/null || { echo "FAIL: python3 is required to extract the task body"; exit 1; }

WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
render() { # out-dir  [break]  [target]
  python3 - "$1" "${2:-}" <<'PY'
import sys, pathlib, yaml
T, brk = sys.argv[1], sys.argv[2]
tasks = yaml.safe_load(open("ansible/tasks/es-mirror-unit.yml"))
body = [t for t in tasks if "install the rendered files" in t["name"]]
if not body:
    print("NO_TASK"); raise SystemExit(0)
cmd = body[0]["ansible.builtin.shell"]["cmd"]
import os, re
# the body shell-QUOTES every value it renders (Ansible's quote filter), so the substitutions have to
# match THOSE expressions; anything left unresolved is reported rather than handed to bash, which
# turned a syntax error into a passing case
subs = {
    '{{ (oe_rendered_dir ~ "/" ~ item.label) | quote }}': "'%s/rendered'" % T,
    "{{ item.mdir | quote }}": "'%s/unit'" % T,
    "{{ item.plist | quote }}": "'%s/u.plist'" % T,
    "{{ item.target | quote }}": "'%s'" % os.environ.get("OE_TEST_TARGET", "127.0.0.1:19092"),
    '{{ (playbook_dir ~ "/..") | quote }}': "'%s'" % os.getcwd(),
}
for k, v in subs.items():
    cmd = cmd.replace(k, v)
left = re.findall(r"\{\{[^}]*\}\}", cmd)
if left:
    print("UNRESOLVED:" + left[0]); raise SystemExit(0)
if brk == "break":
    old = 'put run-mirror.sh       "$M/run-mirror.sh"       0755'
    if old not in cmd:
        print("NO_PUT"); raise SystemExit(0)
    cmd = cmd.replace(old, 'put run-mirror.sh       "/nonexistent-dir/run-mirror.sh" 0755')
elif brk == "break-backup":
    # a failure DURING the backup phase, before anything has been written: restore must not delete the
    # intact old generation just because those files have no backup yet
    old = 'backup "$PL" plist.xml'
    if old not in cmd:
        print("NO_BACKUP_CALL"); raise SystemExit(0)
    cmd = cmd.replace(old, 'cp /nonexistent-dir/x "$BK/plist.xml.part"')
elif brk == "break-mktemp":
    # the exact window the ORDER exists for: if the mkdir ran before the backup directory and the
    # trap, a mktemp failure exited with a new empty unit directory left behind and no trap to undo it
    old = 'BK=$(mktemp -d)'
    if old not in cmd:
        print("NO_MKTEMP"); raise SystemExit(0)
    cmd = cmd.replace(old, 'BK=$(false)')
elif brk == "break-early":
    # the window between the mkdir and the first move: with the trap set after the mkdir, a failure
    # here left a new empty unit directory behind
    old = 'backup "$M/$f" "$f"'
    if old not in cmd:
        print("NO_BACKUP"); raise SystemExit(0)
    cmd = cmd.replace(old, 'backup "$M/$f" "$f"; false')
pathlib.Path(T + "/install.sh").write_text(cmd)
print("OK")
PY
}
seed() { # dir
  rm -rf "$1"; mkdir -p "$1/unit" "$1/rendered"
  for f in consumer.properties producer.properties log4j.properties run-mirror.sh; do
    echo "OLD $f" > "$1/unit/$f"; echo "NEW $f" > "$1/rendered/$f"
  done
  chmod +x "$1/unit/run-mirror.sh"
  echo "OLD plist" > "$1/u.plist"; echo "NEW plist" > "$1/rendered/plist.xml"
}
generation() { # dir -> OLD | NEW | MIXED
  local seen="" g
  for f in unit/consumer.properties unit/producer.properties unit/log4j.properties unit/run-mirror.sh u.plist; do
    g=$(cut -d' ' -f1 "$1/$f" 2>/dev/null || echo MISSING)
    case "$seen" in "") seen="$g" ;; *) [ "$seen" = "$g" ] || seen=MIXED ;; esac
  done
  echo "$seen"
}

fail=0
# ---- a clean install lands the new generation ----
D="$WORK/ok"; seed "$D"
r=$(render "$D"); [ "$r" = OK ] || { echo "FAIL: cannot render the install task body ($r)"; exit 1; }
set +e; out=$(bash "$D/install.sh" 2>&1); rc=$?; set -e
g=$(generation "$D")
if [ "$rc" = 0 ] && [ "$g" = NEW ]; then printf '  ok   %-44s rc=%s generation=%s\n' "a clean install" "$rc" "$g"
else printf '  FAIL %-44s rc=%s generation=%s %s\n' "a clean install" "$rc" "$g" "$out"; fail=1; fi

# ---- a failure at the FOURTH move restores the previous generation, whole ----
D="$WORK/broken"; seed "$D"
r=$(render "$D" break); [ "$r" = OK ] || { echo "FAIL: cannot render/patch the install task body ($r)"; exit 1; }
set +e; out=$(bash "$D/install.sh" 2>&1); rc=$?; set -e
g=$(generation "$D"); strays=$(ls -1 "$D/unit" | grep -c 'ansible-new' || true)
if [ "$rc" != 0 ] && [ "$g" = OLD ] && [ "$strays" = 0 ] && printf '%s' "$out" | grep -q 'put back exactly as it was'; then
  printf '  ok   %-44s rc=%s generation=%s strays=%s\n' "a failed move rolls back" "$rc" "$g" "$strays"
else
  printf '  FAIL %-44s rc=%s generation=%s strays=%s out=%s\n' "a failed move rolls back" "$rc" "$g" "$strays" "$out"; fail=1
fi

# ---- a FIRST install that fails must leave NOTHING behind ----
# Restoring by moving back only the files that EXISTED would leave the ones just written: a partial
# new generation, on a unit that is down, with nothing naming it.
D="$WORK/first"; seed "$D"
# nothing installed yet, and the unit DIRECTORY does not exist either: leaving an empty one behind is
# not "exactly as it was", and a test that pre-creates it cannot see that
rm -rf "$D/unit" "$D/u.plist"
r=$(render "$D" break); [ "$r" = OK ] || { echo "FAIL: cannot render/patch the install task body ($r)"; exit 1; }
set +e; out=$(bash "$D/install.sh" 2>&1); rc=$?; set -e
# counted only when the directory exists: under `set -o pipefail` a failing ls makes the whole
# assignment non-zero, which with `set -e` ended this test silently before it could judge anything
left=0; [ -d "$D/unit" ] && left=$(ls -1 "$D/unit" | wc -l | tr -d ' ')
plist_left=$([ -e "$D/u.plist" ] && echo 1 || echo 0)
dir_left=$([ -d "$D/unit" ] && echo 1 || echo 0)
if [ "$rc" != 0 ] && [ "$left" = 0 ] && [ "$plist_left" = 0 ] && [ "$dir_left" = 0 ]; then
  printf '  ok   %-44s rc=%s files=%s plist=%s dir=%s\n' "a failed FIRST install leaves nothing" "$rc" "$left" "$plist_left" "$dir_left"
else
  printf '  FAIL %-44s rc=%s files=%s plist=%s dir=%s out=%s\n' "a failed FIRST install leaves nothing" "$rc" "$left" "$plist_left" "$dir_left" "$out"; fail=1
fi

# ---- a failure during the BACKUP phase must not cost the old generation ----
D="$WORK/backup"; seed "$D"
r=$(render "$D" break-backup); [ "$r" = OK ] || { echo "FAIL: cannot render/patch the install task body ($r)"; exit 1; }
set +e; out=$(bash "$D/install.sh" 2>&1); rc=$?; set -e
g=$(generation "$D")
if [ "$rc" != 0 ] && [ "$g" = OLD ]; then printf '  ok   %-44s rc=%s generation=%s\n' "a backup-phase failure keeps the old files" "$rc" "$g"
else printf '  FAIL %-44s rc=%s generation=%s out=%s\n' "a backup-phase failure keeps the old files" "$rc" "$g" "$out"; fail=1; fi

# ---- a mktemp failure must not leave a unit directory behind ----
D="$WORK/mktemp"; seed "$D"; rm -rf "$D/unit" "$D/u.plist"
r=$(render "$D" break-mktemp); [ "$r" = OK ] || { echo "FAIL: cannot render/patch the install task body ($r)"; exit 1; }
set +e; out=$(bash "$D/install.sh" 2>&1); rc=$?; set -e
dir_left=$([ -d "$D/unit" ] && echo 1 || echo 0)
if [ "$rc" != 0 ] && [ "$dir_left" = 0 ]; then
  printf '  ok   %-44s rc=%s dir=%s\n' "a mktemp failure leaves no directory" "$rc" "$dir_left"
else
  printf '  FAIL %-44s rc=%s dir=%s out=%s\n' "a mktemp failure leaves no directory" "$rc" "$dir_left" "$out"; fail=1
fi

# ---- a failure in the window right after the mkdir also leaves nothing ----
D="$WORK/early"; seed "$D"; rm -rf "$D/unit" "$D/u.plist"
r=$(render "$D" break-early); [ "$r" = OK ] || { echo "FAIL: cannot render/patch the install task body ($r)"; exit 1; }
set +e; out=$(bash "$D/install.sh" 2>&1); rc=$?; set -e
dir_left=$([ -d "$D/unit" ] && echo 1 || echo 0)
if [ "$rc" != 0 ] && [ "$dir_left" = 0 ]; then
  printf '  ok   %-44s rc=%s dir=%s\n' "a failure right after the mkdir" "$rc" "$dir_left"
else
  printf '  FAIL %-44s rc=%s dir=%s out=%s\n' "a failure right after the mkdir" "$rc" "$dir_left" "$out"; fail=1
fi

# ---- the WRITE path carries the production lock itself ----
# The task's conditions are Ansible variables, and an extra var outranks every var, fact and register:
# `-e oe_stop='{"rc":0}'` makes them true however the stop ended. So the body re-runs the
# permitted-commit guard for a production target — from this checkout it must refuse, writing nothing.
D="$WORK/prodlock"; seed "$D"
r=$(OE_TEST_TARGET=192.168.100.252:9092 render "$D"); [ "$r" = OK ] || { echo "FAIL: cannot render the install task body ($r)"; exit 1; }
set +e; out=$(bash "$D/install.sh" 2>&1); rc=$?; set -e
g=$(generation "$D")
if [ "$rc" != 0 ] && [ "$g" = OLD ] && printf '%s' "$out" | grep -q 'REFUSED this PRODUCTION unit'; then
  printf '  ok   %-44s rc=%s generation=%s\n' "the write path refuses an unpermitted prod" "$rc" "$g"
else
  printf '  FAIL %-44s rc=%s generation=%s out=%s\n' "the write path refuses an unpermitted prod" "$rc" "$g" "$out"; fail=1
fi

[ "$fail" = 0 ] || { echo "mirror install rollback: FAILED"; exit 1; }
echo "mirror install rollback: a clean install lands the new generation, a failed move restores the previous one whole, a failed FIRST install leaves nothing behind, a backup-phase failure keeps the old generation, and so does a failure in the window right after the mkdir"
