#!/usr/bin/env bash
# ansible/templates/mirror-diff.sh.j2 decides whether installing a mirror unit would change anything,
# and therefore whether a LIVE mirror is reloaded. Both wrong answers are expensive: "in sync" when a
# property changed leaves the old settings running, and "would install" for a differing comment opens
# a gap in fourteen healthy mirrors. It is also easy to break silently — an apostrophe inside its
# single-quoted awk program closed the program and made every unit read "unparseable", which only a
# human reading a plan noticed. So it is driven here, per case, with stubbed launchctl/ps/date.
set -euo pipefail
cd "$(dirname "$0")/../.."
TPL=ansible/templates/mirror-diff.sh.j2
[ -r "$TPL" ] || { echo "FAIL: $TPL not readable"; exit 1; }

WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
R="$WORK/rendered"; M="$WORK/live"; PL="$WORK/unit.plist"; LBL=com.optionsedge.test-mirror
mkdir -p "$R" "$M" "$WORK/bin"
# the template shell-QUOTES what it renders, so these patterns carry the quote filter too
sed -E -e "s#\{\{ \(oe_rendered_dir ~ \"/\" ~ item\.label\) \| quote \}\}#'$R'#g" \
       -e "s#\{\{ item\.mdir \| quote \}\}#'$M'#g" \
       -e "s#\{\{ item\.plist \| quote \}\}#'$PL'#g" \
       -e "s#\{\{ item\.label \| quote \}\}#'$LBL'#g" "$TPL" > "$WORK/diff.sh"
grep -q '{{' "$WORK/diff.sh" && { echo "FAIL: the rendered comparison still has unresolved expressions — this test's substitution list is stale"; exit 1; }

# stubs: a live pid whose start time is controlled by $WORK/pid_epoch
cat > "$WORK/bin/launchctl" <<L
#!/usr/bin/env bash
[ -e "$WORK/launchctl_fails" ] && exit 1
[ -e "$WORK/no_process" ] && exit 0
echo "4242 0 $LBL"
L
cat > "$WORK/bin/ps" <<P
#!/usr/bin/env bash
echo "Mon Jan  1 00:00:00 2030"
P
cat > "$WORK/bin/date" <<D
#!/usr/bin/env bash
# only the guard's form: date -j -f <fmt> <stamp> +%s
cat "$WORK/pid_epoch"
D
# a GNU-only stat: `stat -f %m` fails, `stat -c %Y` works. The units run on this Mac, but this test
# runs on a Linux runner too, and a silently-failing BSD form made every process look NEWER than its
# files — the staleness case could not fail there however broken the script was.
cat > "$WORK/bin/stat" <<T
#!/usr/bin/env bash
[ -e "$WORK/gnu_stat" ] || exec /usr/bin/stat "\$@"
case "\$1" in
  -f) exit 1 ;;                                  # the BSD form is what GNU coreutils rejects
  -c) # answer the GNU form with GNU semantics, computed rather than delegated to the host's BSD stat
      exec python3 -c 'import os,sys; print(int(os.stat(sys.argv[1]).st_mtime))' "\$3" ;;
esac
exit 1
T
chmod +x "$WORK/bin"/*
export PATH="$WORK/bin:$PATH"

seed() { # write a healthy, identical pair
  rm -f "$WORK/no_process"
  printf 'bootstrap.servers=a:1\ngroup.id=g\nauto.offset.reset=earliest\n' > "$R/consumer.properties"
  printf '# a rendered comment\nbootstrap.servers=a:1\ngroup.id=g\nauto.offset.reset=earliest\n' > "$M/consumer.properties"
  printf 'bootstrap.servers=b:2\nacks=all\nenable.idempotence=true\n' > "$R/producer.properties"
  printf 'bootstrap.servers=b:2\nacks=all\nenable.idempotence=true\n' > "$M/producer.properties"
  printf 'log4j.rootLogger=INFO, stdout\n' > "$R/log4j.properties"; cp "$R/log4j.properties" "$M/log4j.properties"
  printf '#!/usr/bin/env bash\nexec mirror\n' > "$R/run-mirror.sh"; cp "$R/run-mirror.sh" "$M/run-mirror.sh"; chmod +x "$M/run-mirror.sh"
  printf '<plist/>\n' > "$R/plist.xml"; cp "$R/plist.xml" "$PL"
  echo 9999999999 > "$WORK/pid_epoch"     # the process is newer than every file
}

fail=0
case_is() { # name  expected-substring  setup...
  local name="$1" want="$2"; shift 2
  seed; rm -f "$WORK/gnu_stat" "$WORK/launchctl_fails"; "$@"
  set +e; out=$(bash "$WORK/diff.sh" 2>&1); rc=$?; set -e
  if [ "$rc" = 0 ] && printf '%s' "$out" | grep -q -- "$want"; then
    printf '  ok   %-50s %s\n' "$name" "$(printf '%s' "$out" | cut -c1-74)"
  else
    printf '  FAIL %-50s rc=%s want=%s got=%s\n' "$name" "$rc" "$want" "$out"; fail=1
  fi
}

case_is "identical files, fresh process" "in sync" true
case_is "a COMMENT-only difference is not a change" "in sync" true
# the live files carry paragraphs of prose, and some of it contains a BACKSLASH. Refusing the
# property parse for that put every unit on a byte comparison — which differs by those same comments,
# i.e. it would have reloaded all fourteen live mirrors to change nothing.
case_is "a BACKSLASH in a COMMENT is still in sync" "in sync" \
  bash -c "printf '# a comment with a backslash: use \\\\. to escape a dot\nbootstrap.servers=a:1\ngroup.id=g\nauto.offset.reset=earliest\n' > '$M/consumer.properties'"

case_is "a property VALUE differs" "consumer.properties=properties-differ" \
  bash -c "printf 'bootstrap.servers=a:1\ngroup.id=OTHER\nauto.offset.reset=earliest\n' > '$M/consumer.properties'"
case_is "a trailing space on a VALUE differs" "producer.properties=properties-differ" \
  bash -c "printf 'bootstrap.servers=b:2\nacks=all \nenable.idempotence=true\n' > '$M/producer.properties'"
case_is "acks=1 under a different spelling differs" "producer.properties=properties-differ" \
  bash -c "printf 'bootstrap.servers=b:2\nacks : 1\nenable.idempotence=true\n' > '$M/producer.properties'"
case_is "a DUPLICATE key falls back to bytes" "producer.properties=bytes-differ(unparseable-as-properties)" \
  bash -c "printf 'bootstrap.servers=b:2\nacks=all\nacks=1\nenable.idempotence=true\n' > '$M/producer.properties'"
case_is "a BACKSLASH in a property line falls back to bytes" "producer.properties=bytes-differ(unparseable-as-properties)" \
  bash -c "printf 'bootstrap.servers=b:2\nacks=al\\\\\n  l\nenable.idempotence=true\n' > '$M/producer.properties'"
case_is "log4j bytes differ" "log4j.properties=bytes-differ" \
  bash -c "printf 'log4j.rootLogger=DEBUG, stdout\n' > '$M/log4j.properties'"
case_is "the runner lost its executable bit" "run-mirror.sh=not-executable" \
  chmod 644 "$M/run-mirror.sh"
case_is "the plist differs" "plist=bytes-differ" \
  bash -c "printf '<plist>other</plist>\n' > '$PL'"
case_is "a missing live file" "consumer.properties=absent" rm -f "$M/consumer.properties"
case_is "no process at all" "process=absent" touch "$WORK/no_process"

# an unreadable JOB TABLE is a different answer from "no process", and acting on it would be acting on
# a state never read — the comparison fails closed, so the unit is reported and not touched
case_fails() { # name  expected-substring  setup...
  local name="$1" want="$2"; shift 2
  seed; rm -f "$WORK/gnu_stat" "$WORK/launchctl_fails"; "$@"
  set +e; out=$(bash "$WORK/diff.sh" 2>&1); rc=$?; set -e
  if [ "$rc" != 0 ] && printf '%s' "$out" | grep -q -- "$want"; then
    printf '  ok   %-50s rc=%s %s\n' "$name" "$rc" "$(printf '%s' "$out" | tail -1 | cut -c1-62)"
  else
    printf '  FAIL %-50s rc=%s want=%s got=%s\n' "$name" "$rc" "$want" "$out"; fail=1
  fi
}
case_fails "an unreadable job table fails the comparison" "cannot read launchctl list" \
  bash -c ": > '$WORK/launchctl_fails'"
case_is "the process is OLDER than its config" "process=older-than-its-config" \
  bash -c "echo 1 > '$WORK/pid_epoch'"

# the same case where only the GNU stat form works, which is what CI has
case_is "older than its config, with a GNU-only stat" "process=older-than-its-config" \
  bash -c "echo 1 > '$WORK/pid_epoch'; : > '$WORK/gnu_stat'"

[ "$fail" = 0 ] || { echo "mirror diff guard: FAILED"; exit 1; }
echo "mirror diff guard: 16 cases behave as specified"
