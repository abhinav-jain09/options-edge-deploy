#!/usr/bin/env bash
# do_clean must PIN the topics ref in ITS OWN shell before it reads either keep-list.
#
# The 2026-11-09 defect this pins: do_clean read the dev keep-list as DEV_KEEP="$(dev_clean_preserved_topics)",
# and that function was the only thing that ran topics_ref_pin. A command substitution is a subshell, so
# the TOPICS_ENV_SHA it set died with it; reset_preserved_from_main then ran in the parent with the
# variable empty and refused ("TOPICS_ENV_SHA is not set"). EVERY full clean refused the wipe. The earlier
# reset-preserved test never saw it: it pre-seeds TOPICS_ENV_SHA by hand and calls the function directly.
#
# So this test runs the REAL do_clean (cut out of dev-cleanup.sh by name) against a REAL git repository
# that has a REAL remote, with only the destructive and environment-bound steps stubbed:
#   clean_guard / require_kafka_cli -> succeed;  clean_logs -> prints a marker and exits the shell, i.e.
#   the point at which do_clean has resolved both keep-lists and is about to scale anything down.
# Nothing here touches a broker or a cluster.
#
# Section 2 mutates the script (removes the pin from do_clean) and requires THIS FILE to fail.
set -o pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${DEV_CLEANUP_UNDER_TEST:-$HERE/dev-cleanup.sh}"
RPT_REAL="$HERE/../kafka/reset-preserved-topics.sh"
fails=0
ok()  { printf '  ok   %s\n' "$1"; }
bad() { printf '  FAIL %s\n' "$1"; fails=$((fails+1)); }
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
for f in "$SRC" "$RPT_REAL"; do
  [ -f "$f" ] || { echo "FAIL: $f missing" >&2; exit 1; }
done

# A bare "origin" and a clone of it, so topics_ref_pin's `git fetch origin main` is a real fetch.
ORIGIN="$WORK/origin.git"; REPO="$WORK/repo"
git init -q --bare -b main "$ORIGIN" || { echo "FAIL: cannot build the fixture origin" >&2; exit 1; }
git clone -q "$ORIGIN" "$REPO" 2>/dev/null
mkdir -p "$REPO/scripts/kafka"
cp "$RPT_REAL" "$REPO/scripts/kafka/reset-preserved-topics.sh"
cat > "$REPO/scripts/kafka/topics.env" <<'EOF'
OPTIONS_EDGE_RESET_PRESERVED_TOPICS="t.rp"
OPTIONS_EDGE_PROD_ONLY_RESET_PRESERVED_TOPICS="t.prodkeep"
OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS="t.devkeep"
EOF
( cd "$REPO" && git checkout -q -b main 2>/dev/null; git -c user.email=t@t -c user.name=t add -A \
  && git -c user.email=t@t -c user.name=t commit -qm c && git push -q origin main ) >/dev/null 2>&1 \
  || { echo "FAIL: could not build the fixture repository" >&2; exit 1; }

# Extract by name, from the file under test.
fn() { awk -v n="$1" '$0 ~ "^"n"\\(\\) \\{" {p=1} p {print} p && /^\}/ {exit}' "$2"; }
build() { # $1 = script to cut from -> $WORK/functions.sh
  : > "$WORK/functions.sh"
  local _f BODY
  for _f in topics_ref_pin dev_clean_preserved_topics reset_preserved_from_main do_clean; do
    BODY="$(fn "$_f" "$1")"
    [ -n "$BODY" ] || { echo "FAIL: function $_f not found in $1" >&2; exit 1; }
    printf '%s\n' "$BODY" >> "$WORK/functions.sh"
  done
}
est="$(grep -E '^DEV_ONLY_ESTIMATE_KEEP=' "$SRC" | head -1)"
[ -n "$est" ] || { echo "FAIL: DEV_ONLY_ESTIMATE_KEEP not found" >&2; exit 1; }

run_do_clean() { # -> stdout+stderr of do_clean up to the clean_logs marker
  env DEPLOY_REPO="$REPO" TOPICS_ENV_REF="origin/main:scripts/kafka/topics.env" WIPE_KAFKA=true \
      FUNCTIONS="$WORK/functions.sh" EST="$est" bash -c '
    TOPICS_ENV_SHA=""; TOPICS_ENV_PINNED_REF=""
    eval "$EST"
    . "$FUNCTIONS"
    clean_guard() { return 0; }
    require_kafka_cli() { return 0; }
    clean_logs() { echo "REACHED-CLEAN-LOGS sha=${TOPICS_ENV_SHA:-<empty>}"; exit 0; }
    do_clean; echo "do_clean-returned rc=$?"' 2>&1
}

echo "== 1. the real do_clean resolves both keep-lists (it used to refuse every time)"
build "$SRC"
got="$(run_do_clean)"
want="$(git -C "$REPO" rev-parse HEAD)"
case "$got" in
  *"REFUSING the wipe"*|*"TOPICS_ENV_SHA is not set"*) bad "do_clean REFUSED a resolvable keep-list: [$got]" ;;
  *"REACHED-CLEAN-LOGS sha=$want"*) ok "do_clean reaches clean_logs with TOPICS_ENV_SHA = the fixture's commit" ;;
  *) bad "do_clean did not reach clean_logs with the pinned commit: [$got]" ;;
esac
case "$got" in *"keep-lists resolved at origin/main = $want"*) ok "the pinned commit is logged" ;; *) bad "no pinned-commit log line: [$got]" ;; esac
case "$got" in *"t.devkeep"*) ok "the dev keep-list is read" ;; *) bad "dev keep-list missing: [$got]" ;; esac
case "$got" in *"reset-preserved keep list: t.rp"*) ok "the reset-preserved keep-list is read" ;; *) bad "reset-preserved list missing: [$got]" ;; esac

echo "== 2. the pin is the thing under test: remove it from do_clean and this file must fail"
# Drop the pin block from do_clean (the topics_ref_pin call in do_clean's own shell) and the guard that
# dev_clean_preserved_topics uses to skip re-pinning, i.e. restore the pre-fix behaviour exactly.
python3 - "$SRC" "$WORK/mutant.sh" <<'EOF'
import re, sys
s = open(sys.argv[1]).read()
a = s.index("do_clean() {")
b = s.index("\n}\n", a)
body = s[a:b]
m = re.sub(r"    topics_ref_pin \|\| \{\n.*?return 1; \}\n", "", body, count=1, flags=re.S)
assert m != body, "mutation did not apply: the pin block was not found in do_clean"
s = s[:a] + m + s[b:]
s = s.replace('[ -n "$TOPICS_ENV_SHA" ] || topics_ref_pin || return 1', 'topics_ref_pin || return 1')
open(sys.argv[2], "w").write(s)
EOF
if [ -s "$WORK/mutant.sh" ]; then
  build "$WORK/mutant.sh"
  mut="$(run_do_clean)"
  case "$mut" in
    *"TOPICS_ENV_SHA is not set"*|*"REFUSING the wipe"*) ok "the pre-fix do_clean REFUSES (the test can fail)" ;;
    *) bad "the mutant (no pin in do_clean) was not caught: [$mut]" ;;
  esac
else
  bad "mutation script produced nothing"
fi

echo
[ "$fails" -eq 0 ] && { echo "ALL OK"; exit 0; }
echo "$fails FAILED"; exit 1
