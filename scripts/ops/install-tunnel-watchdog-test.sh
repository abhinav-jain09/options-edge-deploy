#!/usr/bin/env bash
# Exercises the real Ansible installer against a throwaway local directory, never production.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
TEST_DIR="$(mktemp -d)"
trap 'rm -rf "$TEST_DIR"' EXIT
live="$TEST_DIR/oe-tunnel-watchdog.sh"
record="$TEST_DIR/.oe-tunnel-watchdog.sha256"
printf '#!/usr/bin/env bash\nexit 0\n' > "$live"
initial_sha="$(sha256sum "$live" | awk '{print $1}')"
desired_sha="$(sha256sum "$HERE/oe-tunnel-watchdog.sh" | awk '{print $1}')"
extra="watchdog_path=$live record_path=$record watchdog_owner=$(id -un) watchdog_group=$(id -gn) first_adoption_sha=$initial_sha"

ansible-playbook -i 'localhost,' -c local -e "$extra" "$HERE/install-tunnel-watchdog.yml" > "$TEST_DIR/install.log"
[ "$(sha256sum "$live" | awk '{print $1}')" = "$desired_sha" ]
[ "$(tr -d '\n' < "$record")" = "$desired_sha" ]

ansible-playbook -i 'localhost,' -c local -e "$extra" "$HERE/install-tunnel-watchdog.yml" > "$TEST_DIR/repeat.log"
[ "$(sha256sum "$live" | awk '{print $1}')" = "$desired_sha" ]

printf '# tampered outside Jenkins\n' >> "$live"
tampered_sha="$(sha256sum "$live" | awk '{print $1}')"
if ansible-playbook -i 'localhost,' -c local -e "$extra" "$HERE/install-tunnel-watchdog.yml" > "$TEST_DIR/drift.log" 2>&1; then
  echo 'FAIL: installer accepted host drift' >&2
  exit 1
fi
grep -q 'Watchdog host drift' "$TEST_DIR/drift.log"
[ "$(sha256sum "$live" | awk '{print $1}')" = "$tampered_sha" ]
[ "$(tr -d '\n' < "$record")" = "$desired_sha" ]
echo 'install-tunnel-watchdog-test: PASS'
