#!/usr/bin/env bash
# Own ephemeral source-test browsers only. No service changes or broker connection.
set -euo pipefail
cd "$(dirname "$0")/../.."
[ "$(uname -s)" = Linux ] || exit 1
hostname -I | tr ' ' '\n' | grep -Fxq '192.168.100.4' || exit 1
sha256sum --check manifest.sha256 > /dev/null
export RESPONSIVE_BENCH_HOST=es4 VISUAL_BENCH_HOST=es4
export NODE_PATH="$PWD/node_modules"
mkdir -p results
outside_rth(){
  node - <<'JS'
const p=Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',weekday:'short',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date()).map(x=>[x.type,x.value]));const minute=+p.hour*60+(+p.minute);if(!['Sat','Sun'].includes(p.weekday)&&minute>=570&&minute<960)throw Error('OUTSIDE_RTH_REQUIRED');
JS
}
outside_rth
python3 -m unittest discover -s tests -p test_responsive_supervisor.py -v > results/supervisor-functional.log 2>&1
uname -a > results/host.txt
node --version >> results/host.txt
/usr/bin/google-chrome --version >> results/host.txt
failed=0
for repeat in 1 2 3 4 5; do
  outside_rth
  taskset -c 2 node scripts/validation/linked-exits-browser-probe.cjs "$PWD/web" > "results/legacy-$repeat.log" 2>&1 || failed=1
  for count in 390 1439; do
    for variant in baseline candidate; do
      outside_rth
      taskset -c 2 node scripts/validation/responsive-stack-probe.cjs "$PWD/web/$variant" "$PWD/web/candidate" "$variant" "$count" "$repeat" > "results/stack-$variant-$count-$repeat.log" 2>&1 || failed=1
    done
  done
done
python3 scripts/validation/qualify-responsive-stack.py results "$failed"
