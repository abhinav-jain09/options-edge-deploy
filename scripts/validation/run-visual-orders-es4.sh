#!/usr/bin/env bash
# Isolated source validation only. No runtime image publication, cluster operation, Kafka or IBKR access.
set -euo pipefail
set +x
cd "$(dirname "$0")/../.."
[ "$(uname -s)" = Linux ] || { echo 'Validation probes must run on es4/Linux.' >&2; exit 1; }
hostname -I | tr ' ' '\n' | grep -Fxq '192.168.100.4' || { echo 'Pinned es4 host address required.' >&2; exit 1; }
export VISUAL_BENCH_HOST=es4
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
# Hard date/time gate before starting even the isolated test database.
node - <<'JS'
const p=Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',weekday:'short',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date()).map(x=>[x.type,x.value]));
const t=Number(p.hour)*60+Number(p.minute);if(!['Sat','Sun'].includes(p.weekday)&&t>=570&&t<960)throw Error('Outside RTH required');
JS
mkdir -p results
java -version 2>results/java.txt
uname -a >results/host.txt
getconf _NPROCESSORS_ONLN >>results/host.txt
secret_file="$(mktemp)"; pg_id=''
cleanup(){ if [ -n "$pg_id" ]; then sudo -n docker rm -f "$pg_id" >/dev/null 2>&1 || true; fi; rm -f "$secret_file"; }
trap cleanup EXIT
umask 077
python3 - "$secret_file" <<'PY'
import pathlib,secrets,sys
pathlib.Path(sys.argv[1]).write_text('POSTGRES_USER=validation\nPOSTGRES_DB=visual_validation_0\nPOSTGRES_PASSWORD='+secrets.token_urlsafe(36)+'\n')
PY
pg_id="$(sudo -n docker run -d --rm --cpus=1 --cpuset-cpus=3 --memory=512m --tmpfs /var/lib/postgresql/data:rw,size=256m --env-file "$secret_file" -p 127.0.0.1::5432 postgres:16)"
for i in $(seq 1 30); do
  if sudo -n docker exec "$pg_id" pg_isready -U validation >/dev/null 2>&1; then break; fi
  [ "$i" != 30 ] || { echo 'Isolated validation database did not become ready.' >&2; exit 1; }
  sleep 1
done
port="$(sudo -n docker port "$pg_id" 5432/tcp | sed -n 's/^127.0.0.1://p')"
[[ "$port" =~ ^[2-6][0-9]{4}$ ]] || { echo 'Unexpected isolated test port.' >&2; exit 1; }
export VISUAL_BENCH_PG_PASSWORD="$(sed -n 's/^POSTGRES_PASSWORD=//p' "$secret_file")"
for i in $(seq 1 5); do
  for variant in baseline candidate; do
    taskset -c 2 java -Xms128m -Xmx512m -XX:ActiveProcessorCount=1 -XX:+UseSerialGC -cp "candidate/test-classes:$variant/classes:lib/*" com.optionsedge.processing.brokerexec.NativeCallbackPerformanceProbe >"results/native-$variant-$i.log" 2>&1
  done
  taskset -c 2 java -Xms128m -Xmx512m -XX:ActiveProcessorCount=1 -XX:+UseSerialGC -cp 'candidate/test-classes:candidate/classes:lib/*' com.optionsedge.processing.brokerexec.VisualOrderPerformanceProbe quotes >"results/quotes-$i.log" 2>&1
  database="visual_validation_$i"
  sudo -n docker exec "$pg_id" psql -U validation -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $database" >/dev/null
  export VISUAL_BENCH_JDBC_URL="jdbc:postgresql://127.0.0.1:$port/$database"
  taskset -c 2 java -Xms128m -Xmx512m -XX:ActiveProcessorCount=1 -XX:+UseSerialGC -cp 'candidate/test-classes:candidate/classes:lib/*' com.optionsedge.processing.brokerexec.VisualOrderPerformanceProbe journal >"results/journal-$i.log" 2>&1
  taskset -c 2 node scripts/validation/visual-orders-model-probe.cjs "$PWD/web/context-tape-orders-model.js" >"results/model-$i.log" 2>&1
done
python3 - <<'PY'
import json,pathlib,statistics
p=pathlib.Path('results'); groups={}
for f in p.glob('*.log'):
 for line in f.read_text().splitlines():
  if not line.startswith('VISUAL_PROBE '): continue
  row=json.loads(line.removeprefix('VISUAL_PROBE '));key=row['probe']+('-baseline' if 'baseline' in f.name else '-candidate' if 'native-candidate' in f.name else '')
  groups.setdefault(key,[]).append(row)
out={'classification':'source-validation-only; not a deployment/replay release gate','samples':groups,'medians':{k:statistics.median(x.get('nsPerRecord',x.get('elapsedMs')) for x in v) for k,v in groups.items()}}
if 'native-callback-baseline' in groups and 'native-callback-candidate' in groups:
 out['nativeRelativeChange']=out['medians']['native-callback-candidate']/out['medians']['native-callback-baseline']-1
out['nativeRelativeBudgetPass']=out.get('nativeRelativeChange',float('inf'))<=0.10
out['allNetNewBudgetsPass']=all(x.get('withinBudget',True) for v in groups.values() for x in v)
(p/'summary.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out['medians']));print('allNetNewBudgetsPass',out['allNetNewBudgetsPass'])
assert set(groups)=={'native-callback-baseline','native-callback-candidate','quote-book','journal-postgres','model-geometry','model-history-page'},'Every declared probe must run'
assert all(len(v)==5 for v in groups.values()),'Five repeated commands required for each scenario'
assert out['allNetNewBudgetsPass'],'A declared net-new budget failed'
assert out['nativeRelativeBudgetPass'],'Native callback median regressed by more than 10 percent'
PY
