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
probe_failed=0
for i in $(seq 1 5); do
  for variant in baseline candidate; do
    taskset -c 2 java -Xms128m -Xmx512m -XX:ActiveProcessorCount=1 -XX:+UseSerialGC -cp "candidate/test-classes:$variant/classes:lib/*" com.optionsedge.processing.brokerexec.NativeCallbackPerformanceProbe >"results/native-$variant-$i.log" 2>&1 || probe_failed=1
  done
  taskset -c 2 java -Xms128m -Xmx512m -XX:ActiveProcessorCount=1 -XX:+UseSerialGC -cp 'candidate/test-classes:candidate/classes:lib/*' com.optionsedge.processing.brokerexec.NativeQueuePerformanceProbe >"results/native-queue-$i.log" 2>&1 || probe_failed=1
  taskset -c 2 java -Xms128m -Xmx512m -XX:ActiveProcessorCount=1 -XX:+UseSerialGC -cp 'candidate/test-classes:candidate/classes:lib/*' com.optionsedge.processing.brokerexec.VisualOrderPerformanceProbe quotes >"results/quotes-$i.log" 2>&1 || probe_failed=1
  for variant in baseline candidate; do
    suffix=0; [ "$variant" = baseline ] || suffix=1
    database="visual_validation_${i}_${suffix}"
    sudo -n docker exec "$pg_id" psql -U validation -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $database" >/dev/null
    VISUAL_BENCH_JDBC_URL="jdbc:postgresql://127.0.0.1:$port/$database" taskset -c 2 java -Xms128m -Xmx512m -XX:ActiveProcessorCount=1 -XX:+UseSerialGC -cp "candidate/test-classes:$variant/classes:lib/*" com.optionsedge.processing.brokerexec.VisualOrderPerformanceProbe journal >"results/journal-$variant-$i.log" 2>&1 || probe_failed=1
  done
  taskset -c 2 java -Xms128m -Xmx512m -XX:ActiveProcessorCount=1 -XX:+UseSerialGC -cp 'candidate/test-classes:candidate/classes:lib/*' com.optionsedge.processing.brokerexec.VisualLinkedExitPerformanceProbe >"results/linked-evaluator-$i.log" 2>&1 || probe_failed=1
  linked_database="visual_validation_${i}_2"
  sudo -n docker exec "$pg_id" psql -U validation -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $linked_database" >/dev/null
  VISUAL_BENCH_JDBC_URL="jdbc:postgresql://127.0.0.1:$port/$linked_database" taskset -c 2 java -Xms128m -Xmx512m -XX:ActiveProcessorCount=1 -XX:+UseSerialGC -cp 'candidate/test-classes:candidate/classes:lib/*' com.optionsedge.processing.brokerexec.VisualLinkedExitPostgresProbe >"results/linked-postgres-$i.log" 2>&1 || probe_failed=1
  taskset -c 2 node scripts/validation/linked-exits-browser-probe.cjs "$PWD/web" >"results/linked-browser-$i.log" 2>&1 || probe_failed=1
  taskset -c 2 node scripts/validation/visual-orders-model-probe.cjs "$PWD/web/context-tape-orders-model.js" >"results/model-$i.log" 2>&1 || probe_failed=1
done
export VISUAL_VALIDATION_COMMANDS_FAILED="$probe_failed"
python3 - <<'PY'
import json,math,os,pathlib,statistics
p=pathlib.Path('results'); groups={}
for f in p.glob('*.log'):
 for line in f.read_text().splitlines():
  if not line.startswith('VISUAL_PROBE '): continue
  row=json.loads(line.removeprefix('VISUAL_PROBE '));key=row['probe']+('-'+row['variant'] if 'variant' in row else '-baseline' if 'baseline' in f.name else '-candidate' if 'candidate' in f.name else '')
  groups.setdefault(key,[]).append(row)
out={'allCommandsPass':os.environ['VISUAL_VALIDATION_COMMANDS_FAILED']=='0','classification':'source-validation-only; not a deployment/replay release gate','samples':groups,'medians':{k:statistics.median(x.get('nsPerRecord',x.get('elapsedMs')) for x in v) for k,v in groups.items() if all('nsPerRecord' in x or 'elapsedMs' in x for x in v)}}
if 'native-callback-baseline' in groups and 'native-callback-candidate' in groups:
 out['nativeRelativeChange']=out['medians']['native-callback-candidate']/out['medians']['native-callback-baseline']-1
out['nativeRelativeBudgetPass']=out.get('nativeRelativeChange',float('inf'))<=0.10
# Owner accepted this specific median wrapper budget on 17 October 2026.
# Keep the relative diagnostic; all other probe gates are unchanged.
out['nativeWrapperBudgetNs']=1000
out['nativeWrapperBudgetPass']=0<=out['medians'].get('native-callback-candidate',float('inf'))<=out['nativeWrapperBudgetNs']
budgets={'native-adapter-queue':('nsPerRecord','budgetNsPerRecord',10000),'quote-book':('nsPerRecord','budgetNsPerRecord',500000),'journal-postgres-candidate':('nsPerRecord','budgetNsPerRecord',20000000),'model-geometry':('nsPerRecord','budgetNsPerRecord',500000),'model-history-page':('elapsedMs','budgetMs',100)}
out['allNetNewBudgetsPass']=all(x.get('withinBudget') is True and type(x.get(metric)) in (int,float) and math.isfinite(x[metric]) and 0<=x[metric]<=limit and type(x.get(budget)) is int and x[budget]==limit for name,(metric,budget,limit) in budgets.items() for x in groups.get(name,[]))
(p/'summary.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out['medians']));print('allNetNewBudgetsPass',out['allNetNewBudgetsPass'])
linked={'linked-evaluator-risk','linked-postgres-races','existing-overlay-baseline','existing-overlay-candidate','linked-overlay-candidate'}
assert all(x.get('withinBudget') is True for k in linked for x in groups.get(k,[])), 'Linked evaluator, actual PostgreSQL or browser budget/semantic failure'
def bounded(row,key,limit):
 return type(row.get(key)) in (int,float) and math.isfinite(row[key]) and 0<=row[key]<=limit
for row in groups.get('linked-evaluator-risk',[]):
 assert row.get('records')==100000 and row.get('budgetP99Ns')==5000000 and bounded(row,'sampleP99Ns',5000000), 'Pinned evaluator latency gate failed'
 assert row.get('allocationBudget')==4096 and bounded(row,'allocatedBytesPerRecord',4096), 'Pinned allocation gate failed'
 assert row.get('retainedBudgetBytes')==8388608 and bounded(row,'retainedUpperBytes',8388608), 'Pinned retained-state gate failed'
for key in ['existing-overlay-baseline','existing-overlay-candidate','linked-overlay-candidate']:
 for row in groups.get(key,[]):
  assert row.get('fixedBindings') is True and row.get('viewportUpdates')==250 and row.get('workload')=='fixed-trade-viewport-pan-and-latched-stop', 'Valid immutable contract bindings/changed viewport workload missing'
  assert row.get('sameCandle') is True and row.get('frameScheduler')=='native-requestAnimationFrame' and row.get('nativeFrames')==250 and row.get('distinctFrameTimestamps') is True, 'Actual distinct animation frames/candle preservation missing'
  assert row.get('records')==200 and row.get('budgetMs')==16 and bounded(row,'sampleP99Ms',16) and row.get('shades')==16, 'Pinned browser latency/area-count gate failed'
  if key=='linked-overlay-candidate':assert row.get('stateTransitions')==1 and bounded(row,'transitionMaxMs',float('inf')) and row.get('threads')==32 and row.get('labels')==32, 'Linked geometry is missing'
for row in groups.get('linked-postgres-races',[]):
 assert row.get('records')==4 and all(row.get(k) is True for k in ['singleTrigger','singleReservation','otherLotPreserved','independentStoreLock']), 'Required PostgreSQL race assertion missing'
out['journalRelativeChange']=out['medians'].get('journal-postgres-candidate',float('inf'))/out['medians'].get('journal-postgres-baseline',1)-1
assert out['journalRelativeChange']<=0.10, 'Existing journal median regressed more than ten percent'
out['overlayRelativeChange']=out['medians'].get('existing-overlay-candidate',float('inf'))/out['medians'].get('existing-overlay-baseline',1)-1
assert out['overlayRelativeChange']<=0.10, 'Existing overlay median regressed more than ten percent'
(p/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
assert set(groups)==linked|{'native-callback-baseline','native-callback-candidate','native-adapter-queue','quote-book','journal-postgres-baseline','journal-postgres-candidate','model-geometry','model-history-page'},'Every declared probe must run'
assert all(len(v)==5 for v in groups.values()),'Five repeated commands required for each scenario'
assert out['allNetNewBudgetsPass'],'A declared net-new budget failed'
assert out['allCommandsPass'],'A probe command failed; later samples cannot qualify it'
assert out['nativeWrapperBudgetPass'],'Native callback wrapper median exceeded owner-approved 1000 ns budget or was negative'
PY
