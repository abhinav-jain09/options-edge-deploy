#!/usr/bin/env bash
# scripts/es4/create-es-topics.sh must PROVE its own reconciliation took.
#
# es4 is the one environment CI cannot reach: its declaration
# (OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES) and its broker exist only behind the shims on that
# box, so the es4 arm of validate-declared-overrides-are-explicit.sh is never exercised against a
# real broker anywhere else. This test runs create-es-topics.sh END TO END against a fake es4 Kafka
# and asserts the guard runs there, sees the es4 declaration, and fails the script when the
# reconciliation silently left a declared topic on the broker's default.
#
# It EXECUTES the script. A test that greps create-es-topics.sh for the guard's name would pass on a
# line that never runs — commented out, after an `exit`, or inside a branch this deploy never takes.
set -euo pipefail
cd "$(dirname "$0")/../.."
REPO=$PWD

WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/bin" "$WORK/state"

# The shims run `sudo -n docker exec -i <container> <cli> <args>`. Faking `sudo` intercepts every
# CLI call the real shims make, so the shims themselves stay under test rather than being bypassed.
#
# The shim dir must come OFF the PATH before the CLI is exec'd. create-es-topics.sh puts it first,
# so a fake sudo that simply exec'd `kafka-topics` would find the shim again — shim calls sudo calls
# shim, forever. Dropping it here is also what the real thing does in spirit: inside the container
# the CLI is the CLI, not a proxy back out.
cat > "$WORK/bin/sudo" <<'SUDO'
#!/usr/bin/env bash
while [ $# -gt 0 ]; do
  case "$1" in -n) shift;; docker) shift 4;; *) break;; esac   # docker exec -i <container>
done
PATH=$(printf '%s' "$PATH" | tr ':' '\n' | grep -v 'kafka-cli-shim' | paste -sd: -)
export PATH
exec "$@"
SUDO

# A fake es4 broker. State is two files: the topics that exist, and the topic-level retention each
# one carries. NO_ALTER names a topic whose `kafka-configs --alter` silently does nothing — which is
# what a topic the Kafka Topics stage never reached looks like from the outside.
cat > "$WORK/bin/kafka-topics" <<'KT'
#!/usr/bin/env bash
op=""; topic=""; parts=4; ret=""
while [ $# -gt 0 ]; do
  case "$1" in
    --create|--delete|--list|--describe|--alter) op="${1#--}"; shift;;
    --topic) topic=$2; shift 2;;
    --partitions) parts=$2; shift 2;;
    --config) case "$2" in retention.ms=*) ret=${2#retention.ms=};; esac; shift 2;;
    *) shift;;
  esac
done
# State is `topic partitions` per line: the broker must report back the shape it was CREATED with,
# or apply-topics.sh waits forever for a topic to reach a partition count the fake never grants.
S=$STATE/topics; C=$STATE/configs; touch "$S" "$C"
have=$(sed -n "s/^$topic //p" "$S" | head -1)
case "$op" in
  create) [ -n "$have" ] || echo "$topic $parts" >> "$S"
          [ -n "$ret" ] && { grep -v "^$topic=" "$C" > "$C.t" || true; mv "$C.t" "$C"; echo "$topic=$ret" >> "$C"; }
          echo "Created topic $topic.";;
  alter)  [ -n "$have" ] && { grep -v "^$topic " "$S" > "$S.t" || true; mv "$S.t" "$S"; echo "$topic $parts" >> "$S"; };;
  delete) grep -v "^$topic " "$S" > "$S.t" || true; mv "$S.t" "$S";;
  list)   if [ -n "$topic" ]; then [ -n "$have" ] && echo "$topic"; else sed 's/ .*//' "$S"; fi;;
  describe) [ -n "$have" ] && printf 'Topic: %s\tTopicId: fake\tPartitionCount: %s\tReplicationFactor: 1\tConfigs: \n' "$topic" "$have";;
esac
exit 0
KT

cat > "$WORK/bin/kafka-configs" <<'KC'
#!/usr/bin/env bash
op=""; topic=""; add=""
while [ $# -gt 0 ]; do
  case "$1" in
    --alter) op=alter; shift;;
    --describe) op=describe; shift;;
    --entity-name) topic=$2; shift 2;;
    --add-config) add=$2; shift 2;;
    *) shift;;
  esac
done
C=$STATE/configs; touch "$C"
case "$op" in
  alter)
    # The defect this whole guard exists for: the stage does not reach this topic, so its config is
    # never written and it keeps sitting on the broker's default.
    [ "$topic" = "${NO_ALTER:-}" ] && exit 0
    ret=$(printf '%s' "$add" | tr ',' '\n' | sed -n 's/^retention\.ms=//p' | head -1)
    [ -n "$ret" ] && { grep -v "^$topic=" "$C" > "$C.t" || true; mv "$C.t" "$C"; echo "$topic=$ret" >> "$C"; }
    ;;
  describe)
    echo "Dynamic configs for topic $topic are:"
    v=$(sed -n "s/^$topic=//p" "$C" | head -1)
    [ -n "$v" ] && echo "  retention.ms=$v sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=$v}"
    ;;
esac
exit 0
KC

printf '#!/usr/bin/env bash\necho "fake (id: 1 rack: null)"\n'   > "$WORK/bin/kafka-broker-api-versions"
printf '#!/usr/bin/env bash\nexit 0\n'                           > "$WORK/bin/kafka-consumer-groups"
chmod +x "$WORK/bin/"*

export STATE="$WORK/state"
export PATH="$WORK/bin:$PATH"

# The es4 topic whose reconciliation we sabotage. Taken from the es4 declaration itself, so this
# test cannot drift into naming a topic es4 does not declare.
VICTIM=$(sed -n 's/^OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES="\(.*\)"$/\1/p' scripts/kafka/topics.env \
         | tr ' ' '\n' | sed -n 's/=.*//p' | grep -v '^\$' | head -1)
[ -n "$VICTIM" ] || { echo "could not read a topic from OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES" >&2; exit 1; }
echo "es4 declaration under test, victim topic: $VICTIM"

reset_broker() { : > "$STATE/topics"; : > "$STATE/configs"; }

echo "baseline"
reset_broker
out=$(bash scripts/es4/create-es-topics.sh 2>&1) || {
    printf 'a clean reconciliation must pass\n' >&2; printf '%s\n' "$out" | sed 's/^/    | /' >&2; exit 1; }
printf '%s' "$out" | grep -q "every declared retention override is set on the topic itself" \
    || { printf 'the guard did not run inside create-es-topics.sh\n' >&2
         printf '%s\n' "$out" | tail -20 | sed 's/^/    | /' >&2; exit 1; }
# ...and it ran against the es4 declaration, not the dev/prod one.
printf '%s' "$out" | grep -qE "\($(sed -n 's/^OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES="\(.*\)"$/\1/p' scripts/kafka/topics.env | tr ' ' '\n' | grep -c '=') checked" \
    || { printf 'the guard checked a different number of topics than the es4 declaration names\n' >&2
         printf '%s\n' "$out" | tail -5 | sed 's/^/    | /' >&2; exit 1; }
printf '  killed: %s\n' "create-es-topics.sh runs the overrides guard, over the es4 declaration"

echo "mutations"
# The real shape of the defect: the topic ALREADY exists — auto-created by a still-running producer
# — so nothing creates it with `--config retention.ms=`, and the one thing that would make its
# retention explicit is the `kafka-configs --alter` this stage does not reach. Creating it here
# instead would hand it the config on the create call and prove nothing.
VICTIM_PARTS=$(sed -n 's/^OPTIONS_EDGE_ES4_TOPICS="\(.*\)"$/\1/p' scripts/kafka/topics.env \
               | tr ' ' '\n' | sed -n "s/^$VICTIM://p" | head -1)
[ -n "$VICTIM_PARTS" ] || { echo "$VICTIM is not in OPTIONS_EDGE_ES4_TOPICS" >&2; exit 1; }
reset_broker
echo "$VICTIM $VICTIM_PARTS" > "$STATE/topics"
rc=0
out=$(NO_ALTER="$VICTIM" bash scripts/es4/create-es-topics.sh 2>&1) || rc=$?
[ "$rc" != "0" ] || { printf 'a topic the stage never reached must FAIL create-es-topics.sh\n' >&2
                      printf '%s\n' "$out" | tail -20 | sed 's/^/    | /' >&2; exit 1; }
printf '%s' "$out" | grep -qF "DECLARED BUT NOT SET: $VICTIM" \
    || { printf 'and must name the topic: %s\n' "$(printf '%s' "$out" | tail -5)" >&2; exit 1; }
printf '  killed: %s\n' "a declared topic left on the broker default fails the es4 reconciliation"

# The guard line is load-bearing: with it removed, the same broken broker passes.
reset_broker
echo "$VICTIM $VICTIM_PARTS" > "$STATE/topics"
# The copy has to live NEXT TO the original: create-es-topics.sh resolves the shim dir, the applier
# and the guard from its own location, so a copy in a temp dir fails for a reason that has nothing
# to do with the mutation.
UNGUARDED=scripts/es4/.unguarded-under-test.sh
trap 'rm -rf "$WORK" "$REPO/$UNGUARDED"' EXIT
grep -vF 'bash "$OVERRIDES_GUARD" "$BROKER"' scripts/es4/create-es-topics.sh > "$UNGUARDED"
cmp -s scripts/es4/create-es-topics.sh "$UNGUARDED" \
    && { printf 'the mutation removed nothing — the guard invocation has moved or been reworded\n' >&2; exit 1; }
rc=0
NO_ALTER="$VICTIM" bash "$UNGUARDED" > "$WORK/unguarded.out" 2>&1 || rc=$?
[ "$rc" = "0" ] || { printf 'the unguarded script should still succeed — the failure above must come from the GUARD, not from something else\n' >&2
                     tail -20 "$WORK/unguarded.out" | sed 's/^/    | /' >&2; exit 1; }
printf '  killed: %s\n' "removing the guard invocation lets the same broken broker through"

echo "the es4 reconciliation proves itself"
