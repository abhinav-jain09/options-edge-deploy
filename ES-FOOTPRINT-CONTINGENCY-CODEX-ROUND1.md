Reading additional input from stdin...
2026-09-08T18:47:04.251480Z ERROR codex_models_manager::manager: failed to load models cache: missing field `supports_parallel_tool_calls` at line 93 column 5
OpenAI Codex v0.147.0
--------
workdir: /private/tmp/oe-deploy-mirror
model: gpt-5.6-sol
provider: openai
approval: never
sandbox: read-only
reasoning effort: low
reasoning summaries: none
session id: 01a08258-5f07-7481-8d65-b8dfd6a35651
--------
user
You are reviewing a change to options-edge-deploy at /private/tmp/oe-deploy-mirror on branch
feat/footprint-gateway-contingency-guard (PR #1010). Review LINE BY LINE for institutional-grade
accuracy, military-grade completeness, NASA-grade preparation.

The change adds two CI scripts and wires them into .github/workflows/deploy-validation.yml:
  scripts/ci/validate-footprint-gateway-contingency.sh
  scripts/ci/validate-footprint-gateway-contingency-test.sh

Claims to check, each against the actual files:
 1. The guard reads GATEWAY_ES_FOOTPRINT_ENABLED, GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES and
    JAVA_TOOL_OPTIONS from k8s/es4/services/es-feed-gateway.yaml, and FOOTPRINT_MAX_RECORD_BYTES
    from k8s/es4/services/es-cvd.yaml, CORRECTLY — including values containing spaces, values
    appearing under an unrelated `- name:` earlier in the file, and any commented-out occurrence.
 2. The memory-limit extraction picks the CONTAINER's limits.memory and not a requests.memory or
    another container's, in this file as it is actually written.
 3. The G-R8 inequality is limit - Xmx >= 512 MiB and the boundary is inclusive.
 4. The relay-off exemption cannot silently hide a real violation on a manifest that IS enabled.
 5. The mutation test's cases actually mutate what their labels say — verify each sed/awk against
    the real manifest, and say whether any case would pass for the wrong reason.
 6. Anything in the repository that this guard duplicates, contradicts, or should have been added to
    instead.
 7. Whether the CI wiring places the steps where they will actually run for the relevant changes.

Do NOT accept the PR description as evidence; verify against the files. End with exactly one line:
VERDICT: APPROVE  or  VERDICT: REQUEST_CHANGES
codex
I’ll inspect the branch diff, both scripts, the real manifests, workflow path filters/conditions, and nearby validation infrastructure. I’ll treat each mutation mechanically against a temporary copy so labels and failure reasons are verified, not inferred.
exec
/bin/bash -lc 'git status --short && git branch --show-current && git diff --stat HEAD''^..HEAD && git diff -- .github/workflows/deploy-validation.yml scripts/ci/validate-footprint-gateway-contingency.sh scripts/ci/validate-footprint-gateway-contingency-test.sh && sed -n '"'1,260p' scripts/ci/validate-footprint-gateway-contingency.sh && sed -n '1,340p' scripts/ci/validate-footprint-gateway-contingency-test.sh" in /private/tmp/oe-deploy-mirror
codex
The sandbox’s Apple developer shim is interfering with `git`, so I’m separating repository metadata from direct file inspection. The review can still be grounded in the checked-out files; I’ll use non-mutating shell tooling and, where possible, bypass the shim for history/diff context.
exec
/bin/bash -lc "command -v git; ls -l scripts/ci/validate-footprint-gateway-contingency*.sh .github/workflows/deploy-validation.yml; sed -n '1,280p' scripts/ci/validate-footprint-gateway-contingency.sh; sed -n '1,380p' scripts/ci/validate-footprint-gateway-contingency-test.sh" in /private/tmp/oe-deploy-mirror
 succeeded in 0ms:
/usr/bin/git
-rw-r--r--@ 1 abhinav  wheel  11680 Sep  8 20:46 .github/workflows/deploy-validation.yml
-rwxr-xr-x@ 1 abhinav  wheel   4032 Sep  8 20:45 scripts/ci/validate-footprint-gateway-contingency-test.sh
-rwxr-xr-x@ 1 abhinav  wheel   3728 Sep  8 20:44 scripts/ci/validate-footprint-gateway-contingency.sh
#!/usr/bin/env bash
# ES Footprint deployment contingency (G-R8), enforced on the manifests rather than trusted.
#
# Two facts about this subsystem are only true if two SEPARATE manifests agree, and nothing else
# in CI notices when they stop agreeing:
#
#   1. The record ceiling is ONE value. The gateway drops any record above its own ceiling, so a
#      producer allowed to emit larger records loses those records SILENTLY at the relay — the
#      producer is happy, the topic has the bytes, and the page never sees them.
#   2. G-R8's memory inequality. The relay's extra working set lives OUTSIDE the heap, so a
#      container whose limit equals its -Xmx is OOM-killed on native growth the heap never sees.
#      The rule is limit - Xmx >= 512 MiB, and it must hold wherever the relay is ENABLED.
#
# Both are checked only where the relay is actually on: a manifest with the flag off is not
# holding anything up, and demanding the headroom there would be noise.
set -euo pipefail
cd "$(dirname "$0")/../.."

fail() { printf 'FOOTPRINT CONTINGENCY: %s\n' "$*" >&2; exit 1; }

# The value of an env var in a manifest: the `value:` line that FOLLOWS its `- name:` line.
# The value of an env var: the `value:` line that FOLLOWS its `- name:` line. Taking $2 alone
# truncates a value with spaces to its first word — which is how "-Xms256m -Xmx1536m" read as
# "-Xms256m" and this check reported a missing -Xmx that was right there.
env_value() {
    awk -v want="$2" '
        $1 == "-" && $2 == "name:" { name = $3; next }
        $1 == "value:" && name == want {
            sub(/^ *value: */, ""); gsub(/^"|"$/, ""); print; exit
        }
    ' "$1"
}

mib() { printf '%s' "$1" | awk '{ if ($0 ~ /Mi$/) { sub(/Mi$/, ""); print $0 } else if ($0 ~ /Gi$/) { sub(/Gi$/, ""); print $0 * 1024 } else { print "" } }'; }

GW=k8s/es4/services/es-feed-gateway.yaml
PROD=k8s/es4/services/es-cvd.yaml
[ -f "$GW" ] || fail "$GW is missing"
[ -f "$PROD" ] || fail "$PROD is missing"

enabled=$(env_value "$GW" GATEWAY_ES_FOOTPRINT_ENABLED)
if [ "$enabled" != "true" ]; then
    printf 'footprint relay is not enabled in %s: nothing to check\n' "$GW"
    exit 0
fi

# ---- (1) one ceiling, both sides -------------------------------------------------------------
gw_bytes=$(env_value "$GW" GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES)
pr_bytes=$(env_value "$PROD" FOOTPRINT_MAX_RECORD_BYTES)
[ -n "$gw_bytes" ] || fail "the relay is enabled but $GW sets no GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES"
[ -n "$pr_bytes" ] || fail "the relay is enabled but $PROD sets no FOOTPRINT_MAX_RECORD_BYTES"
[ "$gw_bytes" = "$pr_bytes" ] \
    || fail "the record ceiling differs: gateway $gw_bytes vs producer $pr_bytes. A producer allowed to emit larger records loses them silently at the relay"

# ---- (2) G-R8: limit - Xmx >= 512 MiB ---------------------------------------------------------
limit_raw=$(awk '/^ *limits:/ { inlim = 1; next } inlim && /memory:/ { v = $2; gsub(/"/, "", v); print v; exit }' "$GW")
[ -n "$limit_raw" ] || fail "no memory limit found in $GW"
limit=$(mib "$limit_raw")
[ -n "$limit" ] || fail "memory limit '$limit_raw' is neither Mi nor Gi; this check cannot compare it"

xmx_raw=$(env_value "$GW" JAVA_TOOL_OPTIONS)
xmx=$(printf '%s' "$xmx_raw" | sed -n 's/.*-Xmx\([0-9]*\)m.*/\1/p')
[ -n "$xmx" ] || fail "no -Xmx found in JAVA_TOOL_OPTIONS ('$xmx_raw')"

headroom=$(( limit - xmx ))
[ "$headroom" -ge 512 ] \
    || fail "G-R8 native headroom is ${headroom} MiB (limit ${limit} MiB - Xmx ${xmx} MiB); the relay's working set is OUTSIDE the heap and needs >= 512 MiB"

printf 'footprint contingency OK: ceiling %s on both sides; headroom %s MiB (limit %s, Xmx %s)\n' \
    "$gw_bytes" "$headroom" "$limit" "$xmx"
#!/usr/bin/env bash
# Mutation test for validate-footprint-gateway-contingency.sh: each guard must REFUSE its own
# violation. A guard nobody has watched fail is a guard nobody knows is wired up.
set -euo pipefail
cd "$(dirname "$0")/../.."

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
GUARD=scripts/ci/validate-footprint-gateway-contingency.sh
GW=k8s/es4/services/es-feed-gateway.yaml
PROD=k8s/es4/services/es-cvd.yaml

run_against() {   # $1 = gateway manifest, $2 = producer manifest -> exit code of the guard
    local d="$WORK/case"; rm -rf "$d"; mkdir -p "$d/scripts/ci" "$d/k8s/es4/services"
    cp "$GUARD" "$d/scripts/ci/"
    cp "$1" "$d/k8s/es4/services/es-feed-gateway.yaml"
    cp "$2" "$d/k8s/es4/services/es-cvd.yaml"
    ( cd "$d" && ./scripts/ci/validate-footprint-gateway-contingency.sh >/dev/null 2>&1 ) && echo 0 || echo $?
}

expect() {   # $1 = expected code, $2 = label, $3.. = manifests
    local want="$1" label="$2"; shift 2
    local got; got=$(run_against "$@")
    if [ "$got" != "$want" ]; then
        printf 'MUTATION SURVIVED: %s (guard exited %s, expected %s)\n' "$label" "$got" "$want" >&2
        exit 1
    fi
    printf '  killed: %s\n' "$label"
}

echo "baseline"
expect 0 "the manifests as committed pass" "$GW" "$PROD"

echo "mutations"
# 1. the two ceilings diverge
sed 's/^\( *\)value: "262144"/\1value: "524288"/' "$PROD" > "$WORK/prod-bigger.yaml"
expect 1 "producer ceiling above the gateway's" "$GW" "$WORK/prod-bigger.yaml"

# 2. the producer stops declaring a ceiling at all
grep -v 'FOOTPRINT_MAX_RECORD_BYTES' "$PROD" | grep -v '^ *value: "262144"' > "$WORK/prod-none.yaml"
expect 1 "producer declares no ceiling" "$GW" "$WORK/prod-none.yaml"

# 3. the limit is lowered back to the heap size (the incident this rule exists for)
sed 's/^\( *\)memory: "2560Mi"/\1memory: "1536Mi"/' "$GW" > "$WORK/gw-tight.yaml"
expect 1 "limit equal to -Xmx: no native headroom" "$WORK/gw-tight.yaml" "$PROD"

# 4. the heap is raised into the headroom instead
sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2176m/' "$GW" > "$WORK/gw-fat-heap.yaml"
expect 1 "heap raised to leave under 512 MiB" "$WORK/gw-fat-heap.yaml" "$PROD"

# 5. exactly at the boundary is ACCEPTED — the rule is >=, and a guard that refuses its own
#    boundary would force every future budget to overshoot it
sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2048m/' "$GW" > "$WORK/gw-boundary.yaml"
expect 0 "exactly 512 MiB of headroom is legal" "$WORK/gw-boundary.yaml" "$PROD"

# 6. relay off: the guard has nothing to hold up and says so. Flip the flag's OWN value line —
#    a blanket s/"true"/"false"/ hits the first unrelated flag in the file and passes for the
#    wrong reason, which is exactly what the first version of this case did.
awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/true/, "false") }
     { print }' "$GW" > "$WORK/gw-off.yaml"
grep -A1 GATEWAY_ES_FOOTPRINT_ENABLED "$WORK/gw-off.yaml" | grep -q 'value: "false"' \
    || { echo "the disable mutation did not take: the case would pass for the wrong reason" >&2; exit 1; }
expect 0 "relay disabled: not checked" "$WORK/gw-off.yaml" "$PROD"

# 6b. and with the relay off the guard is not merely quiet — it is quiet BECAUSE of the flag:
#     the same manifest with a broken ceiling still passes while disabled, and fails once enabled
awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES" { sub(/262144/, "999999") }
     { print }' "$WORK/gw-off.yaml" > "$WORK/gw-off-broken.yaml"
expect 0 "relay disabled hides a ceiling mismatch (by design)" "$WORK/gw-off-broken.yaml" "$PROD"
awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/false/, "true") }
     { print }' "$WORK/gw-off-broken.yaml" > "$WORK/gw-on-broken.yaml"
expect 1 "and refuses the same manifest once the relay is enabled" "$WORK/gw-on-broken.yaml" "$PROD"

echo "every guard refused its own violation"

exec
/bin/bash -lc "nl -ba k8s/es4/services/es-feed-gateway.yaml | sed -n '1,300p'; nl -ba k8s/es4/services/es-cvd.yaml | sed -n '1,280p'; nl -ba .github/workflows/deploy-validation.yml | sed -n '1,300p'; rg -n \"FOOTPRINT|footprint|G-R8|MAX_RECORD_BYTES|JAVA_TOOL_OPTIONS|limits:|requests:\" .github scripts k8s --glob '"'!scripts/ci/validate-footprint-gateway-contingency*.sh'"'" in /private/tmp/oe-deploy-mirror
 succeeded in 0ms:
     1	# ES feed-gateway on es4. Same options-edge-feed-gateway binary as prod; env-only divergence.
     2	# Reads es.* topics from the .4 Kafka/SchemaRegistry via TOPIC_PREFIX (es4-common-env) — the
     3	# gateway applies TOPIC_PREFIX to every _TOPIC value (GatewaySettings, PR #73). WS handshake is
     4	# Keycloak-protected against the PRODUCTION realm (auth.bleadingoptions.com). Exposed on NodePort 30091
     5	# so the ES web (NodePort 30080) can open ws://192.168.100.4:30091/ws/events.
     6	# Deploy ONLY via Jenkinsfile.es4-deploy (digest-pins :prod before apply).
     7	apiVersion: v1
     8	kind: Service
     9	metadata:
    10	  name: es-feed-gateway
    11	  namespace: options-edge
    12	  labels:
    13	    app.kubernetes.io/name: es-feed-gateway
    14	    app.kubernetes.io/part-of: options-edge
    15	spec:
    16	  type: NodePort
    17	  ports:
    18	  - name: http
    19	    port: 18091
    20	    targetPort: http
    21	    nodePort: 30091
    22	  selector:
    23	    app.kubernetes.io/name: es-feed-gateway
    24	---
    25	apiVersion: apps/v1
    26	kind: Deployment
    27	metadata:
    28	  name: es-feed-gateway
    29	  namespace: options-edge
    30	  labels:
    31	    app.kubernetes.io/name: es-feed-gateway
    32	    app.kubernetes.io/part-of: options-edge
    33	spec:
    34	  replicas: 1
    35	  strategy:
    36	    type: Recreate
    37	  selector:
    38	    matchLabels:
    39	      app.kubernetes.io/name: es-feed-gateway
    40	  template:
    41	    metadata:
    42	      labels:
    43	        app.kubernetes.io/name: es-feed-gateway
    44	        app.kubernetes.io/part-of: options-edge
    45	    spec:
    46	      # Durable fix for the boot-time "Timeout expired while fetching topic metadata" race: the gateway
    47	      # (k3s) can start before es4-kafka / schema-registry (docker-compose on the .4 HOST) are serving —
    48	      # e.g. after a host reboot where k3s comes up before the compose stack. Its Kafka consumers then
    49	      # time out on the first metadata fetch and recover only slowly (once left es.options.databento.gex.strike
    50	      # unsubscribed ~2h -> GEX blank on the ES UI). Gate startup on BOTH deps being ready so the first
    51	      # metadata fetch succeeds and every topic (incl. the 32-partition gex.strike) is subscribed at boot.
    52	      initContainers:
    53	      # Reuse the gateway image (already present + digest-pinned by es4-deploy — a foreign image like
    54	      # busybox is NOT anchorable to a prod-registry sha256 and fail-closes the pin step). It ships
    55	      # bash + curl, so bash /dev/tcp checks the kafka broker port and curl checks SR /subjects (a 200
    56	      # implies kafka is up, since SR depends on it).
    57	      - name: wait-for-kafka-sr
    58	        image: 192.168.100.252:5000/options-edge-feed-gateway:prod
    59	        command:
    60	        - bash
    61	        - -c
    62	        - |
    63	          until (echo > /dev/tcp/192.168.100.4/9092) 2>/dev/null; do echo "waiting for kafka 192.168.100.4:9092"; sleep 2; done
    64	          until curl -sf -o /dev/null http://192.168.100.4:8081/subjects; do echo "waiting for schema-registry 192.168.100.4:8081"; sleep 2; done
    65	          echo "kafka + schema-registry reachable"
    66	      containers:
    67	      - name: feed-gateway
    68	        image: 192.168.100.252:5000/options-edge-feed-gateway:prod
    69	        ports:
    70	        - name: http
    71	          containerPort: 18091
    72	        env:
    73	        - name: APP_WEB_PORT
    74	          value: "18091"
    75	        # The es4 heatmap service creates its dashboard topic with the 32-partition default
    76	        # (KAFKA_TOPIC_PARTITIONS_DEFAULT=32), and topics.env (OPTIONS_EDGE_ES4_TOPICS) declares it
    77	        # at 32 too. Calibrate the gateway's liquidity-history topology guard to expect 32 so it does
    78	        # not WARN a false 4-vs-actual policy drift.
    79	        - name: HEATMAP_HISTORY_EXPECTED_PARTITIONS
    80	          value: "32"
    81	        - name: KAFKA_VIX_OPTION_INTELIGENCE_TOPIC
    82	          value: options.spx.vix-option-inteligence-service.current
    83	        # spread-skew on es4 emits options.es.* (proc PR#316 relabel) — point the gateway's two skew
    84	        # subscriptions there (TOPIC_PREFIX then yields es.options.es.spread-skew.*).
    85	        - name: KAFKA_SPREAD_SKEW_TOPIC
    86	          value: options.es.spread-skew.current
    87	        - name: KAFKA_SPREAD_SKEW_EVENTS_TOPIC
    88	          value: options.es.spread-skew.events
    89	        # Option Truth emits an ES-labelled base topic on this cluster. TOPIC_PREFIX from
    90	        # es4-common-env then isolates it as es.options.es.option-truth-engine-service.by-strike.
    91	        - name: KAFKA_OPTION_TRUTH_BY_STRIKE_TOPIC
    92	          value: options.es.option-truth-engine-service.by-strike
    93	        # ES-only continuous native futures buyer/seller flow card. The gateway code keeps this opt-in
    94	        # so the SPX production cluster never requires the ES-only topic.
    95	        - name: GATEWAY_ES_AGGRESSOR_FLOW_ENABLED
    96	          value: "true"
    97	        - name: KAFKA_ES_AGGRESSOR_FLOW_TOPIC
    98	          value: futures.aggressor-flow
    99	        # ES CVD stream (Gate 2 of ES-CVD-DESIGN.md): live 1 Hz snapshot + compacted bar-close
   100	        # records, published by es-cvd-service on this cluster. Opt-in for the same reason as the
   101	        # aggressor card — the ES-only topics do not exist on the SPX production cluster. Topic
   102	        # names are unprefixed; the gateway's TOPIC_PREFIX=es. resolves them to es.futures.cvd*.
   103	        - name: GATEWAY_ES_CVD_ENABLED
   104	          value: "true"
   105	        - name: KAFKA_ES_CVD_TOPIC
   106	          value: futures.cvd
   107	        - name: KAFKA_ES_CVD_BARS_TOPIC
   108	          value: futures.cvd.bars
   109	        # ES Footprint relay (ES-FOOTPRINT-GATEWAY-DESIGN.md G-R1..G-R11). Same opt-in reason as the
   110	        # CVD block above, and the same unprefixed names — TOPIC_PREFIX=es. resolves them to
   111	        # es.futures.footprint*. These topics are NATIVE here: es-cvd-service publishes them on this
   112	        # cluster, so unlike dev and prod there is no mirror in the path.
   113	        #
   114	        # G-R8a is fail-closed — a topic whose effective max.message.bytes exceeds the ceiling or
   115	        # whose compression.type is not producer/uncompressed makes the gateway REFUSE TO START, as
   116	        # do the three HotSpot layout flags the retained-heap arithmetic depends on. All verified on
   117	        # es4 before enabling: every one of the four topics reads compression.type=producer and
   118	        # max.message.bytes=1048588 (equal to the ceiling, and the check rejects only ABOVE it), and
   119	        # UseCompressedOops / UseCompressedClassPointers / CompactStrings are all true on the
   120	        # running JVM.
   121	        - name: GATEWAY_ES_FOOTPRINT_ENABLED
   122	          value: "true"
   123	        - name: KAFKA_ES_FOOTPRINT_TOPIC
   124	          value: futures.footprint
   125	        - name: KAFKA_ES_FOOTPRINT_EVIDENCE_TOPIC
   126	          value: futures.footprint.evidence
   127	        - name: KAFKA_ES_FOOTPRINT_BARS_TOPIC
   128	          value: futures.footprint.bars
   129	        - name: KAFKA_ES_FOOTPRINT_OUTCOMES_TOPIC
   130	          value: futures.footprint.outcomes
   131	        # G-R8 requires this and the producer's FOOTPRINT_MAX_RECORD_BYTES to be ONE value, set
   132	        # together. Both defaults are 262144, so they agreed only by coincidence of two independent
   133	        # defaults — a change to either side alone would have silently broken the bound the whole
   134	        # retained-heap argument rests on. Pinned explicitly on both sides; see the matching entry in
   135	        # k8s/es4/services/es-cvd.yaml. MOVE THEM TOGETHER OR NOT AT ALL.
   136	        - name: GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES
   137	          value: "262144"
   138	        # SPX Auction Desk (es-amt-service on this cluster): one payload per RTH minute keyed
   139	        # <tradeDate>|<HH:mm>. Opt-in like the CVD stream. Unprefixed: TOPIC_PREFIX=es. resolves
   140	        # it to es.futures.auction, the name Jenkinsfile.es-auction-mirror copies to dev and prod.
   141	        - name: GATEWAY_ES_AUCTION_ENABLED
   142	          value: "true"
   143	        - name: KAFKA_ES_AUCTION_TOPIC
   144	          value: futures.auction
   145	        # es4 serves ES, not SPX: the gateway's active market-data selection symbol is IB_SYMBOL
   146	        # (default SPX). Without this the selection is SPX and the ES option records (underlying=ES)
   147	        # never match -> empty chain even at market open. The 0DTE daily roll comes from es4-base-env's
   148	        # IB_EXPIRY: "AUTO" — the LITERAL string, not an unset value: GatewaySettings.isAutoExpiry treats
   149	        # blank as NOT auto (the handshake then fails closed on a blank expiry), so clearing that key
   150	        # would stop the roll AND break every WS connection. Do not "tidy" it away.
   151	        - name: IB_SYMBOL
   152	          value: "ES"
   153	        # Roll the AUTO 0DTE expiry to the next trade date at 17:30 ET (mirrors the es-feed + SPX).
   154	        # Without this the gateway keeps activeSelection on the just-expired date and drops the feed's
   155	        # next-expiry records until midnight ET, freezing the chain all evening (gateway PR#74).
   156	        - name: IB_EXPIRY_ROLL_AFTER
   157	          value: "17:30"
   158	        # The gateway resolves its session calendar from its OWN keys, NOT from the HPSF_MARKET_* pair
   159	        # es4-base-env carries. Without these the calendar was EMPTY: on 2026-09-07 (Labor Day) the
   160	        # gateway treated the holiday as a session, AUTO-resolved the expiry to 20260907, and rejected
   161	        # every 20260908 record the ES feed published as belonging to an inactive expiry —
   162	        # inactiveDroppedEvents into the millions, UI connected to a chain that stayed blank all day.
   163	        # The gateway now also falls back to HPSF_MARKET_* when these are unset, but keep these
   164	        # explicit: HPSF is being retired (PR #995) and those keys may not outlive it. Refresh both
   165	        # pairs together from the annual OPRA/NYSE schedule.
   166	        - name: GATEWAY_MARKET_HOLIDAYS
   167	          value: "2026-01-01,2026-01-19,2026-02-16,2026-04-03,2026-05-25,2026-06-19,2026-07-03,2026-09-07,2026-11-26,2026-12-25"
   168	        - name: GATEWAY_MARKET_EARLY_CLOSES
   169	          value: "2026-11-27=13:00,2026-12-24=13:00"
   170	        - name: JAVA_TOOL_OPTIONS
   171	          value: "-Xms256m -Xmx1536m"
   172	        # Flow Explorer (/api/pin-flow) default strike band. The gateway's built-in default is a
   173	        # hardcoded SPX-era 7490..7590 — only 100 points wide and NOT centred on spot, so on ES
   174	        # (spot ~7500, strikes 6850..7755) it started AT the spot and looked only upward: ~28% of
   175	        # rows, with every put below the spot excluded. Widen to cover the live ES strike ladder
   176	        # (gateway caps band width at 1000). Superseded once the band is derived from spot.
   177	        - name: PIN_FLOW_STRIKE_LO
   178	          value: "7000"
   179	        - name: PIN_FLOW_STRIKE_HI
   180	          value: "7800"
   181	        # ES trades 18:00 ET -> 17:00 ET the NEXT day. The gateway's default pin-flow window is the SPX
   182	        # cash session (09:30..16:01 ET), so an ES request in the evening resolved to a window where ES
   183	        # was CLOSED (zero rows) and the Flow Explorer showed nothing for ES's main trading hours. These
   184	        # make the window span midnight; the gateway rolls the end to date+1 and resolves the live
   185	        # session date (gateway PR #90).
   186	        - name: PIN_FLOW_SESSION_START
   187	          value: "18:00"
   188	        - name: PIN_FLOW_SESSION_END
   189	          value: "17:00"
   190	        # WS handshake auth against the PRODUCTION Keycloak realm.
   191	        - name: WS_AUTH_ENABLED
   192	          value: "true"
   193	        - name: WS_AUTH_ISSUER_URI
   194	          value: "https://auth.bleadingoptions.com/realms/optionsedge"
   195	        - name: WS_AUTH_AUDIENCE
   196	          value: "options-edge-web"
   197	        # Browser origins of the ES web UI. Public edge origin first; the LAN NodePort origin is
   198	        # kept for direct-on-.4 debugging. (fullfunding.nl retired 2026-08-08.)
   199	        - name: WS_ALLOWED_ORIGINS
   200	          value: "https://es.bleadingoptions.com,http://192.168.100.4:30080"
   201	        # Multi-tenant session auth (Redis-backed) stays OFF — only the Keycloak WS bearer is used.
   202	        - name: GATEWAY_AUTH_ENABLED
   203	          value: "false"
   204	        envFrom:
   205	        - configMapRef:
   206	            name: es4-base-env
   207	        - configMapRef:
   208	            name: es4-common-env
   209	        - secretRef:
   210	            name: es4-runtime-secrets
   211	        readinessProbe:
   212	          httpGet:
   213	            path: /health
   214	            port: http
   215	          initialDelaySeconds: 30
   216	          periodSeconds: 10
   217	        livenessProbe:
   218	          httpGet:
   219	            path: /health
   220	            port: http
   221	          initialDelaySeconds: 30
   222	          periodSeconds: 15
   223	        # The limit was EQUAL to the heap: limit == Xmx == 1536Mi. That is not a tight budget, it
   224	        # is an absent one — metaspace, code cache, thread stacks and direct/socket buffers all live
   225	        # OUTSIDE -Xmx, so the container can be OOM-killed on native growth the heap never sees. No
   226	        # amount of GC tuning, and no alarm that watches heap occupancy, can see that coming while
   227	        # Xmx == limit; the only other way out would be to LOWER -Xmx, which buys native headroom by
   228	        # taking it from the heap. G-R8 (options-edge-feed-gateway/ES-FOOTPRINT-GATEWAY-DESIGN.md)
   229	        # states the rule as limit - Xmx >= 512 MiB; this manifest violated it outright, and it does
   230	        # so today with the footprint relay OFF. Fixed here on that ground alone.
   231	        #
   232	        # Measured on es-feed-gateway-755c5544d5-6ftk6 (6h53m, CVD on, footprint off), from the
   233	        # container itself rather than a sampled `kubectl top`:
   234	        #   cgroup memory.peak  668 426 240 B = 638 MiB
   235	        #   cgroup memory.current 665 686 016 B = 635 MiB
   236	        #   /proc/1 VmHWM       664 432 KiB    = 649 MiB   (pid 1 is the only real process)
   237	        # Only ONE G-R8 quantity is derivable from these: working set = memory.current -
   238	        # inactive_file <= memory.current, and memory.peak is the high-water of memory.current, so
   239	        #   W_peak <= 638 MiB   ->   limit - W_peak >= 2560 - 638 = 1922 MiB
   240	        # H_peak is NOT derivable here. G-R8 defines it as peak jvm_memory_bytes_used{area="heap"},
   241	        # which counts USED heap bytes; those need not all be resident, so process RSS does not
   242	        # bound it and VmHWM proves nothing about it. This manifest therefore makes no claim about
   243	        # Xmx - H_peak. That inequality belongs to the enablement change, which needs the real heap
   244	        # series anyway.
   245	        # (VmHWM exceeds memory.peak by ~12 MB, and VmRSS exceeds memory.current by the same amount
   246	        # on a re-read. The cause was NOT established and no mechanism is claimed for it; it does not
   247	        # affect the W_peak bound, which comes from the cgroup counters alone.)
   248	        #   limit - Xmx = 2560 - 1536 = 1024 MiB   (the inequality that was violated)
   249	        #
   250	        # NOT sufficient to enable the footprint relay. G-R8's deployment contingency requires
   251	        # H_peak from jvm_memory_bytes_used{area="heap"} and W_peak from
   252	        # container_memory_working_set_bytes, over a FULL ES session. This gateway exports no JVM
   253	        # heap series on :18091, and 2026-09-07 is in GATEWAY_MARKET_HOLIDAYS above — a shortened
   254	        # holiday session. Enablement is a separate change, after a regular session.
   255	        #
   256	        # requests.memory rises with it: 512Mi sits below this pod's observed usage — 635 MiB at the
   257	        # single point in time above; no claim is made about every moment of its life. While usage is
   258	        # above the request the kubelet's memory eviction ranking counts the pod as over-request, and
   259	        # raising the request above the observed baseline improves that ranking. It does NOT change
   260	        # the QoS class — the
   261	        # pod stays Burstable (memory request < limit, cpu request with no limit, and the
   262	        # wait-for-kafka-sr init container declares neither) and remains an eviction candidate. The
   263	        # es4 node is at 24% of allocatable requests (15 628 MiB of 63.6 GiB), so 256 MiB is
   264	        # affordable; declared limit overcommit goes from ~107% to ~109%, which this node already
   265	        # runs at with 35 GiB actually free.
   266	        resources:
   267	          requests:
   268	            cpu: "250m"
   269	            memory: "768Mi"
   270	          limits:
   271	            memory: "2560Mi"
     1	# ES session-anchored Cumulative Volume Delta. Deploy via Jenkinsfile.es4-deploy.
     2	# Cross-env wiring (ES-CVD-DESIGN.md 6a): the ES tape and output topics live on the es4 broker;
     3	# the SPX index feed is es4's OWN es.underlying.spx.price (see ES_CVD_SPX_BOOTSTRAP_SERVERS below).
     4	apiVersion: v1
     5	kind: Service
     6	metadata:
     7	  annotations:
     8	    prometheus.io/path: /metrics
     9	    prometheus.io/port: '8141'
    10	    prometheus.io/scrape: 'true'
    11	  labels:
    12	    app.kubernetes.io/name: es-cvd-service
    13	    app.kubernetes.io/part-of: options-edge
    14	  name: es-cvd-service
    15	  namespace: options-edge
    16	spec:
    17	  ports:
    18	  - name: health
    19	    port: 8141
    20	    targetPort: health
    21	  selector:
    22	    app.kubernetes.io/name: es-cvd-service
    23	---
    24	apiVersion: apps/v1
    25	kind: Deployment
    26	metadata:
    27	  labels:
    28	    app.kubernetes.io/name: es-cvd-service
    29	    app.kubernetes.io/part-of: options-edge
    30	  name: es-cvd-service
    31	  namespace: options-edge
    32	spec:
    33	  progressDeadlineSeconds: 600
    34	  replicas: 1
    35	  strategy:
    36	    type: Recreate
    37	  selector:
    38	    matchLabels:
    39	      app.kubernetes.io/name: es-cvd-service
    40	  template:
    41	    metadata:
    42	      labels:
    43	        app.kubernetes.io/name: es-cvd-service
    44	        app.kubernetes.io/part-of: options-edge
    45	    spec:
    46	      containers:
    47	      - name: es-cvd
    48	        image: 192.168.100.252:5000/options-edge-es-cvd:prod
    49	        env:
    50	        - name: JAVA_TOOL_OPTIONS
    51	          value: -Xms128m -Xmx768m
    52	        - name: HEALTH_PORT
    53	          value: '8141'
    54	        - name: ES_CVD_BOOTSTRAP_SERVERS
    55	          value: 192.168.100.4:9092
    56	        # SPX index feed: es4's OWN broker and topic, NOT the dev Mac.
    57	        #
    58	        # This used to read underlying.spx.index.price from 192.168.100.102:19092 (Docker Desktop
    59	        # on the dev Mac), which needed a hostAliases entry to resolve that broker's advertised
    60	        # host.docker.internal listener. On 2026-08-17 that topic was RECREATED on the dev Mac with
    61	        # 32 partitions, its data landing on partition 8; this pod had assign()-pinned partition 0
    62	        # at startup, so it polled an empty partition and es_cvd_spx_records_received sat at 0 for
    63	        # the whole session. lastSpx went null and every SPX-vs-CVD view lost its SPX series, while
    64	        # the consumer still reported itself UP — a silent break with nothing to alert on.
    65	        #
    66	        # es4 publishes its own live index feed on es.underlying.spx.price (source
    67	        # NATIVE_SPX_INDEX, quality LIVE_INDEX) carrying canonicalSpot + spotEventTimeNs — the
    68	        # fields ingestSpx PREFERS. Reading it here removes the cross-environment dependency, the
    69	        # advertised-listener hack, and this entire class of failure.
    70	        - name: ES_CVD_SPX_BOOTSTRAP_SERVERS
    71	          value: 192.168.100.4:9092
    72	        - name: KAFKA_ES_TRADES_TOPIC
    73	          value: underlying.es.trades
    74	        # Literal: EsCvdSettings reads this with KafkaSettings.value (no TOPIC_PREFIX), so the
    75	        # es4 name is spelled out in full rather than relying on prefixing.
    76	        - name: KAFKA_SPX_INDEX_PRICE_TOPIC
    77	          value: es.underlying.spx.price
    78	        - name: KAFKA_ES_CVD_TOPIC
    79	          value: futures.cvd
    80	        - name: KAFKA_ES_CVD_BARS_TOPIC
    81	          value: futures.cvd.bars
    82	        - name: ES_CVD_GROUP_ID
    83	          value: es-cvd-service
    84	        - name: ES_CVD_SYMBOL
    85	          value: ES.v.0
    86	        - name: ES_CVD_SPX_SYMBOL
    87	          value: SPX
    88	        - name: ES_CVD_SESSION_ZONE
    89	          value: America/New_York
    90	        - name: ES_CVD_SESSION_START
    91	          value: '18:00'
    92	        - name: ES_CVD_PUBLISH_INTERVAL_MS
    93	          value: '1000'
    94	        - name: ES_CVD_REPLAY_LOOKBACK_HOURS
    95	          value: '24'
    96	        # R29 amendment 22 (options-edge-processing PR #748): the instant, epoch ms, at which
    97	        # message.timestamp.type=LogAppendTime was verified effective on es.underlying.es.trades
    98	        # (kafka-configs --alter on es4, 2026-09-06T23:18:50.699Z, single broker). A config change is
    99	        # not retroactive and the broker keeps no history, so this is the operator's attestation:
   100	        # only a session that opened at/after it can be attested complete through the retention
   101	        # window after an in-session restart. 0 = never attested (conservative). If the topic is
   102	        # ever switched back to CreateTime, set this to 0 in the same change.
   103	        - name: ES_CVD_APPEND_TIME_SINCE_MS
   104	          value: '1788736730699'
   105	        - name: ES_CVD_GRACE_PERIOD_MS
   106	          value: '2000'
   107	        - name: ES_CVD_IDLE_TIMEOUT_MS
   108	          value: '10000'
   109	        - name: ES_CVD_MAX_FUTURE_SKEW_MS
   110	          value: '5000'
   111	        - name: ES_CVD_STALE_AFTER_MS
   112	          value: '5000'
   113	        - name: ES_CVD_SPX_STALE_AFTER_MS
   114	          value: '30000'
   115	        # ES Footprint subsystem (options-edge-processing ES-FOOTPRINT-DESIGN.md F-R14/F-R18).
   116	        # Strict true|false; the service refuses to start on anything else. Flag on, the service
   117	        # ensures the four single-partition topics below (resolved through the es4 TOPIC_PREFIX like
   118	        # futures.cvd*: es.futures.footprint...) and exports es_footprint_*. They are stamped
   119	        # cleanup.policy=delete, not compact, because es4 sets OPTIONS_EDGE_UNCOMPACTED_SERVED_TOPICS.
   120	        # Flag off (or an image without the subsystem) is byte-identical CVD output.
   121	        - name: ES_FOOTPRINT_ENABLED
   122	          value: 'true'
   123	        - name: KAFKA_ES_FOOTPRINT_BARS_TOPIC
   124	          value: futures.footprint.bars
   125	        - name: KAFKA_ES_FOOTPRINT_OUTCOMES_TOPIC
   126	          value: futures.footprint.outcomes
   127	        - name: KAFKA_ES_FOOTPRINT_TOPIC
   128	          value: futures.footprint
   129	        - name: KAFKA_ES_FOOTPRINT_EVIDENCE_TOPIC
   130	          value: futures.footprint.evidence
   131	        # G-R8 (options-edge-feed-gateway/ES-FOOTPRINT-GATEWAY-DESIGN.md) requires this and the
   132	        # gateway's GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES to be ONE value, set together: the gateway
   133	        # drops any record above its own ceiling, so a producer allowed to emit larger records would
   134	        # publish records the relay silently discards. Both defaults are 262144 and agreed only by
   135	        # coincidence until now. See the matching entry in k8s/es4/services/es-feed-gateway.yaml.
   136	        # MOVE THEM TOGETHER OR NOT AT ALL.
   137	        - name: FOOTPRINT_MAX_RECORD_BYTES
   138	          value: "262144"
   139	        envFrom:
   140	        - configMapRef:
   141	            name: es4-base-env
   142	        - configMapRef:
   143	            name: es4-common-env
   144	        - secretRef:
   145	            name: es4-runtime-secrets
   146	        ports:
   147	        - containerPort: 8141
   148	          name: health
   149	        livenessProbe:
   150	          httpGet:
   151	            path: /health/live
   152	            port: health
   153	          initialDelaySeconds: 15
   154	          periodSeconds: 15
   155	        readinessProbe:
   156	          httpGet:
   157	            path: /health/ready
   158	            port: health
   159	          initialDelaySeconds: 10
   160	          periodSeconds: 10
   161	        resources:
   162	          requests:
   163	            cpu: 100m
   164	            memory: 256Mi
   165	          limits:
   166	            memory: 1Gi
     1	name: Deploy Validation
     2	
     3	on:
     4	  pull_request:
     5	    branches:
     6	      - main
     7	  push:
     8	    branches:
     9	      - main
    10	  workflow_dispatch:
    11	
    12	permissions:
    13	  contents: read
    14	
    15	jobs:
    16	  deploy-validation:
    17	    runs-on: ubuntu-latest
    18	    steps:
    19	      - name: Check out repository
    20	        uses: actions/checkout@v4
    21	
    22	      - name: Install kubectl
    23	        run: |
    24	          set -euo pipefail
    25	          curl -fsSLo kubectl "https://dl.k8s.io/release/v1.36.2/bin/linux/amd64/kubectl"
    26	          chmod +x kubectl
    27	          sudo mv kubectl /usr/local/bin/kubectl
    28	
    29	      - name: Install yq
    30	        run: |
    31	          set -euo pipefail
    32	          # PINNED, and the validators must be exercised against THIS version. A yq-version
    33	          # difference (4.44 vs 4.53) silently turned validate-preopen-single-publisher into a
    34	          # permanently red check that could not distinguish a real misconfiguration from the
    35	          # tool: the `//` alternative operator is evaluated per document, so every non-matching
    36	          # document in a rendered multi-doc stream produced its own default line.
    37	          curl -fsSLo yq "https://github.com/mikefarah/yq/releases/download/v4.44.3/yq_linux_amd64"
    38	          chmod +x yq
    39	          sudo mv yq /usr/local/bin/yq
    40	
    41	      - name: Service slices are regenerated (no drift)
    42	        # Catches the recurring mirror-drift class: someone tunes a service's patch in the
    43	        # monolith overlay (k8s/overlays/*) but forgets to re-run the slice generator, so the
    44	        # generated k8s/services/*/overlays/* slices go stale and every deploy's Validate stage
    45	        # fails later. Run the generator here and fail the PR if it would change any committed
    46	        # slice — i.e. the author must commit the regenerated output.
    47	        run: |
    48	          set -euo pipefail
    49	          bash scripts/deploy/generate-service-slices.sh
    50	          if ! git diff --quiet -- k8s/services; then
    51	            echo "::error::Service slices are out of date. Run scripts/deploy/generate-service-slices.sh and commit the regenerated k8s/services/**."
    52	            git --no-pager diff --stat -- k8s/services
    53	            echo "----- full diff -----"
    54	            git --no-pager diff -- k8s/services
    55	            exit 1
    56	          fi
    57	          echo "Service slices are up to date."
    58	
    59	      # The same drift class one environment over, and a worse blast radius: k8s/es4/services/**
    60	      # is generated from the PRODUCTION slices, so a prod env change silently queues itself up
    61	      # for es4 — a LIVE environment — and lands whenever someone next re-renders for an
    62	      # unrelated reason. Three such problems were sitting on main when this gate was added: an
    63	      # inherited DATABENTO_GEX_DYNAMIC_OI_ENABLED flip, the IBKR_GEX_* pre-open wiring, and two
    64	      # manifests that fell out of the renderer's SERVICES list yet are still deployed by
    65	      # deploy-all. The gate therefore checks the file SET as well as file contents — deploy-all
    66	      # globs the directory, so an un-rendered or uncommitted file there still reaches the box.
    67	      # PyYAML is pinned to the version the committed manifests were rendered with: the check
    68	      # compares emitted YAML byte-for-byte, so a formatting change between releases would read
    69	      # as drift.
    70	      - name: Install PyYAML
    71	        run: python3 -m pip install --quiet 'pyyaml==6.0.3'
    72	
    73	      - name: es4 manifests are regenerated and fully accounted for (no drift)
    74	        run: bash scripts/ci/validate-es4-render.sh
    75	
    76	      - name: Validate service registry (completeness + mirror + image name)
    77	        run: scripts/ci/validate-services.sh
    78	
    79	      # ES Footprint G-R8: the record ceiling is ONE value across two manifests, and the relay's
    80	      # working set lives outside the heap. Neither fact is visible from either manifest alone,
    81	      # and both fail SILENTLY in production — a producer above the gateway's ceiling loses records
    82	      # at the relay with nothing logged upstream, and a container whose limit equals its -Xmx is
    83	      # OOM-killed on native growth the heap never sees. Checked only where the relay is enabled.
    84	      - name: ES Footprint deployment contingency (paired ceiling, G-R8 headroom)
    85	        run: scripts/ci/validate-footprint-gateway-contingency.sh
    86	
    87	      - name: ES Footprint contingency guard refuses each of its own violations
    88	        run: scripts/ci/validate-footprint-gateway-contingency-test.sh
    89	
    90	      # VIX feed separation §7: validate-services.sh already runs the single-publisher
    91	      # assertion against the REAL render; this additionally exercises the assertion's
    92	      # full predicate matrix (five planned rollout states, both-owners violations,
    93	      # runtime-equivalent spellings, fail-closed states) against crafted fixtures, so
    94	      # a predicate regression cannot hide behind a currently-green render.
    95	      - name: VIX single-publisher assertion test matrix
    96	        run: python3 -m unittest -v tests.test_vix_single_publisher
    97	
    98	      # One Service One Identity Rule: no service identity (Streams application.id / consumer group)
    99	      # may carry a version suffix. FAIL-CLOSED — a red result here blocks the merge. This exists
   100	      # because oi-shadow was born as "oi-shadow-v1" eleven days AFTER the platform-wide unversioning
   101	      # migration and survived five weeks: a point-in-time sweep cannot see what is created after it.
   102	      - name: Service identities are unversioned (One Service One Identity Rule)
   103	        run: python3 -m unittest -v tests.test_service_identity_unversioned
   104	
   105	      # Every tests/test_*.py must be COLLECTABLE by unittest, the only runner this repo has.
   106	      # Three files were written as bare module-level test_ functions (pytest style) and so ran as
   107	      # "0 tests ... OK" — including one carrying a genuinely failing assertion nobody could see.
   108	      - name: Test files are collectable by unittest (no silently-empty suites)
   109	        run: bash scripts/ci/validate-tests-are-collectable.sh
   110	
   111	      # The es4 topic contract: topics.env is the single source of truth for the es4 broker, and
   112	      # these two modules are what hold it to that. Both were among the uncollectable files above,
   113	      # so neither had ever run here.
   114	      # The close chain's session arithmetic, and the rule that no shell script may do date
   115	      # arithmetic at all: `date -v` (BSD) vs `date -d` (GNU) is decided by the agent's PATH, and
   116	      # that bet cost the 2026-09-01 morning catch-up its entire run.
   117	      - name: Close-board session resolution + shell date-portability guard
   118	        run: python3 -m unittest -v tests.test_close_board_session_resolution
   119	
   120	      - name: es4 topic SSOT + partition contract
   121	        run: python3 -m unittest -v tests.test_es4_topics_ssot tests.test_es4_topic_partition_contract tests.test_remove_retired_workloads
   122	
   123	      - name: Validate dev GEX availability safeguards
   124	        run: bash scripts/ci/validate-gex-dev-safety.sh
   125	
   126	      # A mirrored es4 topic has two owners that both write its shape — the MM1 job that creates
   127	      # and asserts it, and apply-topics.sh, which rewrites retention.ms + cleanup.policy on every
   128	      # declared topic on every deploy. Undeclared, it was a deletion candidate for
   129	      # cleanup-topics.sh; declared with the wrong shape, the deploy reconciles the mirror's own
   130	      # contract away. This makes the two declarations unmergeable when they disagree.
   131	      # (The retention=-1 preserved-or-rebuildable classification that es.tape-zones.board now
   132	      # depends on is already gated: validate-services.sh runs
   133	      # scripts/ci/validate-durable-topic-preservation.sh as its section 6.)
   134	      - name: Validate mirrored es4 topic contracts (mirror jobs vs topics.env)
   135	        run: bash scripts/ci/validate-mirrored-topic-contracts.sh
   136	
   137	      # A Groovy pipeline is not shell-checked by anything. Jenkinsfile.es-auction-mirror shipped with
   138	      # an unterminated brace group (`fi; { : }`) that Groovy and review both accepted and that failed
   139	      # at RUNTIME on the job's first install, after the preflight had already talked to both brokers.
   140	      - name: Validate the es-auction mirror's shell blocks parse
   141	        run: bash scripts/ci/validate-jenkinsfile-shell-blocks.sh
   142	
   143	      # topics.env is only the DEPLOY-TIME half of "the archive is never compacted". The other half
   144	      # is the boot-time stamp: processing-common KafkaTopics.ensureTopic() runs
   145	      # incrementalAlterConfigs on every service start, so a topic the deploy just set to delete is
   146	      # re-compacted by the next restart unless OPTIONS_EDGE_UNCOMPACTED_SERVED_TOPICS="true" is in
   147	      # the environment the services actually read. Compaction kept coming back on prod for exactly
   148	      # this reason — each repair fixed one writer. This asserts the rendered environment carries
   149	      # the flag on prod and es4, and that it never leaks into dev (which must stay compacted).
   150	      - name: Validate archive uncompaction flag reaches prod + es4 and not dev
   151	        run: bash scripts/ci/validate-archive-uncompaction-env.sh
   152	
   153	      # "checked 5 mirrored topic(s)" proves only that five names were found. This drives the
   154	      # validator against crafted fixtures — one broken dimension each, plus the mutations that
   155	      # make its own parsing return nothing — so a checker that quietly starts approving
   156	      # everything fails here instead of going green.
   157	      - name: Mirrored-topic contract validator mutation suite
   158	        run: bash scripts/ci/validate-mirrored-topic-contracts-test.sh
   159	
   160	      # topics.env agreeing with the mirror job is only half the contract: --create --if-not-exists
   161	      # binds nothing on an EXISTING target, so the job's own preflight assertions are what stand
   162	      # between a drifted topic and a mirror that runs against it. This drives those assertions —
   163	      # extracted from the Jenkinsfile, never copied — against mocked brokers, including a
   164	      # pre-existing pure-compact target sitting at compact,delete.
   165	      - name: es-cvd-mirror source/target shape assertions
   166	        run: bash scripts/ci/es-cvd-mirror-shape-test.sh
   167	
   168	      # Same contract for the auction-desk mirror (es.futures.auction), whose job additionally
   169	      # RECONCILES a pre-existing target's config before asserting it -- so the suite also proves
   170	      # a wrong-partition target is refused untouched and an ineffective alter still fails.
   171	      - name: es-auction-mirror source/target shape assertions
   172	        run: bash scripts/ci/es-auction-mirror-shape-test.sh
   173	
   174	      - name: Validate image-pinning completeness (every rendered image wired into resolve-images + apply)
   175	        run: scripts/ci/validate-image-pinning.sh
   176	
   177	      # DBP-R4 drift gate. The 2026-07-24 Databento incident was caused by a feed deployment that
   178	      # mounted only its API-key Secret and therefore inherited library code defaults
   179	      # (DATABENTO_USE_LIVE_REPLAY=true, DATABENTO_PUBLISH_INTERVAL_MS=250). This resolves the FULL
   180	      # rendered environment (env over envFrom, as Kubernetes does) and fails on any mandatory key
   181	      # that is left to a default.
   182	      - name: Validate feed configuration is fully explicit (no inherited library defaults)
   183	        run: bash scripts/ci/validate-feed-config-explicit.sh
   184	
   185	      # U16 (CL-R10 L4/O3): promtool check + unit tests over the CVD SPX levels paging rules.
   186	      # The script downloads a sha256-pinned promtool when none is on PATH, so it is mandatory
   187	      # here on the GitHub-hosted runner, not just runnable where prometheus is installed.
   188	      - name: Validate CVD SPX levels alert rules (promtool check + test)
   189	        run: bash scripts/ci/validate-cvd-levels-alerts.sh
   190	
   191	      - name: Validate deploy image locks
   192	        run: scripts/deploy/validate-image-lock-ci.sh
k8s/replay/dev/databento-timewarp-replay-job.yaml:64:            requests:
k8s/replay/dev/databento-timewarp-replay-job.yaml:67:            limits:
.github/workflows/deploy-validation.yml:79:      # ES Footprint G-R8: the record ceiling is ONE value across two manifests, and the relay's
.github/workflows/deploy-validation.yml:84:      - name: ES Footprint deployment contingency (paired ceiling, G-R8 headroom)
.github/workflows/deploy-validation.yml:85:        run: scripts/ci/validate-footprint-gateway-contingency.sh
.github/workflows/deploy-validation.yml:88:        run: scripts/ci/validate-footprint-gateway-contingency-test.sh
k8s/services/unified-sr/overlays/dev/manifest.yaml:79:            - name: JAVA_TOOL_OPTIONS
k8s/services/unified-sr/overlays/dev/manifest.yaml:105:            limits:
k8s/services/unified-sr/overlays/dev/manifest.yaml:107:            requests:
k8s/keycloak/keycloak-deployment.yaml:216:            requests:
k8s/keycloak/keycloak-deployment.yaml:219:            limits:
k8s/tenants/fullfunding/limitrange.yaml:12:  limits:
k8s/base/gamma-leadership-deployment.yaml:52:            - name: JAVA_TOOL_OPTIONS
k8s/base/gamma-leadership-deployment.yaml:72:            requests:
k8s/base/gamma-leadership-deployment.yaml:75:            limits:
k8s/es4/services/pressure-postgres-writer.yaml:71:          limits:
k8s/es4/services/pressure-postgres-writer.yaml:73:          requests:
k8s/es4/services/databento-gex-history.yaml:76:          limits:
k8s/es4/services/databento-gex-history.yaml:78:          requests:
k8s/es4/services/databento-gex-history.yaml:100:    requests:
k8s/services/unified-sr/overlays/experiment/manifest.yaml:77:            - name: JAVA_TOOL_OPTIONS
k8s/services/unified-sr/overlays/experiment/manifest.yaml:103:            limits:
k8s/services/unified-sr/overlays/experiment/manifest.yaml:105:            requests:
k8s/keycloak/keycloak-postgres.yaml:114:            requests:
k8s/keycloak/keycloak-postgres.yaml:117:            limits:
k8s/keycloak/keycloak-postgres.yaml:126:          requests:
k8s/es4/services/strike-flow-classifier.yaml:107:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/strike-flow-classifier.yaml:134:          limits:
k8s/es4/services/strike-flow-classifier.yaml:136:          requests:
k8s/es4/services/strike-flow-classifier.yaml:158:    requests:
k8s/base/signal-follower-spx-deployment.yaml:66:            - name: JAVA_TOOL_OPTIONS
k8s/base/signal-follower-spx-deployment.yaml:84:            requests:
k8s/base/signal-follower-spx-deployment.yaml:87:            limits:
k8s/es4/services/databento-mission-sandwich.yaml:106:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/databento-mission-sandwich.yaml:133:          limits:
k8s/es4/services/databento-mission-sandwich.yaml:135:          requests:
k8s/es4/services/databento-mission-sandwich.yaml:157:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:12:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:25:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:38:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:51:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:64:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:77:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:90:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:103:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:116:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:129:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:142:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:155:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:168:    requests:
k8s/infra/base/kafka-streams-state-pvcs.yaml:191:    requests:
k8s/jobs/stock-gex-close-board-job.yaml:159:            requests:
k8s/jobs/stock-gex-close-board-job.yaml:165:            limits:
k8s/es4/services/directional-pressure.yaml:69:          limits:
k8s/es4/services/directional-pressure.yaml:71:          requests:
k8s/es4/services/directional-pressure.yaml:93:    requests:
k8s/infra/base/offhours-clean-slate-cronjob.yaml:73:                requests:
k8s/infra/base/offhours-clean-slate-cronjob.yaml:76:                limits:
k8s/es4/services/close-direction.yaml:65:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/close-direction.yaml:94:          limits:
k8s/es4/services/close-direction.yaml:96:          requests:
k8s/tenants/fullfunding/postgres.yaml:129:            requests:
k8s/tenants/fullfunding/postgres.yaml:133:            limits:
k8s/tenants/fullfunding/postgres.yaml:147:          requests:
k8s/es4/services/raw-to-display.yaml:52:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/raw-to-display.yaml:79:          limits:
k8s/es4/services/raw-to-display.yaml:81:          requests:
k8s/es4/services/raw-to-display.yaml:103:    requests:
k8s/infra/base/kafka-changelog-cleanup-cronjob.yaml:136:                requests:
k8s/infra/base/kafka-changelog-cleanup-cronjob.yaml:139:                limits:
k8s/es4/services/context-tape.yaml:93:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/context-tape.yaml:131:          limits:
k8s/es4/services/context-tape.yaml:133:          requests:
k8s/base/context-tape-deployment.yaml:64:            - name: JAVA_TOOL_OPTIONS
k8s/base/context-tape-deployment.yaml:91:            requests:
k8s/base/context-tape-deployment.yaml:94:            limits:
k8s/es4/services/greek-move-authenticity.yaml:55:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/greek-move-authenticity.yaml:86:          limits:
k8s/es4/services/greek-move-authenticity.yaml:88:          requests:
k8s/jobs/stock-gex-oi-snapshot-job.yaml:191:                  footprint is one parse at a time, never the loader's index plus a second full
k8s/jobs/stock-gex-oi-snapshot-job.yaml:419:            requests:
k8s/jobs/stock-gex-oi-snapshot-job.yaml:422:            limits:
k8s/base/databento-feed-deployment.yaml:85:            requests:
k8s/base/databento-feed-deployment.yaml:88:            limits:
k8s/services/unified-sr/overlays/production/manifest.yaml:79:            - name: JAVA_TOOL_OPTIONS
k8s/services/unified-sr/overlays/production/manifest.yaml:105:            limits:
k8s/services/unified-sr/overlays/production/manifest.yaml:107:            requests:
k8s/es4/services/databento-maxpain.yaml:69:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/databento-maxpain.yaml:96:          limits:
k8s/es4/services/databento-maxpain.yaml:98:          requests:
k8s/es4/services/databento-maxpain.yaml:120:    requests:
k8s/es4/services/dealer-ledger-calibration.yaml:47:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/dealer-ledger-calibration.yaml:71:          limits:
k8s/es4/services/dealer-ledger-calibration.yaml:73:          requests:
k8s/es4/services/dealer-ledger-calibration.yaml:141:          limits:
k8s/es4/services/dealer-ledger-calibration.yaml:143:          requests:
k8s/es4/services/databento-volume-aggregator.yaml:72:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/databento-volume-aggregator.yaml:99:          limits:
k8s/es4/services/databento-volume-aggregator.yaml:101:          requests:
k8s/es4/services/databento-volume-aggregator.yaml:123:    requests:
k8s/es4/services/es-aggressor-flow.yaml:48:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/es-aggressor-flow.yaml:97:          requests:
k8s/es4/services/es-aggressor-flow.yaml:100:          limits:
k8s/es4/services/indicator-service.yaml:69:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/indicator-service.yaml:96:          limits:
k8s/es4/services/indicator-service.yaml:98:          requests:
k8s/es4/services/es-feed.yaml:268:            requests: { cpu: "250m", memory: "512Mi" }
k8s/es4/services/es-feed.yaml:269:            limits:   { memory: "1536Mi" }
k8s/jobs/stock-gex-close-batch-job.yaml:121:            - name: JAVA_TOOL_OPTIONS
k8s/jobs/stock-gex-close-batch-job.yaml:149:            requests:
k8s/jobs/stock-gex-close-batch-job.yaml:154:            limits:
k8s/base/invasion-postgres-writer-deployment.yaml:42:            - name: JAVA_TOOL_OPTIONS
k8s/base/invasion-postgres-writer-deployment.yaml:61:            requests:
k8s/base/invasion-postgres-writer-deployment.yaml:64:            limits:
k8s/es4/services/option-truth-engine.yaml:66:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/option-truth-engine.yaml:93:          limits:
k8s/es4/services/option-truth-engine.yaml:95:          requests:
k8s/es4/services/vix-option-inteligence.yaml:70:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/vix-option-inteligence.yaml:97:          limits:
k8s/es4/services/vix-option-inteligence.yaml:99:          requests:
k8s/es4/services/dealer-ledger.yaml:61:        - name: KAFKA_DEALER_LEDGER_MAX_RECORD_BYTES
k8s/es4/services/dealer-ledger.yaml:63:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/dealer-ledger.yaml:132:          limits:
k8s/es4/services/dealer-ledger.yaml:134:          requests:
k8s/es4/services/es-amt.yaml:55:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/es-amt.yaml:116:          requests:
k8s/es4/services/es-amt.yaml:119:          limits:
k8s/es4/services/tape-zones.yaml:78:          limits:
k8s/es4/services/tape-zones.yaml:81:          requests:
k8s/es4/services/es-feed-gateway.yaml:109:        # ES Footprint relay (ES-FOOTPRINT-GATEWAY-DESIGN.md G-R1..G-R11). Same opt-in reason as the
k8s/es4/services/es-feed-gateway.yaml:111:        # es.futures.footprint*. These topics are NATIVE here: es-cvd-service publishes them on this
k8s/es4/services/es-feed-gateway.yaml:114:        # G-R8a is fail-closed — a topic whose effective max.message.bytes exceeds the ceiling or
k8s/es4/services/es-feed-gateway.yaml:121:        - name: GATEWAY_ES_FOOTPRINT_ENABLED
k8s/es4/services/es-feed-gateway.yaml:123:        - name: KAFKA_ES_FOOTPRINT_TOPIC
k8s/es4/services/es-feed-gateway.yaml:124:          value: futures.footprint
k8s/es4/services/es-feed-gateway.yaml:125:        - name: KAFKA_ES_FOOTPRINT_EVIDENCE_TOPIC
k8s/es4/services/es-feed-gateway.yaml:126:          value: futures.footprint.evidence
k8s/es4/services/es-feed-gateway.yaml:127:        - name: KAFKA_ES_FOOTPRINT_BARS_TOPIC
k8s/es4/services/es-feed-gateway.yaml:128:          value: futures.footprint.bars
k8s/es4/services/es-feed-gateway.yaml:129:        - name: KAFKA_ES_FOOTPRINT_OUTCOMES_TOPIC
k8s/es4/services/es-feed-gateway.yaml:130:          value: futures.footprint.outcomes
k8s/es4/services/es-feed-gateway.yaml:131:        # G-R8 requires this and the producer's FOOTPRINT_MAX_RECORD_BYTES to be ONE value, set
k8s/es4/services/es-feed-gateway.yaml:136:        - name: GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES
k8s/es4/services/es-feed-gateway.yaml:170:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/es-feed-gateway.yaml:228:        # taking it from the heap. G-R8 (options-edge-feed-gateway/ES-FOOTPRINT-GATEWAY-DESIGN.md)
k8s/es4/services/es-feed-gateway.yaml:230:        # so today with the footprint relay OFF. Fixed here on that ground alone.
k8s/es4/services/es-feed-gateway.yaml:232:        # Measured on es-feed-gateway-755c5544d5-6ftk6 (6h53m, CVD on, footprint off), from the
k8s/es4/services/es-feed-gateway.yaml:237:        # Only ONE G-R8 quantity is derivable from these: working set = memory.current -
k8s/es4/services/es-feed-gateway.yaml:240:        # H_peak is NOT derivable here. G-R8 defines it as peak jvm_memory_bytes_used{area="heap"},
k8s/es4/services/es-feed-gateway.yaml:250:        # NOT sufficient to enable the footprint relay. G-R8's deployment contingency requires
k8s/es4/services/es-feed-gateway.yaml:267:          requests:
k8s/es4/services/es-feed-gateway.yaml:270:          limits:
k8s/es4/services/strike-liquidity-heatmap.yaml:67:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/strike-liquidity-heatmap.yaml:94:          limits:
k8s/es4/services/strike-liquidity-heatmap.yaml:96:          requests:
k8s/es4/services/strike-liquidity-heatmap.yaml:118:    requests:
k8s/overlays/dev/databento-gex-preopen-dev-patch.yaml:180:            requests:
k8s/es4/services/es-cvd.yaml:50:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/es-cvd.yaml:115:        # ES Footprint subsystem (options-edge-processing ES-FOOTPRINT-DESIGN.md F-R14/F-R18).
k8s/es4/services/es-cvd.yaml:118:        # futures.cvd*: es.futures.footprint...) and exports es_footprint_*. They are stamped
k8s/es4/services/es-cvd.yaml:121:        - name: ES_FOOTPRINT_ENABLED
k8s/es4/services/es-cvd.yaml:123:        - name: KAFKA_ES_FOOTPRINT_BARS_TOPIC
k8s/es4/services/es-cvd.yaml:124:          value: futures.footprint.bars
k8s/es4/services/es-cvd.yaml:125:        - name: KAFKA_ES_FOOTPRINT_OUTCOMES_TOPIC
k8s/es4/services/es-cvd.yaml:126:          value: futures.footprint.outcomes
k8s/es4/services/es-cvd.yaml:127:        - name: KAFKA_ES_FOOTPRINT_TOPIC
k8s/es4/services/es-cvd.yaml:128:          value: futures.footprint
k8s/es4/services/es-cvd.yaml:129:        - name: KAFKA_ES_FOOTPRINT_EVIDENCE_TOPIC
k8s/es4/services/es-cvd.yaml:130:          value: futures.footprint.evidence
k8s/es4/services/es-cvd.yaml:131:        # G-R8 (options-edge-feed-gateway/ES-FOOTPRINT-GATEWAY-DESIGN.md) requires this and the
k8s/es4/services/es-cvd.yaml:132:        # gateway's GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES to be ONE value, set together: the gateway
k8s/es4/services/es-cvd.yaml:137:        - name: FOOTPRINT_MAX_RECORD_BYTES
k8s/es4/services/es-cvd.yaml:162:          requests:
k8s/es4/services/es-cvd.yaml:165:          limits:
k8s/es4/services/delta-flow.yaml:69:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/delta-flow.yaml:110:          limits:
k8s/es4/services/delta-flow.yaml:112:          requests:
k8s/services/volume-pace/overlays/dev/manifest.yaml:53:            - name: JAVA_TOOL_OPTIONS
k8s/services/volume-pace/overlays/dev/manifest.yaml:85:            limits:
k8s/services/volume-pace/overlays/dev/manifest.yaml:87:            requests:
k8s/base/greek-move-authenticity-deployment.yaml:47:            - name: JAVA_TOOL_OPTIONS
k8s/base/greek-move-authenticity-deployment.yaml:65:            requests:
k8s/base/greek-move-authenticity-deployment.yaml:68:            limits:
k8s/es4/services/gamma-migration.yaml:112:          limits:
k8s/es4/services/gamma-migration.yaml:114:          requests:
k8s/es4/services/strike-intelligence.yaml:83:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/strike-intelligence.yaml:119:          limits:
k8s/es4/services/strike-intelligence.yaml:121:          requests:
k8s/es4/services/spread-skew-postgres-writer.yaml:57:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/spread-skew-postgres-writer.yaml:84:          limits:
k8s/es4/services/spread-skew-postgres-writer.yaml:86:          requests:
k8s/es4/services/es-web.yaml:134:          requests:
k8s/es4/services/es-web.yaml:137:          limits:
k8s/es4/services/databento-gex.yaml:109:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/databento-gex.yaml:160:          limits:
k8s/es4/services/databento-gex.yaml:162:          requests:
k8s/es4/services/databento-gex.yaml:184:    requests:
k8s/es4/services/gex-delta-redis-writer.yaml:65:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/gex-delta-redis-writer.yaml:92:          limits:
k8s/es4/services/gex-delta-redis-writer.yaml:94:          requests:
k8s/es4/services/volume-pace.yaml:43:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/volume-pace.yaml:80:          limits:
k8s/es4/services/volume-pace.yaml:82:          requests:
k8s/es4/services/volume-pace.yaml:104:    requests:
k8s/es4/services/pin-postgres-writer.yaml:67:          limits:
k8s/es4/services/pin-postgres-writer.yaml:69:          requests:
k8s/es4/services/spread-skew.yaml:63:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/spread-skew.yaml:92:          limits:
k8s/es4/services/spread-skew.yaml:94:          requests:
k8s/services/volume-pace/overlays/experiment/manifest.yaml:51:            - name: JAVA_TOOL_OPTIONS
k8s/services/volume-pace/overlays/experiment/manifest.yaml:83:            limits:
k8s/services/volume-pace/overlays/experiment/manifest.yaml:85:            requests:
k8s/es4/services/strike-flow-avro-adapter.yaml:59:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/strike-flow-avro-adapter.yaml:86:          limits:
k8s/es4/services/strike-flow-avro-adapter.yaml:88:          requests:
k8s/bleedingoptions/app-postgres.yaml:174:            requests:
k8s/bleedingoptions/app-postgres.yaml:177:            limits:
k8s/bleedingoptions/app-postgres.yaml:223:          requests:
k8s/bleedingoptions/app-postgres.yaml:234:          requests:
k8s/overlays/dev/dealer-ledger-dev-env-patch.yaml:55:            - { name: KAFKA_DEALER_LEDGER_MAX_RECORD_BYTES, value: "16777216" }
k8s/es4/services/option-price-behavior.yaml:47:        - name: JAVA_TOOL_OPTIONS
k8s/es4/services/option-price-behavior.yaml:140:          limits:
k8s/es4/services/option-price-behavior.yaml:142:          requests:
k8s/es4/services/option-price-behavior.yaml:164:    requests:
k8s/base/kafka-streams-state-patches.yaml:18:            limits:
k8s/base/kafka-streams-state-patches.yaml:42:            limits:
k8s/base/kafka-streams-state-patches.yaml:66:            limits:
k8s/base/kafka-streams-state-patches.yaml:90:            limits:
k8s/base/kafka-streams-state-patches.yaml:114:            limits:
k8s/base/kafka-streams-state-patches.yaml:138:            limits:
k8s/base/kafka-streams-state-patches.yaml:162:            limits:
k8s/base/kafka-streams-state-patches.yaml:185:            limits:
k8s/base/kafka-streams-state-patches.yaml:211:            limits:
k8s/base/kafka-streams-state-patches.yaml:235:            limits:
k8s/bleedingoptions/backup-and-quota.yaml:90:                requests:
k8s/bleedingoptions/backup-and-quota.yaml:93:                limits:
k8s/bleedingoptions/backup-and-quota.yaml:146:                requests:
k8s/bleedingoptions/backup-and-quota.yaml:149:                limits:
k8s/bleedingoptions/backup-and-quota.yaml:197:  limits:
k8s/services/volume-pace/overlays/production/manifest.yaml:53:            - name: JAVA_TOOL_OPTIONS
k8s/services/volume-pace/overlays/production/manifest.yaml:85:            limits:
k8s/services/volume-pace/overlays/production/manifest.yaml:87:            requests:
k8s/base/delta-flow-deployment.yaml:56:            - name: JAVA_TOOL_OPTIONS
k8s/base/delta-flow-deployment.yaml:77:            requests:
k8s/base/delta-flow-deployment.yaml:80:            limits:
scripts/kafka/topics.env:58:#   es.futures.footprint
scripts/kafka/topics.env:59:#   es.futures.footprint.evidence
scripts/kafka/topics.env:60:#   es.futures.footprint.bars
scripts/kafka/topics.env:61:#   es.futures.footprint.outcomes
scripts/kafka/topics.env:193:OPTIONS_EDGE_RESET_REBUILDABLE_TOPICS="options.spx.spot-vol-regime.current options.indicators.bootstrap.control es.futures.cvd.bars es.futures.cvd.levels es.futures.footprint.bars es.futures.footprint.outcomes options.es-cvd-spx-levels"
scripts/kafka/topics.env:211:OPTIONS_EDGE_EXACT_PARTITION_TOPICS="spx.basis.state options.ibkr.gex.status options.ibkr.gex.strike.shadow es.options.indicators.snapshot.current es.options.indicators.bars es.futures.cvd es.futures.cvd.bars es.futures.footprint es.futures.footprint.evidence es.futures.footprint.bars es.futures.footprint.outcomes es.tape-zones.board es.futures.auction"
scripts/kafka/topics.env:236:OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES="es.futures.footprint.bars=-1 es.futures.footprint.outcomes=-1 es.futures.footprint=43200000 es.futures.footprint.evidence=43200000 options.spx.gamma-migration.crossings=-1 es.options.spx.gamma-migration.crossings=-1 options.spx.gamma-migration.wall-touch=-1 es.options.spx.gamma-migration.wall-touch=-1 options.spx.gamma-migration.fragility=-1 es.options.spx.gamma-migration.fragility=-1 corridor-gauge-event-log=-1 strike-liquidity-heatmap-dashboard=86400000 es.underlying.es.trades=86400000 es.drop.final-summary=-1 es.drop.outcome=-1 spx.drop.final-summary=-1 spx.drop.outcome=-1 es.drop.nowcast=604800000 spx.drop.nowcast=604800000 rates.sofr.databento-sr3-feed-service.history=1800000 spx.basis.state=-1 spx.basis.a1.evidence=259200000 options.spx.gamma-migration.scoring=-1 options.databento.oi.anchor-manifest=-1 options.spx.spot-vol-regime.current=-1 options.indicators.snapshot.current=604800000 options.indicators.bars=3888000000 options.indicators.bootstrap.control=-1 es.options.indicators.snapshot.current=604800000 es.options.indicators.bars=3888000000 es.tape-zones.board=-1 es.futures.cvd.bars=-1 es.futures.cvd=43200000 options.databento.gex.strike.preopen=259200000 es.futures.auction=604800000"
scripts/kafka/topics.env:286:OPTIONS_EDGE_TOPICS="$OPTIONS_EDGE_TOPICS es.options.indicators.snapshot.current:1 es.options.indicators.bars:8 es.futures.aggressor-flow:1 es.futures.cvd:1 es.futures.cvd.bars:1 es.futures.footprint:1 es.futures.footprint.evidence:1 es.futures.footprint.bars:1 es.futures.footprint.outcomes:1 es.tape-zones.board:1 es.strike-intelligence-by-strike:32 es.options.databento.gex.spxbridge:4 es.options.databento.gex.strike:4 option-price-behavior-dashboard:1 options.ibkr.unusualwhales.gex.strike:1 options.ibkr.unusualwhales.gex.strike.history:1 options.spx.strike-invasion.current:1 rates.sofr.databento-sr3-feed-service.current:1 signal-follower.hot-strike:1 strike-intelligence-turn-alert:1 corridor-gauge-state:1 corridor-gauge-event-log:4 underlying.spx.price:1 spx.drop.nowcast:1 spx.drop.final-summary:1 spx.drop.outcome:1"
scripts/kafka/topics.env:475:# The four ES FOOTPRINT topics are deliberately NOT here. es-cvd-service creates them with
scripts/kafka/topics.env:481:# the footprint topics applies last-write-per-key itself, so delete + a pinned retention below gives
scripts/kafka/topics.env:488:OPTIONS_EDGE_ES4_EXACT_PARTITION_TOPICS="es.reversal.verdicts es.reversal.strength es.reversal.final-summary es.reversal.outcome es.signal-follower.hot-strike es.futures.aggressor-flow es.futures.cvd es.futures.cvd.bars es.futures.cvd.levels es.futures.footprint.bars es.futures.footprint.outcomes es.futures.footprint es.futures.footprint.evidence es.close.direction.state es.heartbeats es.tape-zones.board es.options.indicators.snapshot.current es.options.indicators.bars es.futures.auction"
scripts/kafka/topics.env:513:# ES Footprint (options-edge-processing ES-FOOTPRINT-DESIGN.md F-R14): four single-partition topics the
scripts/kafka/topics.env:520:OPTIONS_EDGE_ES4_TOPICS="$OPTIONS_EDGE_ES4_TOPICS es.futures.footprint.bars:1 es.futures.footprint.outcomes:1 es.futures.footprint:1 es.futures.footprint.evidence:1"
scripts/kafka/topics.env:576:OPTIONS_EDGE_ES4_TOPIC_RETENTION_OVERRIDES="es.futures.cvd.bars=-1 es.futures.cvd=43200000 es.futures.cvd.levels=-1 es.futures.footprint.bars=-1 es.futures.footprint.outcomes=-1 es.futures.footprint=43200000 es.futures.footprint.evidence=43200000 es.reversal.verdicts=604800000 es.reversal.strength=172800000 es.options.indicators.snapshot.current=604800000 es.options.indicators.bars=3888000000 es.options.indicators.bootstrap.control=-1 es.signal-follower.hot-strike=604800000 es.tape-zones.board=-1 es.options.databento.gex.strike.preopen=259200000 es.futures.auction=604800000 es.underlying.es.trades=604800000"
k8s/base/databento-sr3-feed-deployment.yaml:67:            requests:
k8s/base/databento-sr3-feed-deployment.yaml:70:            limits:
k8s/base/vol-premium-deployment.yaml:195:            - name: JAVA_TOOL_OPTIONS
k8s/base/vol-premium-deployment.yaml:220:            requests:
k8s/base/vol-premium-deployment.yaml:223:            limits:
k8s/overlays/dev/option-price-behavior-dev-env-patch.yaml:18:            - { name: JAVA_TOOL_OPTIONS, value: "-Xms512m -Xmx3g" }
k8s/overlays/dev/databento-feed-dev-patch.yaml:46:            requests:
k8s/overlays/dev/databento-feed-dev-patch.yaml:49:            limits:
k8s/base/raw-postgres-writer-deployment.yaml:50:            requests:
k8s/base/raw-postgres-writer-deployment.yaml:53:            limits:
k8s/bleedingoptions/keycloak-deployment.yaml:191:            requests:
k8s/bleedingoptions/keycloak-deployment.yaml:194:            limits:
k8s/base/feed-gateway-deployment.yaml:61:            # ES FOOTPRINT relay (ES-FOOTPRINT-GATEWAY-DESIGN.md; es-cvd-service on es4, mirrored
k8s/base/feed-gateway-deployment.yaml:62:            # here as es.futures.footprint* by Jenkinsfile.es-cvd-mirror). Same opt-in shape as the
k8s/base/feed-gateway-deployment.yaml:66:            - name: GATEWAY_ES_FOOTPRINT_ENABLED
k8s/base/feed-gateway-deployment.yaml:68:            - name: KAFKA_ES_FOOTPRINT_TOPIC
k8s/base/feed-gateway-deployment.yaml:69:              value: es.futures.footprint
k8s/base/feed-gateway-deployment.yaml:70:            - name: KAFKA_ES_FOOTPRINT_EVIDENCE_TOPIC
k8s/base/feed-gateway-deployment.yaml:71:              value: es.futures.footprint.evidence
k8s/base/feed-gateway-deployment.yaml:72:            - name: KAFKA_ES_FOOTPRINT_BARS_TOPIC
k8s/base/feed-gateway-deployment.yaml:73:              value: es.futures.footprint.bars
k8s/base/feed-gateway-deployment.yaml:74:            - name: KAFKA_ES_FOOTPRINT_OUTCOMES_TOPIC
k8s/base/feed-gateway-deployment.yaml:75:              value: es.futures.footprint.outcomes
k8s/base/feed-gateway-deployment.yaml:87:            - name: JAVA_TOOL_OPTIONS
k8s/base/feed-gateway-deployment.yaml:136:            requests:
k8s/base/feed-gateway-deployment.yaml:139:            limits:
k8s/base/raw-to-display-databento-deployment.yaml:55:            - name: JAVA_TOOL_OPTIONS
k8s/base/raw-to-display-databento-deployment.yaml:76:            requests:
k8s/base/raw-to-display-databento-deployment.yaml:79:            limits:
k8s/base/oi-shadow-deployment.yaml:81:            - name: JAVA_TOOL_OPTIONS
k8s/base/oi-shadow-deployment.yaml:99:            requests:
k8s/base/oi-shadow-deployment.yaml:102:            limits:
k8s/services/strike-flow-avro-adapter/overlays/dev/manifest.yaml:69:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-flow-avro-adapter/overlays/dev/manifest.yaml:95:            limits:
k8s/services/strike-flow-avro-adapter/overlays/dev/manifest.yaml:97:            requests:
k8s/base/multileg-structure-deployment.yaml:81:            - name: JAVA_TOOL_OPTIONS
k8s/base/multileg-structure-deployment.yaml:104:            requests:
k8s/base/multileg-structure-deployment.yaml:107:            limits:
k8s/bleedingoptions/keycloak-postgres.yaml:143:            requests:
k8s/bleedingoptions/keycloak-postgres.yaml:146:            limits:
k8s/bleedingoptions/keycloak-postgres.yaml:163:          requests:
k8s/base/volume-pace-databento-deployment.yaml:50:            - name: JAVA_TOOL_OPTIONS
k8s/base/volume-pace-databento-deployment.yaml:80:            requests:
k8s/base/volume-pace-databento-deployment.yaml:83:            limits:
k8s/base/spread-skew-postgres-writer-deployment.yaml:46:            - name: JAVA_TOOL_OPTIONS
k8s/base/spread-skew-postgres-writer-deployment.yaml:65:            requests:
k8s/base/spread-skew-postgres-writer-deployment.yaml:68:            limits:
k8s/overlays/dev/kustomization.yaml:184:                  - name: JAVA_TOOL_OPTIONS
k8s/overlays/dev/kustomization.yaml:187:                  requests:
k8s/overlays/dev/kustomization.yaml:190:                  limits:
k8s/overlays/dev/kustomization.yaml:569:  # json6902): the prior index patch targeted env/19 for JAVA_TOOL_OPTIONS, but PR#364 inserted
k8s/overlays/dev/kustomization.yaml:571:  # QUOTE_STALE_MS (=> 30s default) and JAVA_TOOL_OPTIONS reverted to the 1GB base default. Merging env by
k8s/overlays/dev/kustomization.yaml:590:                  - name: JAVA_TOOL_OPTIONS
k8s/overlays/dev/kustomization.yaml:593:                  limits:
k8s/overlays/dev/kustomization.yaml:595:                  requests:
k8s/services/strike-flow-avro-adapter/overlays/experiment/manifest.yaml:67:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-flow-avro-adapter/overlays/experiment/manifest.yaml:93:            limits:
k8s/services/strike-flow-avro-adapter/overlays/experiment/manifest.yaml:95:            requests:
scripts/ops/offhours-clean-slate.sh:5:# before open, weekends, holidays) the env must hold a NEAR-ZERO footprint to fit
scripts/ops/offhours-clean-slate.sh:464:# job is a near-zero off-hours footprint. Silent disk growth would just be the other silent
scripts/ops/offhours-clean-slate.sh:484:#                              the single biggest footprint contributor, and pure session telemetry
k8s/base/unified-sr-deployment.yaml:56:            - name: JAVA_TOOL_OPTIONS
k8s/base/unified-sr-deployment.yaml:77:            requests:
k8s/base/unified-sr-deployment.yaml:80:            limits:
k8s/base/dealer-ledger-calibration-accumulator-deployment.yaml:61:            requests:
k8s/base/dealer-ledger-calibration-accumulator-deployment.yaml:64:            limits:
k8s/overlays/production/option-price-behavior-prod-patch.yaml:31:            - name: JAVA_TOOL_OPTIONS
k8s/overlays/production/option-price-behavior-prod-patch.yaml:47:            limits:
k8s/overlays/production/option-price-behavior-prod-patch.yaml:49:            requests:
k8s/base/close-direction-deployment.yaml:54:            - name: JAVA_TOOL_OPTIONS
k8s/base/close-direction-deployment.yaml:74:            requests:
k8s/base/close-direction-deployment.yaml:77:            limits:
k8s/base/strike-invasion-deployment.yaml:74:            - name: JAVA_TOOL_OPTIONS
k8s/base/strike-invasion-deployment.yaml:95:            requests:
k8s/base/strike-invasion-deployment.yaml:98:            limits:
k8s/base/dealer-ledger-deployment.yaml:149:            - name: JAVA_TOOL_OPTIONS
k8s/base/dealer-ledger-deployment.yaml:170:            requests:
k8s/base/dealer-ledger-deployment.yaml:185:            limits:
k8s/base/reversal-confirmation-deployment.yaml:127:            - name: JAVA_TOOL_OPTIONS
k8s/base/reversal-confirmation-deployment.yaml:146:            requests:
k8s/base/reversal-confirmation-deployment.yaml:149:            limits:
k8s/base/databento-gex-deployment.yaml:167:            - name: JAVA_TOOL_OPTIONS
k8s/base/databento-gex-deployment.yaml:199:            requests:
k8s/base/databento-gex-deployment.yaml:202:            limits:
k8s/base/raw-to-display-deployment.yaml:48:            - name: JAVA_TOOL_OPTIONS
k8s/base/raw-to-display-deployment.yaml:69:            requests:
k8s/base/raw-to-display-deployment.yaml:72:            limits:
k8s/base/databento-mission-sandwich-deployment.yaml:92:            - name: JAVA_TOOL_OPTIONS
k8s/base/databento-mission-sandwich-deployment.yaml:113:            requests:
k8s/base/databento-mission-sandwich-deployment.yaml:116:            limits:
k8s/services/strike-flow-avro-adapter/overlays/production/manifest.yaml:69:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-flow-avro-adapter/overlays/production/manifest.yaml:95:            limits:
k8s/services/strike-flow-avro-adapter/overlays/production/manifest.yaml:97:            requests:
k8s/base/databento-gex-history-deployment.yaml:55:            requests:
k8s/base/databento-gex-history-deployment.yaml:58:            limits:
k8s/base/gamma-migration-deployment.yaml:148:          # gone quiet, without which every expiry the pod ever saw would keep its full footprint.
k8s/base/gamma-migration-deployment.yaml:150:            requests:
k8s/base/gamma-migration-deployment.yaml:153:            limits:
k8s/base/corridor-gauge-deployment.yaml:57:            requests:
k8s/base/corridor-gauge-deployment.yaml:60:            limits:
k8s/base/strike-intelligence-deployment.yaml:79:            - name: JAVA_TOOL_OPTIONS
k8s/base/strike-intelligence-deployment.yaml:100:            requests:
k8s/base/strike-intelligence-deployment.yaml:103:            limits:
k8s/base/strike-liquidity-heatmap-deployment.yaml:55:            - name: JAVA_TOOL_OPTIONS
k8s/base/strike-liquidity-heatmap-deployment.yaml:81:            requests:
k8s/base/strike-liquidity-heatmap-deployment.yaml:84:            limits:
k8s/base/es-open-direction-postgres-writer-deployment.yaml:60:            - name: JAVA_TOOL_OPTIONS
k8s/base/es-open-direction-postgres-writer-deployment.yaml:78:            requests:
k8s/base/es-open-direction-postgres-writer-deployment.yaml:81:            limits:
k8s/base/es-open-direction-deployment.yaml:80:            - name: JAVA_TOOL_OPTIONS
k8s/base/es-open-direction-deployment.yaml:101:            requests:
k8s/base/es-open-direction-deployment.yaml:104:            limits:
k8s/base/es-spx-align-deployment.yaml:82:            - name: JAVA_TOOL_OPTIONS
k8s/base/es-spx-align-deployment.yaml:100:            requests:
k8s/base/es-spx-align-deployment.yaml:103:            limits:
k8s/base/stock-gex-deployment.yaml:13:  # limits: 10 connections/dataset).
k8s/base/stock-gex-deployment.yaml:105:            requests:
k8s/base/stock-gex-deployment.yaml:108:            limits:
k8s/base/strike-flow-classifier-deployment.yaml:135:            - name: JAVA_TOOL_OPTIONS
k8s/base/strike-flow-classifier-deployment.yaml:153:            requests:
k8s/base/strike-flow-classifier-deployment.yaml:158:              # burst — restore streams changelog batches into RocksDB, so its footprint is mostly
k8s/base/strike-flow-classifier-deployment.yaml:161:            limits:
k8s/base/signal-follower-es-deployment.yaml:84:            - name: JAVA_TOOL_OPTIONS
k8s/base/signal-follower-es-deployment.yaml:102:            requests:
k8s/base/signal-follower-es-deployment.yaml:105:            limits:
k8s/overlays/production/databento-feed-prod-patch.yaml:20:            requests:
k8s/base/indicator-service-deployment.yaml:62:            - name: JAVA_TOOL_OPTIONS
k8s/base/indicator-service-deployment.yaml:85:            requests:
k8s/base/indicator-service-deployment.yaml:88:            limits:
k8s/base/approach-position-publisher-deployment.yaml:78:            - name: JAVA_TOOL_OPTIONS
k8s/base/approach-position-publisher-deployment.yaml:100:            requests: { cpu: "50m", memory: "256Mi" }
k8s/base/approach-position-publisher-deployment.yaml:101:            limits: { memory: "512Mi" }
k8s/base/option-price-behavior-deployment.yaml:116:            - name: JAVA_TOOL_OPTIONS
k8s/base/option-price-behavior-deployment.yaml:137:            requests:
k8s/base/option-price-behavior-deployment.yaml:140:            limits:
k8s/base/spread-skew-deployment.yaml:57:            - name: JAVA_TOOL_OPTIONS
k8s/base/spread-skew-deployment.yaml:75:            requests:
k8s/base/spread-skew-deployment.yaml:78:            limits:
k8s/base/option-truth-engine-deployment.yaml:56:            - name: JAVA_TOOL_OPTIONS
k8s/base/option-truth-engine-deployment.yaml:74:            requests:
k8s/base/option-truth-engine-deployment.yaml:77:            limits:
k8s/base/directional-pressure-databento-deployment.yaml:67:            requests:
k8s/base/directional-pressure-databento-deployment.yaml:70:            limits:
k8s/services/bleedingoptions-gamma-lab/base/gamma-lab-deployment.yaml:206:            requests:
k8s/services/bleedingoptions-gamma-lab/base/gamma-lab-deployment.yaml:209:            limits:
scripts/ops/dev-cleanup.sh:473:    printf 'apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: %s\n  namespace: options-edge\n  labels:\n    app.kubernetes.io/component: kafka-streams-state\nspec:\n  accessModes: [ReadWriteOnce]\n  storageClassName: %s\n  resources:\n    requests:\n      storage: %s\n' "$pvc" "$sc" "$sz" | $K apply -f - >/dev/null 2>&1
k8s/base/databento-maxpain-deployment.yaml:67:            - name: JAVA_TOOL_OPTIONS
k8s/base/databento-maxpain-deployment.yaml:88:            requests:
k8s/base/databento-maxpain-deployment.yaml:91:            limits:
k8s/base/pressure-postgres-writer-deployment.yaml:45:            requests:
k8s/base/pressure-postgres-writer-deployment.yaml:48:            limits:
k8s/base/dealer-ledger-calibration-scorer-deployment.yaml:87:            - name: JAVA_TOOL_OPTIONS
k8s/base/dealer-ledger-calibration-scorer-deployment.yaml:105:            requests:
k8s/base/dealer-ledger-calibration-scorer-deployment.yaml:108:            limits:
k8s/base/spot-vol-regime-deployment.yaml:53:            - name: JAVA_TOOL_OPTIONS
k8s/base/spot-vol-regime-deployment.yaml:73:            requests:
k8s/base/spot-vol-regime-deployment.yaml:76:            limits:
k8s/base/feed-gateway-nightly-restart-cronjob.yaml:89:                requests:
k8s/base/feed-gateway-nightly-restart-cronjob.yaml:92:                limits:
k8s/base/spx-mission-control-deployment.yaml:66:            - name: JAVA_TOOL_OPTIONS
k8s/base/spx-mission-control-deployment.yaml:99:            requests:
k8s/base/spx-mission-control-deployment.yaml:102:            limits:
k8s/base/drop-classifier-deployment.yaml:85:            - name: JAVA_TOOL_OPTIONS
k8s/base/drop-classifier-deployment.yaml:108:            requests:
k8s/base/drop-classifier-deployment.yaml:111:            limits:
k8s/base/approach-monitor-deployment.yaml:117:            - name: JAVA_TOOL_OPTIONS
k8s/base/approach-monitor-deployment.yaml:141:            requests:
k8s/base/approach-monitor-deployment.yaml:144:            limits:
k8s/base/reversal-postgres-writer-deployment.yaml:67:            - name: JAVA_TOOL_OPTIONS
k8s/base/reversal-postgres-writer-deployment.yaml:85:            requests:
k8s/base/reversal-postgres-writer-deployment.yaml:88:            limits:
k8s/overlays/production/kustomization.yaml:241:                  - name: JAVA_TOOL_OPTIONS
k8s/overlays/production/kustomization.yaml:244:                  limits:
k8s/overlays/production/kustomization.yaml:246:                  requests:
k8s/overlays/production/kustomization.yaml:275:  # env/19 for JAVA_TOOL_OPTIONS, but PR#364 inserted DEALER_LEDGER_QUOTE_STALE_MS ahead of it, shifting the
k8s/overlays/production/kustomization.yaml:276:  # index so the heap string landed on QUOTE_STALE_MS (=> parsed as 0 -> 30s default) and JAVA_TOOL_OPTIONS
k8s/overlays/production/kustomization.yaml:330:                  - name: KAFKA_DEALER_LEDGER_MAX_RECORD_BYTES
k8s/overlays/production/kustomization.yaml:332:                  - name: JAVA_TOOL_OPTIONS
k8s/overlays/production/kustomization.yaml:335:                  limits:
k8s/overlays/production/kustomization.yaml:337:                  requests:
k8s/base/vix-option-inteligence-deployment.yaml:70:            - name: JAVA_TOOL_OPTIONS
k8s/base/vix-option-inteligence-deployment.yaml:87:            requests: { cpu: 500m, memory: 256Mi }
k8s/base/vix-option-inteligence-deployment.yaml:88:            limits: { memory: 512Mi }
k8s/services/es-spx-align/overlays/dev/manifest.yaml:61:            - name: JAVA_TOOL_OPTIONS
k8s/services/es-spx-align/overlays/dev/manifest.yaml:87:            limits:
k8s/services/es-spx-align/overlays/dev/manifest.yaml:89:            requests:
k8s/base/ibkr-feed-deployment.yaml:69:            requests:
k8s/base/ibkr-feed-deployment.yaml:72:            limits:
k8s/base/databento-volume-aggregator-deployment.yaml:67:            - name: JAVA_TOOL_OPTIONS
k8s/base/databento-volume-aggregator-deployment.yaml:88:            requests:
k8s/base/databento-volume-aggregator-deployment.yaml:91:            limits:
k8s/base/directional-pressure-deployment.yaml:64:            requests:
k8s/base/directional-pressure-deployment.yaml:67:            limits:
k8s/base/market-carry-deployment.yaml:63:            - name: JAVA_TOOL_OPTIONS
k8s/base/market-carry-deployment.yaml:81:            requests:
k8s/base/market-carry-deployment.yaml:84:            limits:
k8s/services/gamma-migration/overlays/dev/manifest.yaml:115:            limits:
k8s/services/gamma-migration/overlays/dev/manifest.yaml:117:            requests:
k8s/base/strike-flow-avro-adapter-deployment.yaml:44:            - name: JAVA_TOOL_OPTIONS
k8s/base/strike-flow-avro-adapter-deployment.yaml:65:            requests:
k8s/base/strike-flow-avro-adapter-deployment.yaml:68:            limits:
scripts/ops/archive/oe-topics.env:76:# 2026-09-07 addition: the ES Footprint history (ES-FOOTPRINT-DESIGN.md F-R14), same loss class.
scripts/ops/archive/oe-topics.env:77:#   es.futures.footprint.bars — retention.ms=-1, UNIQUE key per closed bar (symbol|timeframe|barStartMs),
scripts/ops/archive/oe-topics.env:79:#   es.futures.footprint.outcomes — retention.ms=-1, UNIQUE key per F-E6 resolution (the identity); every
scripts/ops/archive/oe-topics.env:82:#   es.futures.footprint and es.futures.footprint.evidence (the change-driven LIVE snapshots with a
scripts/ops/archive/oe-topics.env:96:OE_ES4_TOPICS="es.underlying.es.trades es.tape-zones.cells es.futures.cvd.bars es.futures.footprint.bars es.futures.footprint.outcomes es.futures.auction"
scripts/ops/archive/oe-topics.env:153:es.futures.footprint.bars:500 \
scripts/ops/archive/oe-topics.env:154:es.futures.footprint.outcomes:500 \
k8s/services/es-spx-align/overlays/experiment/manifest.yaml:59:            - name: JAVA_TOOL_OPTIONS
k8s/services/es-spx-align/overlays/experiment/manifest.yaml:85:            limits:
k8s/services/es-spx-align/overlays/experiment/manifest.yaml:87:            requests:
k8s/services/spx-mission-control/overlays/dev/manifest.yaml:101:            - name: JAVA_TOOL_OPTIONS
k8s/services/spx-mission-control/overlays/dev/manifest.yaml:129:            limits:
k8s/services/spx-mission-control/overlays/dev/manifest.yaml:131:            requests:
k8s/base/pin-postgres-writer-deployment.yaml:54:            requests:
k8s/base/pin-postgres-writer-deployment.yaml:57:            limits:
k8s/base/redis-deployment.yaml:55:            requests:
k8s/base/redis-deployment.yaml:58:            limits:
k8s/services/vol-premium/overlays/dev/manifest.yaml:81:            - name: JAVA_TOOL_OPTIONS
k8s/services/vol-premium/overlays/dev/manifest.yaml:107:            limits:
k8s/services/vol-premium/overlays/dev/manifest.yaml:109:            requests:
k8s/services/greek-move-authenticity/overlays/dev/manifest.yaml:63:            - name: JAVA_TOOL_OPTIONS
k8s/services/greek-move-authenticity/overlays/dev/manifest.yaml:89:            limits:
k8s/services/greek-move-authenticity/overlays/dev/manifest.yaml:91:            requests:
k8s/base/gex-delta-redis-writer-deployment.yaml:53:            - name: JAVA_TOOL_OPTIONS
k8s/base/gex-delta-redis-writer-deployment.yaml:71:            requests:
k8s/base/gex-delta-redis-writer-deployment.yaml:74:            limits:
k8s/services/vol-premium/overlays/experiment/manifest.yaml:79:            - name: JAVA_TOOL_OPTIONS
k8s/services/vol-premium/overlays/experiment/manifest.yaml:105:            limits:
k8s/services/vol-premium/overlays/experiment/manifest.yaml:107:            requests:
k8s/services/multileg-structure/overlays/dev/manifest.yaml:87:            - name: JAVA_TOOL_OPTIONS
k8s/services/multileg-structure/overlays/dev/manifest.yaml:114:            limits:
k8s/services/multileg-structure/overlays/dev/manifest.yaml:116:            requests:
k8s/services/gamma-migration/overlays/experiment/manifest.yaml:113:            limits:
k8s/services/gamma-migration/overlays/experiment/manifest.yaml:115:            requests:
k8s/services/gamma-migration/overlays/production/manifest.yaml:121:            limits:
k8s/services/gamma-migration/overlays/production/manifest.yaml:123:            requests:
k8s/services/spx-mission-control/overlays/experiment/manifest.yaml:99:            - name: JAVA_TOOL_OPTIONS
k8s/services/spx-mission-control/overlays/experiment/manifest.yaml:127:            limits:
k8s/services/spx-mission-control/overlays/experiment/manifest.yaml:129:            requests:
k8s/services/vol-premium/overlays/production/manifest.yaml:81:            - name: JAVA_TOOL_OPTIONS
k8s/services/vol-premium/overlays/production/manifest.yaml:107:            limits:
k8s/services/vol-premium/overlays/production/manifest.yaml:109:            requests:
k8s/services/multileg-structure/overlays/experiment/manifest.yaml:85:            - name: JAVA_TOOL_OPTIONS
k8s/services/multileg-structure/overlays/experiment/manifest.yaml:112:            limits:
k8s/services/multileg-structure/overlays/experiment/manifest.yaml:114:            requests:
k8s/services/raw-postgres-writer/overlays/dev/manifest.yaml:88:            limits:
k8s/services/raw-postgres-writer/overlays/dev/manifest.yaml:90:            requests:
k8s/services/approach-monitor/overlays/dev/manifest.yaml:75:            - name: JAVA_TOOL_OPTIONS
k8s/services/approach-monitor/overlays/dev/manifest.yaml:101:            limits:
k8s/services/approach-monitor/overlays/dev/manifest.yaml:103:            requests:
k8s/services/raw-postgres-writer/overlays/experiment/manifest.yaml:83:            limits:
k8s/services/raw-postgres-writer/overlays/experiment/manifest.yaml:85:            requests:
k8s/services/greek-move-authenticity/overlays/experiment/manifest.yaml:61:            - name: JAVA_TOOL_OPTIONS
k8s/services/greek-move-authenticity/overlays/experiment/manifest.yaml:87:            limits:
k8s/services/greek-move-authenticity/overlays/experiment/manifest.yaml:89:            requests:
k8s/services/drop-classifier/overlays/dev/manifest.yaml:91:            - name: JAVA_TOOL_OPTIONS
k8s/services/drop-classifier/overlays/dev/manifest.yaml:117:            limits:
k8s/services/drop-classifier/overlays/dev/manifest.yaml:119:            requests:
k8s/services/es-spx-align/overlays/production/manifest.yaml:61:            - name: JAVA_TOOL_OPTIONS
k8s/services/es-spx-align/overlays/production/manifest.yaml:87:            limits:
k8s/services/es-spx-align/overlays/production/manifest.yaml:89:            requests:
k8s/services/multileg-structure/overlays/production/manifest.yaml:87:            - name: JAVA_TOOL_OPTIONS
k8s/services/multileg-structure/overlays/production/manifest.yaml:114:            limits:
k8s/services/multileg-structure/overlays/production/manifest.yaml:116:            requests:
k8s/services/databento-maxpain/overlays/dev/manifest.yaml:79:            - name: JAVA_TOOL_OPTIONS
k8s/services/databento-maxpain/overlays/dev/manifest.yaml:105:            limits:
k8s/services/databento-maxpain/overlays/dev/manifest.yaml:107:            requests:
k8s/services/directional-pressure/overlays/dev/manifest.yaml:95:            limits:
k8s/services/directional-pressure/overlays/dev/manifest.yaml:97:            requests:
k8s/services/directional-pressure/overlays/dev/manifest.yaml:171:            limits:
k8s/services/directional-pressure/overlays/dev/manifest.yaml:173:            requests:
k8s/services/dealer-ledger-calibration/overlays/dev/manifest.yaml:57:            - name: JAVA_TOOL_OPTIONS
k8s/services/dealer-ledger-calibration/overlays/dev/manifest.yaml:80:            limits:
k8s/services/dealer-ledger-calibration/overlays/dev/manifest.yaml:82:            requests:
k8s/services/dealer-ledger-calibration/overlays/dev/manifest.yaml:149:            limits:
k8s/services/dealer-ledger-calibration/overlays/dev/manifest.yaml:151:            requests:
k8s/services/spx-mission-control/overlays/production/manifest.yaml:97:            - name: JAVA_TOOL_OPTIONS
k8s/services/spx-mission-control/overlays/production/manifest.yaml:129:            limits:
k8s/services/spx-mission-control/overlays/production/manifest.yaml:131:            requests:
k8s/services/dealer-ledger-calibration/overlays/experiment/manifest.yaml:55:            - name: JAVA_TOOL_OPTIONS
k8s/services/dealer-ledger-calibration/overlays/experiment/manifest.yaml:78:            limits:
k8s/services/dealer-ledger-calibration/overlays/experiment/manifest.yaml:80:            requests:
k8s/services/dealer-ledger-calibration/overlays/experiment/manifest.yaml:147:            limits:
k8s/services/dealer-ledger-calibration/overlays/experiment/manifest.yaml:149:            requests:
k8s/services/drop-classifier/overlays/experiment/manifest.yaml:89:            - name: JAVA_TOOL_OPTIONS
k8s/services/drop-classifier/overlays/experiment/manifest.yaml:115:            limits:
k8s/services/drop-classifier/overlays/experiment/manifest.yaml:117:            requests:
k8s/services/es-aggressor-flow/base/es-aggressor-flow-deployment.yaml:85:            - name: JAVA_TOOL_OPTIONS
k8s/services/es-aggressor-flow/base/es-aggressor-flow-deployment.yaml:105:            requests:
k8s/services/es-aggressor-flow/base/es-aggressor-flow-deployment.yaml:108:            limits:
k8s/services/reversal-postgres-writer/overlays/dev/manifest.yaml:84:            - name: JAVA_TOOL_OPTIONS
k8s/services/reversal-postgres-writer/overlays/dev/manifest.yaml:110:            limits:
k8s/services/reversal-postgres-writer/overlays/dev/manifest.yaml:112:            requests:
k8s/services/databento-maxpain/overlays/experiment/manifest.yaml:77:            - name: JAVA_TOOL_OPTIONS
k8s/services/databento-maxpain/overlays/experiment/manifest.yaml:103:            limits:
k8s/services/databento-maxpain/overlays/experiment/manifest.yaml:105:            requests:
scripts/ci/validate-mirrored-topic-contracts-test.sh:184:R="$(mkfixture)"; edit "$R" "$TENV_REL" '/^OPTIONS_EDGE_TOPIC_RETENTION_OVERRIDES=/s/ es\.futures\.footprint\.outcomes=-1/ es.futures.footprint.outcomes=43200000/'
scripts/ci/validate-mirrored-topic-contracts-test.sh:186:R="$(mkfixture)"; edit "$R" Jenkinsfile.es-cvd-mirror "s/'es\.futures\.footprint\.evidence'\], description:/'es.futures.footprint.evidence', description:/"
k8s/services/raw-postgres-writer/overlays/production/manifest.yaml:88:            limits:
k8s/services/raw-postgres-writer/overlays/production/manifest.yaml:90:            requests:
k8s/services/directional-pressure/overlays/experiment/manifest.yaml:93:            limits:
k8s/services/directional-pressure/overlays/experiment/manifest.yaml:95:            requests:
k8s/services/directional-pressure/overlays/experiment/manifest.yaml:169:            limits:
k8s/services/directional-pressure/overlays/experiment/manifest.yaml:171:            requests:
k8s/services/stock-gex-close-scheduler/base/scheduler-deployment.yaml:79:            - name: JAVA_TOOL_OPTIONS
k8s/services/stock-gex-close-scheduler/base/scheduler-deployment.yaml:87:            requests:
k8s/services/stock-gex-close-scheduler/base/scheduler-deployment.yaml:90:            limits:
k8s/services/es-aggressor-flow/overlays/production/manifest.yaml:79:            - name: JAVA_TOOL_OPTIONS
k8s/services/es-aggressor-flow/overlays/production/manifest.yaml:105:            limits:
k8s/services/es-aggressor-flow/overlays/production/manifest.yaml:107:            requests:
k8s/services/greek-move-authenticity/overlays/production/manifest.yaml:65:            - name: JAVA_TOOL_OPTIONS
k8s/services/greek-move-authenticity/overlays/production/manifest.yaml:91:            limits:
k8s/services/greek-move-authenticity/overlays/production/manifest.yaml:93:            requests:
k8s/services/directional-pressure/overlays/production/manifest.yaml:95:            limits:
k8s/services/directional-pressure/overlays/production/manifest.yaml:97:            requests:
k8s/services/directional-pressure/overlays/production/manifest.yaml:171:            limits:
k8s/services/directional-pressure/overlays/production/manifest.yaml:173:            requests:
k8s/services/drop-classifier/overlays/production/manifest.yaml:91:            - name: JAVA_TOOL_OPTIONS
k8s/services/drop-classifier/overlays/production/manifest.yaml:117:            limits:
k8s/services/drop-classifier/overlays/production/manifest.yaml:119:            requests:
k8s/services/indicator-service/overlays/dev/manifest.yaml:79:            - name: JAVA_TOOL_OPTIONS
k8s/services/indicator-service/overlays/dev/manifest.yaml:105:            limits:
k8s/services/indicator-service/overlays/dev/manifest.yaml:107:            requests:
k8s/services/databento-maxpain/overlays/production/manifest.yaml:79:            - name: JAVA_TOOL_OPTIONS
k8s/services/databento-maxpain/overlays/production/manifest.yaml:105:            limits:
k8s/services/databento-maxpain/overlays/production/manifest.yaml:107:            requests:
k8s/services/dealer-ledger-calibration/overlays/production/manifest.yaml:57:            - name: JAVA_TOOL_OPTIONS
k8s/services/dealer-ledger-calibration/overlays/production/manifest.yaml:80:            limits:
k8s/services/dealer-ledger-calibration/overlays/production/manifest.yaml:82:            requests:
k8s/services/dealer-ledger-calibration/overlays/production/manifest.yaml:149:            limits:
k8s/services/dealer-ledger-calibration/overlays/production/manifest.yaml:151:            requests:
k8s/services/market-carry/overlays/dev/manifest.yaml:54:            - name: JAVA_TOOL_OPTIONS
k8s/services/market-carry/overlays/dev/manifest.yaml:102:            limits:
k8s/services/market-carry/overlays/dev/manifest.yaml:104:            requests:
k8s/services/databento-volume-aggregator/overlays/dev/manifest.yaml:82:            - name: JAVA_TOOL_OPTIONS
k8s/services/databento-volume-aggregator/overlays/dev/manifest.yaml:108:            limits:
k8s/services/databento-volume-aggregator/overlays/dev/manifest.yaml:110:            requests:
k8s/services/close-direction/overlays/dev/manifest.yaml:75:            - name: JAVA_TOOL_OPTIONS
k8s/services/close-direction/overlays/dev/manifest.yaml:101:            limits:
k8s/services/close-direction/overlays/dev/manifest.yaml:103:            requests:
k8s/services/databento-sr3-feed/overlays/dev/manifest.yaml:96:            limits:
k8s/services/databento-sr3-feed/overlays/dev/manifest.yaml:98:            requests:
k8s/services/corridor-gauge/overlays/dev/manifest.yaml:76:            limits:
k8s/services/corridor-gauge/overlays/dev/manifest.yaml:78:            requests:
k8s/services/delta-flow/overlays/dev/manifest.yaml:81:            - name: JAVA_TOOL_OPTIONS
k8s/services/delta-flow/overlays/dev/manifest.yaml:107:            limits:
k8s/services/delta-flow/overlays/dev/manifest.yaml:109:            requests:
k8s/services/reversal-postgres-writer/overlays/experiment/manifest.yaml:82:            - name: JAVA_TOOL_OPTIONS
k8s/services/reversal-postgres-writer/overlays/experiment/manifest.yaml:108:            limits:
k8s/services/reversal-postgres-writer/overlays/experiment/manifest.yaml:110:            requests:
k8s/services/databento-sr3-feed/overlays/experiment/manifest.yaml:94:            limits:
k8s/services/databento-sr3-feed/overlays/experiment/manifest.yaml:96:            requests:
k8s/services/indicator-service/overlays/experiment/manifest.yaml:77:            - name: JAVA_TOOL_OPTIONS
k8s/services/indicator-service/overlays/experiment/manifest.yaml:103:            limits:
k8s/services/indicator-service/overlays/experiment/manifest.yaml:105:            requests:
k8s/services/reversal-postgres-writer/overlays/production/manifest.yaml:84:            - name: JAVA_TOOL_OPTIONS
k8s/services/reversal-postgres-writer/overlays/production/manifest.yaml:110:            limits:
k8s/services/reversal-postgres-writer/overlays/production/manifest.yaml:112:            requests:
k8s/services/spread-skew/overlays/dev/manifest.yaml:73:            - name: JAVA_TOOL_OPTIONS
k8s/services/spread-skew/overlays/dev/manifest.yaml:99:            limits:
k8s/services/spread-skew/overlays/dev/manifest.yaml:101:            requests:
k8s/services/tape-zones/overlays/production/manifest.yaml:96:            limits:
k8s/services/tape-zones/overlays/production/manifest.yaml:99:            requests:
k8s/services/close-direction/overlays/experiment/manifest.yaml:73:            - name: JAVA_TOOL_OPTIONS
k8s/services/close-direction/overlays/experiment/manifest.yaml:99:            limits:
k8s/services/close-direction/overlays/experiment/manifest.yaml:101:            requests:
k8s/services/delta-flow/overlays/experiment/manifest.yaml:77:            - name: JAVA_TOOL_OPTIONS
k8s/services/delta-flow/overlays/experiment/manifest.yaml:103:            limits:
k8s/services/delta-flow/overlays/experiment/manifest.yaml:105:            requests:
k8s/services/market-carry/overlays/experiment/manifest.yaml:74:            - name: JAVA_TOOL_OPTIONS
k8s/services/market-carry/overlays/experiment/manifest.yaml:100:            limits:
k8s/services/market-carry/overlays/experiment/manifest.yaml:102:            requests:
k8s/services/es-open-direction/overlays/dev/manifest.yaml:88:            - name: JAVA_TOOL_OPTIONS
k8s/services/es-open-direction/overlays/dev/manifest.yaml:114:            limits:
k8s/services/es-open-direction/overlays/dev/manifest.yaml:116:            requests:
k8s/services/databento-sr3-feed/overlays/production/manifest.yaml:96:            limits:
k8s/services/databento-sr3-feed/overlays/production/manifest.yaml:98:            requests:
k8s/services/spread-skew/overlays/experiment/manifest.yaml:71:            - name: JAVA_TOOL_OPTIONS
k8s/services/spread-skew/overlays/experiment/manifest.yaml:97:            limits:
k8s/services/spread-skew/overlays/experiment/manifest.yaml:99:            requests:
k8s/services/pressure-postgres-writer/overlays/dev/manifest.yaml:80:            limits:
k8s/services/pressure-postgres-writer/overlays/dev/manifest.yaml:82:            requests:
k8s/services/pressure-postgres-writer/overlays/experiment/manifest.yaml:78:            limits:
k8s/services/pressure-postgres-writer/overlays/experiment/manifest.yaml:80:            requests:
k8s/services/delta-flow/overlays/production/manifest.yaml:79:            - name: JAVA_TOOL_OPTIONS
k8s/services/delta-flow/overlays/production/manifest.yaml:105:            limits:
k8s/services/delta-flow/overlays/production/manifest.yaml:107:            requests:
k8s/services/indicator-service/overlays/production/manifest.yaml:79:            - name: JAVA_TOOL_OPTIONS
k8s/services/indicator-service/overlays/production/manifest.yaml:105:            limits:
k8s/services/indicator-service/overlays/production/manifest.yaml:107:            requests:
k8s/services/close-direction/overlays/production/manifest.yaml:75:            - name: JAVA_TOOL_OPTIONS
k8s/services/close-direction/overlays/production/manifest.yaml:101:            limits:
k8s/services/close-direction/overlays/production/manifest.yaml:103:            requests:
k8s/services/pressure-postgres-writer/overlays/production/manifest.yaml:80:            limits:
k8s/services/pressure-postgres-writer/overlays/production/manifest.yaml:82:            requests:
k8s/services/es-open-direction/overlays/experiment/manifest.yaml:86:            - name: JAVA_TOOL_OPTIONS
k8s/services/es-open-direction/overlays/experiment/manifest.yaml:112:            limits:
k8s/services/es-open-direction/overlays/experiment/manifest.yaml:114:            requests:
k8s/services/corridor-gauge/overlays/experiment/manifest.yaml:74:            limits:
k8s/services/corridor-gauge/overlays/experiment/manifest.yaml:76:            requests:
k8s/services/databento-volume-aggregator/overlays/experiment/manifest.yaml:80:            - name: JAVA_TOOL_OPTIONS
k8s/services/databento-volume-aggregator/overlays/experiment/manifest.yaml:106:            limits:
k8s/services/databento-volume-aggregator/overlays/experiment/manifest.yaml:108:            requests:
k8s/services/databento-volume-aggregator/overlays/production/manifest.yaml:82:            - name: JAVA_TOOL_OPTIONS
k8s/services/databento-volume-aggregator/overlays/production/manifest.yaml:108:            limits:
k8s/services/databento-volume-aggregator/overlays/production/manifest.yaml:110:            requests:
k8s/services/raw-to-display/overlays/dev/manifest.yaml:77:            - name: JAVA_TOOL_OPTIONS
k8s/services/raw-to-display/overlays/dev/manifest.yaml:103:            limits:
k8s/services/raw-to-display/overlays/dev/manifest.yaml:105:            requests:
k8s/services/raw-to-display/overlays/dev/manifest.yaml:161:            - name: JAVA_TOOL_OPTIONS
k8s/services/raw-to-display/overlays/dev/manifest.yaml:187:            limits:
k8s/services/raw-to-display/overlays/dev/manifest.yaml:189:            requests:
k8s/services/strike-intelligence/overlays/dev/manifest.yaml:93:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-intelligence/overlays/dev/manifest.yaml:131:            limits:
k8s/services/strike-intelligence/overlays/dev/manifest.yaml:133:            requests:
k8s/services/spread-skew/overlays/production/manifest.yaml:73:            - name: JAVA_TOOL_OPTIONS
k8s/services/spread-skew/overlays/production/manifest.yaml:99:            limits:
k8s/services/spread-skew/overlays/production/manifest.yaml:101:            requests:
k8s/services/market-carry/overlays/production/manifest.yaml:54:            - name: JAVA_TOOL_OPTIONS
k8s/services/market-carry/overlays/production/manifest.yaml:102:            limits:
k8s/services/market-carry/overlays/production/manifest.yaml:104:            requests:
k8s/services/corridor-gauge/overlays/production/manifest.yaml:76:            limits:
k8s/services/corridor-gauge/overlays/production/manifest.yaml:78:            requests:
k8s/services/vix-option-inteligence/overlays/dev/manifest.yaml:84:            - name: JAVA_TOOL_OPTIONS
k8s/services/vix-option-inteligence/overlays/dev/manifest.yaml:110:            limits:
k8s/services/vix-option-inteligence/overlays/dev/manifest.yaml:112:            requests:
k8s/services/oi-shadow/overlays/dev/manifest.yaml:83:            - name: JAVA_TOOL_OPTIONS
k8s/services/oi-shadow/overlays/dev/manifest.yaml:109:            limits:
k8s/services/oi-shadow/overlays/dev/manifest.yaml:111:            requests:
k8s/services/pin-postgres-writer/overlays/dev/manifest.yaml:76:            limits:
k8s/services/pin-postgres-writer/overlays/dev/manifest.yaml:78:            requests:
k8s/services/oi-shadow/overlays/experiment/manifest.yaml:79:            - name: JAVA_TOOL_OPTIONS
k8s/services/oi-shadow/overlays/experiment/manifest.yaml:105:            limits:
k8s/services/oi-shadow/overlays/experiment/manifest.yaml:107:            requests:
k8s/services/strike-intelligence/overlays/experiment/manifest.yaml:91:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-intelligence/overlays/experiment/manifest.yaml:117:            limits:
k8s/services/strike-intelligence/overlays/experiment/manifest.yaml:119:            requests:
k8s/services/raw-to-display/overlays/experiment/manifest.yaml:75:            - name: JAVA_TOOL_OPTIONS
k8s/services/raw-to-display/overlays/experiment/manifest.yaml:101:            limits:
k8s/services/raw-to-display/overlays/experiment/manifest.yaml:103:            requests:
k8s/services/raw-to-display/overlays/experiment/manifest.yaml:159:            - name: JAVA_TOOL_OPTIONS
k8s/services/raw-to-display/overlays/experiment/manifest.yaml:185:            limits:
k8s/services/raw-to-display/overlays/experiment/manifest.yaml:187:            requests:
k8s/services/pin-postgres-writer/overlays/experiment/manifest.yaml:74:            limits:
k8s/services/pin-postgres-writer/overlays/experiment/manifest.yaml:76:            requests:
k8s/services/databento-gex/overlays/dev/manifest.yaml:137:            - name: JAVA_TOOL_OPTIONS
k8s/services/databento-gex/overlays/dev/manifest.yaml:168:            limits:
k8s/services/databento-gex/overlays/dev/manifest.yaml:170:            requests:
k8s/services/es-open-direction/overlays/production/manifest.yaml:88:            - name: JAVA_TOOL_OPTIONS
k8s/services/es-open-direction/overlays/production/manifest.yaml:114:            limits:
k8s/services/es-open-direction/overlays/production/manifest.yaml:116:            requests:
k8s/services/pin-postgres-writer/overlays/production/manifest.yaml:76:            limits:
k8s/services/pin-postgres-writer/overlays/production/manifest.yaml:78:            requests:
k8s/services/vix-option-inteligence/overlays/experiment/manifest.yaml:80:            - name: JAVA_TOOL_OPTIONS
k8s/services/vix-option-inteligence/overlays/experiment/manifest.yaml:106:            limits:
k8s/services/vix-option-inteligence/overlays/experiment/manifest.yaml:108:            requests:
k8s/services/vix-option-inteligence/overlays/production/manifest.yaml:82:            - name: JAVA_TOOL_OPTIONS
k8s/services/vix-option-inteligence/overlays/production/manifest.yaml:108:            limits:
k8s/services/vix-option-inteligence/overlays/production/manifest.yaml:110:            requests:
k8s/services/raw-to-display/overlays/production/manifest.yaml:77:            - name: JAVA_TOOL_OPTIONS
k8s/services/raw-to-display/overlays/production/manifest.yaml:103:            limits:
k8s/services/raw-to-display/overlays/production/manifest.yaml:105:            requests:
k8s/services/raw-to-display/overlays/production/manifest.yaml:163:            - name: JAVA_TOOL_OPTIONS
k8s/services/raw-to-display/overlays/production/manifest.yaml:189:            limits:
k8s/services/raw-to-display/overlays/production/manifest.yaml:191:            requests:
k8s/services/option-truth-engine/overlays/dev/manifest.yaml:76:            - name: JAVA_TOOL_OPTIONS
k8s/services/option-truth-engine/overlays/dev/manifest.yaml:102:            limits:
k8s/services/option-truth-engine/overlays/dev/manifest.yaml:104:            requests:
k8s/services/strike-intelligence/overlays/production/manifest.yaml:93:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-intelligence/overlays/production/manifest.yaml:137:            limits:
k8s/services/strike-intelligence/overlays/production/manifest.yaml:139:            requests:
k8s/services/gamma-leadership/overlays/dev/manifest.yaml:71:            - name: JAVA_TOOL_OPTIONS
k8s/services/gamma-leadership/overlays/dev/manifest.yaml:97:            limits:
k8s/services/gamma-leadership/overlays/dev/manifest.yaml:99:            requests:
k8s/services/oi-shadow/overlays/production/manifest.yaml:83:            - name: JAVA_TOOL_OPTIONS
k8s/services/oi-shadow/overlays/production/manifest.yaml:109:            limits:
k8s/services/oi-shadow/overlays/production/manifest.yaml:111:            requests:
k8s/services/databento-gex/overlays/experiment/manifest.yaml:107:            - name: JAVA_TOOL_OPTIONS
k8s/services/databento-gex/overlays/experiment/manifest.yaml:142:            limits:
k8s/services/databento-gex/overlays/experiment/manifest.yaml:144:            requests:
k8s/services/reversal-confirmation/overlays/dev/manifest.yaml:117:            - name: JAVA_TOOL_OPTIONS
k8s/services/reversal-confirmation/overlays/dev/manifest.yaml:159:            limits:
k8s/services/reversal-confirmation/overlays/dev/manifest.yaml:161:            requests:
k8s/services/strike-invasion/overlays/dev/manifest.yaml:93:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-invasion/overlays/dev/manifest.yaml:119:            limits:
k8s/services/strike-invasion/overlays/dev/manifest.yaml:121:            requests:
k8s/services/reversal-confirmation/overlays/production/manifest.yaml:117:            - name: JAVA_TOOL_OPTIONS
k8s/services/reversal-confirmation/overlays/production/manifest.yaml:159:            limits:
k8s/services/reversal-confirmation/overlays/production/manifest.yaml:161:            requests:
k8s/services/stock-gex/overlays/dev/manifest.yaml:97:            limits:
k8s/services/stock-gex/overlays/dev/manifest.yaml:99:            requests:
k8s/services/gamma-leadership/overlays/experiment/manifest.yaml:69:            - name: JAVA_TOOL_OPTIONS
k8s/services/gamma-leadership/overlays/experiment/manifest.yaml:95:            limits:
k8s/services/gamma-leadership/overlays/experiment/manifest.yaml:97:            requests:
k8s/services/option-truth-engine/overlays/experiment/manifest.yaml:74:            - name: JAVA_TOOL_OPTIONS
k8s/services/option-truth-engine/overlays/experiment/manifest.yaml:100:            limits:
k8s/services/option-truth-engine/overlays/experiment/manifest.yaml:102:            requests:
k8s/services/strike-flow-classifier/overlays/dev/manifest.yaml:117:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-flow-classifier/overlays/dev/manifest.yaml:143:            limits:
k8s/services/strike-flow-classifier/overlays/dev/manifest.yaml:145:            requests:
k8s/services/reversal-confirmation/overlays/experiment/manifest.yaml:115:            - name: JAVA_TOOL_OPTIONS
k8s/services/reversal-confirmation/overlays/experiment/manifest.yaml:141:            limits:
k8s/services/reversal-confirmation/overlays/experiment/manifest.yaml:143:            requests:
k8s/services/strike-flow-classifier/overlays/experiment/manifest.yaml:115:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-flow-classifier/overlays/experiment/manifest.yaml:141:            limits:
k8s/services/strike-flow-classifier/overlays/experiment/manifest.yaml:143:            requests:
k8s/services/es-open-direction-postgres-writer/overlays/production/manifest.yaml:80:            - name: JAVA_TOOL_OPTIONS
k8s/services/es-open-direction-postgres-writer/overlays/production/manifest.yaml:106:            limits:
k8s/services/es-open-direction-postgres-writer/overlays/production/manifest.yaml:108:            requests:
k8s/services/es-open-direction-postgres-writer/overlays/dev/manifest.yaml:80:            - name: JAVA_TOOL_OPTIONS
k8s/services/es-open-direction-postgres-writer/overlays/dev/manifest.yaml:106:            limits:
k8s/services/es-open-direction-postgres-writer/overlays/dev/manifest.yaml:108:            requests:
k8s/services/strike-invasion/overlays/experiment/manifest.yaml:91:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-invasion/overlays/experiment/manifest.yaml:117:            limits:
k8s/services/strike-invasion/overlays/experiment/manifest.yaml:119:            requests:
k8s/services/gamma-leadership/overlays/production/manifest.yaml:71:            - name: JAVA_TOOL_OPTIONS
k8s/services/gamma-leadership/overlays/production/manifest.yaml:97:            limits:
k8s/services/gamma-leadership/overlays/production/manifest.yaml:99:            requests:
k8s/services/es-open-direction-postgres-writer/overlays/experiment/manifest.yaml:78:            - name: JAVA_TOOL_OPTIONS
k8s/services/es-open-direction-postgres-writer/overlays/experiment/manifest.yaml:104:            limits:
k8s/services/es-open-direction-postgres-writer/overlays/experiment/manifest.yaml:106:            requests:
k8s/services/strike-flow-classifier/overlays/production/manifest.yaml:117:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-flow-classifier/overlays/production/manifest.yaml:143:            limits:
k8s/services/strike-flow-classifier/overlays/production/manifest.yaml:145:            requests:
k8s/services/strike-invasion/overlays/production/manifest.yaml:93:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-invasion/overlays/production/manifest.yaml:119:            limits:
k8s/services/strike-invasion/overlays/production/manifest.yaml:121:            requests:
k8s/services/databento-gex/overlays/production/manifest.yaml:129:            - name: JAVA_TOOL_OPTIONS
k8s/services/databento-gex/overlays/production/manifest.yaml:162:            limits:
k8s/services/databento-gex/overlays/production/manifest.yaml:164:            requests:
k8s/services/short-premium-agent/base/short-premium-agent-deployment.yaml:114:            requests: {cpu: 50m, memory: 256Mi}
k8s/services/short-premium-agent/base/short-premium-agent-deployment.yaml:115:            limits:   {memory: 512Mi}
k8s/services/short-premium-agent/base/short-premium-agent-deployment.yaml:143:            requests: {cpu: 25m, memory: 128Mi}
k8s/services/short-premium-agent/base/short-premium-agent-deployment.yaml:144:            limits:   {memory: 256Mi}
k8s/services/feed-gateway/overlays/dev/manifest.yaml:135:            - name: GATEWAY_ES_FOOTPRINT_ENABLED
k8s/services/feed-gateway/overlays/dev/manifest.yaml:137:            - name: KAFKA_ES_FOOTPRINT_TOPIC
k8s/services/feed-gateway/overlays/dev/manifest.yaml:138:              value: es.futures.footprint
k8s/services/feed-gateway/overlays/dev/manifest.yaml:139:            - name: KAFKA_ES_FOOTPRINT_EVIDENCE_TOPIC
k8s/services/feed-gateway/overlays/dev/manifest.yaml:140:              value: es.futures.footprint.evidence
k8s/services/feed-gateway/overlays/dev/manifest.yaml:141:            - name: KAFKA_ES_FOOTPRINT_BARS_TOPIC
k8s/services/feed-gateway/overlays/dev/manifest.yaml:142:              value: es.futures.footprint.bars
k8s/services/feed-gateway/overlays/dev/manifest.yaml:143:            - name: KAFKA_ES_FOOTPRINT_OUTCOMES_TOPIC
k8s/services/feed-gateway/overlays/dev/manifest.yaml:144:              value: es.futures.footprint.outcomes
k8s/services/feed-gateway/overlays/dev/manifest.yaml:151:            - name: JAVA_TOOL_OPTIONS
k8s/services/feed-gateway/overlays/dev/manifest.yaml:193:            limits:
k8s/services/feed-gateway/overlays/dev/manifest.yaml:195:            requests:
k8s/services/option-truth-engine/overlays/production/manifest.yaml:76:            - name: JAVA_TOOL_OPTIONS
k8s/services/option-truth-engine/overlays/production/manifest.yaml:102:            limits:
k8s/services/option-truth-engine/overlays/production/manifest.yaml:104:            requests:
k8s/services/stock-gex/overlays/experiment/manifest.yaml:95:            limits:
k8s/services/stock-gex/overlays/experiment/manifest.yaml:97:            requests:
k8s/services/databento-gex-history/overlays/dev/manifest.yaml:85:            limits:
k8s/services/databento-gex-history/overlays/dev/manifest.yaml:87:            requests:
k8s/services/ibkr-feed/overlays/dev/manifest.yaml:115:            limits:
k8s/services/ibkr-feed/overlays/dev/manifest.yaml:117:            requests:
k8s/services/invasion-postgres-writer/overlays/dev/manifest.yaml:65:            - name: JAVA_TOOL_OPTIONS
k8s/services/invasion-postgres-writer/overlays/dev/manifest.yaml:91:            limits:
k8s/services/invasion-postgres-writer/overlays/dev/manifest.yaml:93:            requests:
k8s/services/ibkr-feed/overlays/production/manifest.yaml:84:            limits:
k8s/services/ibkr-feed/overlays/production/manifest.yaml:86:            requests:
k8s/services/web/base/options-edge-web-deployment.yaml:54:            - name: JAVA_TOOL_OPTIONS
k8s/services/web/base/options-edge-web-deployment.yaml:138:            requests:
k8s/services/web/base/options-edge-web-deployment.yaml:141:            limits:
k8s/services/fullfunding-app/overlays/production/manifest.yaml:69:            requests: { cpu: 25m,  memory: 48Mi,  ephemeral-storage: 16Mi }
k8s/services/fullfunding-app/overlays/production/manifest.yaml:70:            limits:   { cpu: 300m, memory: 192Mi, ephemeral-storage: 128Mi }
k8s/services/databento-mission-sandwich/overlays/dev/manifest.yaml:116:            - name: JAVA_TOOL_OPTIONS
k8s/services/databento-mission-sandwich/overlays/dev/manifest.yaml:142:            limits:
k8s/services/databento-mission-sandwich/overlays/dev/manifest.yaml:144:            requests:
k8s/services/short-premium-agent/overlays/dev/manifest.yaml:117:            limits:
k8s/services/short-premium-agent/overlays/dev/manifest.yaml:119:            requests:
k8s/services/short-premium-agent/overlays/dev/manifest.yaml:161:            limits:
k8s/services/short-premium-agent/overlays/dev/manifest.yaml:163:            requests:
k8s/services/stock-gex/overlays/production/manifest.yaml:99:            limits:
k8s/services/stock-gex/overlays/production/manifest.yaml:101:            requests:
k8s/services/bleedingoptions-email/base/email-deployment.yaml:163:            requests:
k8s/services/bleedingoptions-email/base/email-deployment.yaml:166:            limits:
k8s/services/dealer-ledger/overlays/dev/manifest.yaml:61:            - name: JAVA_TOOL_OPTIONS
k8s/services/dealer-ledger/overlays/dev/manifest.yaml:113:            - name: KAFKA_DEALER_LEDGER_MAX_RECORD_BYTES
k8s/services/dealer-ledger/overlays/dev/manifest.yaml:183:            limits:
k8s/services/dealer-ledger/overlays/dev/manifest.yaml:185:            requests:
k8s/services/invasion-postgres-writer/overlays/experiment/manifest.yaml:63:            - name: JAVA_TOOL_OPTIONS
k8s/services/invasion-postgres-writer/overlays/experiment/manifest.yaml:89:            limits:
k8s/services/invasion-postgres-writer/overlays/experiment/manifest.yaml:91:            requests:
k8s/services/dealer-ledger/overlays/experiment/manifest.yaml:101:            - name: JAVA_TOOL_OPTIONS
k8s/services/dealer-ledger/overlays/experiment/manifest.yaml:127:            limits:
k8s/services/dealer-ledger/overlays/experiment/manifest.yaml:129:            requests:
k8s/services/databento-mission-sandwich/overlays/experiment/manifest.yaml:114:            - name: JAVA_TOOL_OPTIONS
k8s/services/databento-mission-sandwich/overlays/experiment/manifest.yaml:140:            limits:
k8s/services/databento-mission-sandwich/overlays/experiment/manifest.yaml:142:            requests:
k8s/services/approach-position-publisher/overlays/dev/manifest.yaml:52:            - name: JAVA_TOOL_OPTIONS
k8s/services/approach-position-publisher/overlays/dev/manifest.yaml:80:            limits:
k8s/services/approach-position-publisher/overlays/dev/manifest.yaml:82:            requests:
k8s/services/dealer-ledger/overlays/production/manifest.yaml:71:            - name: KAFKA_DEALER_LEDGER_MAX_RECORD_BYTES
k8s/services/dealer-ledger/overlays/production/manifest.yaml:73:            - name: JAVA_TOOL_OPTIONS
k8s/services/dealer-ledger/overlays/production/manifest.yaml:141:            limits:
k8s/services/dealer-ledger/overlays/production/manifest.yaml:143:            requests:
k8s/services/short-premium-agent/overlays/production/manifest.yaml:117:            limits:
k8s/services/short-premium-agent/overlays/production/manifest.yaml:119:            requests:
k8s/services/short-premium-agent/overlays/production/manifest.yaml:161:            limits:
k8s/services/short-premium-agent/overlays/production/manifest.yaml:163:            requests:
k8s/services/invasion-postgres-writer/overlays/production/manifest.yaml:65:            - name: JAVA_TOOL_OPTIONS
k8s/services/invasion-postgres-writer/overlays/production/manifest.yaml:91:            limits:
k8s/services/invasion-postgres-writer/overlays/production/manifest.yaml:93:            requests:
k8s/services/databento-gex-history/overlays/experiment/manifest.yaml:83:            limits:
k8s/services/databento-gex-history/overlays/experiment/manifest.yaml:85:            requests:
k8s/services/databento-mission-sandwich/overlays/production/manifest.yaml:116:            - name: JAVA_TOOL_OPTIONS
k8s/services/databento-mission-sandwich/overlays/production/manifest.yaml:142:            limits:
k8s/services/databento-mission-sandwich/overlays/production/manifest.yaml:144:            requests:
k8s/services/databento-feed/overlays/dev/manifest.yaml:109:            limits:
k8s/services/databento-feed/overlays/dev/manifest.yaml:111:            requests:
k8s/services/databento-feed/overlays/experiment/manifest.yaml:91:            limits:
k8s/services/databento-feed/overlays/experiment/manifest.yaml:93:            requests:
k8s/services/feed-gateway/overlays/production/manifest.yaml:107:            - name: GATEWAY_ES_FOOTPRINT_ENABLED
k8s/services/feed-gateway/overlays/production/manifest.yaml:109:            - name: KAFKA_ES_FOOTPRINT_TOPIC
k8s/services/feed-gateway/overlays/production/manifest.yaml:110:              value: es.futures.footprint
k8s/services/feed-gateway/overlays/production/manifest.yaml:111:            - name: KAFKA_ES_FOOTPRINT_EVIDENCE_TOPIC
k8s/services/feed-gateway/overlays/production/manifest.yaml:112:              value: es.futures.footprint.evidence
k8s/services/feed-gateway/overlays/production/manifest.yaml:113:            - name: KAFKA_ES_FOOTPRINT_BARS_TOPIC
k8s/services/feed-gateway/overlays/production/manifest.yaml:114:              value: es.futures.footprint.bars
k8s/services/feed-gateway/overlays/production/manifest.yaml:115:            - name: KAFKA_ES_FOOTPRINT_OUTCOMES_TOPIC
k8s/services/feed-gateway/overlays/production/manifest.yaml:116:              value: es.futures.footprint.outcomes
k8s/services/feed-gateway/overlays/production/manifest.yaml:123:            - name: JAVA_TOOL_OPTIONS
k8s/services/feed-gateway/overlays/production/manifest.yaml:167:            limits:
k8s/services/feed-gateway/overlays/production/manifest.yaml:169:            requests:
k8s/services/nifty-gex/base/nifty-gex-deployment.yaml:50:            requests:
k8s/services/nifty-gex/base/nifty-gex-deployment.yaml:53:            limits:
k8s/services/signal-follower/overlays/dev/manifest.yaml:84:            - name: JAVA_TOOL_OPTIONS
k8s/services/signal-follower/overlays/dev/manifest.yaml:110:            limits:
k8s/services/signal-follower/overlays/dev/manifest.yaml:112:            requests:
k8s/services/signal-follower/overlays/dev/manifest.yaml:195:            - name: JAVA_TOOL_OPTIONS
k8s/services/signal-follower/overlays/dev/manifest.yaml:221:            limits:
k8s/services/signal-follower/overlays/dev/manifest.yaml:223:            requests:
k8s/services/databento-gex-history/overlays/production/manifest.yaml:85:            limits:
k8s/services/databento-gex-history/overlays/production/manifest.yaml:87:            requests:
k8s/services/strike-liquidity-heatmap/overlays/experiment/manifest.yaml:75:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-liquidity-heatmap/overlays/experiment/manifest.yaml:101:            limits:
k8s/services/strike-liquidity-heatmap/overlays/experiment/manifest.yaml:103:            requests:
k8s/services/databento-feed/overlays/production/manifest.yaml:109:            limits:
k8s/services/databento-feed/overlays/production/manifest.yaml:111:            requests:
k8s/services/option-price-behavior/overlays/dev/manifest.yaml:59:            - name: JAVA_TOOL_OPTIONS
k8s/services/option-price-behavior/overlays/dev/manifest.yaml:147:            limits:
k8s/services/option-price-behavior/overlays/dev/manifest.yaml:149:            requests:
k8s/services/strike-liquidity-heatmap/overlays/production/manifest.yaml:77:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-liquidity-heatmap/overlays/production/manifest.yaml:103:            limits:
k8s/services/strike-liquidity-heatmap/overlays/production/manifest.yaml:105:            requests:
k8s/services/option-price-behavior/overlays/production/manifest.yaml:57:            - name: JAVA_TOOL_OPTIONS
k8s/services/option-price-behavior/overlays/production/manifest.yaml:149:            limits:
k8s/services/option-price-behavior/overlays/production/manifest.yaml:151:            requests:
k8s/services/signal-follower/overlays/experiment/manifest.yaml:82:            - name: JAVA_TOOL_OPTIONS
k8s/services/signal-follower/overlays/experiment/manifest.yaml:108:            limits:
k8s/services/signal-follower/overlays/experiment/manifest.yaml:110:            requests:
k8s/services/signal-follower/overlays/experiment/manifest.yaml:193:            - name: JAVA_TOOL_OPTIONS
k8s/services/signal-follower/overlays/experiment/manifest.yaml:219:            limits:
k8s/services/signal-follower/overlays/experiment/manifest.yaml:221:            requests:
k8s/services/feed-gateway/overlays/experiment/manifest.yaml:98:            - name: GATEWAY_ES_FOOTPRINT_ENABLED
k8s/services/feed-gateway/overlays/experiment/manifest.yaml:100:            - name: KAFKA_ES_FOOTPRINT_TOPIC
k8s/services/feed-gateway/overlays/experiment/manifest.yaml:101:              value: es.futures.footprint
k8s/services/feed-gateway/overlays/experiment/manifest.yaml:102:            - name: KAFKA_ES_FOOTPRINT_EVIDENCE_TOPIC
k8s/services/feed-gateway/overlays/experiment/manifest.yaml:103:              value: es.futures.footprint.evidence
k8s/services/feed-gateway/overlays/experiment/manifest.yaml:104:            - name: KAFKA_ES_FOOTPRINT_BARS_TOPIC
k8s/services/feed-gateway/overlays/experiment/manifest.yaml:105:              value: es.futures.footprint.bars
k8s/services/feed-gateway/overlays/experiment/manifest.yaml:106:            - name: KAFKA_ES_FOOTPRINT_OUTCOMES_TOPIC
k8s/services/feed-gateway/overlays/experiment/manifest.yaml:107:              value: es.futures.footprint.outcomes
k8s/services/feed-gateway/overlays/experiment/manifest.yaml:114:            - name: JAVA_TOOL_OPTIONS
k8s/services/feed-gateway/overlays/experiment/manifest.yaml:154:            limits:
k8s/services/feed-gateway/overlays/experiment/manifest.yaml:156:            requests:
k8s/services/strike-liquidity-heatmap/overlays/dev/manifest.yaml:77:            - name: JAVA_TOOL_OPTIONS
k8s/services/strike-liquidity-heatmap/overlays/dev/manifest.yaml:103:            limits:
k8s/services/strike-liquidity-heatmap/overlays/dev/manifest.yaml:105:            requests:
k8s/services/databento-vix-feed/base/databento-vix-feed-deployment.yaml:146:            # bounded by the VIX state's known small footprint (134 legs). Post-shadow,
k8s/services/databento-vix-feed/base/databento-vix-feed-deployment.yaml:148:            requests: { cpu: "250m", memory: "256Mi" }
k8s/services/databento-vix-feed/base/databento-vix-feed-deployment.yaml:149:            limits:   { memory: "1Gi" }
k8s/services/spread-skew-postgres-writer/overlays/production/manifest.yaml:67:            - name: JAVA_TOOL_OPTIONS
k8s/services/spread-skew-postgres-writer/overlays/production/manifest.yaml:93:            limits:
k8s/services/spread-skew-postgres-writer/overlays/production/manifest.yaml:95:            requests:
k8s/services/databento-vix-feed/overlays/dev/manifest.yaml:106:            limits:
k8s/services/databento-vix-feed/overlays/dev/manifest.yaml:108:            requests:
k8s/services/spot-vol-regime/overlays/dev/manifest.yaml:75:            - name: JAVA_TOOL_OPTIONS
k8s/services/spot-vol-regime/overlays/dev/manifest.yaml:101:            limits:
k8s/services/spot-vol-regime/overlays/dev/manifest.yaml:103:            requests:
k8s/services/context-tape/overlays/dev/manifest.yaml:103:            - name: JAVA_TOOL_OPTIONS
k8s/services/context-tape/overlays/dev/manifest.yaml:134:            limits:
k8s/services/context-tape/overlays/dev/manifest.yaml:136:            requests:
k8s/services/spot-vol-regime/overlays/experiment/manifest.yaml:73:            - name: JAVA_TOOL_OPTIONS
k8s/services/spot-vol-regime/overlays/experiment/manifest.yaml:99:            limits:
k8s/services/spot-vol-regime/overlays/experiment/manifest.yaml:101:            requests:
k8s/services/spread-skew-postgres-writer/overlays/dev/manifest.yaml:67:            - name: JAVA_TOOL_OPTIONS
k8s/services/spread-skew-postgres-writer/overlays/dev/manifest.yaml:93:            limits:
k8s/services/spread-skew-postgres-writer/overlays/dev/manifest.yaml:95:            requests:
k8s/services/context-tape/overlays/experiment/manifest.yaml:78:            - name: JAVA_TOOL_OPTIONS
k8s/services/context-tape/overlays/experiment/manifest.yaml:107:            limits:
k8s/services/context-tape/overlays/experiment/manifest.yaml:109:            requests:
k8s/services/signal-follower/overlays/production/manifest.yaml:84:            - name: JAVA_TOOL_OPTIONS
k8s/services/signal-follower/overlays/production/manifest.yaml:110:            limits:
k8s/services/signal-follower/overlays/production/manifest.yaml:112:            requests:
k8s/services/signal-follower/overlays/production/manifest.yaml:195:            - name: JAVA_TOOL_OPTIONS
k8s/services/signal-follower/overlays/production/manifest.yaml:221:            limits:
k8s/services/signal-follower/overlays/production/manifest.yaml:223:            requests:
k8s/services/gex-delta-redis-writer/overlays/experiment/manifest.yaml:73:            - name: JAVA_TOOL_OPTIONS
k8s/services/gex-delta-redis-writer/overlays/experiment/manifest.yaml:99:            limits:
k8s/services/gex-delta-redis-writer/overlays/experiment/manifest.yaml:101:            requests:
k8s/services/spread-skew-postgres-writer/overlays/experiment/manifest.yaml:65:            - name: JAVA_TOOL_OPTIONS
k8s/services/spread-skew-postgres-writer/overlays/experiment/manifest.yaml:91:            limits:
k8s/services/spread-skew-postgres-writer/overlays/experiment/manifest.yaml:93:            requests:
k8s/services/context-tape/overlays/production/manifest.yaml:103:            - name: JAVA_TOOL_OPTIONS
k8s/services/context-tape/overlays/production/manifest.yaml:134:            limits:
k8s/services/context-tape/overlays/production/manifest.yaml:136:            requests:
k8s/services/gex-delta-redis-writer/overlays/production/manifest.yaml:75:            - name: JAVA_TOOL_OPTIONS
k8s/services/gex-delta-redis-writer/overlays/production/manifest.yaml:101:            limits:
k8s/services/gex-delta-redis-writer/overlays/production/manifest.yaml:103:            requests:
k8s/services/gex-delta-redis-writer/overlays/dev/manifest.yaml:75:            - name: JAVA_TOOL_OPTIONS
k8s/services/gex-delta-redis-writer/overlays/dev/manifest.yaml:101:            limits:
k8s/services/gex-delta-redis-writer/overlays/dev/manifest.yaml:103:            requests:
k8s/services/option-price-behavior/overlays/experiment/manifest.yaml:117:            - name: JAVA_TOOL_OPTIONS
k8s/services/option-price-behavior/overlays/experiment/manifest.yaml:143:            limits:
k8s/services/option-price-behavior/overlays/experiment/manifest.yaml:145:            requests:
k8s/services/databento-vix-feed/overlays/production/manifest.yaml:104:            limits:
k8s/services/databento-vix-feed/overlays/production/manifest.yaml:106:            requests:
k8s/services/spot-vol-regime/overlays/production/manifest.yaml:75:            - name: JAVA_TOOL_OPTIONS
k8s/services/spot-vol-regime/overlays/production/manifest.yaml:101:            limits:
k8s/services/spot-vol-regime/overlays/production/manifest.yaml:103:            requests:

exec
/bin/bash -lc "nl -ba k8s/es4/services/es-cvd.yaml | sed -n '40,190p'; nl -ba .github/workflows/deploy-validation.yml | sed -n '1,260p'; rg -n -C 3 \"validate-footprint|paths:|paths-ignore:|pull_request:|push:\" .github/workflows/deploy-validation.yml; rg -n \"262144|FOOTPRINT_MAX_RECORD_BYTES|GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES|contingency\" k8s/es4 scripts .github Jenkinsfile* README* docs 2>/dev/null" in /private/tmp/oe-deploy-mirror
 succeeded in 39ms:
.github/workflows/deploy-validation.yml:84:      - name: ES Footprint deployment contingency (paired ceiling, G-R8 headroom)
.github/workflows/deploy-validation.yml:85:        run: scripts/ci/validate-footprint-gateway-contingency.sh
.github/workflows/deploy-validation.yml:87:      - name: ES Footprint contingency guard refuses each of its own violations
.github/workflows/deploy-validation.yml:88:        run: scripts/ci/validate-footprint-gateway-contingency-test.sh
k8s/es4/services/es-feed-gateway.yaml:131:        # G-R8 requires this and the producer's FOOTPRINT_MAX_RECORD_BYTES to be ONE value, set
k8s/es4/services/es-feed-gateway.yaml:132:        # together. Both defaults are 262144, so they agreed only by coincidence of two independent
k8s/es4/services/es-feed-gateway.yaml:136:        - name: GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES
k8s/es4/services/es-feed-gateway.yaml:137:          value: "262144"
k8s/es4/services/es-feed-gateway.yaml:250:        # NOT sufficient to enable the footprint relay. G-R8's deployment contingency requires
k8s/es4/services/es-cvd.yaml:132:        # gateway's GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES to be ONE value, set together: the gateway
k8s/es4/services/es-cvd.yaml:134:        # publish records the relay silently discards. Both defaults are 262144 and agreed only by
k8s/es4/services/es-cvd.yaml:137:        - name: FOOTPRINT_MAX_RECORD_BYTES
k8s/es4/services/es-cvd.yaml:138:          value: "262144"
scripts/ci/validate-footprint-gateway-contingency-test.sh:2:# Mutation test for validate-footprint-gateway-contingency.sh: each guard must REFUSE its own
scripts/ci/validate-footprint-gateway-contingency-test.sh:9:GUARD=scripts/ci/validate-footprint-gateway-contingency.sh
scripts/ci/validate-footprint-gateway-contingency-test.sh:18:    ( cd "$d" && ./scripts/ci/validate-footprint-gateway-contingency.sh >/dev/null 2>&1 ) && echo 0 || echo $?
scripts/ci/validate-footprint-gateway-contingency-test.sh:36:sed 's/^\( *\)value: "262144"/\1value: "524288"/' "$PROD" > "$WORK/prod-bigger.yaml"
scripts/ci/validate-footprint-gateway-contingency-test.sh:40:grep -v 'FOOTPRINT_MAX_RECORD_BYTES' "$PROD" | grep -v '^ *value: "262144"' > "$WORK/prod-none.yaml"
scripts/ci/validate-footprint-gateway-contingency-test.sh:69:     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES" { sub(/262144/, "999999") }
scripts/ci/validate-footprint-gateway-contingency.sh:2:# ES Footprint deployment contingency (G-R8), enforced on the manifests rather than trusted.
scripts/ci/validate-footprint-gateway-contingency.sh:48:gw_bytes=$(env_value "$GW" GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES)
scripts/ci/validate-footprint-gateway-contingency.sh:49:pr_bytes=$(env_value "$PROD" FOOTPRINT_MAX_RECORD_BYTES)
scripts/ci/validate-footprint-gateway-contingency.sh:50:[ -n "$gw_bytes" ] || fail "the relay is enabled but $GW sets no GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES"
scripts/ci/validate-footprint-gateway-contingency.sh:51:[ -n "$pr_bytes" ] || fail "the relay is enabled but $PROD sets no FOOTPRINT_MAX_RECORD_BYTES"
scripts/ci/validate-footprint-gateway-contingency.sh:69:printf 'footprint contingency OK: ceiling %s on both sides; headroom %s MiB (limit %s, Xmx %s)\n' \

 succeeded in 34127ms:
2026-09-08 20:47:18.389 xcodebuild[31554:265366429]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:47:19.247 xcodebuild[31554:265366423] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-goQYPFzB' (errno=Operation not permitted)
2026-09-08 20:47:21.778 xcodebuild[31748:265366947]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:47:22.525 xcodebuild[31748:265366942] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-bbSowS3w' (errno=Operation not permitted)
2026-09-08 20:47:25.732 xcodebuild[31901:265367467]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:47:26.638 xcodebuild[31901:265367464] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-wemNQ0cA' (errno=Operation not permitted)
2026-09-08 20:47:28.263 xcodebuild[32001:265367744]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:47:29.034 xcodebuild[32001:265367743] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
feat/footprint-gateway-contingency-guard
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-rIaJrh3L' (errno=Operation not permitted)
2026-09-08 20:47:35.633 xcodebuild[32306:265368634]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:47:36.225 xcodebuild[32306:265368631] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-cUhVoZrO' (errno=Operation not permitted)
2026-09-08 20:47:38.446 xcodebuild[32399:265368903]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:47:38.918 xcodebuild[32399:265368902] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
 .github/workflows/deploy-validation.yml            | 11 ++++
 .../validate-footprint-gateway-contingency-test.sh | 77 ++++++++++++++++++++++
 .../ci/validate-footprint-gateway-contingency.sh   | 70 ++++++++++++++++++++
 3 files changed, 158 insertions(+)
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-Qx2sK5S1' (errno=Operation not permitted)
2026-09-08 20:47:42.241 xcodebuild[32519:265369264]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:47:43.174 xcodebuild[32519:265369260] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-2jPVrQjM' (errno=Operation not permitted)
2026-09-08 20:47:45.734 xcodebuild[32661:265369653]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:47:46.628 xcodebuild[32661:265369631] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
#!/usr/bin/env bash
# ES Footprint deployment contingency (G-R8), enforced on the manifests rather than trusted.
#
# Two facts about this subsystem are only true if two SEPARATE manifests agree, and nothing else
# in CI notices when they stop agreeing:
#
#   1. The record ceiling is ONE value. The gateway drops any record above its own ceiling, so a
#      producer allowed to emit larger records loses those records SILENTLY at the relay — the
#      producer is happy, the topic has the bytes, and the page never sees them.
#   2. G-R8's memory inequality. The relay's extra working set lives OUTSIDE the heap, so a
#      container whose limit equals its -Xmx is OOM-killed on native growth the heap never sees.
#      The rule is limit - Xmx >= 512 MiB, and it must hold wherever the relay is ENABLED.
#
# Both are checked only where the relay is actually on: a manifest with the flag off is not
# holding anything up, and demanding the headroom there would be noise.
set -euo pipefail
cd "$(dirname "$0")/../.."

fail() { printf 'FOOTPRINT CONTINGENCY: %s\n' "$*" >&2; exit 1; }

# The value of an env var in a manifest: the `value:` line that FOLLOWS its `- name:` line.
# The value of an env var: the `value:` line that FOLLOWS its `- name:` line. Taking $2 alone
# truncates a value with spaces to its first word — which is how "-Xms256m -Xmx1536m" read as
# "-Xms256m" and this check reported a missing -Xmx that was right there.
env_value() {
    awk -v want="$2" '
        $1 == "-" && $2 == "name:" { name = $3; next }
        $1 == "value:" && name == want {
            sub(/^ *value: */, ""); gsub(/^"|"$/, ""); print; exit
        }
    ' "$1"
}

mib() { printf '%s' "$1" | awk '{ if ($0 ~ /Mi$/) { sub(/Mi$/, ""); print $0 } else if ($0 ~ /Gi$/) { sub(/Gi$/, ""); print $0 * 1024 } else { print "" } }'; }

GW=k8s/es4/services/es-feed-gateway.yaml
PROD=k8s/es4/services/es-cvd.yaml
[ -f "$GW" ] || fail "$GW is missing"
[ -f "$PROD" ] || fail "$PROD is missing"

enabled=$(env_value "$GW" GATEWAY_ES_FOOTPRINT_ENABLED)
if [ "$enabled" != "true" ]; then
    printf 'footprint relay is not enabled in %s: nothing to check\n' "$GW"
    exit 0
fi

# ---- (1) one ceiling, both sides -------------------------------------------------------------
gw_bytes=$(env_value "$GW" GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES)
pr_bytes=$(env_value "$PROD" FOOTPRINT_MAX_RECORD_BYTES)
[ -n "$gw_bytes" ] || fail "the relay is enabled but $GW sets no GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES"
[ -n "$pr_bytes" ] || fail "the relay is enabled but $PROD sets no FOOTPRINT_MAX_RECORD_BYTES"
[ "$gw_bytes" = "$pr_bytes" ] \
    || fail "the record ceiling differs: gateway $gw_bytes vs producer $pr_bytes. A producer allowed to emit larger records loses them silently at the relay"

# ---- (2) G-R8: limit - Xmx >= 512 MiB ---------------------------------------------------------
limit_raw=$(awk '/^ *limits:/ { inlim = 1; next } inlim && /memory:/ { v = $2; gsub(/"/, "", v); print v; exit }' "$GW")
[ -n "$limit_raw" ] || fail "no memory limit found in $GW"
limit=$(mib "$limit_raw")
[ -n "$limit" ] || fail "memory limit '$limit_raw' is neither Mi nor Gi; this check cannot compare it"

xmx_raw=$(env_value "$GW" JAVA_TOOL_OPTIONS)
xmx=$(printf '%s' "$xmx_raw" | sed -n 's/.*-Xmx\([0-9]*\)m.*/\1/p')
[ -n "$xmx" ] || fail "no -Xmx found in JAVA_TOOL_OPTIONS ('$xmx_raw')"

headroom=$(( limit - xmx ))
[ "$headroom" -ge 512 ] \
    || fail "G-R8 native headroom is ${headroom} MiB (limit ${limit} MiB - Xmx ${xmx} MiB); the relay's working set is OUTSIDE the heap and needs >= 512 MiB"

printf 'footprint contingency OK: ceiling %s on both sides; headroom %s MiB (limit %s, Xmx %s)\n' \
    "$gw_bytes" "$headroom" "$limit" "$xmx"
#!/usr/bin/env bash
# Mutation test for validate-footprint-gateway-contingency.sh: each guard must REFUSE its own
# violation. A guard nobody has watched fail is a guard nobody knows is wired up.
set -euo pipefail
cd "$(dirname "$0")/../.."

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
GUARD=scripts/ci/validate-footprint-gateway-contingency.sh
GW=k8s/es4/services/es-feed-gateway.yaml
PROD=k8s/es4/services/es-cvd.yaml

run_against() {   # $1 = gateway manifest, $2 = producer manifest -> exit code of the guard
    local d="$WORK/case"; rm -rf "$d"; mkdir -p "$d/scripts/ci" "$d/k8s/es4/services"
    cp "$GUARD" "$d/scripts/ci/"
    cp "$1" "$d/k8s/es4/services/es-feed-gateway.yaml"
    cp "$2" "$d/k8s/es4/services/es-cvd.yaml"
    ( cd "$d" && ./scripts/ci/validate-footprint-gateway-contingency.sh >/dev/null 2>&1 ) && echo 0 || echo $?
}

expect() {   # $1 = expected code, $2 = label, $3.. = manifests
    local want="$1" label="$2"; shift 2
    local got; got=$(run_against "$@")
    if [ "$got" != "$want" ]; then
        printf 'MUTATION SURVIVED: %s (guard exited %s, expected %s)\n' "$label" "$got" "$want" >&2
        exit 1
    fi
    printf '  killed: %s\n' "$label"
}

echo "baseline"
expect 0 "the manifests as committed pass" "$GW" "$PROD"

echo "mutations"
# 1. the two ceilings diverge
sed 's/^\( *\)value: "262144"/\1value: "524288"/' "$PROD" > "$WORK/prod-bigger.yaml"
expect 1 "producer ceiling above the gateway's" "$GW" "$WORK/prod-bigger.yaml"

# 2. the producer stops declaring a ceiling at all
grep -v 'FOOTPRINT_MAX_RECORD_BYTES' "$PROD" | grep -v '^ *value: "262144"' > "$WORK/prod-none.yaml"
expect 1 "producer declares no ceiling" "$GW" "$WORK/prod-none.yaml"

# 3. the limit is lowered back to the heap size (the incident this rule exists for)
sed 's/^\( *\)memory: "2560Mi"/\1memory: "1536Mi"/' "$GW" > "$WORK/gw-tight.yaml"
expect 1 "limit equal to -Xmx: no native headroom" "$WORK/gw-tight.yaml" "$PROD"

# 4. the heap is raised into the headroom instead
sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2176m/' "$GW" > "$WORK/gw-fat-heap.yaml"
expect 1 "heap raised to leave under 512 MiB" "$WORK/gw-fat-heap.yaml" "$PROD"

# 5. exactly at the boundary is ACCEPTED — the rule is >=, and a guard that refuses its own
#    boundary would force every future budget to overshoot it
sed 's/-Xms256m -Xmx1536m/-Xms256m -Xmx2048m/' "$GW" > "$WORK/gw-boundary.yaml"
expect 0 "exactly 512 MiB of headroom is legal" "$WORK/gw-boundary.yaml" "$PROD"

# 6. relay off: the guard has nothing to hold up and says so. Flip the flag's OWN value line —
#    a blanket s/"true"/"false"/ hits the first unrelated flag in the file and passes for the
#    wrong reason, which is exactly what the first version of this case did.
awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/true/, "false") }
     { print }' "$GW" > "$WORK/gw-off.yaml"
grep -A1 GATEWAY_ES_FOOTPRINT_ENABLED "$WORK/gw-off.yaml" | grep -q 'value: "false"' \
    || { echo "the disable mutation did not take: the case would pass for the wrong reason" >&2; exit 1; }
expect 0 "relay disabled: not checked" "$WORK/gw-off.yaml" "$PROD"

# 6b. and with the relay off the guard is not merely quiet — it is quiet BECAUSE of the flag:
#     the same manifest with a broken ceiling still passes while disabled, and fails once enabled
awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES" { sub(/262144/, "999999") }
     { print }' "$WORK/gw-off.yaml" > "$WORK/gw-off-broken.yaml"
expect 0 "relay disabled hides a ceiling mismatch (by design)" "$WORK/gw-off-broken.yaml" "$PROD"
awk '$1 == "-" && $2 == "name:" { name = $3 }
     $1 == "value:" && name == "GATEWAY_ES_FOOTPRINT_ENABLED" { sub(/false/, "true") }
     { print }' "$WORK/gw-off-broken.yaml" > "$WORK/gw-on-broken.yaml"
expect 1 "and refuses the same manifest once the relay is enabled" "$WORK/gw-on-broken.yaml" "$PROD"

echo "every guard refused its own violation"

codex
The first substantive issue is now clear: the exemption is fail-open for every enablement value other than the exact extracted string `true`, including a missing/unparseable value and an earlier duplicate `false` followed by an effective later `true`. I’m finishing the line-level mutation and integration audit to determine whether there are additional request-changes findings.
exec
/bin/bash -lc "nl -ba scripts/ci/validate-footprint-gateway-contingency.sh; nl -ba scripts/ci/validate-footprint-gateway-contingency-test.sh; sed -n '1,240p' scripts/ci/validate-services.sh; rg -n \"validate-[a-z0-9-]+\\.sh|scripts/ci\" scripts/ci/validate-services.sh .github/workflows | head -250; awk '"'$1=="-"&&$2=="name:"{name=$3;next}$1=="value:"&&name=="JAVA_TOOL_OPTIONS"{sub(/''^ *value: */,"");gsub(/''^'"\\\"|\\\""'$/,"");print;exit}'"' k8s/es4/services/es-feed-gateway.yaml; awk '/"'^ *limits:/{inlim=1;next} inlim&&/memory:/{print NR ":" $0;exit}'"' k8s/es4/services/es-feed-gateway.yaml; shasum scripts/ci/validate-footprint-gateway-contingency*.sh" in /private/tmp/oe-deploy-mirror
 succeeded in 144ms:
scripts/ci/validate-services.sh:136:# assertion; the monolith path gets it via scripts/deploy/validate-platform.sh.
scripts/ci/validate-services.sh:137:bash scripts/ci/validate-vix-single-publisher.sh
scripts/ci/validate-services.sh:144:bash scripts/ci/validate-preopen-single-publisher.sh
scripts/ci/validate-services.sh:150:bash scripts/ci/validate-durable-topic-preservation.sh
scripts/ci/validate-services.sh:158:         scripts/ci/validate-durable-topic-preservation-mutation-test.sh \
scripts/ci/validate-services.sh:159:         scripts/ci/es-cvd-mirror-shape-test.sh \
scripts/ci/validate-services.sh:160:         scripts/ci/es-auction-mirror-shape-test.sh; do
.github/workflows/deploy-validation.yml:74:        run: bash scripts/ci/validate-es4-render.sh
.github/workflows/deploy-validation.yml:77:        run: scripts/ci/validate-services.sh
.github/workflows/deploy-validation.yml:85:        run: scripts/ci/validate-footprint-gateway-contingency.sh
.github/workflows/deploy-validation.yml:88:        run: scripts/ci/validate-footprint-gateway-contingency-test.sh
.github/workflows/deploy-validation.yml:90:      # VIX feed separation §7: validate-services.sh already runs the single-publisher
.github/workflows/deploy-validation.yml:109:        run: bash scripts/ci/validate-tests-are-collectable.sh
.github/workflows/deploy-validation.yml:124:        run: bash scripts/ci/validate-gex-dev-safety.sh
.github/workflows/deploy-validation.yml:132:      # depends on is already gated: validate-services.sh runs
.github/workflows/deploy-validation.yml:133:      # scripts/ci/validate-durable-topic-preservation.sh as its section 6.)
.github/workflows/deploy-validation.yml:135:        run: bash scripts/ci/validate-mirrored-topic-contracts.sh
.github/workflows/deploy-validation.yml:141:        run: bash scripts/ci/validate-jenkinsfile-shell-blocks.sh
.github/workflows/deploy-validation.yml:151:        run: bash scripts/ci/validate-archive-uncompaction-env.sh
.github/workflows/deploy-validation.yml:158:        run: bash scripts/ci/validate-mirrored-topic-contracts-test.sh
.github/workflows/deploy-validation.yml:166:        run: bash scripts/ci/es-cvd-mirror-shape-test.sh
.github/workflows/deploy-validation.yml:172:        run: bash scripts/ci/es-auction-mirror-shape-test.sh
.github/workflows/deploy-validation.yml:175:        run: scripts/ci/validate-image-pinning.sh
.github/workflows/deploy-validation.yml:183:        run: bash scripts/ci/validate-feed-config-explicit.sh
.github/workflows/deploy-validation.yml:189:        run: bash scripts/ci/validate-cvd-levels-alerts.sh
.github/workflows/deploy-validation.yml:192:        run: scripts/deploy/validate-image-lock-ci.sh
-Xms256m -Xmx1536m
271:            memory: "2560Mi"
df38e42dd53c0f932610ccdfc229254c9fc200e9  scripts/ci/validate-footprint-gateway-contingency-test.sh
37231ebbc97bf574f49592ba6838fe95e4e73493  scripts/ci/validate-footprint-gateway-contingency.sh

2026-09-08T18:47:58.873328Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
exec
/bin/bash -lc 'rg -n -C 4 "validate-es4-render|validate-services|validate-platform|Validate" Jenkinsfile.es4-deploy Jenkinsfile.service-deploy scripts/deploy/validate-platform.sh; rg -n "es-feed-gateway.yaml|es-cvd.yaml" scripts/deploy k8s/es4 | head -100; git show --stat --oneline HEAD 2>&1 | tail -30' in /private/tmp/oe-deploy-mirror
 succeeded in 5954ms:
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-ruRHYb1l' (errno=Operation not permitted)
2026-09-08 20:48:13.650 xcodebuild[33706:265372401]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:48:14.419 xcodebuild[33706:265372394] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
git: warning: confstr() failed with code 5: couldn't get path of DARWIN_USER_TEMP_DIR; using /tmp instead
git: error: couldn't create cache file '/tmp/xcrun_db-mAKQCz2r' (errno=Operation not permitted)
2026-09-08 20:48:15.786 xcodebuild[33769:265372576]  DVTFilePathFSEvents: Failed to start fs event stream.
2026-09-08 20:48:16.403 xcodebuild[33769:265372569] [MT] DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR from confstr(3), error = Error Domain=NSPOSIXErrorDomain Code=5 "Input/output error". Using NSCachesDirectory instead.
52c60ebb ci(footprint): the G-R8 contingency is checked, not trusted
 .github/workflows/deploy-validation.yml            | 11 ++++
 .../validate-footprint-gateway-contingency-test.sh | 77 ++++++++++++++++++++++
 .../ci/validate-footprint-gateway-contingency.sh   | 70 ++++++++++++++++++++
 3 files changed, 158 insertions(+)

2026-09-08T18:48:19.355505Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
exec
/bin/bash -lc "rg -n -C 8 \"stage\\(|validate|kubectl apply|services/\" Jenkinsfile.es4-deploy | head -240; sed -n '1,130p' scripts/deploy/validate-platform.sh" in /private/tmp/oe-deploy-mirror
 succeeded in 0ms:
5-// Manifests live in k8s/es4/ (generated from the production slices by
6-// scripts/es4/render_es4_manifests.py — reviewed + committed).
7-//
8-// ACTIONS:
9-//   infra-sync     rsync infra/es4 + scripts/es4 to the box and run bootstrap-es4.sh
10-//                  (k3s, registries.yaml, compose stack Kafka/SR/PG/Redis/MM2, topics,
11-//                  namespace + pg secret). Idempotent.
12-//   create-topics  run create-es-topics.sh on the box (idempotent).
13://   clean-reset    Snapshot every es4 Deployment, stop all services/infra, wipe Kafka/SR, Streams
14-//                  state stores and service logs, reconcile topics from the topics.env SSOT (NOT
15-//                  from a live-broker snapshot — undeclared topics are NOT preserved), then start
16-//                  only the snapshotted services at captured replicas —
17-//                  EXCEPT es-feed, which is always left at 0 (DBP-R22 reserves starting it to
18-//                  ACTION=activate-es-feed, so no reset can raise a Databento session).
19-//                  DESTRUCTIVE (drops all es4 Kafka data); honors DEPLOY_DRY_RUN.
20://   deploy-service apply ONE service from k8s/es4/services/<SERVICE>.yaml (digest-pinned).
21-//   deploy-all     apply es4-common-env + every manifest in k8s/es4/services (digest-pinned).
22-//   activate-es-feed / deactivate-es-feed  start or stop the es4 es-feed. Activation is its own
23-//                  action so no deploy can raise the feed as a side effect.
24-//   (deploy-es-feed was REMOVED 2026-07-25, DBP-R33: the prod-cluster es-feed is retired, so no
25-//    second location exists that could run a duplicate.)
26-//
27-// Image policy: every apply digest-pins the :prod tag against 192.168.100.252:5000 at
28-// deploy time (Docker-Content-Digest) — a floating tag is never applied (Codex D4).
--
41-      'strike-intelligence', 'vix-option-inteligence', 'greek-move-authenticity', 'gamma-migration', 'indicator-service', 'context-tape', 'tape-zones', 'es-aggressor-flow', 'es-cvd', 'es-amt', 'es-feed-gateway', 'es-web',
42-      'es-feed',
43-      // The held spread-skew stack is deploy-service-only ON PURPOSE: deploy-all excludes it, so the
44-      // ONLY sanctioned way to reconcile the ES4_KEEP_DOWN replicas:0 hold onto live pods (or to ever
45-      // deliberately re-apply the stack) is an explicit per-service run of each of these two.
46-      'spread-skew', 'spread-skew-postgres-writer'
47-    ], description: 'Service (deploy-service only). NOTE: es-feed is the RELOCATED ES options feed now running on the es4 cluster; it always deploys at replicas 0 — starting it is a separate ACTION=activate-es-feed.')
48-    booleanParam(name: 'DEPLOY_DRY_RUN', defaultValue: false,
49:      description: 'true = render + digest-pin + kubectl apply --dry-run=server only.')
50-    choice(name: 'ES_FEED_LOCATION', choices: ['none', 'es4'],
51-      description: 'DECLARED location of the single live es-feed. es4 is the ONLY option: the prod-cluster es-feed was retired 2026-07-25 (DBP-R33) so two copies cannot exist at all, rather than being prevented by convention. none = es-feed must be DOWN. Rollback to prod is deliberately not a one-click path any more - it requires a full revert of the retirement commits (both prod manifests, the deploy stage, the digest check, the prod activation branch and the cross-cluster fencing), not just restoring one file.')
52-    booleanParam(name: 'ES_FEED_ACK_OI_AND_CAPACITY', defaultValue: false,
53-      description: 'activate-es-feed only. Explicit acknowledgement of the two preconditions the requirement will not let a deploy assume: (1) the ES-OI consequence of DATABENTO_USE_LIVE_REPLAY=false has been resolved (ESM-Q4 — either es4 GEX moved to direct OI fetch, or the bounded out-of-band OI request landed), and (2) real .4 headroom has been MEASURED with the full es4 fleet running (DBP-V10/ESM-V6). Activation refuses to proceed without this.')
54-  }
55-  options { disableConcurrentBuilds(); timeout(time: 60, unit: 'MINUTES') }
56-  environment {
57-    ES4_HOST       = 'abhinav@192.168.100.4'
--
68-    // authorise cross-cluster deletion, so they are pinned rather than discovered — an unrecognised
69-    // UID fails closed. Proving the two kubeconfigs merely DIFFER is not enough: swapped configs
70-    // would still pass and the reconcile-delete would run against es4. Update only if a cluster is
71-    // genuinely rebuilt: kubectl get ns options-edge -o jsonpath='{.metadata.uid}'
72-    PROD_NS_UID    = 'c62ee606-688d-4373-a889-a52c8b919e73'
73-    ES4_NS_UID     = 'cbf047d2-45ea-4ba7-b83c-5b462c7aa1d3'
74-  }
75-  stages {
76:    stage('Infra sync + bootstrap') {
77-      when { expression { params.ACTION == 'infra-sync' } }
78-      steps {
79-        sh '''
80-          set -euo pipefail
81-          # /home is root-owned on the box — create the es4 home via passwordless sudo, owned by the deploy user
82-          ssh -o BatchMode=yes "$ES4_HOST" 'sudo install -d -o "$(id -un)" -g "$(id -gn)" /home/es4 /home/es4/repo /home/es4/repo/infra /home/es4/repo/scripts'
83-          rsync -az --delete infra/es4  "$ES4_HOST":/home/es4/repo/infra/
84-          # ES helpers call shared siblings under scripts/kafka. Sync the complete repo-owned
85-          # scripts tree so an on-box cleanup never runs with a partial dependency graph.
86-          rsync -az --delete scripts/ "$ES4_HOST":/home/es4/repo/scripts/
87-          ssh -o BatchMode=yes "$ES4_HOST" 'bash /home/es4/repo/scripts/es4/bootstrap-es4.sh'
88-        '''
89-      }
90-    }
91:    stage('Create topics') {
92-      when { expression { params.ACTION == 'create-topics' } }
93-      steps {
94-        sh '''
95-          set -euo pipefail
96-          rsync -az --delete scripts/ "$ES4_HOST":/home/es4/repo/scripts/
97-          ssh -o BatchMode=yes "$ES4_HOST" 'bash /home/es4/repo/scripts/es4/create-es-topics.sh'
98-          # STEADY-state audit: this action runs against a LIVE es4, so every owner has already
99-          # applied its own topic contract and the broker supplies the numbers. For the es4 set
--
101-          # width the file does not record does not survive. A live-vs-declared disagreement is
102-          # therefore something to ADJUDICATE (record the deliberate widening, or fix the topic),
103-          # and the script prints both remedies rather than assuming which applies. Running
104-          # create-topics WITHOUT this audit is exactly how topics.env drifted on 24 topics.
105-          ssh -o BatchMode=yes "$ES4_HOST" 'bash /home/es4/repo/scripts/es4/verify-topic-partition-contract.sh steady'
106-        '''
107-      }
108-    }
109:    stage('Clean reset (atomic Kafka + Schema Registry wipe)') {
110-      // Wipe Kafka data (all topics + _schemas) TOGETHER so no message can outlive its schema
111-      // id — the fix for the dead-schema-id poison ("Malformed data. Length is negative") that
112-      // wedged the scorer. DESTRUCTIVE: drops all es4 Kafka data; honor DEPLOY_DRY_RUN to preview.
113-      //
114-      // Historically the es-feed lived OUTSIDE the es4 cluster (on .252) and had to be fenced here
115-      // with the prod kubeconfig, or it would reconnect mid-wipe and re-emit cached schema ids into
116-      // the fresh cluster, recreating the poison. Since DBP-R33 the feed lives IN the es4 cluster,
117-      // so the on-box cleanup fences it along with every other es4 Deployment and no cross-cluster
--
156-            exit 0
157-          fi
158-
159-          echo "=== running on-box atomic wipe ==="
160-          ssh -o BatchMode=yes "$ES4_HOST" "DEPLOY_DRY_RUN=false ES4_FEED_FENCED=1 ES4_EXPECTED_NS_UID=$ES4_NS_UID bash /home/es4/repo/scripts/es4/cleanup-es4.sh"
161-        '''
162-      }
163-    }
164:    stage('Deploy to es4') {
165-      when { expression { params.ACTION == 'deploy-service' || params.ACTION == 'deploy-all' } }
166-      steps {
167-        sh '''
168-          set -euo pipefail
169-          export KUBECONFIG="$ES4_KUBECONFIG"
170-          DRY=""; [ "${DEPLOY_DRY_RUN:-false}" = "true" ] && DRY="--dry-run=server"
171-          # Identity FIRST: this stage mutates through $KUBECONFIG (es4). Proving identity after the
172-          # first apply would mean a mispointed kubeconfig had already written to the wrong cluster.
173-          bash scripts/es4/assert-cluster-identity.sh "$KUBECONFIG"      es4  "$ES4_NS_UID"
174-          bash scripts/es4/assert-cluster-identity.sh "$PROD_KUBECONFIG" prod "$PROD_NS_UID"
175-
176-          # Dependency-scoped FIRE-input verdict (processing PR #594): es4 applies the COMMITTED
177:          # k8s/es4/services/*.yaml directly, so no overlay render exists for the other deploy paths'
178-          # guard to have caught. Assert here, BEFORE the first apply, and unconditionally — the
179-          # deploy-service path can target dealer-ledger, and deploy-all always includes it.
180:          bash scripts/ci/validate-dealer-ledger-fire-quality.sh
181-
182:          kubectl apply $DRY -f k8s/es4/es4-base-env.yaml
183:          kubectl apply $DRY -f k8s/es4/es4-common-env.yaml
184-          # es-feed's OWN Databento configmap. Applied for every es4 deploy so the feed can
185-          # never run against inherited library defaults again (requirement DBP-R1/R2).
186:          kubectl apply $DRY -f k8s/es4/es4-feed-config.yaml
187-          mkdir -p .jenkins-tmp
188-
189-          # ---- prod es-feed is RETIRED (DBP-R33): reconcile-delete it on every run ---------
190:          # `kubectl apply` never prunes, so removing the manifest from git does not remove the live
191-          # object. --ignore-not-found makes this idempotent, so it is not a one-time cleanup but a
192-          # standing guard: if anything ever recreates es-feed on prod, the next es4 deploy removes
193-          # it. Two copies cannot exist if only one location has a Deployment at all - which is why
194-          # this replaces the publisher-lease work (DBP-R28) rather than complementing it.
195-          if [ -z "$DRY" ]; then
196-            # Prove the kubeconfig really is the PROD cluster before deleting anything through it.
197-            # Without this, a PROD_KUBECONFIG that had been regenerated to point at .4 would delete
198-            # the LIVE es4 es-feed and the ConfigMap applied moments earlier in this same stage.
--
216-          # shared API key, so it must only ever be applied as a deliberate, individually preflighted
217-          # action (SERVICE=es-feed) — never swept in by a bulk deploy that bypasses the preflight
218-          # above. `|| true` because grep exits 1 when it filters nothing, which under
219-          # `set -euo pipefail` would abort the whole deploy.
220-          if [ "$ACTION" = "deploy-all" ]; then
221-            # The spread-skew stack is under an operator hold (2026-07-26): bulk
222-            # deploys must not even apply it, in addition to its manifests being
223-            # pinned at replicas 0.
224:            FILES=$(ls k8s/es4/services/*.yaml | { grep -Ev '/(es-feed|spread-skew|spread-skew-postgres-writer)[.]yaml$' || true; })
225-            [ -n "$FILES" ] || { echo "deploy-all: no service manifests resolved" >&2; exit 1; }
226-            echo "deploy-all: es-feed and the held spread-skew stack intentionally EXCLUDED"
227-          else
228:            FILES="k8s/es4/services/${SERVICE}.yaml"
229-          fi
230-          for f in $FILES; do
231-            bash scripts/es4/pin-and-apply.sh "$f" "$REGISTRY" "$DRY"
232-          done
233-          if [ -z "$DRY" ] && { [ "$ACTION" = "deploy-all" ] || [ "$SERVICE" = "vix-option-inteligence" ]; }; then
234-            # Remove the legacy identity only after pin-and-apply has completed the replacement
235-            # rollout, avoiding duplicate consumers on the isolated ES broker.
236-            kubectl -n options-edge delete deployment zero-dte-intelligence-service --ignore-not-found
--
240-          # path performs no scaling at all — activation is its own ACTION (requirement DBP-R22).
241-          if [ "$ACTION" = "deploy-service" ] && [ "$SERVICE" = "es-feed" ]; then
242-            echo "es-feed manifest applied at replicas 0. To start it: ACTION=activate-es-feed"
243-          fi
244-          [ -n "$DRY" ] || kubectl -n options-edge get pods -o wide
245-        '''
246-      }
247-    }
248:    stage('Activate es-feed (separate authorized action)') {
249-      when { expression { params.ACTION == 'activate-es-feed' } }
250-      steps {
251-        sh '''
252-          set -euo pipefail
253-          # Activation is deliberately NOT part of any deploy path (requirement DBP-R22).
254-          [ "${ES_FEED_LOCATION:-none}" = "es4" ] || {
255-            echo "activate-es-feed requires ES_FEED_LOCATION=es4 (got '${ES_FEED_LOCATION:-none}'); prod is retired per DBP-R33" >&2; exit 1; }
256-          # The two preconditions a deploy must never assume on the operator's behalf.
--
273-          fi
274-          KUBECONFIG="$KC" kubectl -n options-edge scale deploy/es-feed --replicas=1
275-          KUBECONFIG="$KC" kubectl -n options-edge rollout status deploy/es-feed --timeout=600s
276-          echo "es-feed ACTIVE at $ES_FEED_LOCATION. Watch source age, not 'no restarts' —"
277-          echo "the widened startup grace can mask a client that is hours behind (DBP-R12)."
278-        '''
279-      }
280-    }
281:    stage('Deactivate es-feed') {
282-      when { expression { params.ACTION == 'deactivate-es-feed' } }
283-      steps {
284-        sh '''
285-          set -euo pipefail
286-          # Identity first: without it, two kubeconfigs both pointing at prod would let this action
287-          # report success while es4 stayed live.
288-          bash scripts/es4/assert-cluster-identity.sh "$ES4_KUBECONFIG"  es4  "$ES4_NS_UID"
289-          bash scripts/es4/assert-cluster-identity.sh "$PROD_KUBECONFIG" prod "$PROD_NS_UID"
#!/usr/bin/env bash
# Validate stage shell body (extracted from Jenkinsfile to keep the declarative
# pipeline's single compiled CPS method under the JVM 64KB per-method bytecode
# limit; mirrors the apply.sh / resolve-images.sh extraction pattern). Invoked
# via `sh 'bash -x scripts/deploy/validate-platform.sh'`; relies on the
# environment exported by the pipeline (ENVIRONMENT, BUILD_PLATFORM,
# REMOTE_APP_HOME, OE_REMOTE_APP_HOME, JENKINS_WORK_DIR).
set -euo pipefail
scripts/jenkins/enforce-main-branch.sh
scripts/jenkins/enforce-local-dev-defaults.sh
test "$REMOTE_APP_HOME" = "$OE_REMOTE_APP_HOME"
test ! -d /root/options-edge
test ! -d /options-edge
mkdir -p "$JENKINS_WORK_DIR"
test -w "$JENKINS_WORK_DIR"
case "${ENVIRONMENT:-dev}" in
  dev)
    effective_build_platform="${BUILD_PLATFORM:-linux/arm64}"
    ;;
  production)
    effective_build_platform="linux/amd64"
    if [ -n "${BUILD_PLATFORM:-}" ] && [ "$BUILD_PLATFORM" != "linux/amd64" ]; then
      echo "BUILD_PLATFORM=$BUILD_PLATFORM is not allowed for ${ENVIRONMENT}; production Kubernetes nodes are CentOS amd64 and require linux/amd64." >&2
      exit 1
    fi
    ;;
  *)
    echo "Unsupported ENVIRONMENT for BUILD_PLATFORM resolution: ${ENVIRONMENT:-}" >&2
    exit 1
    ;;
esac
case "$effective_build_platform" in
  linux/arm64|linux/amd64) ;;
  *)
    echo "Unsupported BUILD_PLATFORM: $effective_build_platform" >&2
    exit 1
    ;;
esac
printf 'EFFECTIVE_BUILD_PLATFORM=%s\n' "$effective_build_platform" >"$JENKINS_WORK_DIR/options-edge-build.env"
echo "Effective build/deploy image platform: $effective_build_platform"
# At-most-one VIX publisher assertion (VIX feed separation design §7): the MONOLITH
# deploy path must also refuse a render where both the SPX in-process VIX block and the
# standalone databento-vix-feed would publish the real underlying.vix.price — this
# stage precedes the deploy stage, so a violating commit never reaches kubectl.
bash scripts/ci/validate-vix-single-publisher.sh
# The dependency-scoped FIRE-input verdict changes which greeks quality gates a G-FIRE EMISSION.
# Its two knobs are registered dormant in the base; this stage refuses a render that turns the flag
# on without the shadow evidence, or that carries a materiality fraction the service would read as
# "block every off-span drop". Runs before the deploy stage, so a violating commit never reaches
# kubectl on the MONOLITH path; Jenkinsfile.service-deploy runs the same script for the standalone
# dealer-ledger path.
bash scripts/ci/validate-dealer-ledger-fire-quality.sh

2026-09-08T18:48:23.363265Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
exec
/bin/bash -lc 'bash -n scripts/ci/validate-footprint-gateway-contingency.sh scripts/ci/validate-footprint-gateway-contingency-test.sh && scripts/ci/validate-footprint-gateway-contingency.sh' in /private/tmp/oe-deploy-mirror
 succeeded in 62ms:
footprint contingency OK: ceiling 262144 on both sides; headroom 1024 MiB (limit 2560, Xmx 1536)

2026-09-08T18:48:28.691151Z ERROR codex_models_manager::manager: failed to renew cache TTL: missing field `supports_parallel_tool_calls` at line 93 column 5
codex
## Findings

1. **High — relay-off exemption fails open.**  
   [validate-footprint-gateway-contingency.sh:41](/private/tmp/oe-deploy-mirror/scripts/ci/validate-footprint-gateway-contingency.sh:41) treats anything other than the first extracted literal `true` as a valid disabled state and exits successfully. Missing, malformed, indirect, duplicated, or unparsable enablement therefore bypasses every check. In particular, duplicate entries ordered `false`, then `true` can be effectively enabled while the guard sees the first `false` and exempts the manifest. Require exactly one entry and accept only `true` or `false`; fail on every other state. Add mutation cases for missing, malformed, and duplicate enablement entries.

2. **High — the deployment path that applies these manifests does not run the guard.**  
   The GitHub workflow wiring at [deploy-validation.yml:79](/private/tmp/oe-deploy-mirror/.github/workflows/deploy-validation.yml:79) runs for every PR/push to `main`, with no path filter, and executes after checkout and es4 regeneration validation. That CI placement works. However, [Jenkinsfile.es4-deploy:164](/private/tmp/oe-deploy-mirror/Jenkinsfile.es4-deploy:164) directly applies `k8s/es4/services/*.yaml` and only runs the dealer-ledger guard before mutation. Repository precedent explicitly puts dependency-scoped checks in the actual deployment path. This guard should also run there before the first apply; otherwise CI is the sole enforcement point.

## Claim-by-claim audit

- **Environment extraction:** For the manifests as written, `env_value` correctly obtains `true`, `262144`, `262144`, and the complete `-Xms256m -Xmx1536m` value. It resets state on each active `- name:`, so earlier unrelated entries do not capture later values. Ordinary commented-out `# - name:` and `# value:` lines are ignored. The duplicate-entry ambiguity above remains a correctness hole.

- **Memory extraction:** In the actual gateway manifest, the first active `limits:` is the `feed-gateway` container’s resources block, and the next memory field is `limits.memory: "2560Mi"` at [es-feed-gateway.yaml:270](/private/tmp/oe-deploy-mirror/k8s/es4/services/es-feed-gateway.yaml:270). It does not select the preceding `requests.memory` at line 269. The parser is structurally unscoped and would select another container’s earlier limits if the manifest evolved, but it currently selects the right field.

- **G-R8 inequality:** Lines 65–67 calculate `limit - Xmx` and use `-ge 512`, so the inclusive boundary is correct. Current values yield `2560 - 1536 = 1024 MiB`; the boundary mutation yields `2560 - 2048 = 512 MiB`.

- **Relay exemption:** It works for one explicit `false`, but does not prove that the manifest is unambiguously disabled; finding 1 disproves the broader safety claim.

- **Mutation accuracy:**
  - Case 1 changes the producer’s sole exact `value: "262144"` to `524288`, producing the labeled mismatch.
  - Case 2 removes the target name and its sole exact value line, producing a missing ceiling.
  - Case 3 changes the gateway container’s sole `2560Mi` memory line to `1536Mi`, producing zero headroom.
  - Cases 4 and 5 change the actual `JAVA_TOOL_OPTIONS` entry to `Xmx2176m` and `Xmx2048m`, producing 384 MiB and exactly 512 MiB respectively.
  - Case 6 changes the target enablement value, and its explicit post-mutation assertion confirms the edit.
  - Case 6b changes the target ceiling and then the target enablement value as labeled.
  - Against the current manifests, none of these cases passes for a mislabeled mutation. However, expected-failure cases assert only exit code `1`, so an unrelated guard failure returning `1` could still “kill” a mutation for the wrong reason. More importantly, the suite omits the fail-open enablement cases from finding 1.

- **Duplication/integration:** No existing footprint contingency validator duplicates this check. `validate-services.sh` already contains structural, duplicate-safe extraction patterns using `yq`, which this new ad-hoc AWK parser does not reuse. The most consequential missing integration is the actual es4 deployment preflight.

- **Execution check:** Both scripts pass `bash -n`; the committed baseline guard runs successfully and reports ceiling `262144` and headroom `1024 MiB`.

VERDICT: REQUEST_CHANGES
tokens used
56.023
## Findings

1. **High — relay-off exemption fails open.**  
   [validate-footprint-gateway-contingency.sh:41](/private/tmp/oe-deploy-mirror/scripts/ci/validate-footprint-gateway-contingency.sh:41) treats anything other than the first extracted literal `true` as a valid disabled state and exits successfully. Missing, malformed, indirect, duplicated, or unparsable enablement therefore bypasses every check. In particular, duplicate entries ordered `false`, then `true` can be effectively enabled while the guard sees the first `false` and exempts the manifest. Require exactly one entry and accept only `true` or `false`; fail on every other state. Add mutation cases for missing, malformed, and duplicate enablement entries.

2. **High — the deployment path that applies these manifests does not run the guard.**  
   The GitHub workflow wiring at [deploy-validation.yml:79](/private/tmp/oe-deploy-mirror/.github/workflows/deploy-validation.yml:79) runs for every PR/push to `main`, with no path filter, and executes after checkout and es4 regeneration validation. That CI placement works. However, [Jenkinsfile.es4-deploy:164](/private/tmp/oe-deploy-mirror/Jenkinsfile.es4-deploy:164) directly applies `k8s/es4/services/*.yaml` and only runs the dealer-ledger guard before mutation. Repository precedent explicitly puts dependency-scoped checks in the actual deployment path. This guard should also run there before the first apply; otherwise CI is the sole enforcement point.

## Claim-by-claim audit

- **Environment extraction:** For the manifests as written, `env_value` correctly obtains `true`, `262144`, `262144`, and the complete `-Xms256m -Xmx1536m` value. It resets state on each active `- name:`, so earlier unrelated entries do not capture later values. Ordinary commented-out `# - name:` and `# value:` lines are ignored. The duplicate-entry ambiguity above remains a correctness hole.

- **Memory extraction:** In the actual gateway manifest, the first active `limits:` is the `feed-gateway` container’s resources block, and the next memory field is `limits.memory: "2560Mi"` at [es-feed-gateway.yaml:270](/private/tmp/oe-deploy-mirror/k8s/es4/services/es-feed-gateway.yaml:270). It does not select the preceding `requests.memory` at line 269. The parser is structurally unscoped and would select another container’s earlier limits if the manifest evolved, but it currently selects the right field.

- **G-R8 inequality:** Lines 65–67 calculate `limit - Xmx` and use `-ge 512`, so the inclusive boundary is correct. Current values yield `2560 - 1536 = 1024 MiB`; the boundary mutation yields `2560 - 2048 = 512 MiB`.

- **Relay exemption:** It works for one explicit `false`, but does not prove that the manifest is unambiguously disabled; finding 1 disproves the broader safety claim.

- **Mutation accuracy:**
  - Case 1 changes the producer’s sole exact `value: "262144"` to `524288`, producing the labeled mismatch.
  - Case 2 removes the target name and its sole exact value line, producing a missing ceiling.
  - Case 3 changes the gateway container’s sole `2560Mi` memory line to `1536Mi`, producing zero headroom.
  - Cases 4 and 5 change the actual `JAVA_TOOL_OPTIONS` entry to `Xmx2176m` and `Xmx2048m`, producing 384 MiB and exactly 512 MiB respectively.
  - Case 6 changes the target enablement value, and its explicit post-mutation assertion confirms the edit.
  - Case 6b changes the target ceiling and then the target enablement value as labeled.
  - Against the current manifests, none of these cases passes for a mislabeled mutation. However, expected-failure cases assert only exit code `1`, so an unrelated guard failure returning `1` could still “kill” a mutation for the wrong reason. More importantly, the suite omits the fail-open enablement cases from finding 1.

- **Duplication/integration:** No existing footprint contingency validator duplicates this check. `validate-services.sh` already contains structural, duplicate-safe extraction patterns using `yq`, which this new ad-hoc AWK parser does not reuse. The most consequential missing integration is the actual es4 deployment preflight.

- **Execution check:** Both scripts pass `bash -n`; the committed baseline guard runs successfully and reports ceiling `262144` and headroom `1024 MiB`.

VERDICT: REQUEST_CHANGES
