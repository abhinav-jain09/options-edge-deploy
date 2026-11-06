#!/usr/bin/env python3
"""render_es4_manifests.py — generate k8s/es4/services/*.yaml from the PRODUCTION slices.

Run from the repo root; commit the outputs (they are reviewed like any change).

WHY derive from production: the prod manifests carry every service-specific env
(health ports, source switches, per-topic envs). On es4 the runtime applies
TOPIC_PREFIX=es. to every topic — including env-supplied values
(KafkaSettings.topicValue -> RuntimeProfileConfig.topic prefixes both defaults and
env overrides) — so the prod env carries over UNCHANGED and every topic lands
es.-prefixed. Environment-level knobs come from the es4-common-env ConfigMap +
es4-runtime-secrets Secret referenced here instead of the prod ones.

Transforms per manifest:
  1. envFrom: options-edge-config -> es4-common-env; options-edge-runtime-secrets ->
     es4-runtime-secrets; every other envFrom entry dropped (all optional in prod).
  2. image -> 192.168.100.252:5000/<basename>:prod (digest-pinned by the es4-deploy
     Jenkins job before every apply — never rolled out as a floating tag). :prod is the
     moving tag production builds push to this registry (:dev entries are stale dev pushes).
  3. per-service ES env appended (q=r for gex; multiplier 50 for strike-flow /
     delta-flow; gex OI fetch disabled — the OPRA/OSI symbology doesn't apply to GLBX).
  4. per-service env families DROPPED (DROP_ENV_PREFIXES) — prod wiring for pipelines that do
     not exist on es4 and must not be carried in by a re-render.

NOTE the direction of authority: this generator is the source of truth, and the committed
k8s/es4/services/*.yaml are its output. Anything es4 needs must be expressed HERE — a value
hand-edited into a generated file (or merely inherited from prod and left alone) survives only
until the next re-render, and a comment written into one is lost outright, because PyYAML does
not emit comments. .github/workflows/deploy-validation.yml fails any PR where the two disagree.
"""

import copy
import io
import os
import sys

import yaml

REGISTRY = "192.168.100.252:5000"
OUT_DIR = "k8s/es4/services"

# es4 core set (Gate-2 v1 scope; hpsf/replay/unified-sr descoped; mission-pace/pressure
# have no standalone slices yet — follow-up; ibkr/raw-postgres-writer out of the ES path).
SERVICES = [
    "close-direction",
    "databento-mission-sandwich",
    "dealer-ledger", "dealer-ledger-calibration",
    "directional-pressure", "option-truth-engine",
    "strike-intelligence", "volume-pace", "spread-skew", "spread-skew-postgres-writer",
    "greek-move-authenticity", "gamma-ladder-path",
    "indicator-service",
    "context-tape",
    # es4-ONLY (no dev/prod deployment): its slice is hand-authored as the render source —
    # see the header of k8s/services/tape-zones/overlays/production/manifest.yaml.
    "tape-zones",
]

ES_ENV = {
    "indicator-service": [
        # 0DTE indicator service (rev 14 §7.3): es4 is the SOLE ES indicator producer. The
        # production slice ships SPX mode; flip every mode-scoped knob here. Topics prefix via
        # TOPIC_PREFIX at runtime (outputs land as es.options.indicators.* and the two output
        # topics are mirrored VERBATIM to dev+prod by Jenkinsfile.es-indicator-mirror).
        {"name": "INDICATOR_SOURCE_MODE", "value": "ES_TRADES", "_override": True},
        # ES trades flow ~23h/day; the shared liveness gate must watch the Globex session,
        # not NYSE RTH, or the pod restarts all night on a healthy feed.
        {"name": "LIVENESS_SESSION", "value": "globex", "_override": True},
        # es4's es.underlying.es.trades is 4 partitions (the es4 topic-partition contract);
        # the admission watermark blocks on UNSEEN partitions, so an under-declared count
        # would silently release records early — must match the live topic exactly.
        {"name": "INDICATOR_INPUT_PARTITIONS", "value": "4", "_override": True},
        # §6.6 amendment ES_FEED_TRUTH_HB_V1: a VALID es-feed heartbeat (the input topic +
        # ".heartbeat", i.e. es.underlying.es.trades.heartbeat) may sustain ingest liveness, so a
        # quiet overnight tape stops latching STALE and FEED_GAP-damaging the bars that follow —
        # the reason es4's 1h/4h chains never reached READY. ES-only (the service refuses it in
        # SPX mode, so prod can never share this value). Runs plain §6.6 until es-feed publishes.
        # The topic is MANDATORY: if it is missing the indicator REFUSES TO START (fail-closed,
        # Gate-2 r1 M3) — run ACTION=create-topics before deploying. _override: es4 must win.
        {"name": "INDICATOR_ES_HEARTBEAT_LIVENESS", "value": "true", "_override": True},
        # The one stream a heartbeat may vouch for: es-feed's DATABENTO_HPSF_ES_SYMBOL (default).
        {"name": "INDICATOR_HEARTBEAT_SOURCE_STREAM", "value": "ES.v.0", "_override": True},
        # NO control-topic entry here on purpose (PR #734): TOPIC_PREFIX=es. (es4-common-env)
        # prefixes the control-topic default at runtime — an explicit value would double-prefix.
    ],
    "gamma-ladder-path": [
        # es4 has NO underlying.spx.index.price — its spot stream is underlying.spx.price (prefixed to
        # es.underlying.spx.price at runtime by TOPIC_PREFIX like every env-supplied topic).
        {"name": "KAFKA_LADDER_PATH_SPOT_TOPIC", "value": "underlying.spx.price", "_override": True},
        # es4's synthetic spot carries source SYNTHETIC_OPTION_SPOT and no priceField: accept every voice.
        {"name": "LADDER_PATH_SPOT_SOURCE_FILTER", "value": "ANY", "_override": True},
        {"name": "LADDER_PATH_SPOT_FIELD_FILTER", "value": "ANY", "_override": True},
    ],
    "context-tape": [
        # es4 has NO underlying.spx.index.price — its spot stream is underlying.spx.price
        # (prefixed to es.underlying.spx.price by TOPIC_PREFIX at runtime, like every
        # env-supplied topic), carrying source SYNTHETIC_OPTION_SPOT with NO priceField.
        # The prod slice sets the SPX index topic, so es4's value must win explicitly.
        {"name": "CONTEXT_TAPE_SPOT_TOPIC", "value": "underlying.spx.price", "_override": True},
        # 2026-11-06: prod DOES set this now ("ANY", in the globex prod env patch), which is exactly
        # the case the line below used to anticipate -- assert_no_silent_prod_wins demanded the explicit
        # decision, and this is it. es4's value wins: es4's spot stream is the SYNTHETIC_OPTION_SPOT
        # feed named two lines up, and the committed render (k8s/es4/services/context-tape.yaml) has
        # carried SYNTHETIC_OPTION_SPOT all along, so _override changes the DECLARATION, not what es4
        # runs. Prod's "ANY" is right for prod, whose spot is the SPX index stream.
        # (Was: "Not set in the prod slice (SPX defaults apply there); appended here. If prod ever
        # starts setting them, assert_no_silent_prod_wins forces the decision explicitly.")
        {"name": "CONTEXT_TAPE_SPOT_SOURCE_FILTER", "value": "SYNTHETIC_OPTION_SPOT", "_override": True},
        # "ANY" sentinel, not "": KafkaSettings treats a blank env value as unset and
        # would silently restore the "LAST" fallback — the synthetic feed has no
        # priceField at all, so the filter must be explicitly cleared. Prod sets this one too, to the
        # SAME "ANY", so there is no collision to resolve and no _override is needed; if prod's value
        # ever diverges, assert_no_silent_prod_wins will say so.
        {"name": "CONTEXT_TAPE_SPOT_FIELD_FILTER", "value": "ANY"},
        # es4's GEX history producer runs DATABENTO_SYMBOL=ES — the SPX default
        # would silently drop every record there.
        {"name": "CONTEXT_TAPE_GEX_SYMBOL_FILTER", "value": "ES"},
        # es4 shadows CME ES, which trades ~23h — frame the FULL Globex session (opens the prior
        # 18:00 ET) so overnight/pre-market ES gamma is in-session, not rejected as far-future.
        # The prod slice sets NYSE_RTH with a 07:00 ET pre-open (CONTEXT_TAPE_PRE_OPEN_ET, inherited
        # by es4 but IGNORED under GLOBEX — Globex derives its start from the 18:00 rule). es4's
        # GLOBEX must win EXPLICITLY — an implicit append would be exactly the silent-prod-win this
        # guard exists to catch.
        {"name": "CONTEXT_TAPE_SESSION_MODE", "value": "GLOBEX", "_override": True},
        # The 23h session builds ~264 buckets; raise the byte cap to match (service HARD_CAP 4 MiB).
        # The prod slice sets 1 MiB (its 07:00-open NYSE_RTH day is ~108 buckets, and NYSE_RTH caps
        # at 1 MiB anyway); es4's 23h session is far wider, so its 4 MiB must win explicitly.
        {"name": "CONTEXT_TAPE_MAX_SNAPSHOT_BYTES", "value": "4194304", "_override": True},
    ],
    "close-direction": [
        # es4 chains carry symbol "ES" (the es-prefixed dealer-ledger profile stream); the
        # engine's 0DTE chain selector must match it, not the SPX default. Topics prefix via
        # TOPIC_PREFIX at runtime like every sibling. CME ES daily options settle 16:00 ET,
        # so the SPX session calendar's close times hold for the es4 shadow too.
        {"name": "SIGNAL_SYMBOL", "value": "ES", "_override": True},
    ],
    # ES trades ~23h on CME Globex (Sun 18:00 ET - Fri 17:00 ET, daily 17:00-18:00 halt); the default
    # spx-rth calendar wrongly forced the pace board + spot model to SESSION_IDLE outside 09:30-16:15 ET.
    "volume-pace": [
        {"name": "PACE_SESSION_CALENDAR", "value": "es-globex"},
        # ES overnight per-strike updates arrive in bursts minutes apart; the SPX-tuned 15s staleness
        # gate flags the board STALE between bursts and blanks the graph. 120s matches Globex cadence.
        {"name": "PACE_STALE_MS", "value": "120000"},
    ],
    # ES underlying is the mirrored future-trades topic (es.underlying.es.trades after prefix),
    # not the SPX cash-price topic (Codex finding #3, 2026-07-12).
    "option-truth-engine": [
        {"name": "OPTION_TRUTH_OUTPUT_TOPIC", "value": "options.es.option-truth-engine-service.by-strike", "_override": True},
        # ES options are options on a futures contract. The engine deliberately fails closed when
        # an ES record is paired with the SPX spot model, so select Black-76 for this environment.
        {"name": "OPTION_TRUTH_PRICING_MODEL", "value": "FUTURES_BLACK_76", "_override": True},
    ],
    # spread-skew on ES: the label must say ES (SPREAD_SKEW_UNDERLYING, proc PR#316), the spot is the
    # mirrored ES future-trades stream (same source OPB uses — Codex finding #3 applies identically),
    # and the output topics say es not spx (TOPIC_PREFIX then makes es.options.es.spread-skew.*).
    "spread-skew": [
        {"name": "SPREAD_SKEW_UNDERLYING", "value": "ES"},
        {"name": "SPREAD_SKEW_INPUT_SPOT_TOPIC", "value": "underlying.es.trades", "_override": True},
        {"name": "SPREAD_SKEW_OUTPUT_CURRENT_TOPIC", "value": "options.es.spread-skew.current", "_override": True},
        {"name": "SPREAD_SKEW_OUTPUT_EVENTS_TOPIC", "value": "options.es.spread-skew.events", "_override": True},
    ],
    "spread-skew-postgres-writer": [
        {"name": "SKEW_WRITER_CURRENT_TOPIC", "value": "options.es.spread-skew.current", "_override": True},
        {"name": "SKEW_WRITER_EVENTS_TOPIC", "value": "options.es.spread-skew.events", "_override": True},
    ],
    # strike-intelligence reads a latest-per-symbol spot PRICE topic (its underlying global store),
    # not canonicalSpot. On es4 that is the compacted ES-future price the feed republishes
    # (ES_PRICE_ENABLED -> es.underlying.es.price after the es. prefix), NOT the SPX cash topic.
    # Every other input (strike-flow / delta-flow-by-strike / price-behavior / gex.flow.by-strike)
    # carries over from the prod slice and gets es.-prefixed to the es4 topics already flowing.
    "strike-intelligence": [
        {"name": "STRIKE_INTEL_INPUT_UNDERLYING_TOPIC", "value": "underlying.es.price", "_override": True},
        # The static *_SCORE_SCALE divisors are SPX-calibrated (1e7/1e5/1e8); ES notional is 100-1000x
        # smaller so every score collapses to ~0 and no role fires. Enable adaptive normalization
        # (processing #337): volume/delta/gex are divided by a session-relative EWMA of recent active
        # flow instead, so the fixed role thresholds keep selecting the strongest live strikes at ES
        # magnitudes. Self-calibrating — no per-session scale numbers. SPX keeps the flag OFF (default).
        {"name": "STRIKE_INTEL_ADAPTIVE_SCALE_ENABLED", "value": "true"},
        # §7c premium floor (processing #371) — es4-ONLY, not in the prod slice (SPX liquidity never
        # trips it). The adaptive scaler rates the session's top slice volumeScore 1.0 regardless of
        # ABSOLUTE size, so on thin Globex tape a ~$46k far-OTM trickle emitted BUY_CALL (2026-07-17,
        # 7630). Below $100k the action gates to NONE ("below premium floor"); roles/dashboard stay
        # informative. This value lived ONLY in the frozen generated file while strike-intelligence sat
        # in KNOWN_STALE; re-added here so a live render preserves it. Plain add (prod has no such key).
        {"name": "STRIKE_INTEL_ACTION_MIN_TOTAL_PREMIUM", "value": "100000"},
    ],
    # greek-move-authenticity on es4: the SAME engine measures ES greeks to authenticate ES moves.
    "greek-move-authenticity": [
        {"name": "GREEK_MOVE_AUTH_ENABLED", "value": "true", "_override": True},
        {"name": "GREEK_MOVE_AUTH_SYMBOL", "value": "ES", "_override": True},
        {"name": "GREEK_MOVE_AUTH_INPUT_SPOT_TOPIC", "value": "underlying.es.price", "_override": True},
    ],
}

# IBKR-variant workloads have no place in the ES pipeline (Codex finding #5).
DROP_WORKLOADS = {"directional-pressure-service"}

# Same rule, one level down: env FAMILIES the prod slice sets that must never render into es4.
# DROP_WORKLOADS removes whole IBKR workloads; this removes IBKR wiring bolted onto a workload
# es4 *does* run. Prefix-matched on purpose — prod is actively growing this path
# (OVERNIGHT-IBKR-GEX rev13+D15), and a new IBKR_GEX_* knob must not need a second incident to
# be excluded here.
#
DROP_ENV_PREFIXES = {
    # ZDCE-113/114 is calibrated and wired only for the SPX dev/prod source identities. The es4
    # renderer inherits the production Context Tape slice, but prefixing these Kafka topics would
    # silently create an unvalidated ES detector with the SPX artifact and the prod transaction id.
    "context-tape": ("ZERO_DTE_",),
}

# Rendered at replicas:0 on es4 ONLY — still defined, still deployable, just not running.
# USER 2026-07-26, "until further notice": the .4 box was carrying load average 18.4 on 12 cores
# with CPU limits unset cluster-wide, and these two were its heaviest tenants
# (dealer-ledger ~1015m/1088Mi, directional-pressure ~697m/867Mi, volume-pace ~401m/2434Mi).
# Prod and dev are NOT affected by this
# list. Re-enable = delete the name here, re-render, deploy that service.
ES4_KEEP_DOWN = {
    "dealer-ledger-service",
    # USER 2026-08-18: hold the whole dealer-ledger family down on es4 until further notice.
    # dealer-ledger-service was already listed; its two calibration workers were not, so a deploy
    # would render them at replicas:1 and quietly bring half the family back.
    "dealer-ledger-calibration-accumulator",
    "dealer-ledger-calibration-scorer",
    "volume-pace-databento-service",
    "directional-pressure-databento-service",
    # USER 2026-07-27: disable the skew service on es4. The feature is RETIRED; the postgres writer
    # goes down with its producer (nothing to consume, and the es4 box has no spare capacity).
    "spread-skew-service",
    "spread-skew-postgres-writer",
    # USER 2026-07-31: "disable truth service on es env until further notice."
    # Already OFF on production (base replicas:0 + morning-autostart KEEP_DOWN); es4 was still
    # running it. Listing it here is the load-bearing part: a hand-set replicas:0 on the box is
    # undone by the next re-render, and es4 has no morning-autostart to hold it down.
    "option-truth-engine-service",
}

# es4 runs these even though the PRODUCTION slice ships them replicas:0 (none today).
ES4_KEEP_UP = set()

# es4-ONLY container CPU-request overrides, keyed by SERVICE (none today).
ES4_CPU_REQUEST = {}

# es4-ONLY exact env-NAME drops, keyed by SERVICE (none today).
ES4_DROP_ENV_NAMES = {}

HEADER = """\
# GENERATED by scripts/es4/render_es4_manifests.py from {src} — DO NOT EDIT BY HAND.
# es4 environment: env-level knobs come from es4-common-env / es4-runtime-secrets;
# every topic gets the es. prefix at runtime via TOPIC_PREFIX (incl. env-supplied values).
# Deploy ONLY via Jenkinsfile.es4-deploy (digest-pins the :prod tag before apply).
"""


def transform_env_from(env_from):
    out = []
    for ref in env_from or []:
        cm = ref.get("configMapRef", {}).get("name")
        sec = ref.get("secretRef", {}).get("name")
        if cm == "options-edge-config":
            # base layer (full prod config incl. RF=1) THEN overrides — later envFrom wins
            out.append({"configMapRef": {"name": "es4-base-env"}})
            out.append({"configMapRef": {"name": "es4-common-env"}})
        elif sec == "options-edge-runtime-secrets":
            out.append({"secretRef": {"name": "es4-runtime-secrets"}})
        # everything else (feed-env etc., all optional in prod) is intentionally dropped
    names = [x.get("configMapRef", {}).get("name") for x in out]
    if "es4-common-env" not in names:
        out[:0] = [{"configMapRef": {"name": "es4-base-env"}}, {"configMapRef": {"name": "es4-common-env"}}]
    return out


def transform(svc, docs):
    # drop IBKR-variant Deployments/Services wholesale
    docs = [d for d in docs
            if not (isinstance(d, dict) and d.get("kind") in ("Deployment", "Service")
                    and d.get("metadata", {}).get("name") in DROP_WORKLOADS)]
    claim_names = []
    for doc in docs:
        if not isinstance(doc, dict) or doc.get("kind") != "Deployment":
            continue
        # es4-only keep-down. These slices are copied verbatim from production, so a hand-edited
        # replicas:0 in k8s/es4/services/*.yaml is silently undone the next time anyone re-renders.
        # Forcing it HERE is what makes the disable survive a re-render, which is the only way it
        # actually holds. Prod and dev are untouched: this is the es4 renderer.
        if doc.get("metadata", {}).get("name") in ES4_KEEP_DOWN:
            doc["spec"]["replicas"] = 0
        # es4 runs it though the prod slice ships replicas:0 (inverse of KEEP_DOWN). A name is never
        # in both; preserved es4 decision, not a re-enable.
        if doc.get("metadata", {}).get("name") in ES4_KEEP_UP:
            doc["spec"]["replicas"] = 1
        for v in (doc["spec"]["template"]["spec"].get("volumes") or []):
            pvc = v.get("persistentVolumeClaim", {}).get("claimName")
            if pvc:
                claim_names.append(pvc)
        for c in doc["spec"]["template"]["spec"]["containers"]:
            c["envFrom"] = transform_env_from(c.get("envFrom"))
            # drop the env families that have no place on es4 (see DROP_ENV_PREFIXES). Done
            # BEFORE the ES_ENV merge so a dropped name can never shadow an es4 intent.
            drop = DROP_ENV_PREFIXES.get(svc)
            if drop and c.get("env"):
                c["env"] = [e for e in c["env"] if not e.get("name", "").startswith(drop)]
            # es4-only exact env-NAME drops (keep an es4 behaviour the prod slice would change).
            drop_names = ES4_DROP_ENV_NAMES.get(svc)
            if drop_names and c.get("env"):
                c["env"] = [e for e in c["env"] if e.get("name") not in drop_names]
            # es4-only CPU-request override (single capacity-pressured box; owner-pinned, preserved).
            cpu = ES4_CPU_REQUEST.get(svc)
            if cpu:
                c.setdefault("resources", {}).setdefault("requests", {})["cpu"] = cpu
            # redis runs in the es4 compose stack on the HOST, not in-cluster
            for e in c.get("env") or []:
                if e.get("value") == "options-edge-redis":
                    e["value"] = "192.168.100.4"
            base = c["image"].split("/")[-1].split(":")[0]
            c["image"] = f"{REGISTRY}/{base}:prod"
            c.pop("imagePullPolicy", None)
            extra = ES_ENV.get(svc, [])
            if extra:
                env = c.get("env") or []
                by_name = {e["name"]: e for e in env}
                for item in copy.deepcopy(extra):
                    override = item.pop("_override", False)
                    if item["name"] in by_name:
                        if override:
                            by_name[item["name"]]["value"] = item["value"]
                    else:
                        env.append(item)
                c["env"] = env
    # emit a PVC for every claim referenced (fresh cluster has none; k3s default
    # local-path StorageClass provisions on first bind — Codex blocker #1)
    for name in sorted(set(claim_names)):
        docs.append({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": "options-edge",
                         "labels": {"app.kubernetes.io/part-of": "options-edge-es4"}},
            "spec": {"accessModes": ["ReadWriteOnce"],
                     "resources": {"requests": {"storage": "5Gi"}}},
        })
    return docs


def assert_no_silent_prod_wins(svc, docs):
    """An ES_ENV entry without _override LOSES to a differing prod value — fail instead.

    The transform only inserts a non-override entry when the prod slice does not already define
    that name. So the moment prod starts setting the same knob with a different value, the es4
    intent is dropped and nothing says so. That is not theoretical: prod added
    DELTA_FLOW_GREEK_MAX_AGE_MS=15000 and STRIKE_FLOW_CONTRACT_MULTIPLIER=100.0, and es4 silently
    inherited both — the second one had es4's strike-flow computing every ES notional at the SPX
    multiplier, i.e. DOUBLE, live on the box.

    Whether prod or es4 should win is a real decision, so it has to be made explicitly (add
    _override, or drop the entry). A build failure is the only way that decision cannot be skipped.
    """
    prod = {}
    for d in docs:
        if not isinstance(d, dict) or d.get("kind") != "Deployment":
            continue
        for c in d["spec"]["template"]["spec"]["containers"]:
            for e in c.get("env") or []:
                if "value" in e:
                    prod[e["name"]] = e["value"]
    for item in ES_ENV.get(svc, []):
        if item.get("_override"):
            continue
        name = item["name"]
        if name in prod and str(prod[name]) != str(item["value"]):
            sys.exit(
                f"ES_ENV COLLISION in {svc}: {name} — es4 wants {item['value']!r} but the "
                f"production slice sets {prod[name]!r}, so the render would silently use prod's "
                f"value. Add \"_override\": True if es4's value must win, or delete the ES_ENV "
                f"entry if prod's is now correct for es4 too.")


def assert_drop_prefixes_are_coherent():
    """DROP_ENV_PREFIXES must name real services and must not contradict ES_ENV.

    Both are cheap mistakes with silent consequences: a prefix keyed on a service that is not
    rendered does nothing at all, and a prefix that also matches an ES_ENV entry would delete the
    es4 intent the entry exists to state (and would confuse assert_no_silent_prod_wins, which
    compares against the RAW prod env, i.e. before the drop).
    """
    for svc, prefixes in DROP_ENV_PREFIXES.items():
        if svc not in SERVICES:
            sys.exit(f"DROP_ENV_PREFIXES names {svc!r}, which is not in SERVICES — it would never apply.")
        for item in ES_ENV.get(svc, []):
            if item["name"].startswith(tuple(prefixes)):
                sys.exit(
                    f"DROP_ENV_PREFIXES/ES_ENV CONTRADICTION in {svc}: {item['name']} is both "
                    f"dropped and set as es4 intent. Pick one.")


def assert_es4_overrides_are_coherent():
    # Fail CLOSED on an override that cannot take effect — a typo or a double-listing must be a build
    # error here, never a silent es4 behaviour on a LIVE box (Codex MAJOR). Keep-set NAME validity
    # (every keep-up/down name matches a real rendered Deployment) is checked in main() where the
    # rendered deployment names are known.
    both = ES4_KEEP_UP & ES4_KEEP_DOWN
    if both:
        sys.exit(f"ES4_KEEP_UP & ES4_KEEP_DOWN both list {sorted(both)} — a workload cannot be held "
                 f"down and forced up (KEEP_UP runs second and would silently scale a held one to 1).")
    svcset = set(SERVICES)
    for name, m in (("ES4_CPU_REQUEST", ES4_CPU_REQUEST), ("ES4_DROP_ENV_NAMES", ES4_DROP_ENV_NAMES)):
        stray = set(m) - svcset
        if stray:
            sys.exit(f"{name} keys not in SERVICES (so inert — typo?): {sorted(stray)}")


def main():
    assert_drop_prefixes_are_coherent()
    assert_es4_overrides_are_coherent()
    os.makedirs(OUT_DIR, exist_ok=True)
    seen_deployments = set()
    for svc in SERVICES:
        src = f"k8s/services/{svc}/overlays/production/manifest.yaml"
        if not os.path.isfile(src):
            sys.exit(f"MISSING slice: {src}")
        with open(src) as f:
            docs = [d for d in yaml.safe_load_all(f) if d is not None]
        assert_no_silent_prod_wins(svc, docs)      # before transform: compare against RAW prod env
        docs = transform(svc, docs)
        seen_deployments.update(
            d["metadata"]["name"] for d in docs
            if d.get("kind") == "Deployment" and d.get("metadata", {}).get("name")
        )
        buf = io.StringIO()
        yaml.safe_dump_all(docs, buf, sort_keys=False, default_flow_style=False)
        out = f"{OUT_DIR}/{svc}.yaml"
        # Refuse to write through a symlink or onto a non-regular path. open(...,"w") FOLLOWS a
        # symlink, so a manifest path replaced by one would silently overwrite its target while
        # this directory still looks clean. Checked here rather than only in
        # scripts/ci/validate-es4-render.sh because by the time that script's file-set check runs
        # the render has already happened — and because the renderer is normally run by hand.
        if os.path.islink(out) or (os.path.exists(out) and not os.path.isfile(out)):
            sys.exit(f"REFUSING to write {out}: not a regular file (symlink or directory). "
                     f"Every es4 manifest path must be a plain file.")
        with open(out, "w") as f:
            f.write(HEADER.format(src=src))
            f.write(buf.getvalue())
        print(f"rendered {out}")
    # Keep-set names must each match a Deployment that was actually rendered. A misspelt or
    # out-of-scope name is inert (it changes nothing), so fail CLOSED here rather than ship a
    # keep-up/keep-down that silently does not apply on the live box (Codex MAJOR).
    for name, s in (("ES4_KEEP_UP", ES4_KEEP_UP), ("ES4_KEEP_DOWN", ES4_KEEP_DOWN)):
        stray = s - seen_deployments
        if stray:
            sys.exit(f"{name} names matched no rendered Deployment (inert — typo?): {sorted(stray)}")
    print(f"done: {len(SERVICES)} services")


if __name__ == "__main__":
    main()
