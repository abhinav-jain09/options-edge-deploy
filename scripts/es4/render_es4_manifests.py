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
    "databento-gex", "databento-gex-history", "databento-maxpain", "databento-mission-sandwich",
    "databento-volume-aggregator", "dealer-ledger", "dealer-ledger-calibration", "delta-flow",
    "directional-pressure", "gex-delta-redis-writer", "option-price-behavior", "option-truth-engine", "pin-postgres-writer",
    "pressure-postgres-writer", "raw-to-display", "strike-flow-avro-adapter", "strike-flow-classifier",
    "strike-liquidity-heatmap", "volume-pace", "spread-skew", "spread-skew-postgres-writer",
    "greek-move-authenticity", "gamma-migration",
]

ES_ENV = {
    "close-direction": [
        # es4 chains carry symbol "ES" (the es-prefixed dealer-ledger profile stream); the
        # engine's 0DTE chain selector must match it, not the SPX default. Topics prefix via
        # TOPIC_PREFIX at runtime like every sibling. CME ES daily options settle 16:00 ET,
        # so the SPX session calendar's close times hold for the es4 shadow too.
        {"name": "SIGNAL_SYMBOL", "value": "ES", "_override": True},
    ],
    "databento-gex": [
        {"name": "DATABENTO_GEX_RISK_FREE_RATE", "value": "0.04"},
        {"name": "DATABENTO_GEX_DIVIDEND_YIELD", "value": "0.04"},   # q=r -> Black-76 on the future
        # es4 has NO market-carry-service (it is an SPX put-call-parity service; the SERVICES list
        # correctly omits it), so nothing produces the market-carry topic here. The prod slice runs
        # dynamic carry ON (long-standing; PR #582 later widened the age gate to 660s for the 5-min
        # cadence and enabled STATIC_FALLBACK) — without this pin a re-render flips es4 gex to a
        # GlobalKTable over a producer-less auto-created topic and parks every record on the
        # STATIC_FALLBACK rescue path (observed live: 3.07M fallback-priced records after the
        # 2026-07-25 re-render deploy). es4 gex intentionally prices with the static Black-76 carry
        # above (regime STATIC_CONFIG); with dynamic carry off, gex does not even build the carry
        # GlobalKTable.
        {"name": "DATABENTO_GEX_DYNAMIC_CARRY_ENABLED", "value": "false", "_override": True},
        # OI on ES rides live in the feed (GLBX statistics stat_type=9 -> openInterest on every record),
        # NOT via the OPRA/OSI REST fetch (which can't parse CME symbols) -> keep the direct fetch OFF.
        {"name": "DATABENTO_GEX_OI_DIRECT_FETCH_ENABLED", "value": "false", "_override": True},
        # ...but the feed holds that OI only in RAM, so a feed restart drops it to 0 (=> blank GEX) until the
        # next daily publication. PERSIST the live OI to Postgres (databento_option_raw_snapshot) as it is
        # observed, and BACKFILL from it when a later snapshot arrives with OI=0. Together these make ES GEX
        # restart-durable using the same DB path SPX uses, without the GLBX symbology adapter.
        {"name": "DATABENTO_GEX_OI_BASELINE_BACKFILL_ENABLED", "value": "true"},
        {"name": "DATABENTO_GEX_OI_PERSIST_LIVE_ENABLED", "value": "true"},
        # DATABENTO_GEX_FLOW_TOP_N: es4 now INHERITS the production slice's value (40 as of
        # 2026-08-03). The former es4 pin of 3 (proc #466/#467 rollout) starved strike-intelligence
        # — its ONLY GEX input is the flow topic this gate feeds, so top-3 left ~65% of the es4
        # near-ATM ladder GEX-missing (measured on es.strike-intelligence-by-strike). Dev+prod both
        # run 40 with zero streams lag. Watch es4 gex lag after rollout; the rollback is a pin here.
        # ES-GEX-on-SPX bridge (DatabentoGexBridge, behind ES_GEX_SPXBRIDGE_ENABLED): republish the raw
        # es gex.strike as self-describing JSON on KAFKA_ES_GEX_SPXBRIDGE_TOPIC (default
        # options.databento.gex.spxbridge -> es.options.databento.gex.spxbridge via TOPIC_PREFIX). The
        # per-target MM1 mirror carries it to prod .252 where es-spx-align-service joins it against the
        # basis into options.es-gex-spx-aligned (the SPX-chain ES-gamma overlay). es4-ONLY: the SPX prod
        # gex-service leaves this OFF (its bridge output would be a pointless SPX->spx echo). Without this
        # the whole ES-gamma-band pipeline idles on an empty input (the align service fail-closes to blank).
        {"name": "ES_GEX_SPXBRIDGE_ENABLED", "value": "true"},
        # --- OI session identity + audit fixes (audit 2026-07-25, P0/P1/P2) ---------------------
        # P0: the durable OI identity must be keyed by the CME TRADE DATE, not the UTC calendar
        # date. Globex opens 18:00 ET and the session crosses both midnights; under UTC keying one
        # session split into two OI rows, so a post-midnight feed/GEX restart looked up the new UTC
        # day, found nothing, and held ES GEX NOT_READY (the documented 00:00-06:00 ET OI loss).
        # 18:00-ET rollover = timestamps at/after Globex open belong to the NEXT trade date.
        {"name": "DATABENTO_GEX_OI_SESSION_ROLL_AFTER", "value": "18:00"},
        # Overnight OI carry (PR#543) is DISABLED here (USER 2026-08-03): the ES feed stamps live GLBX
        # open interest onto every snapshot, so ES's OI source is the FEED, not the Postgres baseline —
        # and the es4 persist-live table only ever holds each session's own 0DTE expiry, so a prior-session
        # row for TODAY's expiry never exists (0DTE: yesterday's expiry died yesterday). The carry could
        # never fire usefully here; leaving it enabled just adds a dead DB probe per chain. The code stays
        # merged (flag-off default) for expiring-later products / other envs if ever needed.
        {"name": "DATABENTO_GEX_OI_OVERNIGHT_CARRY_ENABLED", "value": "false"},
        # P2: ES contract multiplier is 50 (E-mini), not the SPX 100 the persist SQL used to
        # hardcode; live rows now stamp the snapshot's own multiplier and this covers any path
        # that has no per-record value. Existing same-key rows self-heal on the next upsert.
        {"name": "DATABENTO_GEX_CONTRACT_MULTIPLIER", "value": "50"},
        # P1: the OI watchdog watched [SPX] on the ES pod (prefetch-symbols default), so an ES OI
        # outage could never raise OI_MISSING. Watch the actual instrument; the eager prefetch
        # stays OFF because it can only drive the (disabled-on-ES) OPRA direct fetch.
        {"name": "DATABENTO_GEX_OI_EAGER_PREFETCH_SYMBOLS", "value": "ES", "_override": True},
        {"name": "DATABENTO_GEX_OI_EAGER_PREFETCH_ENABLED", "value": "false", "_override": True},
    ],
    "strike-flow-classifier": [
        {"name": "STRIKE_FLOW_CONTRACT_MULTIPLIER", "value": "50", "_override": True},
        # STRIKE_FLOW_SYMBOL is an AUTHORITATIVE allow-list since processing PR#494. The classifier
        # derives each trade's symbol from the payload `underlying` field, which is "ES" for every GLBX
        # ES-option record (weekly roots E1A..E4E/EW arrive as `product`, not the symbol). The inherited
        # production "SPX" rejected 100% of es4 input once the allow-list went live (2026-07-27 outage:
        # rejected_symbol_total 1073+, processed 0) — this override is the durable fix.
        {"name": "STRIKE_FLOW_SYMBOL", "value": "ES", "_override": True},
    ],
    # ES trades ~23h on CME Globex (Sun 18:00 ET - Fri 17:00 ET, daily 17:00-18:00 halt); the default
    # spx-rth calendar wrongly forced the pace board + spot model to SESSION_IDLE outside 09:30-16:15 ET.
    "volume-pace": [
        {"name": "PACE_SESSION_CALENDAR", "value": "es-globex"},
        # ES overnight per-strike updates arrive in bursts minutes apart; the SPX-tuned 15s staleness
        # gate flags the board STALE between bursts and blanks the graph. 120s matches Globex cadence.
        {"name": "PACE_STALE_MS", "value": "120000"},
    ],
    "delta-flow": [
        {"name": "DELTA_FLOW_DEFAULT_CONTRACT_MULTIPLIER", "value": "50"},
        {"name": "DELTA_FLOW_VERIFIED_MULTIPLIERS", "value": "ES:50"},
        # ES trades on GLBX/CME, which stamps the TRUE aggressor side (trades.side B/A). Use it instead of
        # trade-vs-NBBO estimation. In-code GLBX-dataset provenance gate keeps it inert for non-GLBX data;
        # SPX/OPRA (side='N') is untouched and keeps this OFF (its default).
        {"name": "DELTA_FLOW_USE_NATIVE_SIDE", "value": "true"},
        # ES 0DTE is far thinner than SPX: prints are 1-4 lots, so the SPX-tuned default floor of 5
        # (DELTA_FLOW_MIN_CONTRACTS) rejects essentially all ES flow (BELOW_MIN_CONTRACTS) and the delta
        # cells stay blank. ES has a $50 multiplier and low lot counts, so a floor of 1 surfaces real flow
        # without meaningful noise. SPX keeps its default 5 (unaffected).
        {"name": "DELTA_FLOW_MIN_CONTRACTS", "value": "1"},
        # ES 0DTE greek<->trade freshness widening: thin volume + jittery async event-time streams make
        # the SPX-tuned gates (greek 5s / quote 2s, zero lookahead tolerance) reject a large chunk on
        # STALE_GREEK/LOOKAHEAD_GREEK, starving by-strike delta-flow. Widen the accept window and allow a
        # small lookahead (ordering jitter, not look-ahead bias). SPX keeps its defaults (5000/2000/0/0).
        # The two MAX_AGE knobs already exist in the shipped code; the LOOKAHEAD tolerances take effect
        # once the delta-flow image carrying proc PR#334 is deployed (inert / ignored on older images).
        {"name": "DELTA_FLOW_GREEK_MAX_AGE_MS", "value": "30000", "_override": True},
        {"name": "DELTA_FLOW_QUOTE_MAX_AGE_MS", "value": "10000"},
        {"name": "DELTA_FLOW_GREEK_LOOKAHEAD_TOLERANCE_MS", "value": "2000"},
        {"name": "DELTA_FLOW_QUOTE_LOOKAHEAD_TOLERANCE_MS", "value": "2000"},
    ],
    # ES underlying is the mirrored future-trades topic (es.underlying.es.trades after prefix),
    # not the SPX cash-price topic (Codex finding #3, 2026-07-12).
    "option-price-behavior": [
        {"name": "OPTION_PRICE_BEHAVIOR_INPUT_UNDERLYING_TOPIC", "value": "underlying.es.trades", "_override": True},
    ],
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
    ],
    # greek-move-authenticity on es4: the SAME engine measures ES greeks to authenticate ES moves.
    "greek-move-authenticity": [
        {"name": "GREEK_MOVE_AUTH_ENABLED", "value": "true", "_override": True},
        {"name": "GREEK_MOVE_AUTH_SYMBOL", "value": "ES", "_override": True},
        {"name": "GREEK_MOVE_AUTH_INPUT_SPOT_TOPIC", "value": "underlying.es.price", "_override": True},
    ],
    "vix-option-inteligence": [
        # On es4 the same deterministic engine analyzes the ES 0DTE option tape. TOPIC_PREFIX
        # still isolates all input/output topics on the .4 broker.
        {"name": "ZERO_DTE_SYMBOL", "value": "ES", "_override": True},
    ],
}

# IBKR-variant workloads have no place in the ES pipeline (Codex finding #5).
DROP_WORKLOADS = {"raw-to-display-service", "directional-pressure-service"}

# Rendered at replicas:0 on es4 ONLY — still defined, still deployable, just not running.
# USER 2026-07-26, "until further notice": the .4 box was carrying load average 18.4 on 12 cores
# with CPU limits unset cluster-wide, and these two were its heaviest tenants
# (dealer-ledger ~1015m/1088Mi, directional-pressure ~697m/867Mi, volume-pace ~401m/2434Mi).
# Prod and dev are NOT affected by this
# list. Re-enable = delete the name here, re-render, deploy that service.
ES4_KEEP_DOWN = {
    "dealer-ledger-service",
    "volume-pace-databento-service",
    "directional-pressure-databento-service",
    # USER 2026-07-30: strike-liquidity-heatmap RE-ENABLED on es4 to evaluate the wall-line /
    # buy-bubble overlay against the ES chain. Measured headroom before removing it: .4 was at
    # 8251m/12 cores (68%) live with 39Gi memory available, so its 750m/512Mi request fits.
    # KNOWN AND ACCEPTED (USER decided after being shown the measurement): es.options.databento.
    # strike-flow held only 37 records at the time, so the ribbon colour and wall line (both fed by
    # es.options.databento.events.raw, verified 100% cbbo-1s) will render but the trade bubbles will
    # not until that topic is populated. See the es4 strike-flow wiring follow-up.
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
        for v in (doc["spec"]["template"]["spec"].get("volumes") or []):
            pvc = v.get("persistentVolumeClaim", {}).get("claimName")
            if pvc:
                claim_names.append(pvc)
        for c in doc["spec"]["template"]["spec"]["containers"]:
            c["envFrom"] = transform_env_from(c.get("envFrom"))
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


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    for svc in SERVICES:
        src = f"k8s/services/{svc}/overlays/production/manifest.yaml"
        if not os.path.isfile(src):
            sys.exit(f"MISSING slice: {src}")
        with open(src) as f:
            docs = [d for d in yaml.safe_load_all(f) if d is not None]
        assert_no_silent_prod_wins(svc, docs)      # before transform: compare against RAW prod env
        docs = transform(svc, docs)
        buf = io.StringIO()
        yaml.safe_dump_all(docs, buf, sort_keys=False, default_flow_style=False)
        with open(f"{OUT_DIR}/{svc}.yaml", "w") as f:
            f.write(HEADER.format(src=src))
            f.write(buf.getvalue())
        print(f"rendered {OUT_DIR}/{svc}.yaml")
    print(f"done: {len(SERVICES)} services")


if __name__ == "__main__":
    main()
