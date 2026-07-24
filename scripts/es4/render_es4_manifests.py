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
        # OI on ES rides live in the feed (GLBX statistics stat_type=9 -> openInterest on every record),
        # NOT via the OPRA/OSI REST fetch (which can't parse CME symbols) -> keep the direct fetch OFF.
        {"name": "DATABENTO_GEX_OI_DIRECT_FETCH_ENABLED", "value": "false", "_override": True},
        # ...but the feed holds that OI only in RAM, so a feed restart drops it to 0 (=> blank GEX) until the
        # next daily publication. PERSIST the live OI to Postgres (databento_option_raw_snapshot) as it is
        # observed, and BACKFILL from it when a later snapshot arrives with OI=0. Together these make ES GEX
        # restart-durable using the same DB path SPX uses, without the GLBX symbology adapter.
        {"name": "DATABENTO_GEX_OI_BASELINE_BACKFILL_ENABLED", "value": "true"},
        {"name": "DATABENTO_GEX_OI_PERSIST_LIVE_ENABLED", "value": "true"},
        # Top-3 fade/lifecycle gate (proc #466/#467; dev deploy #584, prod deploy #585+#813): fade +
        # lifecycle publish only the top-3 |netGex| strikes per chain; stage-1 per-strike netGex unchanged.
        # Mirrors prod so ES badges land on the SAME top-gamma strikes the board highlights. Enabling
        # switches fade+lifecycle to isolated "-topn" store lineages (one-time state reset, by design).
        {"name": "DATABENTO_GEX_FLOW_TOP_N", "value": "3"},
        # ES-GEX-on-SPX bridge (DatabentoGexBridge, behind ES_GEX_SPXBRIDGE_ENABLED): republish the raw
        # es gex.strike as self-describing JSON on KAFKA_ES_GEX_SPXBRIDGE_TOPIC (default
        # options.databento.gex.spxbridge -> es.options.databento.gex.spxbridge via TOPIC_PREFIX). The
        # per-target MM1 mirror carries it to prod .252 where es-spx-align-service joins it against the
        # basis into options.es-gex-spx-aligned (the SPX-chain ES-gamma overlay). es4-ONLY: the SPX prod
        # gex-service leaves this OFF (its bridge output would be a pointless SPX->spx echo). Without this
        # the whole ES-gamma-band pipeline idles on an empty input (the align service fail-closes to blank).
        {"name": "ES_GEX_SPXBRIDGE_ENABLED", "value": "true"},
    ],
    "strike-flow-classifier": [
        {"name": "STRIKE_FLOW_CONTRACT_MULTIPLIER", "value": "50"},
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
        {"name": "DELTA_FLOW_GREEK_MAX_AGE_MS", "value": "30000"},
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
    "vix-option-inteligence": [
        # On es4 the same deterministic engine analyzes the ES 0DTE option tape. TOPIC_PREFIX
        # still isolates all input/output topics on the .4 broker.
        {"name": "ZERO_DTE_SYMBOL", "value": "ES", "_override": True},
    ],
}

# IBKR-variant workloads have no place in the ES pipeline (Codex finding #5).
DROP_WORKLOADS = {"raw-to-display-service", "directional-pressure-service"}

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


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    for svc in SERVICES:
        src = f"k8s/services/{svc}/overlays/production/manifest.yaml"
        if not os.path.isfile(src):
            sys.exit(f"MISSING slice: {src}")
        with open(src) as f:
            docs = [d for d in yaml.safe_load_all(f) if d is not None]
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
