#!/usr/bin/env python3
"""Baseline inventory -> schema-manifest (docs/schema-governance R4). Reads the `inventory` output of schema-governance-tool for
dev, prod and es4 and (re)writes the BASELINE part of schema-manifest/manifest.json, leaving every hand-curated entry alone.

  python3 scripts/ci/schema-manifest-init.py --dev inv-dev.json --prod inv-prod.json --es4 inv-es4.json \
      --schema-text-dir DIR   (DIR/<env>/<subject>/v<N>.avsc, written by the same run that produced the inventories)

Classification (stated so a reviewer can disagree with it):
  MIRRORED   EVERY version's id >= 2000 AND, for every version number present in more than one registry, schemaHash agrees. When the id
             differs between registries the manifest id is the first registry's (dev, prod, es4) and the others are `idByEnvironment`
             (R3f); that is safe only while no byte-copying mirror carries the subject, which the validator checks (activeMirrors).
  ENV_LOCAL  everything else, each with the reason it is local: schema content differs per environment ("content-divergent"), or an id
             is a low auto-assigned one ("auto-assigned").
Baseline MIRRORED entries are `retained` (participants are the R5/R6 backlog) and take their source from the matching contracts or
processing .avsc when one exists (FILE) and otherwise ARCHIVAL (captured from the registry). A subject whose id is shared with another
subject is ENV_LOCAL ("shares id") so that no two entries share a manifest id (R3a).
"""
import argparse, collections, json, os, re, sys

ap = argparse.ArgumentParser()
for e in ("dev", "prod", "es4"): ap.add_argument("--" + e, required=True)
ap.add_argument("--schema-text-dir", required=True)
ap.add_argument("--jar", default="tools/schema-governance/schema-governance-tool.jar", help="the tool jar: schemaHash is recomputed from the copied text")
ap.add_argument("--contracts-root", required=True, help="checkout of options-edge-contracts: FILE sources are recorded with their REPO-RELATIVE path and verified to hash to the entry")
ap.add_argument("--manifest", default="schema-manifest/manifest.json")
ap.add_argument("--snapshots", default="schema-manifest/schemas")
a = ap.parse_args()
ENVS = ("dev", "prod", "es4")
inv = {e: {(r["subject"], r["version"]): r for r in json.load(open(getattr(a, e)))["entries"]} for e in ENVS}
subjects = sorted({s for e in ENVS for (s, _) in inv[e]})
manifest = json.load(open(a.manifest))
curated = {e["subject"] for e in manifest["entries"] if not e.get("baseline")}
kept = [e for e in manifest["entries"] if e["subject"] in curated]
blocks = manifest.get("familyBlocks", {})


def family(subject):
    if subject.startswith("com.optionsedge."):
        p = subject.split(".")
        return p[3] if len(p) > 4 else p[2]
    return subject.split(".")[0].split("-")[0]


def topic_of(subject):
    return subject[:-6] if subject.endswith("-value") else None


per = {s: {e: {v: inv[e][(s, v)] for (ss, v) in inv[e] if ss == s} for e in ENVS} for s in subjects}
plan = {}
for s in subjects:
    if s in curated: continue
    vers = sorted({v for e in ENVS for v in per[s][e]})
    ids = [r["id"] for e in ENVS for r in per[s][e].values()]
    content_div = any(len({per[s][e][v]["schemaHash"] for e in ENVS if v in per[s][e]}) > 1 for v in vers)
    id_div = any(len({per[s][e][v]["id"] for e in ENVS if v in per[s][e]}) > 1 for v in vers)
    if content_div: plan[s] = ("ENV_LOCAL", "content-divergent: the same version number holds a different schema in different registries")
    elif any(i < 2000 for i in ids): plan[s] = ("ENV_LOCAL", "auto-assigned: at least one version has a registry-assigned id below 2000 (not a fixed id, so not mirror-safe by construction)")
    else: plan[s] = ("MIRRORED", "")  # id-divergent subjects too (R3f): the dev id is the manifest id, the others are idByEnvironment
# two VERSIONS of one subject may not share an id either (R3a is per (subject, version))
for s in subjects:
    if plan.get(s, ("",))[0] != "MIRRORED": continue
    seen_ids = {}
    for e in ENVS:
        for v, r in per[s][e].items():
            if seen_ids.setdefault(r["id"], v) != v:
                plan[s] = ("ENV_LOCAL", "two versions of this subject share id %d (R3a)" % r["id"])
# a manifest id may belong to ONE entry (R3a). Subjects present in MORE registries claim their id first; a later holder that lives in a
# single registry keeps its real id as `idByEnvironment` and takes an unused manifest id from the ALIAS block (the manifest id is then only a
# label), and a later holder in several registries becomes ENV_LOCAL.
ALIAS_START = 9000
canon = {}
envs_of = {x: {e for e in ENVS if per[x][e]} for x in subjects}
owner = {e["id"]: e["subject"] for e in kept if e.get("kind") == "MIRRORED"}
envs_of.update({e["subject"]: set(e["environments"]) for e in kept})
used_alias = set()
for s in sorted(subjects, key=lambda x: (-sum(1 for e in ENVS if per[x][e]), x)):
    if plan.get(s, ("",))[0] != "MIRRORED": continue
    single = sum(1 for e in ENVS if per[s][e]) == 1
    for v in sorted({v for e in ENVS for v in per[s][e]}):
        i = next(per[s][e][v]["id"] for e in ENVS if v in per[s][e])
        if owner.get(i, s) != s:
            if single and not (envs_of[owner[i]] & envs_of[s]):  # same registry = the same schema registered twice (an alias), not a collision
                j = ALIAS_START
                while j in owner or j in used_alias: j += 1
                used_alias.add(j); owner[j] = s; canon[(s, v)] = j
                continue
            plan[s] = ("ENV_LOCAL", "shares id %d with %s (debug/scratch alias or duplicate registration)" % (i, owner[i])); break
        owner[i] = s

import subprocess


def jar_hash(path):
    return subprocess.check_output(["java", "-jar", a.jar, "hash", path], text=True, stderr=subprocess.DEVNULL).strip()


# PRE-FLIGHT, before anything is written: every schema text the run will copy must hash to the inventory's schemaHash. A stale or
# misrouted --schema-text-dir would otherwise generate snapshots whose text and recorded hash disagree.
for s in subjects:
    if s in curated or plan[s][0] != "MIRRORED": continue
    for e in ENVS:
        for v, r in per[s][e].items():
            p = os.path.join(a.schema_text_dir, e, "%s/v%d.avsc" % (s, v))
            if not os.path.exists(p): sys.exit("FAIL: no schema text for %s v%d in %s (%s)" % (s, v, e, p))
            if jar_hash(p) != r["schemaHash"]: sys.exit("FAIL: %s v%d in %s: the schema text hashes to %s but the inventory says %s - refusing to write anything" % (s, v, e, jar_hash(p), r["schemaHash"]))
entries = list(kept); fam_ids = collections.defaultdict(set)
for s in subjects:
    if s in curated: continue
    kind, reason = plan[s]
    envs = [e for e in ENVS if per[s][e]]
    if kind == "ENV_LOCAL":
        entries.append({"kind": "ENV_LOCAL", "subject": s, "environments": envs, "family": family(s), "baseline": True,
                        "registeredBy": "unknown (inventory baseline 2026-11-03)", "reason": reason})
        continue
    for v in sorted({v for e in ENVS for v in per[s][e]}):
        have = [e for e in ENVS if v in per[s][e]]
        r = per[s][have[0]][v]
        rel = "%s/v%d.avsc" % (s, v)
        os.makedirs(os.path.dirname(os.path.join(a.snapshots, rel)), exist_ok=True)
        text = open(os.path.join(a.schema_text_dir, have[0], rel)).read()
        open(os.path.join(a.snapshots, rel), "w").write(text)
        if r["schemaFile"]:
            # the inventory path is relative to the directory it was found under (schemas/ or schemas-registry/); record the REPO-relative path and
            # prove the file is really there and hashes to this entry (requirement R8a)
            cands = [os.path.join(d, r["schemaFile"]) for d in ("schemas", "schemas-registry")]
            found = [c for c in cands if os.path.exists(os.path.join(a.contracts_root, c)) and jar_hash(os.path.join(a.contracts_root, c)) == r["schemaHash"]]
            if not found: sys.exit("FAIL: %s v%d: the inventory matched %s but no file under %s/{schemas,schemas-registry} hashes to %s" % (s, v, r["schemaFile"], a.contracts_root, r["schemaHash"]))
            source = {"kind": "FILE", "repo": "contracts", "path": found[0]}
        else:
            source = {"kind": "ARCHIVAL", "capturedFrom": {"env": have[0], "id": r["id"]},
                      "reason": "inventory baseline 2026-11-03: no .avsc in the contracts or processing repos hashes to this schema (code-defined or registry-only)"}
        cid = canon.get((s, v), r["id"])
        fam = family(s); fam_ids[fam].add(cid)
        t = topic_of(s)
        override = {e: per[s][e][v]["id"] for e in have if per[s][e][v]["id"] != cid}
        entries.append({"kind": "MIRRORED", "baseline": True, "subject": s, "version": v, "id": cid, "family": fam,
                        **({"idByEnvironment": override} if override else {}),
                        "schemaFile": rel, "schemaHash": r["schemaHash"], "environments": have, "source": source,
                        "topics": [{"name": t if t else "record-name:" + s, "kind": "dlq" if t and "dlq" in t else "data"}],
                        "producers": [], "consumers": [], "retained": True,
                        "retainedReason": "inventory baseline 2026-11-03: participants not yet declared (R5/R6 backlog)"})
for fam, ids in fam_ids.items():
    cur = blocks.setdefault(fam, [])
    for i in sorted(ids):
        if not any(lo <= i <= hi for lo, hi in cur): cur.append([i, i])
manifest["familyBlocks"] = blocks
manifest["entries"] = sorted(entries, key=lambda e: (e["subject"], e.get("version", 0)))
manifest["note"] = "Baseline inventory of dev, prod and es4 (2026-11-03) plus hand-curated entries (no 'baseline' flag). See docs/schema-governance."
json.dump(manifest, open(a.manifest, "w"), indent=1); open(a.manifest, "a").write("\n")
c = collections.Counter(e["kind"] for e in entries)
print("entries:", dict(c), "| subjects:", len(subjects), "| curated kept:", len(curated))
print(collections.Counter(p[1].split(":")[0] for p in plan.values() if p[0] == "ENV_LOCAL"))
