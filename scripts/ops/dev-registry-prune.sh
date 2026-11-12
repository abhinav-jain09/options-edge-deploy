#!/usr/bin/env bash
# dev-registry-prune.sh — keep ONLY the `:dev` tag + every digest the cluster actually references,
# per repo, in the local dev registry (host.docker.internal:5001); delete older build tags, then
# garbage-collect to reclaim the blob store. Docker Desktop auto-TRIMs Docker.raw afterward.
#
# ⚠️ MULTI-ARCH SAFETY: do NOT pass `--delete-untagged=true`. Our `:dev` images are OCI *indexes*
# (buildx multi-arch: an arm64 manifest + an unknown/unknown attestation manifest). The per-arch
# sub-manifests are referenced ONLY by the index, so they are "untagged" — and registry:2's
# `--delete-untagged` does not treat index→sub-manifest references as reachable, so it DELETES the
# arm64 manifest link (and then its now-orphaned layer blobs). Result: `:dev` index still resolves,
# but pulling it 404s on the arm64 sub-manifest → fleet-wide ImagePullBackOff, and a cache-hit
# rebuild can't restore it (content is gone). Plain `garbage-collect` walks index→manifest refs in
# its mark phase and preserves them. See memory: dev-registry-gc-breaks-multiarch. (fixed 2026-07-12)
#
# ⚠️ EVERY CONTAINER, NOT containers[0]. This script used to build its keep set from
# `jsonpath='{.spec.template.spec.containers[0].image}'` — the FIRST container of each Deployment.
# options-edge-web also has an init container, `publish-public-assets`, pinned to its own digest;
# that digest was in no keep set, so the prune deleted its manifest, the GC swept its blobs, and the
# pod later died with ImagePullBackOff on an image the cluster was actively using (2026-11-12). The
# keep set now comes from dev-registry-keep-set.py, which reads initContainers AND containers across
# Deployments, StatefulSets, DaemonSets, Jobs and CronJobs. That file has its own tests.
#
# ⚠️ GC FAILURES ARE NOT SILENT. `garbage-collect` output used to go to /dev/null, so a GC that
# failed, or half-ran, looked exactly like a clean one.
set -uo pipefail
export PATH=/opt/homebrew/bin:/usr/local/bin:$PATH
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REG=${REG:-http://localhost:5001}
K="kubectl --context docker-desktop -n options-edge"

KEEPSET="$HERE/dev-registry-keep-set.py"
[ -r "$KEEPSET" ] || { echo "  REFUSING: $KEEPSET is missing — without it the keep set would be guesswork" >&2; exit 1; }

reg=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -i registry | head -1)
[ -z "$reg" ] && { echo "  (no local registry container — skip)"; exit 0; }

# Every container of every workload kind, not containers[0] of Deployments.
workloads="$($K get deploy,statefulset,daemonset,job,cronjob -o json 2>/dev/null)"
if [ -z "$workloads" ]; then
  echo "  REFUSING: could not read the cluster's workloads — pruning against an EMPTY keep set would delete every image in use" >&2
  exit 1
fi
DEPLOYED="$(printf '%s' "$workloads" | python3 "$KEEPSET")"
echo "  keep set: $(printf '%s\n' "$DEPLOYED" | grep -c 'sha256:') digest(s) referenced by running workloads"

before=$(docker exec "$reg" du -sh /var/lib/registry 2>/dev/null | cut -f1)

python3 - "$DEPLOYED" <<'PY'
import json,urllib.request,sys
REG="http://localhost:5001"
ACC="application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.index.v1+json"
deployed=set(x.strip() for x in sys.argv[1].split() if x.strip())
def req(u,m="GET"):
    r=urllib.request.Request(u,headers={"Accept":ACC},method=m); return urllib.request.urlopen(r,timeout=15)
try: repos=json.load(req(f"{REG}/v2/_catalog?n=1000"))["repositories"]
except Exception as e: print("  registry API unreachable:",str(e)[:50]); sys.exit(0)
dele=0
for repo in repos:
    try: tags=json.load(req(f"{REG}/v2/{repo}/tags/list")).get("tags") or []
    except Exception: continue
    td={}
    for t in tags:
        try: td[t]=req(f"{REG}/v2/{repo}/manifests/{t}","HEAD").headers.get("Docker-Content-Digest")
        except Exception: pass
    keep=set()
    if td.get("dev"): keep.add(td["dev"])
    for t,d in td.items():
        if d in deployed: keep.add(d)
    if not keep and td: keep.add(list(td.values())[-1])
    for d in set(x for x in td.values() if x)-keep:
        try: req(f"{REG}/v2/{repo}/manifests/{d}","DELETE"); dele+=1
        except Exception: pass
print(f"  deleted {dele} old manifests (kept :dev + every digest the cluster references)")
PY

# NOTE: intentionally NO --delete-untagged (would delete multi-arch sub-manifests — see header).
# Output is KEPT: a GC that fails must say so rather than look like a clean one.
gc_log="$(mktemp)"
if ! docker exec "$reg" registry garbage-collect /etc/docker/registry/config.yml >"$gc_log" 2>&1; then
  echo "  GARBAGE COLLECT FAILED — the blob store may be inconsistent:" >&2
  sed 's/^/    /' "$gc_log" >&2
  rm -f "$gc_log"
  exit 1
fi
rm -f "$gc_log"

after=$(docker exec "$reg" du -sh /var/lib/registry 2>/dev/null | cut -f1)
echo "  registry: ${before:-?} -> ${after:-?}"
