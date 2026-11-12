#!/usr/bin/env bash
# dev-registry-prune.sh — keep ONLY the `:dev` tag + every image the cluster actually references,
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
# ⚠️ TAG-PINNED IMAGES ARE LIVE TOO. The keep set was digests only; a workload that names `repo:tag`
# with no digest has no digest in its spec, so the prune deleted the manifest that tag resolves to.
# The keep set now also lists (repo, tag) pins and the prune keeps whatever digest each resolves to.
#
# ⚠️ GC RUNS WITH THE REGISTRY STOPPED. `garbage-collect` against a registry that is accepting pushes
# sweeps the layers of a push that has uploaded blobs but not yet its manifest (the "freshly pushed
# image lost a layer" outage), and the running registry's `blobdescriptor: inmemory` cache keeps
# answering HEAD 200 for the blobs the GC swept, so the next build SKIPS re-uploading a layer that is
# gone (options-edge-web, 2026-11-12). So: stop the registry, collect from a throw-away container on
# the same volume, start it again (the restart is also what empties that cache). A failed push in the
# few seconds in between is loud; a corrupted store is not.
#
# ⚠️ THE PRUNE'S OWN SUCCESS LINE PROVES NOTHING. Before and after, it PULL-checks every image the
# cluster pins (dev-registry-verify-pinned.py: the manifest by the exact reference, then the first and
# last byte of every layer) and exits 1 when one cannot be pulled, which the clean reports, instead of
# letting the next pod restart find out hours later.
#
# ⚠️ GC FAILURES ARE NOT SILENT. `garbage-collect` output used to go to /dev/null, so a GC that
# failed, or half-ran, looked exactly like a clean one.
set -uo pipefail
export PATH="$PATH:/opt/homebrew/bin:/usr/local/bin"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export REG=${REG:-http://localhost:5001}
KCTX="${KCTX-docker-desktop}"
K="kubectl ${KCTX:+--context $KCTX} -n options-edge"

KEEPSET="$HERE/dev-registry-keep-set.py"
VERIFY="$HERE/dev-registry-verify-pinned.sh"
for f in "$KEEPSET" "$VERIFY" "$HERE/dev-registry-verify-pinned.py"; do
  [ -r "$f" ] || { echo "  REFUSING: $f is missing — without it the keep set / the pull check would be guesswork" >&2; exit 1; }
done

reg=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -i registry | head -1)
[ -z "$reg" ] && { echo "  (no local registry container — skip)"; exit 0; }

# Every container of every workload kind, not containers[0] of Deployments.
workloads="$($K get deploy,statefulset,daemonset,job,cronjob -o json 2>/dev/null)"
if ! printf '%s' "$workloads" | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin).get("items") else 1)' 2>/dev/null; then
  echo "  REFUSING: the cluster reported no workloads — pruning against an EMPTY keep set would delete every image in use" >&2
  exit 1
fi
KEEP="$(printf '%s' "$workloads" | python3 "$KEEPSET")" || { echo "  REFUSING: the keep set could not be computed" >&2; exit 1; }
n_dig="$(printf '%s\n' "$KEEP" | grep -c -v -e '^tag ' -e '^$')"
n_tag="$(printf '%s\n' "$KEEP" | grep -c '^tag ')"
echo "  keep set: ${n_dig} digest(s) + ${n_tag} tag pin(s) referenced by workloads"

echo "  pull check BEFORE the prune:"
pre_out="$("$VERIFY" 2>&1)"; pre_rc=$?
printf '%s\n' "$pre_out" | sed 's/^/    /'

before=$(docker exec "$reg" du -sh /var/lib/registry 2>/dev/null | cut -f1)

python3 - "$KEEP" <<'PY'
import json,os,urllib.request,sys
REG=os.environ["REG"]
ACC="application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.index.v1+json"
deployed=set(); pinned_tags=set()
for line in sys.argv[1].split("\n"):
    p=line.split()
    if len(p)==1 and ":" in p[0]: deployed.add(p[0])
    elif len(p)==3 and p[0]=="tag": pinned_tags.add((p[1],p[2]))
def req(u,m="GET"):
    r=urllib.request.Request(u,headers={"Accept":ACC},method=m); return urllib.request.urlopen(r,timeout=15)
try: repos=json.load(req(f"{REG}/v2/_catalog?n=1000"))["repositories"]
except Exception as e: print("  registry API unreachable:",str(e)[:50]); sys.exit(1)
dele=0; failed=[]
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
        if d in deployed or (repo,t) in pinned_tags: keep.add(d)
    if not keep and td: keep.add(list(td.values())[-1])
    for d in set(x for x in td.values() if x)-keep:
        try: req(f"{REG}/v2/{repo}/manifests/{d}","DELETE"); dele+=1
        except Exception as e: failed.append(f"{repo}@{d[:19]}: {str(e)[:40]}")
print(f"  deleted {dele} old manifests (kept :dev + every digest and tag the cluster references)")
for f in failed: print("  could not delete "+f)
PY
[ $? -eq 0 ] || exit 1

# NOTE: intentionally NO --delete-untagged (would delete multi-arch sub-manifests — see header).
# The registry is STOPPED for the collection and ALWAYS started again, whatever happens in between.
regimg="$(docker inspect -f '{{.Config.Image}}' "$reg" 2>/dev/null)"
[ -n "$regimg" ] || { echo "  REFUSING: cannot read the registry container's image" >&2; exit 1; }
stopped=0
registry_up() {
  if [ "$stopped" = 1 ]; then
    docker start "$reg" >/dev/null 2>&1 && stopped=0
  fi
  local i
  for i in $(seq 1 60); do
    curl -sf -m 2 "$REG/v2/" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "  REGISTRY IS NOT ANSWERING after the collection — docker start $reg" >&2
  return 1
}
# EXIT only, deliberately: a trap on INT/TERM would run registry_up and then CARRY ON with the script,
# whereas a signal's default action ENDS it and bash runs the EXIT trap on the way out.
trap 'registry_up' EXIT
echo "  stopping $reg for the garbage collection (a push in this window fails loudly) ..."
# Armed BEFORE the stop: a signal while `docker stop` is in flight can still leave the registry
# stopped, and `docker start` on a running container is a no-op.
stopped=1
if ! docker stop "$reg" >/dev/null 2>&1; then
  echo "  COULD NOT STOP $reg — not collecting against a live registry" >&2
  exit 1
fi
gc_log="$(mktemp)"
gc_rc=0
docker run --rm --volumes-from "$reg" "$regimg" garbage-collect /etc/docker/registry/config.yml >"$gc_log" 2>&1 || gc_rc=$?
registry_up || exit 1
if [ "$gc_rc" -ne 0 ]; then
  echo "  GARBAGE COLLECT FAILED — the blob store may be inconsistent:" >&2
  sed 's/^/    /' "$gc_log" >&2
  rm -f "$gc_log"
  exit 1
fi
rm -f "$gc_log"

after=$(docker exec "$reg" du -sh /var/lib/registry 2>/dev/null | cut -f1)
echo "  registry: ${before:-?} -> ${after:-?}"

echo "  pull check AFTER the prune:"
post_out="$("$VERIFY" 2>&1)"; post_rc=$?
printf '%s\n' "$post_out" | sed 's/^/    /'
if [ "$post_rc" -ne 0 ]; then
  if [ "$pre_rc" -ne 0 ]; then
    echo "  an image cannot be pulled (it could not be BEFORE the prune either) — rebuild or re-push it" >&2
  else
    echo "  THE PRUNE LEFT AN IMAGE THE CLUSTER PINS UNPULLABLE — see BROKEN above" >&2
  fi
  exit 1
fi
