#!/usr/bin/env bash
# Mutation test: every arm of validate-align-schema-provisioning.sh must refuse its own violation, and
# must not refuse the real tree. One mutation at a time, each in a throwaway copy of the repo's two
# relevant files, so a passing arm cannot be masked by another arm also firing.
set -euo pipefail
cd "$(dirname "$0")/../.."
REPO=$PWD
GUARD=scripts/ci/validate-align-schema-provisioning.sh
BASE=k8s/base/es-spx-align-deployment.yaml
DEV=k8s/services/es-spx-align/overlays/dev/manifest.yaml
APPLY=scripts/deploy/apply.sh
SVC=scripts/deploy/service-deploy.sh

fails=0
pass() { echo "  ok   $1"; }
fail() { echo "  FAIL $1"; fails=$((fails + 1)); }

# The real tree must pass, or every "refused" below proves nothing.
if (cd "$REPO" && bash "$GUARD" >/dev/null 2>&1); then pass "real tree passes"; else fail "real tree passes"; fi

# Each case: a python mutation applied to a full copy of the repo, then the guard must exit non-zero
# AND say why. `expect` is matched against the guard's output so an arm cannot pass by failing elsewhere.
run_mutation() { # $1=label $2=expect $3=python mutation
  local label=$1 expect=$2 mutation=$3 work out status
  work=$(mktemp -d)
  # A copy, never the real tree: a mutation left behind would be committed.
  (cd "$REPO" && git archive HEAD | tar -x -C "$work") 2>/dev/null
  # Uncommitted work in progress has to be carried over too, or the mutation lands on the old file.
  for f in "$GUARD" "$BASE" "$DEV" "$APPLY" "$SVC" k8s/services/es-spx-align/overlays/production/manifest.yaml \
           k8s/services/es-spx-align/overlays/experiment/manifest.yaml; do
    mkdir -p "$work/$(dirname "$f")"; cp "$REPO/$f" "$work/$f"
  done
  ( cd "$work" && WORK=$work python3 -c "$mutation" ) || { fail "$label (mutation did not apply)"; rm -rf "$work"; return; }
  out=$( cd "$work" && bash "$GUARD" 2>&1 ) && status=0 || status=$?
  rm -rf "$work"
  if [ "$status" = 0 ]; then fail "$label (guard PASSED a violation)"; return; fi
  case "$out" in *"$expect"*) pass "$label" ;; *) fail "$label (refused, but not for the stated reason: $out)" ;; esac
}

run_mutation "missing init container is refused" "init container 'align-schema-provision' is missing" '
import yaml
p="k8s/base/es-spx-align-deployment.yaml"
d=yaml.safe_load(open(p)); del d["spec"]["template"]["spec"]["initContainers"]
yaml.safe_dump(d, open(p,"w"))'

run_mutation "a different image is refused" "image differs" '
import yaml
p="k8s/base/es-spx-align-deployment.yaml"
d=yaml.safe_load(open(p))
d["spec"]["template"]["spec"]["initContainers"][0]["image"]="somewhere/else:stale"
yaml.safe_dump(d, open(p,"w"))'

run_mutation "running the jar main instead of the provisioner is refused" "does not run com.optionsedge" '
import yaml
p="k8s/base/es-spx-align-deployment.yaml"
d=yaml.safe_load(open(p))
d["spec"]["template"]["spec"]["initContainers"][0]["command"]=["java","-jar","/app/app.jar"]
yaml.safe_dump(d, open(p,"w"))'

run_mutation "a drifted subject-name value is refused" "KAFKA_ES_GEX_SPX_ALIGNED_TOPIC differs" '
import yaml
p="k8s/base/es-spx-align-deployment.yaml"
d=yaml.safe_load(open(p))
for e in d["spec"]["template"]["spec"]["initContainers"][0]["env"]:
    if e["name"]=="KAFKA_ES_GEX_SPX_ALIGNED_TOPIC": e["value"]="options.some-other-topic"
yaml.safe_dump(d, open(p,"w"))'

run_mutation "a dropped subject-name key is refused" "KAFKA_ES_STRIKE_INTEL_SPX_ALIGNED_TOPIC is missing" '
import yaml
p="k8s/base/es-spx-align-deployment.yaml"
d=yaml.safe_load(open(p))
c=d["spec"]["template"]["spec"]["initContainers"][0]
c["env"]=[e for e in c["env"] if e["name"]!="KAFKA_ES_STRIKE_INTEL_SPX_ALIGNED_TOPIC"]
yaml.safe_dump(d, open(p,"w"))'

run_mutation "a differing envFrom is refused" "envFrom differs" '
import yaml
p="k8s/base/es-spx-align-deployment.yaml"
d=yaml.safe_load(open(p))
d["spec"]["template"]["spec"]["initContainers"][0]["envFrom"]=[{"configMapRef":{"name":"options-edge-config"}}]
yaml.safe_dump(d, open(p,"w"))'

run_mutation "a stale generated overlay is refused" "init container 'align-schema-provision' is missing" '
import yaml
p="k8s/services/es-spx-align/overlays/dev/manifest.yaml"
docs=[x for x in yaml.safe_load_all(open(p)) if x]
for x in docs:
    if x.get("kind")=="Deployment" and x["metadata"]["name"]=="es-spx-align-service":
        x["spec"]["template"]["spec"].pop("initContainers", None)
yaml.safe_dump_all(docs, open(p,"w"))'

run_mutation "set image that skips the init container is refused" "set image does not roll" '
import re
p="scripts/deploy/apply.sh"
s=open(p).read()
s=s.replace(" align-schema-provision=\"$ES_SPX_ALIGN_IMAGE\"", "")
open(p,"w").write(s)'

run_mutation "a removed set image line is refused" "the rollout would not roll this service" '
p="scripts/deploy/apply.sh"
s=open(p).read()
s="\n".join(l for l in s.split("\n") if "set image deployment/es-spx-align-service" not in l)
open(p,"w").write(s)'

run_mutation "digest-pinning that skips initContainers is refused" "does not reach initContainers" '
p="scripts/deploy/service-deploy.sh"
s=open(p).read()
old="| .spec.template.spec | (.containers[]?, .initContainers[]?, .ephemeralContainers[]?) | select(.image == strenv(RENDER_IMAGE0))"
new="| .spec.template.spec.containers[] | select(.image == strenv(RENDER_IMAGE0))"
assert old in s
open(p,"w").write(s.replace(old,new,1))'

run_mutation "an unpinned-image check that skips initContainers is refused" "does not read initContainers" '
p="scripts/deploy/service-deploy.sh"
s=open(p).read()
old="(.containers[]?, .initContainers[]?, .ephemeralContainers[]?) | .image"
assert s.count(old)==1, "the unpinned-image image list is not uniquely identifiable"
open(p,"w").write(s.replace(old, ".containers[].image", 1))'

run_mutation "a divergent valueFrom reference is refused" "KAFKA_ES_GEX_SPX_ALIGNED_TOPIC differs" '
import yaml
p="k8s/base/es-spx-align-deployment.yaml"
d=yaml.safe_load(open(p))
spec=d["spec"]["template"]["spec"]
# Both containers source the subject name from a ConfigMap, but from DIFFERENT keys. Comparing only
# e["value"] saw None == None and passed, while the two containers would read different topic names.
for container,key in ((spec["initContainers"][0],"topic-a"),(spec["containers"][0],"topic-b")):
    for e in container["env"]:
        if e["name"]=="KAFKA_ES_GEX_SPX_ALIGNED_TOPIC":
            e.pop("value",None)
            e["valueFrom"]={"configMapKeyRef":{"name":"options-edge-config","key":key}}
yaml.safe_dump(d, open(p,"w"))'

run_mutation "renaming the Deployment out of a named manifest is refused" "no Deployment named" '
import yaml
p="k8s/base/es-spx-align-deployment.yaml"
d=yaml.safe_load(open(p))
# The rename that defeated grep discovery: metadata.name changes, the app label stays behind.
d["metadata"]["name"]="es-spx-align-service-renamed"
yaml.safe_dump(d, open(p,"w"))'

echo
[ "$fails" = 0 ] && { echo "all align schema provisioning guard arms refuse their own violation"; exit 0; }
echo "$fails arm(s) did not hold"; exit 1
