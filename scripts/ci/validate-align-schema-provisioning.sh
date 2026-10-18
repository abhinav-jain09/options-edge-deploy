#!/usr/bin/env bash
# The es-spx-align schema-provisioning init container must agree with the service it provisions for.
#
# WHY: es-spx-align-service reads its canonical Avro subjects with auto.register.schemas=false and refuses
# to start unless each one is already registered with exactly the released schema. The init container
# `align-schema-provision` performs that registration. The subject NAMES are derived from the environment
# and the schema TEXT from the image, so the two containers must agree on both or the init container
# registers something the service does not look up — and the service crash-loops with its subjects
# apparently "registered". Neither manifest half shows the mismatch on its own, which is why this is a
# build gate and not a comment.
#
# Checks, on every overlay that declares the deployment (and the base it is generated from):
#   1. the init container exists, and runs the provisioner main (never the service's own entrypoint, which
#      would start a second Streams app inside an init container and never return)
#   2. its image is byte-identical to the service container's
#   3. its envFrom list is identical to the service container's (that is where the registry URL and
#      bootstrap servers come from)
#   4. every value the provisioner reads is present with an identical value in both containers
# It asserts nothing about the other service env (heap, probes, skew): those do not change what is
# registered, and demanding equality there would make the gate fail on correct changes.
set -euo pipefail
cd "$(dirname "$0")/../.."

DEPLOYMENT=es-spx-align-service
INIT=align-schema-provision
MAIN=es-spx-align
PROVISIONER=com.optionsedge.processing.esspxalign.AlignSchemaProvisioner
# Exactly the keys AlignSchemaProvisioner reads: the master switch it mirrors, and the three knobs that
# decide the subject names and whether the two optional strike-intel subjects exist.
SHARED_ENV="ES_SPX_ALIGN_ENABLED KAFKA_ES_GEX_SPX_ALIGNED_TOPIC ES_SPX_ALIGN_STRIKE_INTEL_ENABLED KAFKA_ES_STRIKE_INTEL_SPX_ALIGNED_TOPIC"

# The manifests that MUST each hold exactly one es-spx-align-service Deployment, named explicitly
# rather than discovered. Discovery by grep was not fail-closed: renaming the Deployment's
# metadata.name while leaving the app.kubernetes.io/name label behind still matched the file, found no
# Deployment in it, and was skipped as "not this file" — so the gate could pass on the manifests that
# happened to be left. A fixed list cannot be emptied by a rename; a moved or renamed path fails here.
FILES="k8s/base/es-spx-align-deployment.yaml
k8s/services/es-spx-align/overlays/dev/manifest.yaml
k8s/services/es-spx-align/overlays/production/manifest.yaml
k8s/services/es-spx-align/overlays/experiment/manifest.yaml"
files=$FILES
for f in $files; do
  [ -f "$f" ] || { echo "::error::$f is missing — this gate names the manifests it checks, so a moved path fails closed"; exit 1; }
done

rc=0
checked=0
for f in $files; do
  # A manifest file may hold several documents; only the Deployment named above is in scope.
  out=$(DEPLOYMENT="$DEPLOYMENT" INIT="$INIT" MAIN="$MAIN" PROVISIONER="$PROVISIONER" \
        SHARED_ENV="$SHARED_ENV" FILE="$f" python3 - <<'PY'
import os, sys, yaml

name, init_name, main_name = os.environ['DEPLOYMENT'], os.environ['INIT'], os.environ['MAIN']
provisioner, path = os.environ['PROVISIONER'], os.environ['FILE']
shared = os.environ['SHARED_ENV'].split()
problems, found = [], False

with open(path) as handle:
    documents = [d for d in yaml.safe_load_all(handle) if isinstance(d, dict)]

for document in documents:
    if document.get('kind') != 'Deployment' or document.get('metadata', {}).get('name') != name:
        continue
    found = True
    spec = document['spec']['template']['spec']
    containers = {c['name']: c for c in spec.get('containers') or []}
    inits = {c['name']: c for c in spec.get('initContainers') or []}

    if main_name not in containers:
        problems.append(f"service container '{main_name}' is missing")
        break
    if init_name not in inits:
        problems.append(f"init container '{init_name}' is missing — nothing would register the canonical "
                        f"subjects and the service refuses to start without them")
        break

    service, provision = containers[main_name], inits[init_name]

    command = provision.get('command') or []
    if provisioner not in command:
        problems.append(f"init container command does not run {provisioner}: {command!r}")
    if 'java' not in (command[:1] or ['']) or '-cp' not in command:
        # `java -jar app.jar` would run the service's own main inside an init container: a second Streams
        # app that never exits, so the pod would never reach the service container.
        problems.append(f"init container must run the provisioner class on the classpath, not the jar's "
                        f"own main: {command!r}")

    if provision.get('image') != service.get('image'):
        problems.append(f"image differs: init {provision.get('image')!r} vs service {service.get('image')!r} "
                        f"— the schemas registered would not be the ones this release looks up")

    if (provision.get('envFrom') or []) != (service.get('envFrom') or []):
        problems.append("envFrom differs — the registry URL and bootstrap servers come from there")

    def values(container):
        # The WHOLE entry, not just e['value']: two entries sourced from different ConfigMap keys both
        # have value=None, so comparing values alone passed a genuine divergence of the subject names.
        # Comparing entries makes differing valueFrom references differ and identical ones match.
        return {e['name']: {k: v for k, v in e.items() if k != 'name'} for e in container.get('env') or []}

    init_env, service_env = values(provision), values(service)
    for key in shared:
        if key not in service_env:
            problems.append(f"{key} is not set on the service container, so there is nothing to agree with")
        elif key not in init_env:
            problems.append(f"{key} is missing from the init container — it would provision for a "
                            f"different subject set than the service reads")
        elif init_env[key] != service_env[key]:
            problems.append(f"{key} differs: init {init_env[key]!r} vs service {service_env[key]!r}")
    break

if not found:
    problems.append(f"no Deployment named {name} in this file")
for problem in problems:
    print(problem)
sys.exit(1 if problems else 0)
PY
      ) && status=0 || status=$?
  if [ "$status" != 0 ]; then
    # Every named file must hold the Deployment. There is no "wrong file" case left to excuse, because
    # the file list is fixed rather than grepped: a missing Deployment here IS the failure.
    echo "::error file=$f::$DEPLOYMENT schema provisioning: $out"
    rc=1
  fi
  checked=$((checked + 1))
done

# 6. There are TWO deploy paths, and the init container must get the same image on both. apply.sh
#    rolls images with `kubectl set image` (checked above); service-deploy.sh renders the overlay and
#    DIGEST-PINS it. That rewrite walked .containers[] only, so it applied a digest-pinned service
#    beside an init container still carrying the mutable :dev/:prod tag -- and its own "not
#    digest-pinned" verification walked .containers[] only too, so nothing failed. If that tag later
#    pointed at a different build, the init container would register one release's schemas for another
#    release's service. Both the rewrite and its verification must therefore reach initContainers.
SVC=scripts/deploy/service-deploy.sh
if [ ! -f "$SVC" ]; then
  echo "::error::$SVC is missing — the per-service deploy path cannot be checked"
  rc=1
else
  # The pinning rewrite and the unpinned-image verification, each identified by what it does.
  pin_line=$(grep -n 'select(.image == strenv(RENDER_IMAGE0))' "$SVC" | head -1 | cut -d: -f1 || true)
  ver_line=$(grep -n 'not digest-pinned' "$SVC" | head -1 | cut -d: -f1 || true)
  if [ -z "$pin_line" ]; then
    echo "::error file=$SVC::no digest-pinning rewrite found (expected a yq selecting .image == strenv(RENDER_IMAGE0))"
    rc=1
  elif ! sed -n "${pin_line}p" "$SVC" | grep -q 'initContainers\[\]'; then
    echo "::error file=$SVC,line=$pin_line::the digest-pinning rewrite does not reach initContainers[], so align-schema-provision would keep a mutable tag while the service is digest-pinned"
    rc=1
  fi
  if [ -z "$ver_line" ]; then
    echo "::error file=$SVC::no unpinned-image verification found"
    rc=1
  else
    # The `unpinned=` assignment itself, not a line window around the FATAL it guards: a window reached
    # back into the pinning expression above, which also mentions initContainers, so a verification
    # narrowed to .containers[].image still satisfied it. This guard's own mutation test caught that.
    ver_assign=$(grep -n '^unpinned=' "$SVC" | head -1 | cut -d: -f1 || true)
    if [ -z "$ver_assign" ]; then
      echo "::error file=$SVC::the unpinned-image verification is not an 'unpinned=' assignment this gate can read"
      rc=1
    elif ! sed -n "${ver_assign}p" "$SVC" | grep -q 'initContainers\[\]'; then
      echo "::error file=$SVC,line=$ver_assign::the unpinned-image verification does not read initContainers[], so an unpinned init container would pass it"
      rc=1
    fi
  fi
fi

expected=$(printf '%s\n' $FILES | grep -c .)
[ "$checked" = "$expected" ] || { echo "::error::checked $checked of $expected named manifests — refusing to report a partial pass"; exit 1; }

# 5. The manifests agreeing is not enough: the deploy rolls images with `kubectl set image`, which only
#    touches the container names it is given. If that line names the service container alone, the init
#    container stays on the previously applied image and can register schemas this release does not look
#    up -- a drift no manifest shows. So the line must name BOTH, with the same image variable.
APPLY=scripts/deploy/apply.sh
line=$(grep -n "set image deployment/$DEPLOYMENT" "$APPLY" | head -1 | cut -d: -f1 || true)
if [ -z "$line" ]; then
  echo "::error file=$APPLY::no 'set image deployment/$DEPLOYMENT' line — the rollout would not roll this service"
  rc=1
else
  text=$(sed -n "${line}p" "$APPLY")
  for container in "$MAIN" "$INIT"; do
    case "$text" in
      *"$container=\"\$ES_SPX_ALIGN_IMAGE\""*) : ;;
      *) echo "::error file=$APPLY,line=$line::set image does not roll '$container' to \$ES_SPX_ALIGN_IMAGE — the init container and the service it provisions for could run different images"
         rc=1 ;;
    esac
  done
fi

[ "$rc" = 0 ] && echo "align schema provisioning: init and service containers agree in $checked manifest(s)"
exit $rc
