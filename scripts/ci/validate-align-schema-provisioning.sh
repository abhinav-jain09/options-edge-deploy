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

files=$(grep -rl "name: $DEPLOYMENT" --include='*.yaml' k8s/base k8s/services 2>/dev/null || true)
[ -n "$files" ] || { echo "::error::no manifest declares $DEPLOYMENT — this gate would pass vacuously"; exit 1; }

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
        return {e['name']: e.get('value') for e in container.get('env') or []}

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
    # "no Deployment named X in this file" means grep matched the name somewhere else (a selector, a
    # comment); that is not a failure of this gate.
    if [ "$out" = "no Deployment named $DEPLOYMENT in this file" ]; then continue; fi
    echo "::error file=$f::$DEPLOYMENT schema provisioning: $out"
    rc=1
  fi
  checked=$((checked + 1))
done

[ "$checked" -gt 0 ] || { echo "::error::$DEPLOYMENT was never actually checked"; exit 1; }

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
