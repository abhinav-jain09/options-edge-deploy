#!/usr/bin/env bash
# Negative controls for validate-schema-sync-wired.py: each arm mutates one fact in a private copy and must fail for its own reason.
set -euo pipefail
cd "$(dirname "$0")/../.."
CHECK=scripts/ci/validate-schema-sync-wired.py
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
fresh() { rm -rf "$T/r"; mkdir -p "$T/r/scripts/deploy" "$T/r/scripts/ci" "$T/r/tools/schema-governance"
  cp scripts/deploy/service-deploy.sh "$T/r/scripts/deploy/"; cp scripts/ci/schema-sync-coverage-allowlist.txt "$T/r/scripts/ci/"
  cp tools/schema-governance/PIN tools/schema-governance/schema-governance-tool.jar "$T/r/tools/schema-governance/"
  for f in Jenkinsfile*; do cp "$f" "$T/r/"; done; }
fail=0
expect() { local name="$1" want="$2" out rc=0; out=$(python3 "$CHECK" "$T/r" 2>&1) || rc=$?
  if [ "$want" = pass ]; then [ "$rc" = 0 ] && echo "ok     $name" || { echo "NOT-OK $name: must pass"; echo "$out"; fail=1; }
  else [ "$rc" != 0 ] && printf '%s' "$out" | grep -q "$want" && echo "ok     $name" || { echo "NOT-OK $name: expected failure matching '$want' (rc=$rc)"; echo "$out"; fail=1; }; fi; }
fresh; expect "the repository as committed passes" pass
fresh; sed -i.bak 's#scripts/ci/schema-sync.sh#scripts/ci/removed.sh#g' "$T/r/scripts/deploy/service-deploy.sh"; expect "no schema-sync call in service-deploy.sh" "does not EXECUTE"
fresh; python3 - "$T/r/scripts/deploy/service-deploy.sh" <<'PY'
import sys,re; p=sys.argv[1]; t=open(p).read()
t=re.sub(r"bash scripts/ci/schema-sync\.sh \\\n\s*\|\| \{[^}]*\}", "echo bash scripts/ci/schema-sync.sh", t, count=1); open(p,"w").write(t)
PY
expect "the text only echoed, not executed" "does not EXECUTE"
fresh; python3 - "$T/r/scripts/deploy/service-deploy.sh" <<'PY'
import sys,re; p=sys.argv[1]; t=open(p).read()
t=re.sub(r"(bash scripts/ci/schema-sync\.sh \\\n\s*)\|\| \{[^}]*\}", r"\1|| true", t, count=1); open(p,"w").write(t)
PY
expect "the call made non-fatal with || true" "does not EXECUTE"
fresh; python3 - "$T/r/scripts/deploy/service-deploy.sh" <<'PY'
import sys; p=sys.argv[1]; t=open(p).read(); i=t.index("# --- Schema sync"); j=t.index("if [ \"$DEPLOY_DRY_RUN\" = \"true\" ]; then\n  echo \"=== DEPLOY_DRY_RUN=true")
blk=t[i:j]; t=t[:i]+t[j:]; k=t.rindex("kubectl apply"); open(p,"w").write(t[:k]+blk.replace("\n","\n")+t[k:])
PY
expect "schema-sync call moved after the first kubectl apply" "AFTER its first kubectl apply"
fresh; printf 'x' >> "$T/r/tools/schema-governance/schema-governance-tool.jar"; expect "a jar that differs from the PIN" "does not match tools/schema-governance/PIN"
fresh; printf 'kubectl apply -f x.yaml\n' > "$T/r/Jenkinsfile.newroute"; sed -i.bak 's#^#pipeline { sh "#; s#$#" }#' "$T/r/Jenkinsfile.newroute"; expect "a new deploy route with no gate and no allow-list entry" "Jenkinsfile.newroute"
fresh; printf '// kubectl apply -f x.yaml (only a comment)\npipeline { }\n' > "$T/r/Jenkinsfile.commentonly"; expect "a mention inside a comment proves nothing and needs no entry" pass
fresh; printf 'def note = "scripts/deploy/service-deploy.sh"\npipeline { stages { stage("x") { steps { sh "kubectl apply -f x.yaml" } } } }\n' > "$T/r/Jenkinsfile.decoy"; expect "service-deploy.sh named only in a string literal does not cover a direct kubectl apply" "Jenkinsfile.decoy"
fresh; printf 'pipeline { stages { stage("x") { steps { sh """ bash scripts/deploy/service-deploy.sh """ } } } }\n' > "$T/r/Jenkinsfile.viasd"; expect "an sh step that really calls service-deploy.sh is covered" pass
fresh; echo 'Jenkinsfile.ghost|ungated|a file that does not exist' >> "$T/r/scripts/ci/schema-sync-coverage-allowlist.txt"; expect "a stale allow-list entry" "stale"
fresh; echo 'Jenkinsfile.hpsf-ops|ungated|' > "$T/r/scripts/ci/schema-sync-coverage-allowlist.txt"; expect "an allow-list entry without a reason" "needs <file>|<kind>|<reason>"
fresh; printf 'pipeline { stages { stage("x") { steps { sh "kubectl apply -f x.yaml; echo scripts/deploy/service-deploy.sh" } } } }\n' > "$T/r/Jenkinsfile.echodecoy"; expect "service-deploy.sh only echoed in the same sh step as a direct kubectl apply" "Jenkinsfile.echodecoy"
fresh; printf '\nsh "kubectl scale deployment/x --replicas=3"\n' >> "$T/r/Jenkinsfile.web-service"; expect "a new direct workload command in a gated Jenkinsfile" "direct workload command"
fresh; sed -i.bak 's/via:direct=5/via:direct=4/' "$T/r/scripts/ci/schema-sync-coverage-allowlist.txt"; expect "a stale direct-command count" "via:direct=4"
fresh; echo 'Jenkinsfile.hpsf-ops|bogus|reason' >> "$T/r/scripts/ci/schema-sync-coverage-allowlist.txt"; expect "an unknown allow-list kind" "unknown kind"
fresh; printf 'pipeline { stages { stage("x") { steps { sh "kubectl --namespace=options-edge apply -f x.yaml" } } } }\n' > "$T/r/Jenkinsfile.flagform"; expect "kubectl --flag=value apply is a workload command" "Jenkinsfile.flagform"
fresh; printf 'pipeline { stages { stage("x") { steps { sh "kubectl -n options-edge \\\n  --context prod \\\n  rollout restart deploy/x" } } } }\n' > "$T/r/Jenkinsfile.continued"; expect "a kubectl command continued over lines is still seen" "Jenkinsfile.continued"
fresh; printf 'pipeline { stages { stage("x") { steps { sh "$KC scale deploy/x --replicas=2" } } } }\n' > "$T/r/Jenkinsfile.alias"; expect "a \$KC alias for kubectl is still seen" "Jenkinsfile.alias"
exit $fail
