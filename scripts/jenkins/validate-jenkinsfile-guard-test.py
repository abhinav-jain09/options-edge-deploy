#!/usr/bin/env python3
"""Mutation suite for validate-jenkinsfile-guard.py — shared byte-for-byte across the four repositories.

A GOOD fixture (a pipeline with a primary guard, a container re-guard, a separate-agent stage with an
inline re-guard, a nested clone bound by its own permission, a compatibility-checked downstream
trigger, and a gated post{}) must pass; then ONE mutation per enforced rule must be refused — every
one of them a regression Codex demonstrated the earlier validators accepted, or an obvious sibling.
A validator that quietly stops checking something fails here instead of going green.

Usage: validate-jenkinsfile-guard-test.py [--validator <path>]   (default: next to this file)
"""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUB_GUARD = "#!/usr/bin/env bash\n# stub guard for the validator's mutation suite\nexit 0\n"
STUB_HASH = hashlib.sha256(STUB_GUARD.encode()).hexdigest()

GOOD = """pipeline {
  agent none
  parameters {
    string(name: 'PERMITTED_SHA', defaultValue: '', trim: true, description: 'the permitted commit')
    string(name: 'PERMITTED_SHA_GUARD_VERSION', defaultValue: '__HASH__', description: 'guard version')
    string(name: 'APP_PERMITTED_SHA', defaultValue: '', trim: true, description: 'app commit')
    string(name: 'CHILD_PERMITTED_SHA', defaultValue: '', trim: true, description: 'child commit')
  }
  options { disableRestartFromStage(); disableConcurrentBuilds() }
  stages {
    stage('Permitted commit guard') {
      agent { label 'deploy-host' }
      steps {
        script {
          def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')
          if (rc != 0) {
            error("Permitted commit guard REFUSED this build (rc=${rc})")
          }
          env.PERMITTED_SHA_GUARD = 'PASSED'
        }
      }
    }
    stage('Deploy path') {
      agent { label 'deploy-host' }
      stages {
        stage('Permitted commit guard (deploy workspace)') {
          steps {
            script {
              def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')
              if (rc != 0) {
                error("Permitted commit guard (deploy workspace) REFUSED this build (rc=${rc})")
              }
              env.DEPLOY_WORKSPACE_PERMITTED = 'PASSED'
            }
          }
        }
        stage('Build app') {
          steps {
            dir('app-src') {
              git url: 'git@example:app.git', branch: 'main'
            }
            script {
              def rc = sh(returnStatus: true, script: 'PERMITTED_SHA="${APP_PERMITTED_SHA:-}" bash scripts/jenkins/permitted-sha-guard.sh --dir app-src --ref main')
              if (rc != 0) {
                error("app checkout refused (rc=${rc})")
              }
            }
            sh 'docker build -t app app-src'
          }
        }
        stage('Trigger child') {
          steps {
            script {
              def compat = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh child-job')
              if (compat != 0) {
                error("child-job does not enforce the guard (rc=${compat})")
              }
              build job: 'child-job', parameters: [string(name: 'PERMITTED_SHA', value: params.CHILD_PERMITTED_SHA)]
            }
          }
        }
        stage('Deploy') {
          steps {
            sh 'kubectl apply -f k8s/fixture.yaml'
          }
        }
      }
      post {
        always {
          script {
            if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {
              sh 'kubectl delete job old'
            } else {
              echo 'not permitted — nothing touched'
            }
          }
        }
      }
    }
    stage('Smoke') {
      agent { label 'mac' }
      steps {
        checkout scm
        script {
          def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')
          if (rc != 0) {
            error("smoke workspace refused (rc=${rc})")
          }
        }
        sh 'mvn -B -Psmoke verify'
      }
    }
  }
}
"""
MANIFEST = "Jenkinsfile.fixture | in | reguard=Deploy path | fixture\n"


def run(validator: str, root: str, manifest: str) -> subprocess.CompletedProcess:
    return subprocess.run(["python3", validator, "--root", root, "--manifest", manifest], capture_output=True, text=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--validator", default=os.path.join(HERE, "validate-jenkinsfile-guard.py"))
    a = ap.parse_args()
    good = GOOD.replace("__HASH__", STUB_HASH)
    passed = failed = 0

    def case(name: str, text: str, manifest: str, expect_ok: bool, must_say: str = "") -> None:
        nonlocal passed, failed
        tmp = tempfile.mkdtemp()
        try:
            os.makedirs(os.path.join(tmp, "scripts/jenkins"))
            with open(os.path.join(tmp, "scripts/jenkins/permitted-sha-guard.sh"), "w") as fh:
                fh.write(STUB_GUARD)
            with open(os.path.join(tmp, "Jenkinsfile.fixture"), "w") as fh:
                fh.write(text)
            mpath = os.path.join(tmp, "scope.txt")
            with open(mpath, "w") as fh:
                fh.write(manifest)
            r = run(a.validator, tmp, mpath)
            ok = (r.returncode == 0) == expect_ok and (must_say in (r.stdout + r.stderr))
            if ok:
                passed += 1
                print(f"ok   [{name}]")
            else:
                failed += 1
                print(f"FAIL [{name}]: rc={r.returncode} expected_ok={expect_ok} must_say={must_say!r}\n{r.stdout}{r.stderr}")
        finally:
            shutil.rmtree(tmp, True)

    def mut(name: str, old: str, new: str, must_say: str, count: int = 1) -> None:
        if old not in good:
            failed_msg = f"FAIL [{name}]: mutation anchor not found: {old[:60]!r}"
            print(failed_msg)
            nonlocal_fail()
            return
        case(name, good.replace(old, new, count), MANIFEST, False, must_say)

    def nonlocal_fail():
        nonlocal failed
        failed += 1

    case("good fixture passes", good, MANIFEST, True, "carry the canonical permitted-commit guard")
    # 1. parameters
    mut("PERMITTED_SHA parameter missing", "    string(name: 'PERMITTED_SHA', defaultValue: '', trim: true, description: 'the permitted commit')\n", "", "lacks string(name: 'PERMITTED_SHA'")
    mut("PERMITTED_SHA with a default", "name: 'PERMITTED_SHA', defaultValue: '', trim: true", "name: 'PERMITTED_SHA', defaultValue: 'main', trim: true", "lacks string(name: 'PERMITTED_SHA'")
    mut("guard version parameter missing", "    string(name: 'PERMITTED_SHA_GUARD_VERSION', defaultValue: '" + STUB_HASH + "', description: 'guard version')\n", "", "lacks string(name: 'PERMITTED_SHA_GUARD_VERSION'")
    mut("guard version default is another hash", STUB_HASH, "0" * 64, "would declare a guard it does not run")
    # 2.
    mut("restart from stage enabled", "disableRestartFromStage(); ", "", "disableRestartFromStage()")
    # 3. primary guard
    mut("primary guard missing", "stage('Permitted commit guard') {", "stage('Preflight') {", "no stage named 'Permitted commit guard'")
    mut("primary guard inverted (rc == 0)", "          if (rc != 0) {\n            error(\"Permitted commit guard REFUSED", "          if (rc == 0) {\n            error(\"Permitted commit guard REFUSED", "not the canonical guard")
    mut("primary guard || true in the shell string", "script: 'bash scripts/jenkins/permitted-sha-guard.sh')\n          if (rc != 0) {\n            error(\"Permitted commit guard REFUSED", "script: 'bash scripts/jenkins/permitted-sha-guard.sh || true')\n          if (rc != 0) {\n            error(\"Permitted commit guard REFUSED", "not the canonical guard")
    mut("primary guard wrapped in catchError", "          if (rc != 0) {\n            error(\"Permitted commit guard REFUSED", "          catchError(buildResult: 'FAILURE') {\n            error(\"Permitted commit guard REFUSED", "not the canonical guard")
    mut("primary guard without error()", "error(\"Permitted commit guard REFUSED this build (rc=${rc})\")", "echo \"refused ${rc}\"", "not the canonical guard")
    mut("primary guard with a smuggled statement", "          def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')\n          if (rc != 0) {\n            error(\"Permitted commit guard REFUSED", "          sh 'kubectl apply -f early.yaml'\n          def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')\n          if (rc != 0) {\n            error(\"Permitted commit guard REFUSED", "not the canonical guard")
    mut("primary guard sets the re-guard flag", "          env.PERMITTED_SHA_GUARD = 'PASSED'\n        }\n      }\n    }\n    stage('Deploy path')", "          env.DEPLOY_WORKSPACE_PERMITTED = 'PASSED'\n        }\n      }\n    }\n    stage('Deploy path')", "must set env.PERMITTED_SHA_GUARD")
    # 4. order / effects before the guard
    mut("unlisted stage before the guard", "  stages {\n    stage('Permitted commit guard')", "  stages {\n    stage('Warm up') { steps { echo 'hi' } }\n    stage('Permitted commit guard')", "precedes the guard but is not in the manifest's before= set")
    case("listed stage before the guard may not push", good.replace("  stages {\n    stage('Permitted commit guard')", "  stages {\n    stage('Refuse anything but main') { steps { sh 'docker push x' } }\n    stage('Permitted commit guard')"), "Jenkinsfile.fixture | in | reguard=Deploy path; before=Refuse anything but main | fixture\n", False, "mutation token before the guard")
    case("listed effect-free stage before the guard is fine", good.replace("  stages {\n    stage('Permitted commit guard')", "  stages {\n    stage('Refuse anything but main') { steps { sh 'git fetch origin main' } }\n    stage('Permitted commit guard')"), "Jenkinsfile.fixture | in | reguard=Deploy path; before=Refuse anything but main | fixture\n", True)
    # 5. container re-guard
    mut("container without the re-guard stage", "        stage('Permitted commit guard (deploy workspace)') {", "        stage('Deploy workspace check') {", "must open with 'Permitted commit guard (deploy workspace)'")
    mut("re-guard sets the wrong flag", "              env.DEPLOY_WORKSPACE_PERMITTED = 'PASSED'", "              env.PERMITTED_SHA_GUARD = 'PASSED'", "must set env.DEPLOY_WORKSPACE_PERMITTED")
    mut("re-guard flag set elsewhere", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            script { env.DEPLOY_WORKSPACE_PERMITTED = 'PASSED' }\n            sh 'kubectl apply -f k8s/fixture.yaml'", "may be set only by the 'Permitted commit guard (deploy workspace)' stage")
    # 6. separate-agent stage
    mut("separate-agent stage without inline re-guard", "        checkout scm\n        script {\n          def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')\n          if (rc != 0) {\n            error(\"smoke workspace refused (rc=${rc})\")\n          }\n        }\n", "        checkout scm\n", "does not open with the canonical inline re-guard")
    mut("separate-agent stage inline re-guard inverted", "          if (rc != 0) {\n            error(\"smoke workspace refused", "          if (rc == 0) {\n            error(\"smoke workspace refused", "does not open with the canonical inline re-guard")
    mut("separate-agent stage effect before its re-guard", "        checkout scm\n        script {\n          def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')\n          if (rc != 0) {\n            error(\"smoke workspace refused", "        checkout scm\n        sh 'docker rm -f options-edge-web'\n        script {\n          def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')\n          if (rc != 0) {\n            error(\"smoke workspace refused", "does not open with the canonical inline re-guard")
    mut("separate-agent stage second checkout", "        sh 'mvn -B -Psmoke verify'", "        checkout scm\n        sh 'mvn -B -Psmoke verify'", "the guarded workspace is re-acquired after the guard")
    # 7. downstream
    mut("downstream without compatibility check", "              def compat = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh child-job')\n              if (compat != 0) {\n                error(\"child-job does not enforce the guard (rc=${compat})\")\n              }\n", "", "not preceded (same stage or the one before) by the canonical compatibility check")
    mut("compatibility check inverted", "              if (compat != 0) {", "              if (compat == 0) {", "not preceded (same stage or the one before) by the canonical compatibility check")
    mut("compatibility check status discarded", "              def compat = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh child-job')\n              if (compat != 0) {\n                error(\"child-job does not enforce the guard (rc=${compat})\")\n              }\n", "              sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh child-job')\n", "not preceded (same stage or the one before) by the canonical compatibility check")
    mut("compatibility check for another job", "require-guarded-downstream.sh child-job'", "require-guarded-downstream.sh other-job'", "not preceded (same stage or the one before) by the canonical compatibility check for child-job")
    mut("downstream without PERMITTED_SHA forward", "parameters: [string(name: 'PERMITTED_SHA', value: params.CHILD_PERMITTED_SHA)]", "parameters: [string(name: 'X', value: 'y')]", "does not forward PERMITTED_SHA")
    mut("UNBOUND annotation", "              build job: 'child-job'", "              // UNBOUND-DOWNSTREAM: child-job has no guard yet\n              build job: 'child-job'", "UNBOUND-DOWNSTREAM annotation is not a gate")
    mut("an extra unguarded downstream", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            build job: 'other-job', parameters: [string(name: 'PERMITTED_SHA', value: 'x')]\n            sh 'kubectl apply -f k8s/fixture.yaml'", "not preceded (same stage or the one before) by the canonical compatibility check for other-job")
    # 8. post gating
    mut("post mutation after the gate block", "            } else {\n              echo 'not permitted — nothing touched'\n            }", "            } else {\n              echo 'not permitted — nothing touched'\n            }\n            sh 'kubectl delete job stale'", "is not inside the true branch")
    mut("post mutation in the else branch", "              echo 'not permitted — nothing touched'", "              sh 'kubectl delete job old'", "is not inside the true branch")
    mut("post gate negated", "if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {", "if (!(env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED')) {", "is not inside the true branch")
    mut("post gate on the first guard's flag only", "if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {", "if (env.PERMITTED_SHA_GUARD == 'PASSED') {", "is not inside the true branch")
    mut("post gate with an || escape", "if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {", "if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED' || params.FORCE) {", "is not inside the true branch")
    mut("post gate is only an echoed name", "            if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {\n              sh 'kubectl delete job old'\n            } else {\n              echo 'not permitted — nothing touched'\n            }", "            echo 'DEPLOY_WORKSPACE_PERMITTED'\n            sh 'kubectl delete job old'", "is not inside the true branch")
    mut("post mutation with a != gate", "if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {\n              sh 'kubectl delete job old'\n            } else {\n              echo 'not permitted — nothing touched'\n            }", "if (env.DEPLOY_WORKSPACE_PERMITTED != 'PASSED') {\n              echo 'no'\n            } else {\n              sh 'kubectl delete job old'\n            }", "is not inside the true branch")
    mut("unconditional post mutation added at pipeline level", "    stage('Smoke') {", "    stage('Smoke') {\n      post { always { node('mac') { sh 'docker rm -f options-edge-web' } } }", "is not inside the true branch")
    # 9. acquisitions after the guard
    mut("git pull of the root after the guard", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            sh 'git pull origin main'\n            sh 'kubectl apply -f k8s/fixture.yaml'", "the guarded workspace is re-acquired after the guard")
    mut("git checkout main after the guard", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            sh 'git checkout main'\n            sh 'kubectl apply -f k8s/fixture.yaml'", "the guarded workspace is re-acquired after the guard")
    mut("git pull rebound by an echo", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            sh 'git pull origin main'\n            echo 'permitted-sha-guard.sh --dir .'\n            sh 'kubectl apply -f k8s/fixture.yaml'", "the guarded workspace is re-acquired after the guard")
    mut("nested clone without its guard", "            script {\n              def rc = sh(returnStatus: true, script: 'PERMITTED_SHA=\"${APP_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir app-src --ref main')\n              if (rc != 0) {\n                error(\"app checkout refused (rc=${rc})\")\n              }\n            }\n", "", "'app-src' acquired after the guard is not re-bound")
    mut("nested guard for another directory", "--dir app-src --ref main", "--dir other-src --ref main", "'app-src' acquired after the guard is not re-bound")
    mut("nested guard with the root permission", "PERMITTED_SHA=\"${APP_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir app-src", "PERMITTED_SHA=\"${PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir app-src", "'app-src' acquired after the guard is not re-bound")
    mut("nested guard with an alias ref", "--dir app-src --ref main", "--dir app-src --ref origin/main", "'app-src' acquired after the guard is not re-bound")
    mut("nested guard inverted", "              if (rc != 0) {\n                error(\"app checkout refused", "              if (rc == 0) {\n                error(\"app checkout refused", "'app-src' acquired after the guard is not re-bound")
    mut("nested clone re-acquired after its guard", "            sh 'docker build -t app app-src'", "            sh 'git -C app-src checkout origin/feature'\n            sh 'docker build -t app app-src'", "'app-src' acquired after the guard is not re-bound")
    # 10./11. contracts + shell form
    shell_ok = good.replace("            sh 'docker build -t app app-src'", "            sh '''\n              set -euo pipefail\n              rm -rf .deps/options-edge-contracts\n              git clone git@example:contracts.git .deps/options-edge-contracts\n              git -C .deps/options-edge-contracts checkout main\n              PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir .deps/options-edge-contracts --ref main || exit 1\n              mvn -B -f .deps/options-edge-contracts/pom.xml install\n            '''\n            sh 'docker build -t app app-src'")
    contracts_manifest = "Jenkinsfile.fixture | in | reguard=Deploy path; contracts=.deps/options-edge-contracts | fixture\n"
    case("shell contracts guard is fine", shell_ok, contracts_manifest, True)
    case("contracts named but never acquired", good, contracts_manifest, False, "no acquisition of it was found")
    case("shell contracts guard || true", shell_ok.replace("--ref main || exit 1", "--ref main || true"), contracts_manifest, False, "must end with `|| exit 1`")
    case("shell contracts guard with root permission", shell_ok.replace("PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash", "PERMITTED_SHA=\"${PERMITTED_SHA:-}\" bash"), contracts_manifest, False, "is not re-bound")
    case("shell contracts guard commented out", shell_ok.replace("              PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash", "              # PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash"), contracts_manifest, False, "is not re-bound")
    case("contracts re-checked-out after its guard", shell_ok.replace("              mvn -B -f .deps/options-edge-contracts/pom.xml install", "              git -C .deps/options-edge-contracts checkout origin/feature\n              mvn -B -f .deps/options-edge-contracts/pom.xml install"), contracts_manifest, False, "is not re-bound")
    case("shell block with set +e around the guard", shell_ok.replace("              set -euo pipefail\n              rm -rf", "              set +e\n              rm -rf"), contracts_manifest, False, "must not `set +e`")
    # manifest hygiene
    case("unclassified Jenkinsfile", good, MANIFEST + "Jenkinsfile.other | in | | x\n", False, "listed in the manifest but not present")
    case("out needs a reason", good, "Jenkinsfile.fixture | out | |\n", False, "needs a reason")

    print(f"validate-jenkinsfile-guard-test: {passed} passed, {failed} failed")
    if failed == 0 and passed >= 50:
        print("validate-jenkinsfile-guard-test: ALL PASS")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
