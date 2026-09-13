#!/usr/bin/env python3
"""Mutation suite for validate-jenkinsfile-guard.py — shared byte-for-byte across the four repositories.

A GOOD fixture (a pipeline with a primary guard, executable `when` gates on every later stage, a
container re-guard, a separate-agent stage with an inline re-guard, a nested clone bound by its own
permission, a same-stage compatibility-checked downstream trigger, a preflight-flagged agentless
trigger, and a gated post{}) must pass; then ONE mutation per enforced rule must be refused — every one
of them a regression Codex demonstrated an earlier validator accepted, or an obvious sibling. A
validator that quietly stops checking something fails here instead of going green.

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
G1 = "env.PERMITTED_SHA_GUARD == 'PASSED'"
G2 = "env.PERMITTED_SHA_GUARD == 'PASSED' && env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED'"

GOOD = """pipeline {
  agent none
  parameters {
    string(name: 'PERMITTED_SHA', defaultValue: '', trim: true, description: 'the permitted commit')
    string(name: 'PERMITTED_SHA_GUARD_VERSION', defaultValue: '__HASH__', description: 'guard version')
    string(name: 'APP_PERMITTED_SHA', defaultValue: '', trim: true, description: 'app commit')
    string(name: 'CHILD_PERMITTED_SHA', defaultValue: '', trim: true, description: 'child commit')
    string(name: 'ROLL_PERMITTED_SHA', defaultValue: '', trim: true, description: 'rollout commit')
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
      when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' } }
      agent { label 'deploy-host' }
      stages {
        stage('Permitted commit guard (deploy workspace)') {
          when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' } }
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
          when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' && env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED' && (params.CHILD_PERMITTED_SHA != 'skip') } }
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
          when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' && env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED' } }
          steps {
            script {
              def compat = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh child-job "${CHILD_PERMITTED_SHA:?}"')
              if (compat != 0) {
                error("child-job definition not confirmed (rc=${compat})")
              }
              build job: 'child-job', parameters: [string(name: 'PERMITTED_SHA', value: params.CHILD_PERMITTED_SHA)]
            }
          }
        }
        stage('Deploy') {
          when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' && env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED' } }
          steps {
            sh 'kubectl apply -f k8s/fixture.yaml'
          }
        }
        stage('Rollout preflight') {
          when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' && env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED' } }
          steps {
            script {
              def rcheck = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh roll-job "${ROLL_PERMITTED_SHA:?}" REQUIRED_IMAGE')
              if (rcheck != 0) {
                error("roll-job definition not confirmed (rc=${rcheck})")
              }
              env.GUARDED_DOWNSTREAM_ROLL_JOB = 'PASSED'
            }
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
    stage('Rollout') {
      agent none
      when {
        beforeAgent true
        expression { env.PERMITTED_SHA_GUARD == 'PASSED' && env.GUARDED_DOWNSTREAM_ROLL_JOB == 'PASSED' && (params.ROLL_PERMITTED_SHA != '') }
      }
      steps {
        build job: 'roll-job', parameters: [string(name: 'PERMITTED_SHA', value: params.ROLL_PERMITTED_SHA.trim())], wait: true
      }
    }
    stage('Smoke') {
      when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' } }
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
COMPAT_BLOCK = """              def compat = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh child-job "${CHILD_PERMITTED_SHA:?}"')
              if (compat != 0) {
                error("child-job definition not confirmed (rc=${compat})")
              }
"""
NESTED_BLOCK = """            script {
              def rc = sh(returnStatus: true, script: 'PERMITTED_SHA="${APP_PERMITTED_SHA:-}" bash scripts/jenkins/permitted-sha-guard.sh --dir app-src --ref main')
              if (rc != 0) {
                error("app checkout refused (rc=${rc})")
              }
            }
"""


def run(validator: str, root: str, manifest: str, extra: list[str] | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(["python3", validator, "--root", root, "--manifest", manifest, *(extra or [])], capture_output=True, text=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--validator", default=os.path.join(HERE, "validate-jenkinsfile-guard.py"))
    a = ap.parse_args()
    good = GOOD.replace("__HASH__", STUB_HASH)
    passed = failed = 0

    def case(name: str, text: str, manifest: str, expect_ok: bool, must_say: str = "", extra: list[str] | None = None,
             other_files: dict[str, str] | None = None) -> None:
        nonlocal passed, failed
        tmp = tempfile.mkdtemp()
        try:
            os.makedirs(os.path.join(tmp, "scripts/jenkins"))
            with open(os.path.join(tmp, "scripts/jenkins/permitted-sha-guard.sh"), "w") as fh:
                fh.write(STUB_GUARD)
            with open(os.path.join(tmp, "Jenkinsfile.fixture"), "w") as fh:
                fh.write(text)
            for n, t in (other_files or {}).items():
                with open(os.path.join(tmp, n), "w") as fh:
                    fh.write(t)
            mpath = os.path.join(tmp, "scope.txt")
            with open(mpath, "w") as fh:
                fh.write(manifest)
            r = run(a.validator, tmp, mpath, extra)
            ok = (r.returncode == 0) == expect_ok and (must_say in (r.stdout + r.stderr))
            if ok:
                passed += 1
                print(f"ok   [{name}]")
            else:
                failed += 1
                print(f"FAIL [{name}]: rc={r.returncode} expected_ok={expect_ok} must_say={must_say!r}\n{r.stdout}{r.stderr}")
        finally:
            shutil.rmtree(tmp, True)

    def mut(name: str, old: str, new: str, must_say: str, count: int = 1, expect_ok: bool = False) -> None:
        nonlocal failed
        if good.count(old) < 1:
            print(f"FAIL [{name}]: mutation anchor not found: {old[:70]!r}")
            failed += 1
            return
        case(name, good.replace(old, new, count), MANIFEST, expect_ok, must_say)

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
    mut("re-guard stage without its gate", "        stage('Permitted commit guard (deploy workspace)') {\n          when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' } }\n", "        stage('Permitted commit guard (deploy workspace)') {\n", "must open with when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' } }")
    # 6. separate-agent stage
    mut("separate-agent stage without inline re-guard", "        checkout scm\n        script {\n          def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')\n          if (rc != 0) {\n            error(\"smoke workspace refused (rc=${rc})\")\n          }\n        }\n", "        checkout scm\n", "does not open with the canonical inline re-guard")
    mut("separate-agent stage inline re-guard inverted", "          if (rc != 0) {\n            error(\"smoke workspace refused", "          if (rc == 0) {\n            error(\"smoke workspace refused", "does not open with the canonical inline re-guard")
    mut("separate-agent stage effect before its re-guard", "        checkout scm\n        script {\n          def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')\n          if (rc != 0) {\n            error(\"smoke workspace refused", "        checkout scm\n        sh 'docker rm -f options-edge-web'\n        script {\n          def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')\n          if (rc != 0) {\n            error(\"smoke workspace refused", "does not open with the canonical inline re-guard")
    mut("separate-agent stage second checkout", "        sh 'mvn -B -Psmoke verify'", "        checkout scm\n        sh 'mvn -B -Psmoke verify'", "the guarded workspace is re-acquired after the guard")
    # 13. executable gates
    mut("effect stage gate removed", "        stage('Deploy') {\n          when { expression { " + G2 + " } }\n", "        stage('Deploy') {\n", "stage 'Deploy' after the guard has no `when` gate")
    mut("effect stage gate inverted (!=)", "        stage('Deploy') {\n          when { expression { " + G2 + " } }", "        stage('Deploy') {\n          when { expression { env.PERMITTED_SHA_GUARD != 'PASSED' && env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED' } }", "is not the canonical gate")
    mut("effect stage gate negated", "        stage('Deploy') {\n          when { expression { " + G2 + " } }", "        stage('Deploy') {\n          when { expression { !(" + G2 + ") } }", "is not the canonical gate")
    mut("effect stage gate or-ed", "        stage('Deploy') {\n          when { expression { " + G2 + " } }", "        stage('Deploy') {\n          when { expression { " + G2 + " || params.FORCE } }", "is not the canonical gate")
    mut("effect stage gate escapes its parentheses", "(params.CHILD_PERMITTED_SHA != 'skip')", "(params.CHILD_PERMITTED_SHA != 'skip') || (true)", "is not the canonical gate")
    mut("effect stage gate escapes through a string", "(params.CHILD_PERMITTED_SHA != 'skip')", "(params.CHILD_PERMITTED_SHA != ')') || (true)", "is not the canonical gate")
    mut("effect stage gate is return true", "        stage('Deploy') {\n          when { expression { " + G2 + " } }", "        stage('Deploy') {\n          when { expression { return true } }", "is not the canonical gate")
    mut("second-workspace stage gated on the first flag only", "        stage('Deploy') {\n          when { expression { " + G2 + " } }", "        stage('Deploy') {\n          when { expression { " + G1 + " } }", "its `when` gate must require env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED'")
    mut("gate flag also defined in environment{}", "  options { disableRestartFromStage(); disableConcurrentBuilds() }", "  options { disableRestartFromStage(); disableConcurrentBuilds() }\n  environment { PERMITTED_SHA_GUARD = 'PASSED' }", "PERMITTED_SHA_GUARD may appear only as")
    mut("gate flag declared as a parameter", "    string(name: 'APP_PERMITTED_SHA'", "    string(name: 'DEPLOY_WORKSPACE_PERMITTED', defaultValue: 'PASSED', description: 'x')\n    string(name: 'APP_PERMITTED_SHA'", "DEPLOY_WORKSPACE_PERMITTED may appear only as")
    mut("gate flag via withEnv", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            withEnv(['PERMITTED_SHA_GUARD=PASSED']) { echo 'x' }\n            sh 'kubectl apply -f k8s/fixture.yaml'", "PERMITTED_SHA_GUARD may appear only as")
    # 7. downstream, form (a): same stage, same control chain
    mut("downstream without compatibility check", COMPAT_BLOCK, "", "is not executably protected by the canonical compatibility check for child-job")
    mut("compatibility check inverted", "              if (compat != 0) {", "              if (compat == 0) {", "is not executably protected")
    mut("compatibility check status discarded", COMPAT_BLOCK, "              sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh child-job \"${CHILD_PERMITTED_SHA:?}\"')\n", "is not executably protected")
    mut("compatibility check for another job", "require-guarded-downstream.sh child-job \"", "require-guarded-downstream.sh other-job \"", "is not executably protected by the canonical compatibility check for child-job")
    mut("compatibility check without the forwarded SHA", "require-guarded-downstream.sh child-job \"${CHILD_PERMITTED_SHA:?}\"'", "require-guarded-downstream.sh child-job'", "is not executably protected")
    mut("compatibility check judges another SHA than forwarded", "require-guarded-downstream.sh child-job \"${CHILD_PERMITTED_SHA:?}\"'", "require-guarded-downstream.sh child-job \"${APP_PERMITTED_SHA:?}\"'", "with \"${CHILD_PERMITTED_SHA:?}\"")
    mut("compatibility check never entered: if (false)", COMPAT_BLOCK, "              if (false) {\n" + COMPAT_BLOCK + "              }\n", "is not executably protected")
    mut("compatibility check caught: catchError", COMPAT_BLOCK, "              catchError(buildResult: 'FAILURE') {\n" + COMPAT_BLOCK + "              }\n", "is not executably protected")
    mut("compatibility check caught: try/catch", COMPAT_BLOCK, "              try {\n" + COMPAT_BLOCK + "              } catch (e) {\n                echo 'ignored'\n              }\n", "is not executably protected")
    mut("compatibility check in a closure never called", COMPAT_BLOCK, "              def later = {\n" + COMPAT_BLOCK + "              }\n", "is not executably protected")
    mut("compatibility check after the trigger", COMPAT_BLOCK + "              build job: 'child-job', parameters: [string(name: 'PERMITTED_SHA', value: params.CHILD_PERMITTED_SHA)]\n", "              build job: 'child-job', parameters: [string(name: 'PERMITTED_SHA', value: params.CHILD_PERMITTED_SHA)]\n" + COMPAT_BLOCK, "is not executably protected")
    mut("check skipped by return in an earlier script block, trigger in a later one (Codex gateway r4)",
        COMPAT_BLOCK + "              build job: 'child-job'",
        "              return\n" + COMPAT_BLOCK + "            }\n            script {\n              build job: 'child-job'",
        "is not executably protected by the canonical compatibility check for child-job")
    mut("check in an earlier sibling script block without a return", COMPAT_BLOCK + "              build job: 'child-job'",
        COMPAT_BLOCK + "            }\n            script {\n              build job: 'child-job'", "is not executably protected")
    mut("check inside dir() (a closure a return can leave), trigger outside", COMPAT_BLOCK,
        "              dir('.') {\n" + COMPAT_BLOCK + "              }\n", "is not executably protected")
    mut("trigger and check share one if-block (runtime-safe)", COMPAT_BLOCK + "              build job: 'child-job', parameters: [string(name: 'PERMITTED_SHA', value: params.CHILD_PERMITTED_SHA)]\n", "              if (params.CHILD_PERMITTED_SHA) {\n" + COMPAT_BLOCK + "              build job: 'child-job', parameters: [string(name: 'PERMITTED_SHA', value: params.CHILD_PERMITTED_SHA)]\n              }\n", "carry the canonical permitted-commit guard", expect_ok=True)
    mut("downstream without PERMITTED_SHA forward", "parameters: [string(name: 'PERMITTED_SHA', value: params.CHILD_PERMITTED_SHA)]", "parameters: [string(name: 'X', value: 'y')]", "does not forward PERMITTED_SHA exactly once")
    mut("downstream forwards a local variable", "parameters: [string(name: 'PERMITTED_SHA', value: params.CHILD_PERMITTED_SHA)]", "parameters: [string(name: 'PERMITTED_SHA', value: psha)]", "forward params.<V>")
    mut("UNBOUND annotation", "              build job: 'child-job'", "              // UNBOUND-DOWNSTREAM: child-job has no guard yet\n              build job: 'child-job'", "UNBOUND-DOWNSTREAM annotation is not a gate")
    mut("an extra unguarded downstream", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            build job: 'other-job', parameters: [string(name: 'PERMITTED_SHA', value: params.CHILD_PERMITTED_SHA)]\n            sh 'kubectl apply -f k8s/fixture.yaml'", "compatibility check for other-job")
    # 7. downstream, form (b): flag after the check, trigger stage gated on it
    mut("agentless trigger gate lacks the downstream flag", "env.PERMITTED_SHA_GUARD == 'PASSED' && env.GUARDED_DOWNSTREAM_ROLL_JOB == 'PASSED' && (params.ROLL_PERMITTED_SHA != '')", "env.PERMITTED_SHA_GUARD == 'PASSED' && (params.ROLL_PERMITTED_SHA != '')", "is not executably protected by the canonical compatibility check for roll-job")
    mut("downstream flag not right after its check", "              env.GUARDED_DOWNSTREAM_ROLL_JOB = 'PASSED'", "              echo 'checked'\n              env.GUARDED_DOWNSTREAM_ROLL_JOB = 'PASSED'", "may be set only as the statement right after its compatibility check")
    mut("downstream flag set without any check", "              def rcheck = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh roll-job \"${ROLL_PERMITTED_SHA:?}\" REQUIRED_IMAGE')\n              if (rcheck != 0) {\n                error(\"roll-job definition not confirmed (rc=${rcheck})\")\n              }\n", "", "may be set only as the statement right after its compatibility check")
    mut("check and flag in if (false) plus a duplicate flag outside (Codex web r3)", "              def rcheck = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh roll-job \"${ROLL_PERMITTED_SHA:?}\" REQUIRED_IMAGE')\n              if (rcheck != 0) {\n                error(\"roll-job definition not confirmed (rc=${rcheck})\")\n              }\n              env.GUARDED_DOWNSTREAM_ROLL_JOB = 'PASSED'\n",
        "              if (false) {\n              def rcheck = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh roll-job \"${ROLL_PERMITTED_SHA:?}\" REQUIRED_IMAGE')\n              if (rcheck != 0) {\n                error(\"roll-job definition not confirmed (rc=${rcheck})\")\n              }\n              env.GUARDED_DOWNSTREAM_ROLL_JOB = 'PASSED'\n              }\n              env.GUARDED_DOWNSTREAM_ROLL_JOB = 'PASSED'\n",
        "env.GUARDED_DOWNSTREAM_ROLL_JOB is assigned 2 times")
    mut("a second flag assignment in a later stage", "        sh 'mvn -B -Psmoke verify'",
        "        sh 'mvn -B -Psmoke verify'\n        script {\n          env.GUARDED_DOWNSTREAM_ROLL_JOB = 'PASSED'\n        }", "env.GUARDED_DOWNSTREAM_ROLL_JOB is assigned 2 times")
    mut("check and its flag nested under if (true) (not top level of the script block)", "              def rcheck = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh roll-job \"${ROLL_PERMITTED_SHA:?}\" REQUIRED_IMAGE')\n              if (rcheck != 0) {\n                error(\"roll-job definition not confirmed (rc=${rcheck})\")\n              }\n              env.GUARDED_DOWNSTREAM_ROLL_JOB = 'PASSED'\n",
        "              if (true) {\n              def rcheck = sh(returnStatus: true, script: 'bash scripts/jenkins/require-guarded-downstream.sh roll-job \"${ROLL_PERMITTED_SHA:?}\" REQUIRED_IMAGE')\n              if (rcheck != 0) {\n                error(\"roll-job definition not confirmed (rc=${rcheck})\")\n              }\n              env.GUARDED_DOWNSTREAM_ROLL_JOB = 'PASSED'\n              }\n",
        "at the top level of the stage's script block")
    mut("downstream flag in environment{}", "  options { disableRestartFromStage(); disableConcurrentBuilds() }", "  options { disableRestartFromStage(); disableConcurrentBuilds() }\n  environment { GUARDED_DOWNSTREAM_ROLL_JOB = 'PASSED' }", "GUARDED_DOWNSTREAM_ROLL_JOB may appear only as")
    mut("preflight stage never runs (runtime-safe: the trigger gate stays shut)", "        stage('Rollout preflight') {\n          when { expression { " + G2 + " } }", "        stage('Rollout preflight') {\n          when { expression { " + G2 + " && (false) } }", "carry the canonical permitted-commit guard", expect_ok=True)
    mut("agentless trigger forwards another SHA than was checked", "value: params.ROLL_PERMITTED_SHA.trim())]", "value: params.CHILD_PERMITTED_SHA)]", "is not executably protected by the canonical compatibility check for roll-job")
    # 8. post gating
    mut("post mutation after the gate block", "            } else {\n              echo 'not permitted — nothing touched'\n            }", "            } else {\n              echo 'not permitted — nothing touched'\n            }\n            sh 'kubectl delete job stale'", "is not inside the true branch")
    mut("post mutation in the else branch", "              echo 'not permitted — nothing touched'", "              sh 'kubectl delete job old'", "is not inside the true branch")
    mut("post gate negated !(…)", "if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {", "if (!(env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED')) {", "is not inside the true branch")
    mut("post gate negated ! (…) with a space", "if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {", "if (! (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED')) {", "is not inside the true branch")
    mut("post gate compared to false", "if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {", "if ((env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') == false) {", "is not inside the true branch")
    mut("post gate on the first guard's flag only", "if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {", "if (env.PERMITTED_SHA_GUARD == 'PASSED') {", "is not inside the true branch")
    mut("post gate with an || escape", "if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {", "if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED' || params.FORCE) {", "is not inside the true branch")
    mut("post gate is only an echoed name", "            if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {\n              sh 'kubectl delete job old'\n            } else {\n              echo 'not permitted — nothing touched'\n            }", "            echo 'DEPLOY_WORKSPACE_PERMITTED'\n            sh 'kubectl delete job old'", "is not inside the true branch")
    mut("post mutation with a != gate", "if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {\n              sh 'kubectl delete job old'\n            } else {\n              echo 'not permitted — nothing touched'\n            }", "if (env.DEPLOY_WORKSPACE_PERMITTED != 'PASSED') {\n              echo 'no'\n            } else {\n              sh 'kubectl delete job old'\n            }", "is not inside the true branch")
    mut("unconditional post mutation added at stage level", "    stage('Smoke') {", "    stage('Smoke') {\n      post { always { node('mac') { sh 'docker rm -f options-edge-web' } } }", "is not inside the true branch")
    mut("unconditional post host-process kill", "            if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {\n              sh 'kubectl delete job old'", "            sh 'kill 1234'\n            if (env.DEPLOY_WORKSPACE_PERMITTED == 'PASSED') {\n              sh 'kubectl delete job old'", "kill/pkill/killall (stops a process) is not inside the true branch")
    mut("post gate with a nested negation inside a positive gate is fine", "              sh 'kubectl delete job old'\n            } else {", "              if (!params.KEEP) {\n                sh 'kubectl delete job old'\n              }\n            } else {", "carry the canonical permitted-commit guard", expect_ok=True)
    # 9. acquisitions after the guard
    mut("git pull of the root after the guard", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            sh 'git pull origin main'\n            sh 'kubectl apply -f k8s/fixture.yaml'", "the guarded workspace is re-acquired after the guard")
    mut("git checkout main after the guard", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            sh 'git checkout main'\n            sh 'kubectl apply -f k8s/fixture.yaml'", "the guarded workspace is re-acquired after the guard")
    mut("git pull rebound by an echo", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            sh 'git pull origin main'\n            echo 'permitted-sha-guard.sh --dir .'\n            sh 'kubectl apply -f k8s/fixture.yaml'", "the guarded workspace is re-acquired after the guard")
    mut("nested clone without its guard", NESTED_BLOCK, "", "'app-src' acquired after the guard is not re-bound")
    mut("nested guard for another directory", "--dir app-src --ref main", "--dir other-src --ref main", "'app-src' acquired after the guard is not re-bound")
    mut("nested guard with the root permission", "PERMITTED_SHA=\"${APP_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir app-src", "PERMITTED_SHA=\"${PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir app-src", "'app-src' acquired after the guard is not re-bound")
    mut("nested guard with an alias ref", "--dir app-src --ref main", "--dir app-src --ref origin/main", "'app-src' acquired after the guard is not re-bound")
    mut("nested guard inverted", "              if (rc != 0) {\n                error(\"app checkout refused", "              if (rc == 0) {\n                error(\"app checkout refused", "'app-src' acquired after the guard is not re-bound")
    mut("nested guard never entered: if (false)", NESTED_BLOCK, "            script {\n              if (false) {\n" + NESTED_BLOCK.replace("            script {\n", "").rsplit("            }\n", 1)[0] + "              }\n            }\n", "is not re-bound")
    mut("nested clone re-acquired after its guard", "            sh 'docker build -t app app-src'", "            sh 'git -C app-src checkout origin/feature'\n            sh 'docker build -t app app-src'", "'app-src' acquired after the guard is not re-bound")
    # 10./11. contracts + shell form
    shell_ok = good.replace("            sh 'docker build -t app app-src'", "            sh '''\n              set -euo pipefail\n              rm -rf .deps/options-edge-contracts\n              git clone git@example:contracts.git .deps/options-edge-contracts\n              git -C .deps/options-edge-contracts checkout main\n              if [ -f x ]; then\n                echo ok\n              fi\n              PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir .deps/options-edge-contracts --ref main || exit 1\n              mvn -B -f .deps/options-edge-contracts/pom.xml install\n            '''\n            sh 'docker build -t app app-src'")
    shell_guard = "              PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir .deps/options-edge-contracts --ref main || exit 1\n"
    contracts_manifest = "Jenkinsfile.fixture | in | reguard=Deploy path; contracts=.deps/options-edge-contracts | fixture\n"
    case("shell contracts guard is fine", shell_ok, contracts_manifest, True)
    case("contracts named but never acquired", good, contracts_manifest, False, "no acquisition of it was found")
    case("shell contracts guard || true", shell_ok.replace("--ref main || exit 1", "--ref main || true"), contracts_manifest, False, "must end with `|| exit 1`")
    case("shell contracts guard with root permission", shell_ok.replace("PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash", "PERMITTED_SHA=\"${PERMITTED_SHA:-}\" bash"), contracts_manifest, False, "is not re-bound")
    case("shell contracts guard commented out", shell_ok.replace("              PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash", "              # PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash"), contracts_manifest, False, "is not re-bound")
    case("shell contracts guard only echoed", shell_ok.replace("              PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash", "              echo PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash"), contracts_manifest, False, "is not re-bound")
    case("shell contracts guard inside if false; then … fi", shell_ok.replace(shell_guard, "              if false; then\n" + shell_guard + "              fi\n"), contracts_manifest, False, "inside a shell if/case/loop/function/group")
    case("shell contracts guard inside a function never called", shell_ok.replace(shell_guard, "              bind_contracts() {\n" + shell_guard + "              }\n"), contracts_manifest, False, "inside a shell if/case/loop/function/group")
    case("shell contracts guard inside a heredoc", shell_ok.replace(shell_guard, "              cat > /dev/null <<'EOF'\n" + shell_guard + "EOF\n"), contracts_manifest, False, "inside a heredoc")
    case("shell contracts guard continued from false &&", shell_ok.replace(shell_guard, "              false && \\\\\n" + shell_guard), contracts_manifest, False, "the previous line continues into it")
    case("shell contracts guard in a subshell whose failure is discarded (Codex #1043 r4)", shell_ok.replace(shell_guard, "              (\n" + shell_guard + "              ) || true\n"), contracts_manifest, False, "inside a subshell")
    case("shell contracts guard in a command substitution", shell_ok.replace(shell_guard, "              out=\"$(\n" + shell_guard + "              )\" || true\n"), contracts_manifest, False, "inside a subshell")
    case("shell contracts guard in a subshell-bodied function", shell_ok.replace(shell_guard, "              bind() (\n" + shell_guard + "              )\n"), contracts_manifest, False, "inside a subshell")
    case("shell contracts guard wrapped in bash -c", shell_ok.replace(shell_guard, "              bash -c '" + shell_guard.strip() + "' || true\n"), contracts_manifest, False, "is not re-bound")
    case("shell contracts guard piped", shell_ok.replace("--ref main || exit 1\n", "--ref main || exit 1 | tee guard.log\n"), contracts_manifest, False, "is not re-bound")
    case("shell contracts guard backgrounded", shell_ok.replace("--ref main || exit 1\n", "--ref main || exit 1 &\n"), contracts_manifest, False, "is not re-bound")
    case("shell contracts guard in an sh(returnStatus: true) block", shell_ok.replace("            sh '''\n              set -euo pipefail\n              rm -rf .deps", "            sh(returnStatus: true, script: '''\n              set -euo pipefail\n              rm -rf .deps").replace("install\n            '''\n", "install\n            ''')\n"), contracts_manifest, False, "whose failure stops the build")
    case("a balanced $( … ) before the shell guard is fine", shell_ok.replace(shell_guard, "              echo \"$(date)\"\n              x=$(printf '%s' \"(a)\")\n" + shell_guard), contracts_manifest, True)
    case("contracts re-checked-out after its guard", shell_ok.replace("              mvn -B -f .deps/options-edge-contracts/pom.xml install", "              git -C .deps/options-edge-contracts checkout origin/feature\n              mvn -B -f .deps/options-edge-contracts/pom.xml install"), contracts_manifest, False, "is not re-bound")
    case("shell block with set +e around the guard", shell_ok.replace("              set -euo pipefail\n              rm -rf", "              set +e\n              rm -rf"), contracts_manifest, False, "must not `set +e`")
    # 15. Groovy escapes
    case("invalid Groovy escape \\. in a ''' block", shell_ok.replace("              mvn -B -f .deps", "              ls target/*.jar | grep -v '\\.original$'\n              mvn -B -f .deps"), contracts_manifest, False, "not a Groovy escape")
    mut("invalid Groovy escape \\d in a single-quoted string", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            sh 'echo x | grep -E \"[\\d]+\"'\n            sh 'kubectl apply -f k8s/fixture.yaml'", "not a Groovy escape")
    mut("octal Groovy escape", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            sh 'sed s/a/\\1/ x'\n            sh 'kubectl apply -f k8s/fixture.yaml'", "OCTAL")
    case("doubled backslash is fine", shell_ok.replace("              mvn -B -f .deps", "              ls target/*.jar | grep -v '\\\\.original$'\n              mvn -B -f .deps"), contracts_manifest, True)
    # manifest hygiene + --only (the downstream-definition mode)
    case("unclassified Jenkinsfile", good, MANIFEST + "Jenkinsfile.other | in | | x\n", False, "listed in the manifest but not present")
    case("out needs a reason", good, "Jenkinsfile.fixture | out | |\n", False, "needs a reason")
    case("--only judges the named file even when others are absent", good, MANIFEST + "Jenkinsfile.other | in | | x\n", True, "1 Jenkinsfile(s) carry", ["--only", "Jenkinsfile.fixture"])
    case("--only refuses a file classified out", good, "Jenkinsfile.fixture | out | | nobody guards it\n", False, "classified out of scope", ["--only", "Jenkinsfile.fixture"])
    case("--only refuses a file the manifest does not list", good, "Jenkinsfile.other | in | | x\n", False, "not classified in the manifest", ["--only", "Jenkinsfile.fixture"])
    case("--only still applies every rule", good.replace("        stage('Deploy') {\n          when { expression { " + G2 + " } }\n", "        stage('Deploy') {\n"), MANIFEST, False, "has no `when` gate", ["--only", "Jenkinsfile.fixture"])

    print(f"validate-jenkinsfile-guard-test: {passed} passed, {failed} failed")
    if failed == 0 and passed >= 110:
        print("validate-jenkinsfile-guard-test: ALL PASS")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
