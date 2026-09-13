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
import re
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
      options { timeout(time: 10, unit: 'MINUTES') }
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
          options { timeout(time: 10, unit: 'MINUTES') }
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
            timeout(time: 10, unit: 'MINUTES') {
              sh 'PERMITTED_SHA="${APP_PERMITTED_SHA:-}" bash scripts/jenkins/permitted-sha-guard.sh --dir app-src --ref main'
            }
            timeout(time: 10, unit: 'MINUTES') {
              sh 'PERMITTED_SHA="${APP_PERMITTED_SHA:-}" bash scripts/jenkins/verify-permitted-tree.sh --dir app-src'
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
          timeout(time: 10, unit: 'MINUTES') {
            def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')
            if (rc != 0) {
              error("smoke workspace refused (rc=${rc})")
            }
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
GUARD_CMD = 'PERMITTED_SHA="${APP_PERMITTED_SHA:-}" bash scripts/jenkins/permitted-sha-guard.sh --dir app-src --ref main'
NESTED_BLOCK = """            timeout(time: 10, unit: 'MINUTES') {
              sh '""" + GUARD_CMD + """'
            }
"""
SMOKE_GUARD = """        script {
          timeout(time: 10, unit: 'MINUTES') {
            def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')
            if (rc != 0) {
              error("smoke workspace refused (rc=${rc})")
            }
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
    VERIFY_APP = "            timeout(time: 10, unit: 'MINUTES') {\n              sh 'PERMITTED_SHA=\"${APP_PERMITTED_SHA:-}\" bash scripts/jenkins/verify-permitted-tree.sh --dir app-src'\n            }\n"
    BUILD = "            sh 'docker build -t app app-src'\n"
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
    mut("separate-agent stage without inline re-guard", "        checkout scm\n" + SMOKE_GUARD, "        checkout scm\n", "does not open with the canonical inline re-guard")
    mut("inline re-guard without its deadline block", SMOKE_GUARD, SMOKE_GUARD.replace("          timeout(time: 10, unit: 'MINUTES') {\n", "").replace("          }\n        }\n", "        }\n"), "does not open with the canonical inline re-guard")
    mut("primary guard stage without its deadline", "      agent { label 'deploy-host' }\n      options { timeout(time: 10, unit: 'MINUTES') }\n", "      agent { label 'deploy-host' }\n", "not the canonical guard")
    mut("primary guard stage deadline above 30 minutes", "      agent { label 'deploy-host' }\n      options { timeout(time: 10, unit: 'MINUTES') }\n", "      agent { label 'deploy-host' }\n      options { timeout(time: 90, unit: 'MINUTES') }\n", "1 <= N <= 30")
    mut("separate-agent stage inline re-guard inverted", "            if (rc != 0) {\n              error(\"smoke workspace refused", "            if (rc == 0) {\n              error(\"smoke workspace refused", "does not open with the canonical inline re-guard")
    mut("separate-agent stage effect before its re-guard", "        checkout scm\n" + SMOKE_GUARD, "        checkout scm\n        sh 'docker rm -f options-edge-web'\n" + SMOKE_GUARD, "does not open with the canonical inline re-guard")
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
    mut("nested guard with the root permission", "PERMITTED_SHA=\"${APP_PERMITTED_SHA:-}\" bash", "PERMITTED_SHA=\"${PERMITTED_SHA:-}\" bash", "'app-src' acquired after the guard is not re-bound")
    mut("nested guard with an alias ref", "--dir app-src --ref main", "--dir app-src --ref origin/main", "'app-src' acquired after the guard is not re-bound")
    mut("nested guard in the old Groovy returnStatus form", NESTED_BLOCK,
        "            script {\n              def rc = sh(returnStatus: true, script: '" + GUARD_CMD + "')\n              if (rc != 0) {\n                error(\"app refused\")\n              }\n            }\n",
        "is not re-bound")
    mut("nested guard as sh(script: …, returnStatus: true)", "              sh '" + GUARD_CMD + "'\n", "              sh(script: '" + GUARD_CMD + "', returnStatus: true)\n", "is not re-bound")
    mut("nested guard as sh(script: …, 'returnStatus': true) — quoted key (Codex gateway r6 M2)", "              sh '" + GUARD_CMD + "'\n", "              sh(script: '" + GUARD_CMD + "', 'returnStatus': true)\n", "is not re-bound")
    mut("nested guard in a triple-quoted shell block with other commands", "              sh '" + GUARD_CMD + "'\n",
        "              sh " + "\'\'\'" + "\n                set -eu\n                " + GUARD_CMD + " || exit 1\n              " + "\'\'\'" + "\n", "is not re-bound")
    mut("nested guard without its deadline block", NESTED_BLOCK, "            sh '" + GUARD_CMD + "'\n", "timeout(time: N, unit: 'MINUTES')")
    mut("nested guard sharing its deadline block with another step", NESTED_BLOCK, NESTED_BLOCK.replace("            }\n", "              echo 'x'\n            }\n"), "only statement")
    ACQ_BLOCK = "            dir('app-src') {\n              git url: 'git@example:app.git', branch: 'main'\n            }\n"
    PAIR = ACQ_BLOCK + NESTED_BLOCK
    mut("acquisition and guard skipped together by an early return, a later sibling step builds whatever app-src holds (Codex gateway I6 / web M2)",
        PAIR, "            script {\n              return\n" + PAIR.replace("            ", "              ") + "            }\n", "can be skipped while later steps still consume 'app-src'")
    mut("acquisition and guard after a conditional return", PAIR,
        "            script {\n              if (params.APP_PERMITTED_SHA == 'skip') {\n                return\n              }\n" + PAIR.replace("            ", "              ") + "            }\n",
        "can be skipped while later steps still consume 'app-src'")
    mut("nested guard never entered: if (false)", NESTED_BLOCK, "            script {\n              if (false) {\n" + NESTED_BLOCK.replace("            ", "                ") + "              }\n            }\n", "is not re-bound")
    mut("nested guard inside try with a catch that continues", NESTED_BLOCK,
        "            script {\n              try {\n" + NESTED_BLOCK.replace("            ", "                ") + "              } catch (e) {\n                echo 'ignored'\n              }\n            }\n", "is not re-bound")
    mut("nested guard inside catchError", NESTED_BLOCK,
        "            catchError(buildResult: 'FAILURE') {\n" + NESTED_BLOCK.replace("            ", "              ") + "            }\n", "is not re-bound")
    mut("a sibling script that returns BEFORE the acquisition is fine (a return leaves only its own closure)",
        PAIR, "            script {\n              return\n            }\n" + PAIR, "carry the canonical permitted-commit guard", expect_ok=True)
    mut("anything between the acquisition step and its guard step is refused", PAIR, ACQ_BLOCK + "            echo 'between'\n" + NESTED_BLOCK, "nothing may run between them")
    mut("nested clone re-acquired after its guard", "            sh 'docker build -t app app-src'", "            sh 'git -C app-src checkout origin/feature'\n            sh 'docker build -t app app-src'", "nested acquisition must be a dedicated acquisition step")
    # Codex web M6: an effect on the acquisition's OWN step runs before the guard
    SH_ACQ = "            sh '''\n              set -euo pipefail\n              git clone git@github.com:example/app.git app-src\n            '''\n"
    case("a dedicated shell acquisition step followed by its guard is fine", good.replace(ACQ_BLOCK, SH_ACQ), MANIFEST, True)
    for label, line in [("`;` then docker build (Codex web M6)", "git clone git@github.com:example/app.git app-src; docker build -t app app-src"),
                        ("`&&` then make", "git clone git@github.com:example/app.git app-src && make -C app-src"),
                        ("|| true", "git clone git@github.com:example/app.git app-src || true")]:
        case(f"acquisition step with an effect on its own line is refused: {label}", good.replace(ACQ_BLOCK, SH_ACQ.replace("git clone git@github.com:example/app.git app-src", line)), MANIFEST, False, "no other command, separator or effect")
    case("acquisition step with a second command in its body is refused", good.replace(ACQ_BLOCK, SH_ACQ.replace("app-src\n", "app-src\n              docker build -t app app-src\n")), MANIFEST, False, "no other command, separator or effect")
    case("acquisition inside sh(script: ..., returnStatus: true) is refused", good.replace(ACQ_BLOCK, SH_ACQ.replace("            sh '''\n", "            sh(returnStatus: true, script: '''\n").replace("            '''\n", "            ''')\n")), MANIFEST, False, "plain `sh '''…'''` step of its own")
    case("a git url: acquisition sharing its dir() block with an effect is refused", good.replace(ACQ_BLOCK, ACQ_BLOCK.replace("branch: 'main'\n", "branch: 'main'\n              sh 'make'\n")), MANIFEST, False, "only statement of its own dir")
    # Codex gateway I9 / web M7: paths resolved through their enclosing dir() blocks
    case("guard wrapped in dir('other') while the acquisition is at the root is refused (Codex web M7)",
         good.replace(NESTED_BLOCK, "            dir('other') {\n" + NESTED_BLOCK.replace("            ", "              ") + "            }\n"), MANIFEST, False, "the guard resolves to 'other/app-src', the acquisition to 'app-src'")
    case("acquisition in dir('other') guarded as app-src at the root is refused (Codex gateway I9)",
         good.replace(ACQ_BLOCK, "            dir('other') {\n" + SH_ACQ.replace("            ", "              ") + "            }\n"), MANIFEST, False, "the guard resolves to 'app-src', the acquisition to 'other/app-src'")
    case("acquisition in dir('other') guarded with --dir other/app-src is fine",
         good.replace(ACQ_BLOCK, "            dir('other') {\n" + SH_ACQ.replace("            ", "              ") + "            }\n").replace("--dir app-src --ref main", "--dir other/app-src --ref main").replace(VERIFY_APP + BUILD, ""), MANIFEST, True)
    case("acquisition and guard both in dir('other') blocks is fine",
         good.replace(PAIR, "            dir('other') {\n" + SH_ACQ.replace("            ", "              ") + "            }\n            dir('other') {\n" + NESTED_BLOCK.replace("            ", "              ") + "            }\n").replace(VERIFY_APP + BUILD, ""), MANIFEST, True)
    case("nested dir('a') { dir('b') { clone c } } guarded with --dir a/b/c is fine",
         good.replace(ACQ_BLOCK, "            dir('a') {\n              dir('b') {\n" + SH_ACQ.replace("            ", "                ") + "              }\n            }\n").replace("--dir app-src --ref main", "--dir a/b/app-src --ref main").replace(VERIFY_APP + BUILD, ""), MANIFEST, True)
    case("nested dir('a') { dir('b') { clone c } } guarded with --dir b/c is refused",
         good.replace(ACQ_BLOCK, "            dir('a') {\n              dir('b') {\n" + SH_ACQ.replace("            ", "                ") + "              }\n            }\n").replace("--dir app-src --ref main", "--dir b/app-src --ref main"), MANIFEST, False, "the guard resolves to 'b/app-src'")
    case("the gateway I9 reproduction: a second checkout into other/app-src with a guard on app-src is refused",
         good.replace(PAIR, PAIR + "            dir('other') {\n" + SH_ACQ.replace("            ", "              ") + "            }\n" + NESTED_BLOCK), MANIFEST, False, "the guard resolves to 'app-src', the acquisition to 'other/app-src'")
    case("a second acquisition into the same resolved path after its guard is refused",
         good.replace(PAIR, PAIR + SH_ACQ + NESTED_BLOCK), MANIFEST, False, "acquired again after its guard")
    case("an acquisition under ws(...) is refused", good.replace(PAIR, "            ws('elsewhere') {\n" + PAIR.replace("            ", "              ") + "            }\n"), MANIFEST, False, "changes the workspace")
    case("an acquisition in dir(variable) is refused", good.replace(ACQ_BLOCK, "            dir(env.SRC) {\n" + SH_ACQ.replace("            ", "              ") + "            }\n"), MANIFEST, False, "cannot be resolved to a literal path")
    for label, d in [("..", "../app-src"), ("a dot component", "./app-src"), ("an absolute path", "/tmp/app-src")]:
        case(f"a guard --dir with {label} is refused", good.replace("--dir app-src --ref main", f"--dir {d} --ref main"), MANIFEST, False, "")
    case("an acquisition in dir('..') is refused", good.replace(ACQ_BLOCK, "            dir('..') {\n" + SH_ACQ.replace("            ", "              ") + "            }\n"), MANIFEST, False, "not a plain relative path")
    case("acquisition in one stage and its guard in the next stage (another agent) is refused",
         good.replace(NESTED_BLOCK + VERIFY_APP + BUILD, BUILD).replace("        stage('Trigger child') {", "        stage('Bind app') {\n          when { expression { " + G2 + " } }\n          agent { label 'other-host' }\n          steps {\n" + NESTED_BLOCK + "          }\n        }\n        stage('Trigger child') {"),
         MANIFEST, False, "is not re-bound")
    mut("effect on the nested source between acquisition and its guard", NESTED_BLOCK, "            sh 'docker build -t early app-src'\n" + NESTED_BLOCK, "is not re-bound")
    mut("guard invoked from an ordinary shell block elsewhere", "            sh 'kubectl apply -f k8s/fixture.yaml'",
        "            sh 'bash scripts/jenkins/permitted-sha-guard.sh || true'\n            sh 'kubectl apply -f k8s/fixture.yaml'", "invoked outside its canonical forms")

    # THE CLASS, generically: the dedicated step body is a fixed template — ANY extra token refuses. Every shape
    # Codex found (exit 0 first, exit 0 hidden by a later nonzero exit, an EXIT trap, backticks, subshells,
    # substitutions, || true, pipes, background, redirections, comments, set +e, extra arguments) is one of these.
    for label, body in [
        ("exit 0 before it", "exit 0; " + GUARD_CMD),
        ("exit 0 hidden by a later nonzero exit (Codex #1043 r6 / gateway I7 / web M5)", "exit 0; exit 1; " + GUARD_CMD),
        ("a conditional exit 0 else exit 1 (Codex gateway I7)", "if [ \"${SKIP:-true}\" = true ]; then exit 0; else exit 1; fi; " + GUARD_CMD),
        ("exit 256 (status 0) before it", "exit 256; " + GUARD_CMD),
        ("an EXIT trap turning refusal into success (Codex gateway I8)", "trap \"exit 0\" EXIT; " + GUARD_CMD),
        ("|| true after it", GUARD_CMD + " || true"),
        ("|| exit 1 after it", GUARD_CMD + " || exit 1"),
        ("&& true after it", GUARD_CMD + " && true"),
        ("; true after it", GUARD_CMD + "; true"),
        ("a pipe after it", GUARD_CMD + " | tee guard.log"),
        ("backgrounded", GUARD_CMD + " &"),
        ("a redirection", GUARD_CMD + " > /dev/null"),
        ("a comment after it", GUARD_CMD + " # checked"),
        ("in a subshell", "( " + GUARD_CMD + " )"),
        ("in a command substitution", "x=$(" + GUARD_CMD + ")"),
        ("in backticks (Codex #1043 r5)", "x=`" + GUARD_CMD + "`"),
        ("prefixed by echo", "echo " + GUARD_CMD),
        ("prefixed by set +e", "set +e; " + GUARD_CMD),
        ("in bash -c", "bash -c \"" + GUARD_CMD.replace('"', '') + "\""),
        ("an extra guard argument", GUARD_CMD + " --branch feature"),
        ("an unquoted permission variable", GUARD_CMD.replace('"${APP_PERMITTED_SHA:-}"', "${APP_PERMITTED_SHA:-}")),
        ("a directory built from a variable", GUARD_CMD.replace("--dir app-src", "--dir $APP_DIR")),
        ("leading whitespace inside the script", " " + GUARD_CMD),
    ]:
        mut(f"dedicated step body with extra tokens is refused: {label}", "              sh '" + GUARD_CMD + "'\n", "              sh '" + body.replace("'", "\\'") + "'\n", "is not re-bound")
    # ... and exhaustively at the template level: every single shell metacharacter or word inserted at every
    # position of the command leaves a string the dedicated-step template does not match.
    import importlib.util
    spec = importlib.util.spec_from_file_location("gv", a.validator)
    gv = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gv)
    base = "sh '" + GUARD_CMD + "'"
    assert gv.DEDICATED_GUARD.match(base)
    inserts = [";", "&", "|", "<", ">", "(", ")", "`", "$(", "#", "\\n", " x", "x ", "\t", "*", "?", "!", "{", "}", "'", " exit 0;", " trap x EXIT;"]
    accepted = []
    tried = 0
    for pos in range(len("sh '"), len(base) - 1):
        for ins in inserts:
            variant = base[:pos] + ins + base[pos:]
            tried += 1
            m = gv.DEDICATED_GUARD.match(variant)
            if m and variant != base:
                # the only legitimate variability: the permission variable name and the directory's own characters
                if not (m.group("own") != "APP" or m.group("dir") != "app-src"):
                    accepted.append(variant)
                elif re.search(r"[;&|<>()`$#\n\t*?!{}' ]", ins):
                    accepted.append(variant)
    case_ok = not accepted
    print(("ok   " if case_ok else "FAIL ") + f"[template sweep: {tried} single insertions into the dedicated command, none accepted]" + ("" if case_ok else f"\n{accepted[:5]}"))
    if case_ok:
        passed += 1
    else:
        failed += 1
    # Codex #1043 r7 M1: the template TEXT inside another literal or a comment is not the Jenkins step
    STEP = "              sh '" + GUARD_CMD + "'\n"
    TQ3 = chr(39) * 3
    for label, repl in [
        ("inside a multi-line triple-single-quoted shell block after exit 0; exit 1 (Codex reproduction)", "              sh " + TQ3 + "\n                exit 0; exit 1\n                sh '" + GUARD_CMD + "'\n              " + TQ3 + "\n"),
        ("inside a triple-single-quoted shell block after a plain exit 0", "              sh " + TQ3 + "\n                exit 0\n                sh '" + GUARD_CMD + "'\n              " + TQ3 + "\n"),
        ("inside a triple-double-quoted GString block", "              sh " + '"""' + "\n                exit 0\n                sh '" + GUARD_CMD + "'\n              " + '"""' + "\n"),
        ("as a line comment", "              // sh '" + GUARD_CMD + "'\n"),
        ("inside a block comment", "              /* sh '" + GUARD_CMD + "' */\n"),
        ("as a def string later passed to sh", "              def cmd = '" + GUARD_CMD + "'\n              sh cmd\n"),
        ("as a double-quoted GString argument", "              sh \"" + GUARD_CMD.replace('"', '\\"').replace("$", "\\$") + "\"\n"),
        ("as an argument that continues after the literal", "              sh '" + GUARD_CMD + "' + ' || true'\n"),
    ]:
        mut(f"the template text {label} is not the dedicated step", STEP, repl, "is not re-bound")
    mut("dedicated step with the :? form is fine", "${APP_PERMITTED_SHA:-}", "${APP_PERMITTED_SHA:?}", "carry the canonical permitted-commit guard", expect_ok=True)

    # 10. contracts
    contracts_ok = good.replace("            sh 'docker build -t app app-src'", "            sh '''\n              set -euo pipefail\n              rm -rf .deps/options-edge-contracts\n              git clone git@github.com:example/contracts.git .deps/options-edge-contracts\n              git -C .deps/options-edge-contracts checkout main\n            '''\n            timeout(time: 10, unit: 'MINUTES') {\n              sh 'PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir .deps/options-edge-contracts --ref main'\n            }\n            timeout(time: 10, unit: 'MINUTES') {\n              sh 'PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash scripts/jenkins/verify-permitted-tree.sh --dir .deps/options-edge-contracts --allow-ignored target'\n            }\n            sh '''\n              mvn -B -f .deps/options-edge-contracts/pom.xml install\n            '''\n            sh 'docker build -t app app-src'")
    contracts_manifest = "Jenkinsfile.fixture | in | reguard=Deploy path; contracts=.deps/options-edge-contracts | fixture\n"
    shell_ok = contracts_ok
    case("contracts acquired in one step, bound by the dedicated step, installed in the next: fine", contracts_ok, contracts_manifest, True)
    case("contracts named but never acquired", good, contracts_manifest, False, "no acquisition of it was found")
    case("contracts guard inside the acquisition's shell block (the old form) is refused",
         contracts_ok.replace("              git -C .deps/options-edge-contracts checkout main\n", "              git -C .deps/options-edge-contracts checkout main\n              PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash scripts/jenkins/permitted-sha-guard.sh --dir .deps/options-edge-contracts --ref main || exit 1\n"),
         contracts_manifest, False, "no other command, separator or effect")
    case("contracts re-checked-out after its guard", contracts_ok.replace("              mvn -B -f .deps/options-edge-contracts/pom.xml install", "              git -C .deps/options-edge-contracts checkout origin/feature\n              mvn -B -f .deps/options-edge-contracts/pom.xml install"), contracts_manifest, False, "no other command, separator or effect")
    # 15. Groovy escapes
    case("invalid Groovy escape \\. in a ''' block", shell_ok.replace("              mvn -B -f .deps", "              ls target/*.jar | grep -v '\\.original$'\n              mvn -B -f .deps"), contracts_manifest, False, "not a Groovy escape")
    mut("invalid Groovy escape \\d in a single-quoted string", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            sh 'echo x | grep -E \"[\\d]+\"'\n            sh 'kubectl apply -f k8s/fixture.yaml'", "not a Groovy escape")
    mut("octal Groovy escape", "            sh 'kubectl apply -f k8s/fixture.yaml'", "            sh 'sed s/a/\\1/ x'\n            sh 'kubectl apply -f k8s/fixture.yaml'", "OCTAL")
    case("doubled backslash is fine", shell_ok.replace("              mvn -B -f .deps", "              ls target/*.jar | grep -v '\\\\.original$'\n              mvn -B -f .deps"), contracts_manifest, True)
    # Codex #1043 r8/r9 → runtime provenance verification. The lexical "nothing may write into the checkout" analysis
    # is replaced by verify-permitted-tree.sh run immediately before every build/publish-from-source effect. Here the
    # validator enforces the PRESENCE and PLACEMENT of that verify step (its runtime behaviour — pull, reset, copy,
    # archive-over-tree, untracked/modified files — is covered by verify-permitted-tree-test.sh).
    TQ3 = chr(39) * 3
    assert good.count(VERIFY_APP) == 1
    case("the verify step before the nested build is missing", good.replace(VERIFY_APP, ""), MANIFEST, False, "no dedicated verify-permitted-tree step")
    case("the verify step is AFTER the build, not before", good.replace(VERIFY_APP + BUILD, BUILD + VERIFY_APP), MANIFEST, False, "no dedicated verify-permitted-tree step")
    case("the verify step names another directory, not the one the build consumes",
         good.replace("verify-permitted-tree.sh --dir app-src", "verify-permitted-tree.sh --dir other"), MANIFEST, False, "no dedicated verify-permitted-tree step")
    case("the verify step can be skipped (inside if (false))",
         good.replace(VERIFY_APP, "            script {\n              if (params.SKIP == 'yes') {\n" + VERIFY_APP.replace("            ", "                ") + "              }\n            }\n"), MANIFEST, False, "can be skipped")
    for label, mutated in [
        ("a trailing token after the literal", "sh 'PERMITTED_SHA=\"${APP_PERMITTED_SHA:-}\" bash scripts/jenkins/verify-permitted-tree.sh --dir app-src' + ''"),
        ("the verify command inside a triple-quoted block", "sh " + TQ3 + "\n                PERMITTED_SHA=\"${APP_PERMITTED_SHA:-}\" bash scripts/jenkins/verify-permitted-tree.sh --dir app-src\n              " + TQ3),
        ("a || true appended in the shell string", "sh 'PERMITTED_SHA=\"${APP_PERMITTED_SHA:-}\" bash scripts/jenkins/verify-permitted-tree.sh --dir app-src || true'"),
    ]:
        case(f"a verify step that is not the dedicated template ({label}) does not count",
             good.replace("sh 'PERMITTED_SHA=\"${APP_PERMITTED_SHA:-}\" bash scripts/jenkins/verify-permitted-tree.sh --dir app-src'", mutated), MANIFEST, False, "no dedicated verify-permitted-tree step")
    case("a verify step with an --allow-ignored list is fine",
         good.replace("verify-permitted-tree.sh --dir app-src", "verify-permitted-tree.sh --dir app-src --allow-ignored target --allow-ignored .jenkins-tmp"), MANIFEST, True)
    # every build/publish-from-source token requires the verify step; with it present each is fine
    for label, effect in [("rsync of the source", "rsync -a app-src/ builder:/tmp/app/"), ("helm upgrade from the source", "helm upgrade app app-src")]:
        b = "            sh '" + effect + "'\n"
        case(f"{label} without a verify step is refused", good.replace(VERIFY_APP + BUILD, b), MANIFEST, False, "no dedicated verify-permitted-tree step")
        case(f"{label} with the verify step is fine", good.replace(VERIFY_APP + BUILD, VERIFY_APP + b), MANIFEST, True)
    # docker push / git push publish an already-built image or ref, not a source tree — no verify step required
    case("docker push of a built image needs no verify step", good.replace(VERIFY_APP + BUILD, "            sh 'docker push registry/app:tag'\n"), MANIFEST, True)
    case("git push of refs needs no verify step", good.replace(VERIFY_APP + BUILD, "            sh 'git -C app-src push origin HEAD'\n"), MANIFEST, True)
    # mvn install/deploy FROM THE PRIMARY workspace (no nested dir named) needs verify --dir .
    case("mvn install from the primary workspace with no verify --dir . is refused",
         good.replace(VERIFY_APP + BUILD, "            sh 'mvn -B install'\n"), MANIFEST, False, "the primary checkout '.'")
    case("mvn install from the primary workspace with verify --dir . is fine",
         good.replace(VERIFY_APP + BUILD, "            timeout(time: 10, unit: 'MINUTES') {\n              sh 'PERMITTED_SHA=\"${PERMITTED_SHA:-}\" bash scripts/jenkins/verify-permitted-tree.sh --dir . --allow-ignored target'\n            }\n            sh 'mvn -B install'\n"), MANIFEST, True)
    # a docker build of the PRIMARY workspace is NOT a required-verify effect (build output makes a whole-tree verify
    # meaningless there); a docker build whose CONTEXT is a nested checkout IS.
    case("a docker build of the primary workspace needs no verify step",
         good.replace(VERIFY_APP + BUILD, "            sh 'docker build -t app -f Dockerfile .'\n"), MANIFEST, True)
    case("a docker build whose context is the nested checkout needs its verify step",
         good.replace(VERIFY_APP + BUILD, BUILD), MANIFEST, False, "the nested checkout 'app-src'")
    case("kubectl apply (config application, not a build/ship from source) needs no verify step",
         good.replace(VERIFY_APP + BUILD, "            sh 'kubectl apply -f k8s/fixture.yaml'\n"), MANIFEST, True)
    # contracts: the mvn install of the nested contracts needs its verify step
    case("contracts mvn install without a verify step for the contracts dir is refused",
         contracts_ok.replace("            timeout(time: 10, unit: 'MINUTES') {\n              sh 'PERMITTED_SHA=\"${CONTRACTS_PERMITTED_SHA:-}\" bash scripts/jenkins/verify-permitted-tree.sh --dir .deps/options-edge-contracts --allow-ignored target'\n            }\n", ""),
         contracts_manifest, False, "the nested checkout '.deps/options-edge-contracts'")
    # Codex #809 M3: directories resolve through EVERY enclosing block, not only single-statement dir() wrappers.
    # Tested on a fixture without the trailing build effect, to isolate rule-9 path resolution.
    noeffect = good.replace(VERIFY_APP + BUILD, "")
    SCRIPTED = lambda d, gdir: noeffect.replace(PAIR, f"            dir('{d}') {{\n              script {{\n" + SH_ACQ.replace("            ", "                ") + NESTED_BLOCK.replace("            ", "                ").replace("--dir app-src", f"--dir {gdir}") + "              }\n            }\n")
    case("dir('a') { script { acquisition; guard --dir app-src } } binds a/app-src: fine", SCRIPTED("a", "app-src"), MANIFEST, True)
    case("dir('a') { script { acquisition; guard --dir a/app-src } } is refused (resolved through script{})", SCRIPTED("a", "a/app-src"), MANIFEST, False, "the guard resolves to 'a/a/app-src', the acquisition to 'a/app-src'")
    case("dir('a') { withEnv { acquisition; guard } } resolves through withEnv too",
         good.replace(PAIR, "            dir('a') {\n              withEnv(['X=1']) {\n" + SH_ACQ.replace("            ", "                ") + NESTED_BLOCK.replace("            ", "                ").replace("--dir app-src", "--dir a/app-src") + "              }\n            }\n"),
         MANIFEST, False, "the guard resolves to 'a/a/app-src'")
    for bad in ["..", "/tmp", "~", "a/../b"]:
        case(f"dir('{bad}') {{ script {{ acquisition; guard }} }} is refused", SCRIPTED(bad, "app-src"), MANIFEST, False, "not a plain relative path")
    case("dir(env.X) { script { acquisition; guard } } is refused",
         good.replace(PAIR, "            dir(env.X) {\n              script {\n" + SH_ACQ.replace("            ", "                ") + NESTED_BLOCK.replace("            ", "                ") + "              }\n            }\n"),
         MANIFEST, False, "cannot be resolved to a literal path")
    contracts_in_other = contracts_ok.replace("            sh '''\n              set -euo pipefail\n              rm -rf .deps", "            dir('other') {\n            script {\n            sh '''\n              set -euo pipefail\n              rm -rf .deps").replace("--dir .deps/options-edge-contracts --ref main'\n            }\n", "--dir .deps/options-edge-contracts --ref main'\n            }\n            }\n            }\n")
    case("the #809 M3 reproduction: contracts acquired and guarded under dir('other') { script { … } } do not satisfy contracts=.deps/…",
         contracts_in_other, contracts_manifest, False, "no acquisition of it was found")
    # manifest hygiene + --only (the downstream-definition mode)
    case("unclassified Jenkinsfile", good, MANIFEST + "Jenkinsfile.other | in | | x\n", False, "listed in the manifest but not present")
    case("out needs a reason", good, "Jenkinsfile.fixture | out | |\n", False, "needs a reason")
    case("--only judges the named file even when others are absent", good, MANIFEST + "Jenkinsfile.other | in | | x\n", True, "1 Jenkinsfile(s) carry", ["--only", "Jenkinsfile.fixture"])
    case("--only refuses a file classified out", good, "Jenkinsfile.fixture | out | | nobody guards it\n", False, "classified out of scope", ["--only", "Jenkinsfile.fixture"])
    case("--only refuses a file the manifest does not list", good, "Jenkinsfile.other | in | | x\n", False, "not classified in the manifest", ["--only", "Jenkinsfile.fixture"])
    case("--only still applies every rule", good.replace("        stage('Deploy') {\n          when { expression { " + G2 + " } }\n", "        stage('Deploy') {\n"), MANIFEST, False, "has no `when` gate", ["--only", "Jenkinsfile.fixture"])

    print(f"validate-jenkinsfile-guard-test: {passed} passed, {failed} failed")
    if failed == 0 and passed >= 195:
        print("validate-jenkinsfile-guard-test: ALL PASS")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
