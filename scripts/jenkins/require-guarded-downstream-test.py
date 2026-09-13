#!/usr/bin/env python3
"""Executed suite for require-guarded-downstream.sh and jenkins-job-path.sh — shared byte-for-byte.

The helper is run for real against: a stand-in controller (a `curl` stub serving files: registered
parameters, config.xml), real git repositories standing in for GitHub (git's url.<base>.insteadOf maps
git@github.com:abhinav-jain09/ and https://github.com/abhinav-jain09/ onto local bare repositories), and
a `gh` stub for the API fallback. What is proven: a child is accepted ONLY when its job configuration
loads the expected Jenkinsfile from the expected repository's */main, the forwarded SHA is main's tip,
and the Jenkinsfile at that commit passes the caller's validator with a guard hashing to the caller's —
and every way of declaring the guard without executing it is refused (the Codex N2/I2/I1 reproduction:
matching registered parameters, no guard in the definition).

Usage: require-guarded-downstream-test.py
"""
from __future__ import annotations

import base64
import hashlib
import os
import shutil
import stat
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
HELPER = os.path.join(HERE, "require-guarded-downstream.sh")
JOBPATH = os.path.join(HERE, "jenkins-job-path.sh")
GUARD = open(os.path.join(HERE, "permitted-sha-guard.sh"), "rb").read()
OWN = hashlib.sha256(GUARD).hexdigest()
URL = "http://jenkins.test:8085/"
TREE = "api/json?tree=property[parameterDefinitions[name,type,defaultParameterValue[value]]]"

CHILD_JF = """pipeline {
  agent any
  parameters {
    string(name: 'PERMITTED_SHA', defaultValue: '', trim: true, description: 'the permitted commit')
    string(name: 'PERMITTED_SHA_GUARD_VERSION', defaultValue: '__HASH__', description: 'guard version')
    string(name: 'REQUIRED_IMAGE', defaultValue: '', trim: true, description: 'digest')
  }
  options { disableRestartFromStage() }
  stages {
    stage('Permitted commit guard') {
      steps {
        script {
          def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')
          if (rc != 0) {
            error("refused (rc=${rc})")
          }
          env.PERMITTED_SHA_GUARD = 'PASSED'
        }
      }
    }
    stage('Deploy') {
      when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' } }
      steps {
        sh 'kubectl apply -f k8s/x.yaml'
      }
    }
  }
}
"""
# Declares everything a caller could read from the controller, and deploys without ever running the guard.
DECLARATION_ONLY_JF = """pipeline {
  agent any
  parameters {
    string(name: 'PERMITTED_SHA', defaultValue: '', trim: true, description: 'the permitted commit')
    string(name: 'PERMITTED_SHA_GUARD_VERSION', defaultValue: '__HASH__', description: 'guard version')
    string(name: 'REQUIRED_IMAGE', defaultValue: '', trim: true, description: 'digest')
  }
  options { disableRestartFromStage() }
  stages {
    stage('Deploy') {
      steps {
        sh 'kubectl apply -f k8s/x.yaml'
      }
    }
  }
}
"""
MANIFEST_IN = "Jenkinsfile.service-deploy | in | | child\n"


# The <definition> elements of the three live children, copied verbatim from the controller's config.xml
# (2026-09-13, read-only). They must keep passing.
REAL_DEFINITIONS = {
    "service-deploy": """<definition class="org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition" plugin="workflow-cps@4331.v9d06ed4658ff">
    <scm class="hudson.plugins.git.GitSCM" plugin="git@5.10.1">
      <configVersion>2</configVersion>
      <userRemoteConfigs>
        <hudson.plugins.git.UserRemoteConfig>
          <url>git@github.com:abhinav-jain09/options-edge-deploy.git</url>
        </hudson.plugins.git.UserRemoteConfig>
      </userRemoteConfigs>
      <branches>
        <hudson.plugins.git.BranchSpec>
          <name>*/main</name>
        </hudson.plugins.git.BranchSpec>
      </branches>
      <doGenerateSubmoduleConfigurations>false</doGenerateSubmoduleConfigurations>
      <submoduleCfg class="empty-list"/>
      <extensions/>
    </scm>
    <scriptPath>Jenkinsfile.service-deploy</scriptPath>
    <lightweight>true</lightweight>
  </definition>""",
    "options-edge-processing": """<definition class="org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition" plugin="workflow-cps@4331.v9d06ed4658ff">
    <scm class="hudson.plugins.git.GitSCM" plugin="git@5.10.1">
      <configVersion>2</configVersion>
      <userRemoteConfigs>
        <hudson.plugins.git.UserRemoteConfig>
          <url>git@github.com:abhinav-jain09/options-edge-processing.git</url>
        </hudson.plugins.git.UserRemoteConfig>
      </userRemoteConfigs>
      <branches>
        <hudson.plugins.git.BranchSpec>
          <name>*/main</name>
        </hudson.plugins.git.BranchSpec>
      </branches>
      <doGenerateSubmoduleConfigurations>false</doGenerateSubmoduleConfigurations>
      <submoduleCfg class="empty-list"/>
      <extensions/>
    </scm>
    <scriptPath>Jenkinsfile</scriptPath>
    <lightweight>true</lightweight>
  </definition>""",
    "options-edge-web-deploy": """<definition class="org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition" plugin="workflow-cps@4331.v9d06ed4658ff">
    <scm class="hudson.plugins.git.GitSCM" plugin="git@5.10.1">
      <configVersion>2</configVersion>
      <userRemoteConfigs>
        <hudson.plugins.git.UserRemoteConfig>
          <url>git@github.com:abhinav-jain09/options-edge.git</url>
        </hudson.plugins.git.UserRemoteConfig>
      </userRemoteConfigs>
      <branches>
        <hudson.plugins.git.BranchSpec>
          <name>*/main</name>
        </hudson.plugins.git.BranchSpec>
      </branches>
      <doGenerateSubmoduleConfigurations>false</doGenerateSubmoduleConfigurations>
      <submoduleCfg class="empty-list"/>
      <extensions/>
    </scm>
    <scriptPath>Jenkinsfile</scriptPath>
    <lightweight>true</lightweight>
  </definition>""",
}


def real_config(job):
    return "<?xml version='1.1' encoding='UTF-8'?>\n<flow-definition plugin=\"workflow-job@1571\">\n  " + REAL_DEFINITIONS[job] + "\n</flow-definition>\n"


def config_xml(url="git@github.com:abhinav-jain09/options-edge-deploy.git", branch="*/main", script="Jenkinsfile.service-deploy",
               definition="org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition", extensions="<extensions/>", extra_remote="",
               remote_extra="", lightweight="<lightweight>true</lightweight>", scm_extra=""):
    return f"""<?xml version='1.1' encoding='UTF-8'?>
<flow-definition plugin="workflow-job@1571">
  <definition class="{definition}" plugin="workflow-cps@4331">
    <scm class="hudson.plugins.git.GitSCM" plugin="git@5.10.1">
      <configVersion>2</configVersion>
      <userRemoteConfigs>
        <hudson.plugins.git.UserRemoteConfig>
          <url>{url}</url>{remote_extra}
        </hudson.plugins.git.UserRemoteConfig>{extra_remote}
      </userRemoteConfigs>
      <branches>
        <hudson.plugins.git.BranchSpec>
          <name>{branch}</name>
        </hudson.plugins.git.BranchSpec>
      </branches>
      <doGenerateSubmoduleConfigurations>false</doGenerateSubmoduleConfigurations>
      <submoduleCfg class="empty-list"/>
      {extensions}{scm_extra}
    </scm>
    <scriptPath>{script}</scriptPath>
    {lightweight}
  </definition>
</flow-definition>
"""


def params_json(version=OWN, extra=("REQUIRED_IMAGE",)):
    import json
    defs = [{"name": "PERMITTED_SHA", "type": "StringParameterDefinition", "defaultParameterValue": {"value": ""}},
            {"name": "PERMITTED_SHA_GUARD_VERSION", "type": "StringParameterDefinition", "defaultParameterValue": {"value": version}}]
    defs += [{"name": e, "type": "StringParameterDefinition", "defaultParameterValue": {"value": ""}} for e in extra]
    return json.dumps({"property": [{"_class": "x"}, {"parameterDefinitions": defs}]})


def git(*args, cwd=None):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True).stdout.strip()


class World:
    def __init__(self):
        self.tmp = tempfile.mkdtemp()
        self.bin = os.path.join(self.tmp, "bin")
        self.jenkins = os.path.join(self.tmp, "jenkins")
        self.repos = os.path.join(self.tmp, "repos")
        for d in (self.bin, self.jenkins, self.repos):
            os.makedirs(d)
        self._stub("curl", """#!/usr/bin/env bash
url="${@: -1}"
rel="${url#"$FAKE_JENKINS_URL"}"
f="$FAKE_JENKINS_DIR/$(printf '%s' "$rel" | sed -e 's/[?]/__q__/g' -e 's/[][,]/_/g')"
[ -f "$f" ] || exit 22
cat "$f"
""")
        self._stub("gh", "#!/usr/bin/env bash\nexit 1\n")
        self.work = os.path.join(self.tmp, "work")
        git("init", "-q", "-b", "main", self.work)
        git("config", "user.email", "t@t", cwd=self.work)
        git("config", "user.name", "t", cwd=self.work)
        self.bare = os.path.join(self.repos, "options-edge-deploy.git")
        self.real_git = shutil.which("git")

    def _stub(self, name, body):
        p = os.path.join(self.bin, name)
        with open(p, "w") as fh:
            fh.write(body)
        os.chmod(p, os.stat(p).st_mode | stat.S_IXUSR)

    def commit(self, jenkinsfile=CHILD_JF, guard=GUARD, manifest=MANIFEST_IN, guard_hash=None, repo="options-edge-deploy",
               script="Jenkinsfile.service-deploy", manifest_path="scripts/ci/jenkins-permitted-sha-scope.txt"):
        if repo != "options-edge-deploy":
            self.work = os.path.join(self.tmp, "work-" + repo)
            self.bare = os.path.join(self.repos, repo + ".git")
            if not os.path.exists(self.work):
                git("init", "-q", "-b", "main", self.work)
                git("config", "user.email", "t@t", cwd=self.work)
                git("config", "user.name", "t", cwd=self.work)
            manifest = manifest.replace("Jenkinsfile.service-deploy", script)
        os.makedirs(os.path.join(self.work, "scripts/jenkins"), exist_ok=True)
        os.makedirs(os.path.dirname(os.path.join(self.work, manifest_path)), exist_ok=True)
        with open(os.path.join(self.work, script), "w") as fh:
            fh.write(jenkinsfile.replace("__HASH__", guard_hash or hashlib.sha256(guard).hexdigest()))
        with open(os.path.join(self.work, "scripts/jenkins/permitted-sha-guard.sh"), "wb") as fh:
            fh.write(guard)
        with open(os.path.join(self.work, manifest_path), "w") as fh:
            fh.write(manifest)
        git("add", "-A", cwd=self.work)
        git("commit", "-q", "--allow-empty", "-m", "c", cwd=self.work)
        sha = git("rev-parse", "HEAD", cwd=self.work)
        if not os.path.exists(self.bare):
            git("clone", "-q", "--bare", self.work, self.bare)
        else:
            git("push", "-q", self.bare, "HEAD:main", cwd=self.work)
        return sha

    def serve(self, job_path, name, body):
        rel = f"{job_path.strip('/')}/{name}".replace("?", "__q__").replace("[", "_").replace("]", "_").replace(",", "_")
        p = os.path.join(self.jenkins, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as fh:
            fh.write(body)

    def serve_child(self, job_path="job/service-deploy", params=None, config=None):
        self.serve(job_path, TREE, params if params is not None else params_json())
        self.serve(job_path, "config.xml", config if config is not None else config_xml())

    def run(self, args, job_name="caller", git_ok=True, jenkins_url=URL, extra_env=None):
        env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "HOME": self.tmp, "TMPDIR": self.tmp,
               "FAKE_JENKINS_URL": URL, "FAKE_JENKINS_DIR": self.jenkins, "JOB_NAME": job_name,
               "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.path.join(self.tmp, "gitconfig"),
               "GIT_SSH_COMMAND": "false"}
        target = f"file://{self.repos}/" if git_ok else f"file://{self.tmp}/no-such-repos/"
        with open(env["GIT_CONFIG_GLOBAL"], "w") as fh:
            fh.write(f'[url "{target}"]\n\tinsteadOf = git@github.com:abhinav-jain09/\n\tinsteadOf = https://github.com/abhinav-jain09/\n')
        if jenkins_url is not None:
            env["JENKINS_URL"] = jenkins_url
        env.update(extra_env or {})
        return subprocess.run(["bash", HELPER, *args], capture_output=True, text=True, env=env)

    def close(self):
        shutil.rmtree(self.tmp, True)


def main() -> int:
    passed = failed = 0

    def check(name, r, ok, say):
        nonlocal passed, failed
        good = (r.returncode == 0) == ok and say in (r.stdout + r.stderr)
        if good:
            passed += 1
            print(f"ok   [{name}]")
        else:
            failed += 1
            print(f"FAIL [{name}]: rc={r.returncode} expected_ok={ok} must_say={say!r}\n--- stdout\n{r.stdout}--- stderr\n{r.stderr}")

    def world():
        w = World()
        return w

    # accepted
    w = world(); a = w.commit(); w.serve_child()
    check("child whose definition runs the guard at the forwarded tip is accepted", w.run(["service-deploy", a, "REQUIRED_IMAGE"]), True, "triggering is allowed")
    w.close()

    # the Codex reproduction: every registered declaration matches, the definition never runs the guard
    w = world(); a = w.commit(jenkinsfile=DECLARATION_ONLY_JF); w.serve_child()
    check("declaration-only child (matching registered parameters, no guard in its Jenkinsfile) is refused",
          w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, "does not pass the permitted-commit validator")
    w.close()

    w = world(); a = w.commit(jenkinsfile=CHILD_JF.replace("      when { expression { env.PERMITTED_SHA_GUARD == 'PASSED' } }\n", "")); w.serve_child()
    check("child whose effect stage is not gated on the guard is refused", w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, "has no `when` gate")
    w.close()

    w = world(); a = w.commit(jenkinsfile=CHILD_JF.replace("          if (rc != 0) {", "          if (rc == 0) {")); w.serve_child()
    check("child whose guard is inverted is refused", w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, "not the canonical guard")
    w.close()

    w = world(); a = w.commit(manifest="Jenkinsfile.service-deploy | out | | nobody guards it\n"); w.serve_child()
    check("child Jenkinsfile classified out of scope at that commit is refused", w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, "classified out of scope")
    w.close()

    other_guard = GUARD + b"\n# a different guard\n"
    w = world(); a = w.commit(guard=other_guard); w.serve_child()
    check("child carrying a different guard at that commit is refused (even though its own validator view is consistent)",
          w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, "would execute a different guard")
    w.close()

    # job configuration
    for name, cfg, say in [
        ("inline pipeline script (not from SCM) is refused", config_xml(definition="org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition"), "not Pipeline script from SCM"),
        ("another repository is refused", config_xml(url="git@github.com:someone/options-edge-deploy.git"), "are not exactly github.com/abhinav-jain09/options-edge-deploy"),
        ("a parameterised branch spec is refused", config_xml(branch="*/${DEPLOY_BRANCH}"), "are not exactly"),
        ("a feature branch spec is refused", config_xml(branch="*/feature"), "are not exactly"),
        ("another Jenkinsfile is refused", config_xml(script="Jenkinsfile"), "scriptPath is 'Jenkinsfile'"),
        ("an SCM extension (pre-build merge) is refused", config_xml(extensions="<extensions><hudson.plugins.git.extensions.impl.PreBuildMerge/></extensions>"), "SCM extensions"),
        ("a second remote is refused", config_xml(extra_remote="<hudson.plugins.git.UserRemoteConfig><url>git@github.com:x/y.git</url></hudson.plugins.git.UserRemoteConfig>"), "exactly one remote is required"),
        ("an unparseable configuration is refused", "<html>login</html", "does not parse"),
    ]:
        w = world(); a = w.commit(); w.serve_child(config=cfg)
        check(name, w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, say)
        w.close()
    w = world(); a = w.commit(); w.serve("job/service-deploy", TREE, params_json())
    check("an unreadable job configuration (403/404) is refused", w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, "could not read the job configuration")
    w.close()

    # remote mapping (Codex round 4, #1043 I11 / gateway I2 / web I1): */main is only main when nothing remaps it
    for name, kw, say in [
        ("tag remapped onto origin/main, heavyweight checkout (Codex #1043 reproduction) is refused",
         dict(remote_extra="\n          <name>origin</name>\n          <refspec>+refs/tags/pre-guard:refs/remotes/origin/main</refspec>", lightweight="<lightweight>false</lightweight>"),
         "can populate refs/remotes/origin/main"),
        ("feature branch remapped onto origin/main, heavyweight (Codex gateway reproduction) is refused",
         dict(remote_extra="\n          <name>origin</name>\n          <refspec>+refs/heads/feature:refs/remotes/origin/main</refspec>", lightweight="<lightweight>false</lightweight>"),
         "can populate refs/remotes/origin/main"),
        ("unguarded tag remapped onto origin/main, heavyweight (Codex web reproduction) is refused",
         dict(remote_extra="\n          <name>origin</name>\n          <refspec>+refs/tags/unguarded:refs/remotes/origin/main</refspec>", lightweight="<lightweight>false</lightweight>"),
         "can populate refs/remotes/origin/main"),
        ("a remapping refspec is refused even with lightweight checkout",
         dict(remote_extra="\n          <refspec>+refs/heads/feature:refs/remotes/origin/main</refspec>"), "can populate refs/remotes/origin/main"),
        ("a default refspec with an extra remapping entry is refused",
         dict(remote_extra="\n          <refspec>+refs/heads/*:refs/remotes/origin/* +refs/tags/x:refs/remotes/origin/main</refspec>"), "can populate refs/remotes/origin/main"),
        ("a remote named other than origin is refused", dict(remote_extra="\n          <name>upstream</name>"), "is not origin"),
        ("a clean configuration with a heavyweight checkout is refused", dict(lightweight="<lightweight>false</lightweight>"), "lightweight checkout is 'false'"),
        ("a configuration without the lightweight element is refused", dict(lightweight=""), "lightweight checkout is None"),
        ("an unknown SCM element is refused", dict(scm_extra="<browser class=\"hudson.plugins.git.browser.GithubWeb\"><url>https://x</url></browser>"), "unexpected element"),
        ("an unknown remote element is refused", dict(remote_extra="\n          <mirror>x</mirror>"), "remote carries unexpected element"),
        ("a second branch spec is refused", dict(branch="*/main</name>\n        </hudson.plugins.git.BranchSpec>\n        <hudson.plugins.git.BranchSpec>\n          <name>*/main"), "are not exactly"),
    ]:
        w = world(); a = w.commit(); w.serve_child(config=config_xml(**kw))
        check(name, w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, say)
        w.close()
    for name, kw in [
        ("origin with the default refspec +refs/heads/*:refs/remotes/origin/* is accepted", dict(remote_extra="\n          <name>origin</name>\n          <refspec>+refs/heads/*:refs/remotes/origin/*</refspec>")),
        ("origin with the main-only refspec is accepted", dict(remote_extra="\n          <name>origin</name>\n          <refspec>+refs/heads/main:refs/remotes/origin/main</refspec>\n          <credentialsId>github</credentialsId>")),
    ]:
        w = world(); a = w.commit(); w.serve_child(config=config_xml(**kw))
        check(name, w.run(["service-deploy", a, "REQUIRED_IMAGE"]), True, "triggering is allowed")
        w.close()
    for job, repo, script, mpath in [("service-deploy", "options-edge-deploy", "Jenkinsfile.service-deploy", "scripts/ci/jenkins-permitted-sha-scope.txt"),
                                     ("options-edge-processing", "options-edge-processing", "Jenkinsfile", "scripts/jenkins/jenkins-permitted-sha-scope.txt"),
                                     ("options-edge-web-deploy", "options-edge", "Jenkinsfile", "scripts/jenkins/jenkins-permitted-sha-scope.txt")]:
        w = world(); a = w.commit(repo=repo, script=script, manifest_path=mpath)
        w.serve_child(job_path=f"job/{job}", config=real_config(job))
        check(f"the live {job} configuration (copied from the controller) is accepted", w.run([job, a, "REQUIRED_IMAGE"]), True, "triggering is allowed")
        w.close()

    # a stalled network operation reaches a named refusal within the deadline
    w = world(); a = w.commit(); w.serve_child()
    w._stub("git", f"""#!/usr/bin/env bash
case "$*" in *ls-remote*) sleep 60 ;; esac
exec '{w.real_git}' "$@"
""")
    import time
    t0 = time.time()
    r = w.run(["service-deploy", a, "REQUIRED_IMAGE"], extra_env={"GUARDED_DOWNSTREAM_NET_DEADLINE": "2"})
    check("a stalled git ls-remote ends in a named refusal within the deadline", r, False, "could not read the tip")
    elapsed = time.time() - t0
    check(f"... within the deadline, not the stalled command's 60 s (took {elapsed:.0f} s)", subprocess.CompletedProcess([], 0 if elapsed < 40 else 1, "", ""), True, "")
    w.close()

    # forwarded SHA
    w = world(); a = w.commit(); b = w.commit(jenkinsfile=CHILD_JF.replace("k8s/x.yaml", "k8s/y.yaml")); w.serve_child()
    check("a forwarded SHA that is not main's tip is refused", w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, "is not the tip of abhinav-jain09/options-edge-deploy main")
    check("the tip is accepted", w.run(["service-deploy", b, "REQUIRED_IMAGE"]), True, "triggering is allowed")
    check("a short SHA is refused", w.run(["service-deploy", b[:12], "REQUIRED_IMAGE"]), False, "not a full 40-character commit id")
    check("an uppercase SHA is refused", w.run(["service-deploy", b.upper(), "REQUIRED_IMAGE"]), False, "not a lowercase commit id")
    check("a missing SHA is refused", w.run(["service-deploy"]), False, "not a lowercase commit id")
    w.close()

    # pre-check (registered parameters)
    w = world(); a = w.commit(); w.serve_child(params=params_json(version="0" * 64))
    check("pre-check: another registered guard version is refused", w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, "has not registered this guard version")
    w.close()
    w = world(); a = w.commit(); w.serve_child(params=params_json(extra=()))
    check("pre-check: a missing extra interface parameter is refused", w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, "REQUIRED_IMAGE is not a registered string parameter")
    w.close()
    w = world(); a = w.commit(); w.serve_child(params="<html>login</html>")
    check("pre-check: a login page instead of JSON is refused", w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, "not JSON")
    w.close()
    w = world(); a = w.commit()
    check("pre-check: an unreachable controller is refused", w.run(["service-deploy", a, "REQUIRED_IMAGE"]), False, "could not read the registered parameters")
    check("JENKINS_URL unset is refused", w.run(["service-deploy", a, "REQUIRED_IMAGE"], jenkins_url=None), False, "JENKINS_URL is not set")
    check("an unknown child is refused", w.run(["some-other-job", a]), False, "is not a downstream job this helper knows")
    w.close()

    # folder resolution (shared resolver)
    w = world(); a = w.commit(); w.serve_child(job_path="job/service-deploy")
    check("folder caller: the folder's child is inspected, not the root job of the same name",
          w.run(["service-deploy", a, "REQUIRED_IMAGE"], job_name="folder/caller"), False, "could not read the registered parameters of 'folder/service-deploy'")
    w.serve_child(job_path="job/folder/job/service-deploy")
    check("folder caller: the folder's child is accepted", w.run(["service-deploy", a, "REQUIRED_IMAGE"], job_name="folder/caller"), True, "'folder/service-deploy' loads")
    w.close()
    r = subprocess.run(["bash", JOBPATH, "child"], capture_output=True, text=True, env={"PATH": os.environ["PATH"], "JOB_NAME": "a/b/caller"})
    check("resolver: folder-relative full name and URL path", r, True, "full=a/b/child\npath=/job/a/job/b/job/child")
    r = subprocess.run(["bash", JOBPATH, "a/child"], capture_output=True, text=True, env={"PATH": os.environ["PATH"], "JOB_NAME": "caller"})
    check("resolver: a non-simple child name is refused", r, False, "must be a simple job name")
    r = subprocess.run(["bash", JOBPATH, "child"], capture_output=True, text=True, env={"PATH": os.environ["PATH"], "JOB_NAME": "my folder/caller"})
    check("resolver: a folder segment needing URL encoding is refused", r, False, "is not a plain name")

    # fetch fallbacks
    w = world(); a = w.commit(); w.serve_child()
    check("git unavailable and no gh: refused", w.run(["service-deploy", a, "REQUIRED_IMAGE"], git_ok=False), False, "could not read the tip")
    w._stub("gh", f"""#!/usr/bin/env bash
[ "$1" = api ] || exit 1
case "$2" in
  repos/abhinav-jain09/options-edge-deploy/commits/main) git -C '{w.bare}' rev-parse main ;;
  repos/abhinav-jain09/options-edge-deploy/contents/*)
    p="${{2#repos/abhinav-jain09/options-edge-deploy/contents/}}"; ref="${{p#*\\?ref=}}"; p="${{p%%\\?ref=*}}"
    git -C '{w.bare}' show "$ref:$p" | base64 ;;
  *) exit 1 ;;
esac
""")
    check("git unavailable, gh api fallback serves the same commit: accepted", w.run(["service-deploy", a, "REQUIRED_IMAGE"], git_ok=False), True, "triggering is allowed")
    w.close()

    print(f"require-guarded-downstream-test: {passed} passed, {failed} failed")
    if failed == 0 and passed >= 50:
        print("require-guarded-downstream-test: ALL PASS")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
