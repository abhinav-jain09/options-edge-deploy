# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
#
# Promoted from CI-COLLECT-ONLY on 2026-11-08, concurrently by PR #1591 (the batch promotion) and by the
# commit that moved the Rithmic release out of Jenkinsfile.es4-deploy. The reason the second one had is
# specific and worth keeping: this module holds the ONLY checks on
# Jenkinsfile.rithmic-depth-bridge-release, which is classified `out` and therefore not rule-checked by
# scripts/jenkins/validate-jenkinsfile-guard.py — its guard's canonical form, its exact content (a sha256
# pin) and its classification. Left dormant, eleven rounds of review would have produced a protection
# that never executes.
import unittest,yaml
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
class RithmicEs4Service(unittest.TestCase):
    def test_mirrors_have_no_credentials_and_correct_dev_listener_alias(self):
        docs=list(yaml.safe_load_all((ROOT/'k8s/es4/services/rithmic-depth-bridge.yaml').read_text()))
        deployments={d['metadata']['name']:d for d in docs if d['kind']=='Deployment'}
        self.assertEqual(set(deployments),{'rithmic-depth-bridge','rithmic-depth-mirror-dev','rithmic-depth-mirror-production','rithmic-mbo-mirror-dev'})
        for name,d in deployments.items():
            pod=d['spec']['template']['spec'];self.assertFalse(pod['automountServiceAccountToken'])
            if name!='rithmic-depth-bridge':
                env=pod['containers'][0]['env'];self.assertFalse(any('secretKeyRef' in e.get('valueFrom',{}) for e in env))
                self.assertIn({'ip':'192.168.100.102','hostnames':['host.docker.internal']},pod['hostAliases'])
    def test_the_release_job_owns_the_rollout_and_es4_deploy_refuses_that_path(self):
        """2026-11-08: the release moved to its own job. Three things must hold together, or the
        collector is either released by an unguardable branch of es4-deploy (what this replaced) or
        rolled by es4-deploy's generic apply path at whatever image the registry already held."""
        release=(ROOT/'Jenkinsfile.rithmic-depth-bridge-release').read_text()
        job=(ROOT/'Jenkinsfile.es4-deploy').read_text()
        # 1. the release job owns the build, the pins and the rollout
        for needed in ('RITHMIC_PERMITTED_SHA','CONTRACTS_PERMITTED_SHA','RITHMIC_SDK_ZIP',
                       'scripts/deploy-kubernetes.py','assert-allowed-services.sh service rithmic-depth-bridge'):
            self.assertIn(needed,release,needed)
        # 2. es4-deploy still RECOGNISES the service and REFUSES it — never falls through to the
        #    generic manifest apply, which would publish no image
        self.assertIn("if (params.ACTION == 'deploy-service' && params.SERVICE == 'rithmic-depth-bridge')",job)
        branch=job[job.index("if (params.ACTION == 'deploy-service' && params.SERVICE == 'rithmic-depth-bridge')"):]
        branch=branch[:branch.index('} else {')]
        self.assertIn('error(',branch)
        self.assertIn('rithmic-depth-bridge-release',branch)
        self.assertNotIn('python" scripts/deploy-kubernetes.py',job)   # it no longer RUNS it
        self.assertEqual(job.count("stage('Deploy to es4')"),1)
        self.assertNotIn("stage('Deploy Rithmic service on ES4')",job)
        self.assertIn('es-feed|rithmic-depth-bridge|spread-skew',job)   # deploy-all still excludes it
        # 3. the registry names the job that actually releases it
        registry=yaml.safe_load((ROOT/'services.yaml').read_text())
        service=next(s for s in registry['services'] if s['name']=='rithmic-depth-bridge')
        self.assertEqual(service['buildJob'],'rithmic-depth-bridge-release');self.assertEqual(service['envs'],['es4'])

    def test_the_release_job_is_classified_out_of_the_guard_scope(self):
        """`out` is the honest statement that the collector's own build is not guard-bound, and it is
        what makes the job non-assistant-triggerable. A silent flip to `in` would claim a protection
        the validator itself refuses to grant (it cannot read a script inside a cloned repository)."""
        scope=(ROOT/'scripts/ci/jenkins-permitted-sha-scope.txt').read_text()
        row=next(l for l in scope.split(chr(10)) if l.startswith('Jenkinsfile.rithmic-depth-bridge-release'))
        self.assertEqual(row.split('|')[1].strip(),'out',row)
        # ...and what it DOES bind is still there: this repository's tree, before any effect
        release=(ROOT/'Jenkinsfile.rithmic-depth-bridge-release').read_text()
        self.assertIn("stage('Permitted commit guard')",release)
        # compared at the COMMANDS, not at the header prose that also names them
        self.assertLess(release.index('bash scripts/jenkins/permitted-sha-guard.sh'),
                        release.index('python" scripts/deploy-kubernetes.py'))
        self.assertLess(release.index('bash scripts/jenkins/verify-permitted-tree.sh'),
                        release.index("git branch: 'main', url:"))

    def test_the_out_jobs_guard_is_pinned_to_the_guarded_jobs_canonical_form(self):
        """An `out` file is NOT rule-checked by scripts/jenkins/validate-jenkinsfile-guard.py - that is what
        `out` means. So the guard this one chooses to run over THIS repository's tree is RULE-CHECKED by no
        validator, and a guard no validator judges is free to rot into a shape that no longer refuses while
        the header still says "the canonical guard" - a false safety claim, worse than no claim. This test
        and test_the_out_job_is_pinned_whole are what check it instead; this one is the half that reads the
        guard's FORM.

        The WHOLE guard stage is compared, as one brace-balanced slice, with the same slice of the
        rule-checked Jenkinsfile.es4-deploy, whitespace-normalised and otherwise character for character.
        An earlier version listed pieces instead and Codex showed the list was a lie: it required
        `if (rc != 0) {` but never the error() inside it, so replacing the error() with anything non-failing
        passed, PERMITTED_SHA_GUARD was set to PASSED anyway, and the gated release proceeded. A test that
        names a property it does not check is worse than no test, so the slice is compared whole."""
        import re
        release=(ROOT/'Jenkinsfile.rithmic-depth-bridge-release').read_text()
        guarded=(ROOT/'Jenkinsfile.es4-deploy').read_text()

        def norm(s):
            return re.sub(r'\s+',' ',s).strip()

        def guard_stage(text):
            # brace-balanced from the stage's own opening brace, not a fixed count of closing braces: the
            # slice must not shrink silently if anything after the flag assignment changes (Codex, round 4).
            a=text.index("stage('Permitted commit guard')")
            o=text.index('{',a); d=0; i=o
            while i<len(text):
                if text[i]=='{': d+=1
                elif text[i]=='}':
                    d-=1
                    if d==0: break
                i+=1
            self.assertEqual(d,0,'unbalanced guard stage')
            slice_=text[a:i+1]
            self.assertIn("env.PERMITTED_SHA_GUARD = 'PASSED'",slice_,'the slice did not reach the flag')
            return norm(slice_)

        self.assertEqual(guard_stage(release),guard_stage(guarded),
                         'the guard stage of the out-of-scope release job has drifted from the rule-checked '
                         'one in Jenkinsfile.es4-deploy. No VALIDATOR judges this file - it is classified '
                         '`out` - so this comparison is what notices that its guard is no longer the '
                         'canonical one; the digest in test_the_out_job_is_pinned_whole cannot, because a '
                         'drift here can happen entirely in the OTHER file.')
        # the slice really does carry the refusal and the deadline, so the equality above is worth something
        for piece in ("options { timeout(time: 10, unit: 'MINUTES') }",
                      "def rc = sh(returnStatus: true, script: 'bash scripts/jenkins/permitted-sha-guard.sh')",
                      'if (rc != 0) {', 'error("Permitted commit guard REFUSED this build'):
            self.assertIn(norm(piece),guard_stage(release),piece)
        self.assertLess(guard_stage(release).index('error("Permitted commit guard REFUSED'),
                        guard_stage(release).index("env.PERMITTED_SHA_GUARD = 'PASSED'"))
        self.assertNotIn('catchError',release)   # a refusal is a stop, not a coloured result

        # NO post{} ANYWHERE in this job. Codex, round 4: a pipeline-level `post { failure { sh 'kubectl …' } }`
        # would run AFTER a guard refusal, sits outside the compared slice, and would pass every assertion
        # above. For an `in` file rule 8 gates post{} tokens on the guard flag; no validator rule does that
        # for an `out` file, and
        # Declarative post conditions cannot express it reliably anyway. So the shape is refused outright:
        # this job has no post{} today and may not acquire one without revisiting this test.
        # post{} and every other top-level directive are covered by test_the_out_job_is_pinned_whole,
        # which pins the file's sha256. Two literal assertions (`post {` and `post{`) stood here and were
        # removed: Codex evaded them with `post` and its brace on the next line, which is why the cover
        # for those shapes is now the digest and not a list of spellings.

        def guard_version(text):
            return re.search(r"name: 'PERMITTED_SHA_GUARD_VERSION', defaultValue: '([0-9a-f]{64})'",text).group(1)
        # the declared guard hash: drift here is what the guard script itself refuses to run under
        self.assertEqual(guard_version(release),guard_version(guarded))

        # the guard is the FIRST stage, and no stage after it runs without its flag
        stages=re.findall(r"stage\('([^']*)'\)",release)
        self.assertEqual(stages[0],'Permitted commit guard',stages)
        for name in stages[1:]:
            gate=release[release.index("stage('"+name+"')"):]
            gate=gate[:gate.index('steps {')]
            self.assertIn("env.PERMITTED_SHA_GUARD == 'PASSED'",gate,name+' is not gated on the guard flag')

        # and the verify over this tree is the same command es4-deploy runs, inside the same deadline block.
        # Compared whitespace-normalised so a reformat of es4-deploy is not read as drift (Codex, round 4).
        verify_line=norm(next(l for l in release.split(chr(10)) if 'verify-permitted-tree.sh --dir .' in l))
        self.assertIn(verify_line,norm(guarded),
                      'es4-deploy no longer runs this exact verify command, so there is nothing left to '
                      'compare against: either it was reformatted (re-establish the pair) or it changed '
                      'meaning (decide which form is canonical, then update both)')
        self.assertIn(norm("timeout(time: 10, unit: 'MINUTES') {")+' '+verify_line,norm(release))
    def test_the_out_job_is_pinned_whole(self):
        """The ONE accepted form for a file no VALIDATOR checks: its exact content.

        `Jenkinsfile.rithmic-depth-bridge-release` is classified `out`, so
        scripts/jenkins/validate-jenkinsfile-guard.py does not rule-check it AT ALL. Six Codex rounds
        were spent trying to assert the properties that matter piece by piece, and each round found the
        next shape the previous assertions missed: the error() inside `if (rc != 0)`, a pipeline-level
        `post {}`, `post` with its brace on the next line, `triggers { cron(...) }`, a PERMITTED_SHA
        default changed from '' to a real SHA, `agent { docker ... }`, `cron ('...')` with a space,
        `stage("...")` in double quotes before the guard, and then options/environment/parameters/stages
        left open. That sequence is the signature of a wrong mechanism, not of a nearly-finished one:
        a blacklist of spellings loses to the spelling nobody listed, and the fix for it is never
        another spelling.

        So the whole file is pinned. Any change to it - any directive, any indentation, any quoting,
        anywhere - fails this test, and the author has to update the digest here deliberately and
        re-read what they changed. That is the correct cost for a definition that deploys the ES4
        collector and that no validator judges. It is the same pattern as the deliberately-pinned
        KAFKA_APPLICATION_ID_SUFFIX set in tests/test_service_identity_unversioned.py: where deriving
        the expectation would delete the tripwire, the expectation is pinned instead.

        Two things this pin CANNOT see, and each has its own test above: it cannot tell whether the
        pinned content is still CANONICAL, because the guard form lives in Jenkinsfile.es4-deploy and
        can change there while this file stays byte-identical
        (test_the_out_jobs_guard_is_pinned_to_the_guarded_jobs_canonical_form), and it cannot tell
        whether the classification is still `out`
        (test_the_release_job_is_classified_out_of_the_guard_scope).

        DIGEST HISTORY, so the next author sees what each change was rather than a bare new hash:
        853c3e41 -> d27af5d5 (2026-11-09): ONE added `//` comment line in the header,
        `// NOT-A-MIRROR: ...`, so scripts/ci/schema-mirrors-generate.py stops demanding a
        MIRROR-TRANSPORT marker from a job that moves no records. Re-read against this test's own
        list: no new top-level directive, no agent change, no non-empty PERMITTED_SHA default, no
        stage before the guard, no change below `pipeline {` at all."""
        import hashlib
        actual=hashlib.sha256((ROOT/'Jenkinsfile.rithmic-depth-bridge-release').read_bytes()).hexdigest()
        self.assertEqual(actual,'d27af5d5283c324d6adfb88d3c9434cbcc6d5c3e9a1f91b5286888dbde925235',
                         'Jenkinsfile.rithmic-depth-bridge-release changed. It is classified `out`, so the '
                         'guard validator does not check it. The two tests above notice SOME changes - a '
                         'guard stage that drifts from the canonical one, a flipped classification - but '
                         'this digest is what notices the rest. '
                         'Re-read the diff: a new top-level directive (post, triggers, tools), a different '
                         'agent, a non-empty PERMITTED_SHA default, or a stage before the guard all make '
                         'this job deploy under an unproven commit. If the change is intended, update the '
                         'digest here in the same commit and say in the message what you checked.')

    def test_reset_restores_schema_before_source_or_mirrors(self):
        reset=(ROOT/'scripts/es4/cleanup-es4.sh').read_text()
        registration=reset.index('schema-id-maps/rithmic-depth.map')
        self.assertLess(registration,reset.index('restoring es4 Deployments to captured replica counts'))
        job=(ROOT/'Jenkinsfile.es4-deploy').read_text()
        self.assertIn('contracts/schemas/rithmic/',job)
        self.assertIn('es.rithmic.depth-value|3301|RithmicDepthFrame.avsc|1',(ROOT/'scripts/kafka/schema-id-maps/rithmic-depth.map').read_text())
    def test_exec_permission_is_provisioned_before_workload_effects(self):
        stage=(ROOT/'Jenkinsfile.rithmic-depth-bridge-release').read_text()
        self.assertLess(stage.index('auth can-i create pods/exec'),stage.index('python" scripts/deploy-kubernetes.py'))
        role=list(yaml.safe_load_all((ROOT/'k8s/es4/security/jenkins-deployer-rbac.yaml').read_text()))
        self.assertTrue(any('pods/exec' in rule.get('resources',[]) and 'create' in rule['verbs'] for d in role for rule in d.get('rules',[])))

    def test_dev_clean_keeps_the_externally_mirrored_raw_topic(self):
        import subprocess
        value=subprocess.check_output(['bash','-c','source scripts/kafka/topics.env; printf "%s" "$OPTIONS_EDGE_DEV_CLEAN_PRESERVED_TOPICS"'],cwd=ROOT,text=True)
        self.assertIn('es.rithmic.depth',value.split())

    def test_unicode_virtualenv_outside_checkout_preserves_strict_source_guard(self):
        import tempfile,subprocess,os
        with tempfile.TemporaryDirectory() as temporary:
            parent=Path(temporary);repo=parent/'checkout';repo.mkdir()
            def git(*args):return subprocess.check_output(['git',*args],cwd=repo,text=True).strip()
            git('init','-q','--initial-branch=main');git('config','core.quotePath','true')
            (repo/'.gitignore').write_text('.jenkins-tmp/\n');(repo/'source.txt').write_text('reviewed\n')
            git('add','.');git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','fixture')
            revision=git('rev-parse','HEAD');venv=repo/'.jenkins-tmp/rithmic-venv';(venv/'bin').mkdir(parents=True)
            (venv/'bin/𝜋thon').write_text('generated test tool\n')
            def guard():return subprocess.run(['bash',str(ROOT/'scripts/jenkins/verify-permitted-tree.sh'),'--dir',str(repo),'--allow-ignored','.jenkins-tmp'],env={**os.environ,'PERMITTED_SHA':revision},capture_output=True,text=True)
            rejected=guard();self.assertNotEqual(rejected.returncode,0);self.assertIn('REFUSED',rejected.stdout+rejected.stderr)
            venv.rename(parent/'jenkins-test-venv')
            accepted=guard();self.assertEqual(accepted.returncode,0,accepted.stdout+accepted.stderr)
            (repo/'source.txt').write_text('unreviewed\n')
            self.assertNotEqual(guard().returncode,0)
