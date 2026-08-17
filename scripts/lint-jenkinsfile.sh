#!/usr/bin/env bash
# Parse a Jenkinsfile's GROOVY, not just the shell inside it.
#
# WHY THIS EXISTS. Jenkinsfile.bleedingoptions-secrets once shipped with `Postgres\'` inside a
# single-quoted Groovy string. The backslash escaped the backslash, not the quote, so the string
# ended early and the file never compiled — every build failed at load, before a single step ran.
# It was validated at the time with `bash -n` on the embedded shell, which passed: bash was checking
# the shell text and never parsed the Groovy wrapping it. Shell validation cannot see this class of
# defect, so it needs its own check.
#
# WHAT IT DOES. Builds the AST and throws it away. Nothing executes, so it is safe to run on a
# Jenkinsfile that deploys to production.
#
# WHAT IT IS NOT. This is not Jenkins' own parser, and it does not know the declarative schema. It
# runs whatever Groovy 4 is in ~/.m2, which is not necessarily the controller's Groovy version, so
# it can accept syntax the controller would reject — a false NEGATIVE, the same class of miss it
# exists to reduce. It also cannot catch a well-formed pipeline that is structurally invalid (an
# unknown directive, a step outside `steps`, a bad `when`). Treat a pass as "the file lexes and
# parses as Groovy", nothing more.
#
# The definitive check is the controller's own linter, which validates against the real schema and
# the real Groovy:
#   curl -X POST -F "jenkinsfile=<Jenkinsfile.x" $JENKINS/pipeline-model-converter/validate
# It needs authentication, which is why it is not the default here. Use it in CI, and use this
# locally for the fast pass that needs nothing but a jar.
#
# The @Library annotation is stripped first: it resolves only inside Jenkins, and leaving it in
# fails with "unable to resolve class Library" — a CLASS RESOLUTION error, not a syntax one, which
# would mask the syntax result this script exists to report. Nothing that string escaping can break
# is removed with it.
#
# Usage:  scripts/lint-jenkinsfile.sh [Jenkinsfile ...]      (default: all Jenkinsfile* in the repo)
# Needs:  java, and a Groovy 4 jar in ~/.m2. Groovy 3 cannot read Java 21 class files
#         ("Unsupported class file major version 65"); fetch one with:
#           mvn dependency:get -Dartifact=org.apache.groovy:groovy:4.0.22
set -euo pipefail

GROOVY_JAR="${GROOVY_JAR:-$(find "$HOME/.m2" -path '*/org/apache/groovy/groovy/*' -name 'groovy-4.*.jar' 2>/dev/null | sort -V | tail -1)}"
[ -n "$GROOVY_JAR" ] || {
  echo "FATAL: no Groovy 4 jar found. Fetch one:" >&2
  echo "  mvn dependency:get -Dartifact=org.apache.groovy:groovy:4.0.22" >&2
  exit 1; }

# groovy-json too. In Groovy 4 `groovy.json.JsonOutput` lives in a SEPARATE jar, so without it two
# Jenkinsfiles that merely import it were reported as parse failures — a false alarm on files that
# are perfectly valid. A lint that fails on correct input teaches people to ignore it, which costs
# more than the check is worth. Optional: if it is absent the core jar still parses everything else.
GROOVY_JSON_JAR="${GROOVY_JSON_JAR:-$(find "$HOME/.m2" -name 'groovy-json-4.*.jar' 2>/dev/null | sort -V | tail -1)}"
CP="$GROOVY_JAR${GROOVY_JSON_JAR:+:$GROOVY_JSON_JAR}"
command -v java >/dev/null || { echo "FATAL: java is required" >&2; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cat > "$WORK/ParseCheck.groovy" <<'GROOVY'
def f = new File(args[0])
try {
  new groovy.lang.GroovyShell().parse(f.text)
  println "  GROOVY PARSE OK"
} catch (Throwable t) {
  println "  GROOVY PARSE FAILED: ${t.class.simpleName}"
  println t.message.readLines().take(15).collect { "    $it" }.join('\n')
  System.exit(1)
}
GROOVY

files=("$@")
if [ "${#files[@]}" -eq 0 ]; then
  # shellcheck disable=SC2207
  files=($(git ls-files 'Jenkinsfile*' 2>/dev/null || ls Jenkinsfile* 2>/dev/null))
fi
[ "${#files[@]}" -gt 0 ] || { echo "no Jenkinsfiles to check"; exit 0; }

rc=0
for f in "${files[@]}"; do
  echo "=== $f ==="
  sed '1s|^@Library.*|// @Library stripped for offline parse|' "$f" > "$WORK/candidate.groovy"
  java -cp "$CP" groovy.ui.GroovyMain "$WORK/ParseCheck.groovy" "$WORK/candidate.groovy" || rc=1
done
exit $rc
