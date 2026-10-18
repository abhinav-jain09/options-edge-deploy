#!/usr/bin/env bash
# A clean-reset must register the schemas it wipes.
#
# WHY: ACTION=clean-reset wipes Kafka AND _schemas together (so no message can outlive its schema id).
# Several services require their schemas to be registered BEFORE they start and refuse to boot otherwise,
# so after a wipe they come up against an empty registry and crash-loop on a 40401 that nothing else
# fixes. Registration used to be a separate ACTION an operator had to remember; on 2026-10-18 a clean left
# context-tape-service dead on "Subject 'es.context-tape.direction.alert-value' not found" with all 18 es4
# subjects gone. The registration stage now also runs for clean-reset, and this gate keeps it that way:
# the failure is silent from the Jenkinsfile's point of view (the build goes green, a pod crash-loops), so
# nothing else would catch the condition coming back.
set -euo pipefail
cd "$(dirname "$0")/../.."

FILE=Jenkinsfile.es4-deploy
STAGE="Register existing CVD schemas"

[ -f "$FILE" ] || { echo "::error::$FILE is missing — this gate cannot check anything"; exit 1; }

# The stage's own `when` expression: the lines from the stage header to the first `steps {`. Reading only
# that window is what makes the check specific; "clean-reset" appears many times elsewhere in the file.
window=$(awk -v stage="stage('$STAGE')" '
  index($0, stage) { inside = 1 }
  inside { print }
  inside && /steps[[:space:]]*\{/ { exit }
' "$FILE")

[ -n "$window" ] || { echo "::error file=$FILE::no stage named \"$STAGE\" — a clean-reset would wipe the registry and register nothing"; exit 1; }

printf '%s' "$window" | grep -q "when" \
  || { echo "::error file=$FILE::the \"$STAGE\" stage has no when expression before its steps"; exit 1; }

printf '%s' "$window" | grep -q "params.ACTION == 'clean-reset'" || {
  echo "::error file=$FILE::the \"$STAGE\" stage does not run for ACTION=clean-reset. The wipe deletes _schemas, so every service that requires pre-registered schemas would crash-loop on a 40401 after the clean while the build still reports success."
  exit 1
}

# And it must still serve its own action, or the standalone registration an operator runs is gone.
printf '%s' "$window" | grep -q "params.ACTION == 'register-cvd-schemas'" || {
  echo "::error file=$FILE::the \"$STAGE\" stage no longer runs for ACTION=register-cvd-schemas, so the standalone registration is unreachable"
  exit 1
}

echo "clean-reset registers the schemas it wipes (\"$STAGE\" covers clean-reset and register-cvd-schemas)"
