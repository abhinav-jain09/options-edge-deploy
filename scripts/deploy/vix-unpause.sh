#!/usr/bin/env bash
# Guaranteed un-pause for the vix-option-inteligence reconcile stage.
#
# The pause in Jenkinsfile.service-deploy is the only step in that pipeline that can leave a
# service DOWN if a later step fails: if the prune or the deploy dies after scale-down, nothing
# else would ever scale it back and the service stays at zero replicas indefinitely. This runs
# from post{always}. On success it is a no-op -- a successful Deploy has already set replicas from
# the manifest, so the marker is simply cleared.
#
# It lives in a file rather than inline in the Jenkinsfile for two reasons: the repo already keeps
# its shell in scripts/, and an inline durable-task script cannot be executed byte-for-byte the way
# Jenkins executes it, which left a syntax error unreproducible across two builds (#680, #681).
#
# Deliberately NOT 'set -e': every kubectl here is expected to be able to fail, and reacting to
# that is the entire point. An earlier inline version paired 'set +e' with 'set -e', which switched
# -e on and killed the script before the restore branch could run -- the guarantee was silently
# absent.
set -uo pipefail

MARK="${1:?usage: vix-unpause.sh <marker-file>}"
DEP=deployment/vix-option-inteligence-service
NS=options-edge

[ -f "$MARK" ] || exit 0

# Validate the marker BEFORE touching the deployment. A corrupt, unreadable or half-written file
# must never become the argument to 'kubectl scale --replicas=' -- that is the unknown-as-a-value
# mistake at the one point that is supposed to be the safety net. The marker is KEPT on failure:
# it is the only record that a restore is still owed.
PREV="$(cat "$MARK" 2>/dev/null)"
cat_rc=$?
case "$PREV" in
  ''|*[!0-9]*) cat_rc=1 ;;
esac
if [ "$cat_rc" -ne 0 ]; then
  echo "FATAL: recovery marker $MARK is unreadable or holds an unusable value [$PREV]." >&2
  echo "$DEP may be paused at zero replicas. Resolve it by hand -- the correct replica count cannot be guessed. Marker kept." >&2
  exit 1
fi

# Structural absence test: a real failure whose message merely contains 'NotFound' must not be read
# as "the workload is gone, nothing to restore", which would delete the marker and leave the
# service down for good. '--ignore-not-found -o name' exits 0 with empty stdout when absent, 0 with
# a name when present, non-zero for any real failure.
exists="$(kubectl -n "$NS" get "$DEP" --ignore-not-found -o name 2>/dev/null)"
exists_rc=$?
if [ "$exists_rc" -eq 0 ] && [ -z "$exists" ]; then
  echo "$DEP no longer exists; nothing to restore."
  rm -f "$MARK"
  exit 0
fi

# stderr captured separately and the value must be digits. Merging stderr would let a successful
# "0" plus a warning read as non-zero, fall through to "no restore needed", and delete the marker.
ERRF="$(mktemp)"
NOW="$(kubectl -n "$NS" get "$DEP" -o jsonpath='{.spec.replicas}' 2>"$ERRF")"
read_rc=$?
NOW_ERR="$(cat "$ERRF")"
rm -f "$ERRF"
case "$NOW" in
  ''|*[!0-9]*) read_rc=1 ;;
esac

if [ "$read_rc" -ne 0 ]; then
  echo "WARNING: could not read $DEP replicas [$NOW] stderr: $NOW_ERR - attempting restore to $PREV regardless." >&2
  if kubectl -n "$NS" scale "$DEP" --replicas="$PREV"; then
    echo "Restored $DEP to $PREV despite the unreadable state."
    rm -f "$MARK"
    exit 0
  fi
  echo "FATAL: $DEP state unreadable AND restore failed -- the service may be DOWN at zero replicas. Marker kept at $MARK for the next run." >&2
  exit 1
fi

if [ "$NOW" = "0" ]; then
  echo "Deploy did not restore $DEP; scaling back to $PREV so the pause cannot strand it."
  if ! kubectl -n "$NS" scale "$DEP" --replicas="$PREV"; then
    echo "FATAL: could not restore $DEP to $PREV replicas -- it is STILL DOWN and needs manual attention. Marker kept at $MARK." >&2
    exit 1
  fi
else
  echo "$DEP is at $NOW replicas; no restore needed."
fi
rm -f "$MARK"
