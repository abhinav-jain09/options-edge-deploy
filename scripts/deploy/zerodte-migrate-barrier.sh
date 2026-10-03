#!/usr/bin/env bash
# THE DEPLOYMENT / MIGRATION EXCLUSION BARRIER (increment 9c, Codex 9c r1): a rollout of the vix-option-inteligence service must not
# start while the 0DTE research migration holds its lock, and the migration refuses to start while a rollout is in flight (the quiescence
# proof requires every declared Deployment settled). The lock is the ConfigMap zerodte-research-migrate-lock in the options-edge namespace,
# created atomically by scripts/ops/zerodte-research-migrate.sh before it judges the pods and released only once its Job is gone or terminal.
#
# Sourced-or-run by scripts/deploy/service-deploy.sh right before `kubectl apply` of the service's render; fail-closed: an unreadable lock
# state refuses the deploy as a held lock does (NotFound is the ONLY "free").
#
#   zerodte_migrate_barrier <namespace> <service>   → 0 when the deploy may proceed; prints FATAL and returns 1 otherwise
ZERODTE_MIGRATE_LOCK_NAME="zerodte-research-migrate-lock"
ZERODTE_MIGRATE_BARRIER_SERVICES="vix-option-inteligence"

zerodte_migrate_barrier() {
  local ns="$1" service="$2" err holder
  case " $ZERODTE_MIGRATE_BARRIER_SERVICES " in *" $service "*) : ;; *) echo "zerodte migration barrier: not applicable to $service"; return 0 ;; esac
  err="$(mktemp)"
  if kubectl -n "$ns" get "configmap/$ZERODTE_MIGRATE_LOCK_NAME" -o jsonpath='{.metadata.name}' >/dev/null 2>"$err"; then
    holder="$(kubectl -n "$ns" get "configmap/$ZERODTE_MIGRATE_LOCK_NAME" -o jsonpath='{.metadata.annotations.options-edge\.io/holder}' 2>/dev/null || echo '<unknown>')"
    rm -f "$err"
    echo "FATAL: the 0DTE research migration holds $ZERODTE_MIGRATE_LOCK_NAME (held by ${holder:-<unknown>}) — a rollout of $service now could replace a pod the migration judged quiescent with one nobody judged. Wait for the migration build to finish (it releases the lock), or read its log before removing a stale lock by hand." >&2
    return 1
  fi
  if grep -q "NotFound" "$err"; then
    rm -f "$err"
    echo "zerodte migration barrier: $ZERODTE_MIGRATE_LOCK_NAME is absent — no migration is running; $service may roll"
    return 0
  fi
  echo "FATAL: the state of $ZERODTE_MIGRATE_LOCK_NAME in $ns could not be read ($(tr '\n' ' ' < "$err")) — refusing to roll $service while a migration may be running." >&2
  rm -f "$err"
  return 1
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  zerodte_migrate_barrier "${1:?namespace}" "${2:?service}"
fi
