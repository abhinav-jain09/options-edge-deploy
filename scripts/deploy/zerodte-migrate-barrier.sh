#!/usr/bin/env bash
# THE DEPLOYMENT / MIGRATION MUTUAL EXCLUSION (increment 9c, Codex 9c r1 + r2): a rollout of the vix-option-inteligence service and the 0DTE
# research migration NEVER run at the same time. ONE primitive for both — the ConfigMap zerodte-research-migrate-lock in the options-edge
# namespace, created ATOMICALLY (a Kubernetes create of an existing name fails) and HELD through the whole effect:
#   * the migration (scripts/ops/zerodte-research-migrate.sh) creates it before it judges the pods and releases it only once its Job is gone or
#     terminal;
#   * a service deploy (scripts/deploy/service-deploy.sh) ACQUIRES it here before `kubectl apply` and RELEASES it after the rollout and the
#     health gate (zerodte_migrate_barrier_release, on EXIT) — so no deploy can be admitted between the migration's proof and its Job, and no
#     migration can start while a rollout is in flight. A check-then-act ("is the lock absent? then apply") was the round-1 shape and is a race.
# Fail-closed: an unreadable lock state, a lock held by anyone (a migration or an earlier deploy that died holding it — read its log, then
# `kubectl -n options-edge delete configmap zerodte-research-migrate-lock` by hand), or a failed create each refuse the deploy.
#
#   zerodte_migrate_barrier_acquire <namespace> <service> <holder> [<kind>] → 0 when the caller holds the lock (or the service is not covered);
#                                   kind = service-deploy (default) | writer-activate (Jenkinsfile.zerodte-writer-activate holds it the same way)
#   zerodte_migrate_barrier_release <namespace>                       → releases what THIS process acquired; a no-op otherwise
ZERODTE_MIGRATE_LOCK_NAME="zerodte-research-migrate-lock"
ZERODTE_MIGRATE_BARRIER_SERVICES="vix-option-inteligence zerodte-research-writer"
ZERODTE_MIGRATE_BARRIER_HELD=false

zerodte_migrate_barrier_acquire() { # acquire <namespace> <service> <holder> [<holder-kind>: service-deploy (default) | writer-activate]
  local ns="$1" service="$2" holder="$3" kind="${4:-service-deploy}" err holder_now
  case " $ZERODTE_MIGRATE_BARRIER_SERVICES " in *" $service "*) : ;; *) echo "zerodte migration barrier: not applicable to $service"; return 0 ;; esac
  [ -n "$holder" ] || { echo "FATAL: zerodte migration barrier: a holder id is required" >&2; return 1; }
  err="$(mktemp)"
  if printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: %s\n  namespace: %s\n  labels:\n    app.kubernetes.io/name: zerodte-research-migrate-lock\n    app.kubernetes.io/part-of: options-edge\n  annotations:\n    options-edge.io/holder: "%s"\n    options-edge.io/holder-kind: "%s"\ndata:\n  held: "true"\n' \
       "$ZERODTE_MIGRATE_LOCK_NAME" "$ns" "$holder" "$kind" | kubectl -n "$ns" create -f - >/dev/null 2>"$err"; then
    rm -f "$err"
    ZERODTE_MIGRATE_BARRIER_HELD=true
    echo "zerodte migration barrier: $ZERODTE_MIGRATE_LOCK_NAME ACQUIRED by $holder — no research migration can start until this deploy releases it"
    return 0
  fi
  if grep -q "AlreadyExists" "$err"; then
    holder_now="$(kubectl -n "$ns" get "configmap/$ZERODTE_MIGRATE_LOCK_NAME" -o jsonpath='{.metadata.annotations.options-edge\.io/holder}' 2>/dev/null || echo '<unknown>')"
    rm -f "$err"
    echo "FATAL: $ZERODTE_MIGRATE_LOCK_NAME is held by ${holder_now:-<unknown>} — a research migration (or an earlier deploy that died holding it) excludes a rollout of $service now. Wait for that run to finish (it releases the lock), or read its log before removing a stale lock by hand." >&2
    return 1
  fi
  echo "FATAL: could not create $ZERODTE_MIGRATE_LOCK_NAME in $ns ($(tr '\n' ' ' < "$err")) — refusing to roll $service without holding the deployment / migration exclusion." >&2
  rm -f "$err"
  return 1
}

zerodte_migrate_barrier_release() {
  local ns="$1"
  [ "$ZERODTE_MIGRATE_BARRIER_HELD" = true ] || return 0
  if kubectl -n "$ns" delete "configmap/$ZERODTE_MIGRATE_LOCK_NAME" --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1; then
    ZERODTE_MIGRATE_BARRIER_HELD=false
    echo "zerodte migration barrier: $ZERODTE_MIGRATE_LOCK_NAME released"
  else
    echo "WARNING: could not release $ZERODTE_MIGRATE_LOCK_NAME (held by this deploy) — remove it by hand after reading this run's log; every migration and every deploy of this service refuses until then" >&2
  fi
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  case "${1:-}" in
    acquire) zerodte_migrate_barrier_acquire "${2:?namespace}" "${3:?service}" "${4-}" "${5:-service-deploy}" ;;
    release) ZERODTE_MIGRATE_BARRIER_HELD=true; zerodte_migrate_barrier_release "${2:?namespace}" ;;
    *) echo "usage: $0 acquire <namespace> <service> <holder> | release <namespace>" >&2; exit 2 ;;
  esac
fi
