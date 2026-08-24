# bleedingoptions tenant — bootstrap RBAC

**Applied once, by a cluster-admin, BEFORE `common-infra-deploy` can create anything in
`bleedingoptions`. Deliberately NOT part of any kustomization.**

Same shape, and the same reason, as [`../../fullfunding/bootstrap/`](../../fullfunding/bootstrap/README.md).

## Why it cannot be part of the deploy

The job runs as `system:serviceaccount:options-edge:jenkins-deployer`, which is scoped to
`options-edge`. Creating a namespace, and the policy objects that bound it, is inherently outside the
credential being used — the deploy cannot hand itself the rights it needs to run.

Discovered by running it, exactly as fullfunding's was. `common-infra-deploy` build #35 failed
cleanly, before any mutation:

```
Error from server (Forbidden): namespaces "bleedingoptions" is forbidden:
User "system:serviceaccount:options-edge:jenkins-deployer" cannot get resource "namespaces"
in API group "" in the namespace "bleedingoptions"
```

## What is granted, and what deliberately is not

- Cluster-scoped rights are pinned by `resourceNames` to `bleedingoptions` itself, so this cannot
  touch options-edge's or fullfunding's namespaces. `create` is the one verb that cannot be
  name-restricted — the name is not known until the request is made.
- Namespaced rights come from the built-in `admin` ClusterRole bound by a **RoleBinding inside
  `bleedingoptions` only**. Never a ClusterRoleBinding: that would grant `admin` everywhere.
- `ResourceQuota` and `LimitRange` get their own Role, because the built-in `admin` role
  **excludes** them — Kubernetes treats them as cluster policy rather than namespace administration.
  `k8s/bleedingoptions/backup-and-quota.yaml` creates both, so without this the deploy cannot create
  the budget that bounds the tenant.

Unlike fullfunding, this tenant needs no PriorityClass or ValidatingAdmissionPolicy rights: it
declares neither. The grant is smaller on purpose — the deployer gets nothing it does not use.

## Applying

```bash
kubectl create namespace bleedingoptions      # the RoleBindings need it to exist first
kubectl apply -f k8s/tenants/bleedingoptions/bootstrap/
```

The namespace being created here and again by the job is intentional and harmless — `kubectl apply`
is idempotent, and `common-infra-deploy` owns it (and its labels, which the NetworkPolicies select
on) from then on.

Then run `scripts/ops/bleedingoptions-bootstrap.sh`, which does the rest.
