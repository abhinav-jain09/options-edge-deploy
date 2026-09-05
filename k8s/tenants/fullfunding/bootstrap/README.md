# fullfunding tenant — bootstrap RBAC

**Applied once, by a cluster-admin, BEFORE the `fullfunding-deploy` Jenkins job can run.
Deliberately NOT part of `../kustomization.yaml`.**

## Why it cannot be part of the tenant deploy

The job runs as `system:serviceaccount:options-edge:jenkins-deployer`, which is scoped to
`options-edge`. Creating a namespace, and the cluster-scoped objects that bound it, is inherently
outside the credential being granted — the deploy cannot hand itself the rights it needs to run.
This is the platform/tenant split the Gate-1 document describes (NS-9), in its smallest form.

Discovered by running it: the first two builds of `fullfunding-deploy` failed here, cleanly and
before any mutation, with `Forbidden` — first on `namespaces`, then on `resourcequotas`.

## What is granted, and what deliberately is not

`01-bootstrap-rbac.yaml`
- Cluster-scoped rights **pinned by `resourceNames` to the tenant's own objects**
  (`fullfunding`, `fullfunding-low`, `fullfunding-tenant-rules`), so this can never touch
  options-edge's PriorityClasses or the existing jenkins-only admission policy. `create` is the one
  verb that cannot be name-restricted — the name is not known until the request is made.
- Namespaced rights via the built-in `admin` ClusterRole, bound by a **RoleBinding inside
  `fullfunding` only**. Never a ClusterRoleBinding: that would grant the same rights everywhere.

`02-policy-rbac.yaml`
- `ResourceQuota` and `LimitRange`, which the built-in `admin` role **deliberately excludes** —
  Kubernetes treats them as cluster policy rather than namespace administration. Without this the
  deploy cannot create the very budget that bounds the tenant.
- `bind` and `escalate` on roles/rolebindings. Kubernetes blocks creating a Role that grants
  permissions the creator does not hold, which is what stopped the tenant's own
  `fullfunding-deployer` Role from being created. These are real privileges, and they are bounded
  to a namespace where the deployer already holds `admin`, so they add no reach it does not
  effectively have there.

## Verified after applying

```
create resourcequotas in fullfunding   -> yes
create namespaces / priorityclasses    -> yes
create validatingadmissionpolicies     -> yes
create resourcequotas in options-edge  -> no      <-- the boundary still holds
```

## Applying

```bash
kubectl create namespace fullfunding          # the RoleBindings need it to exist first
kubectl apply -f k8s/tenants/fullfunding/bootstrap/
```

Then run `fullfunding-deploy` (dry-run first). The namespace being created here and again by the
job is intentional and harmless — `kubectl apply` is idempotent, and the job owns it from then on.
