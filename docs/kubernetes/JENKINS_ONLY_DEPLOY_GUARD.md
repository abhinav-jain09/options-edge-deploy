# Jenkins-Only Kubernetes Deploy Guard

## Purpose

The OptionsEdge Kubernetes deploy guard blocks direct workload changes in the
`options-edge` namespace unless the request is made by the Jenkins deployer
ServiceAccount:

```text
system:serviceaccount:options-edge:jenkins-deployer
```

This keeps Kubernetes deployments on the approved path:

```text
GitHub main -> Jenkins options-edge-deploy -> Kubernetes
```

## Admin Kubeconfig

The cluster-admin kubeconfig must be stored on the Jenkins host at:

```text
/var/jenkins_home/config/kubeconfig
```

This file is break-glass/admin-only. Normal Jenkins deploys must use:

```text
/var/jenkins_home/config/jenkins-deployer.kubeconfig
```

## Normal Jenkins Deploy Verification

After the guard is enabled, verify Jenkins deploy access with:

```bash
kubectl --kubeconfig /var/jenkins_home/config/jenkins-deployer.kubeconfig \
  -n options-edge auth can-i patch deployment
```

Expected output:

```text
yes
```

Then run the Jenkins deploy job from `main`:

```text
http://192.168.100.252:8085/job/options-edge-deploy/
```

Verify the build reaches the deploy stages and that rollout checks complete.

## Break-Glass: Temporarily Disable Namespace Enforcement

Use this only when Jenkins deploy access is broken and the cluster must be
recovered by an operator with the admin kubeconfig.

Remove the namespace label that binds the policy to `options-edge`:

```bash
kubectl --kubeconfig /var/jenkins_home/config/kubeconfig \
  label namespace options-edge options-edge/deploy-guard-
```

Verify the label is gone:

```bash
kubectl --kubeconfig /var/jenkins_home/config/kubeconfig \
  get namespace options-edge --show-labels
```

Re-enable namespace enforcement after recovery:

```bash
kubectl --kubeconfig /var/jenkins_home/config/kubeconfig \
  label namespace options-edge options-edge/deploy-guard=jenkins-only --overwrite
```

## Break-Glass: Disable The Policy Binding

Delete the policy binding to stop enforcement while leaving the policy object
available for later re-apply:

```bash
kubectl --kubeconfig /var/jenkins_home/config/kubeconfig \
  delete validatingadmissionpolicybinding options-edge-jenkins-only-workloads
```

Or change the binding to audit-only mode:

```bash
kubectl --kubeconfig /var/jenkins_home/config/kubeconfig \
  patch validatingadmissionpolicybinding options-edge-jenkins-only-workloads \
  --type merge \
  --patch '{"spec":{"validationActions":["Audit"]}}'
```

Restore deny enforcement:

```bash
kubectl --kubeconfig /var/jenkins_home/config/kubeconfig \
  apply -f k8s/security/jenkins-only-workload-admission.yaml
```

After restoring enforcement, verify Jenkins deploy access again:

```bash
kubectl --kubeconfig /var/jenkins_home/config/jenkins-deployer.kubeconfig \
  -n options-edge auth can-i patch deployment
```

Then run:

```text
http://192.168.100.252:8085/job/options-edge-deploy/
```

The Jenkins deploy is restored when the build reaches the Kubernetes deploy
stage and rollout checks complete without admission denials.

## Break-Glass: Recreate Jenkins Deployer Kubeconfig

If `/var/jenkins_home/config/jenkins-deployer.kubeconfig` is missing or
corrupt, recreate it from the Jenkins deployer token Secret:

```bash
cd /var/jenkins_home/workspace/options-edge-deploy

BREAK_GLASS_RECREATE_JENKINS_DEPLOYER_KUBECONFIG=true \
KUBECONFIG_ADMIN_FILE=/var/jenkins_home/config/kubeconfig \
KUBECONFIG_FILE=/var/jenkins_home/config/jenkins-deployer.kubeconfig \
bash scripts/jenkins/bootstrap-kubernetes-deploy-guard.sh
```

If the Jenkins workspace path differs, run the same command from the checked-out
`options-edge-deploy` workspace that contains `scripts/jenkins/bootstrap-kubernetes-deploy-guard.sh`.

Verify the recreated kubeconfig:

```bash
kubectl --kubeconfig /var/jenkins_home/config/jenkins-deployer.kubeconfig \
  -n options-edge auth can-i patch deployment
```

Expected output:

```text
yes
```

## Direct Mutation Denial Check

Confirm a non-Jenkins user cannot patch a Deployment:

```bash
kubectl --kubeconfig /var/jenkins_home/config/kubeconfig \
  -n options-edge patch deployment raw-to-display-service \
  --type merge \
  --patch '{"metadata":{"annotations":{"manual-deploy-test":"blocked"}}}'
```

Expected result:

```text
ValidatingAdmissionPolicy 'options-edge-jenkins-only-workloads' ... denied request
```

Confirm a non-Jenkins user cannot modify a Secret:

```bash
kubectl --kubeconfig /var/jenkins_home/config/kubeconfig \
  -n options-edge patch secret options-edge-secrets \
  --type merge \
  --patch '{"metadata":{"annotations":{"manual-secret-test":"blocked"}}}'
```

Expected result:

```text
ValidatingAdmissionPolicy 'options-edge-jenkins-only-workloads' ... denied request
```

## Controller Safety

The policy does not guard controller-created child resources such as Pods,
ReplicaSets, Endpoints, Events, PVCs, HPAs, or PDBs. Kubernetes controllers must
remain able to reconcile child resources after Jenkins changes a top-level
Deployment, StatefulSet, DaemonSet, Job, or CronJob.

## Validation Evidence

The guard was tested against the live k3s API in a temporary namespace named
`options-edge-admission-smoke`. The live `options-edge` namespace was not
labeled or protected during this test.

Observed results:

```text
admission-deny-readiness=ok
jenkins-deployment-patch=ok
controller-child-create=ok replicasets=2 pods=2
non-jenkins-deployment-deny=ok
non-jenkins-secret-deny=ok
break-glass-remove-namespace-label=ok
break-glass-delete-policy-binding=ok
```

The test confirmed:

- Jenkins ServiceAccount can patch a Deployment.
- The Deployment controller can create ReplicaSets and Pods.
- A normal non-Jenkins user is denied when patching a Deployment.
- A normal non-Jenkins user is denied when modifying a Secret.
- An admin using the break-glass kubeconfig can remove the namespace label.
- An admin using the break-glass kubeconfig can delete the policy binding.

After the test, the temporary namespace, policy, binding, ServiceAccount, token
Secret, RBAC, and temporary files were deleted.
