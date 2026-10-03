#!/usr/bin/env python3
"""THE QUIESCENCE PROOF of the 0DTE research migration (increment 9c; design "BUILD decisions for increment 9" Q3): is every legacy (v6)
research writer GONE from the namespace, so the migrator may be told --legacy-writers-quiesced?

The legacy writer lives inside the vix-option-inteligence SERVICE image and runs when ZERODTE_RESEARCH_ENABLED is (case-insensitively) "true"
— ResearchWriterSettings.fromEnvironment judges the variable exactly so. What is proven here, from the live API and a CLOSED WORLD of workloads:

  1. EVERY pod of the namespace whose container (app, init or ephemeral) runs that image — judged by its spec image AND by the kubelet's status
     imageID, never by a label alone — is a writer-capable pod. A pod is exempt only when it belongs to a Job (ownerReference) whose
     app.kubernetes.io/name is one of the declared maintenance Jobs that run the same image and never write research rows.
  2. A writer-capable pod that is TERMINATING (deletionTimestamp set) still runs through its grace period: NOT quiescent (transient — the
     wrapper waits for it to be gone, bounded). A pod that is Pending has unverified containers: transient too.
  3. Every container of such a pod that runs the image must be on the PINNED digest (status imageID ends with "@<digest>"; a bare
     "sha256:…" imageID equals the digest) — an init container or a sidecar on another build is a refusal, whatever its state.
  4. The EFFECTIVE flag of every such container must resolve OFF: a literal env value; a configMapKeyRef resolved by reading that ConfigMap;
     envFrom sources resolved in order (a later source overrides an earlier one, a direct env entry overrides every envFrom, as the kubelet
     does); a secretKeyRef, a fieldRef / resourceFieldRef, a "$(…)" expansion, a duplicated env name, a key found in an envFrom secret, an
     unreadable or missing non-optional source — each is a REFUSAL (the value cannot be known here, so it is not known to be off).
  5. The CONTROLLERS: every Deployment, StatefulSet, DaemonSet, ReplicaSet, Job and CronJob of the namespace whose pod template runs the
     image must be a DECLARED writer Deployment (or a ReplicaSet it owns), or an exempt maintenance Job; anything else — a CronJob, an
     undeclared Deployment — is a refusal (it could create a writer pod at any moment). A declared Deployment's template must run the pinned
     digest with the flag off, and its rollout must be SETTLED (observedGeneration == generation, updated == available == desired, nothing
     unavailable): a rollout in flight could replace a verified pod with one nobody judged.
  6. The pod SET is digested (uid, phase, every imageID, terminating) so the wrapper can re-list immediately before creating the Job and
     refuse if anything changed since the proof (--expect-set).

Exit codes (the wrapper maps each): 0 QUIESCENT, 3 NOT_QUIESCENT (a refusal that waiting cannot cure), 5 TRANSIENT (only terminating /
pending pods stand in the way), 4 UNREADABLE (the API could not be read — never treated as "nothing there"), 2 usage. Secrets are read as
KEY NAMES ONLY (a go-template listing keys); no value of any Secret reaches this process, its output or a log.
"""
import argparse
import hashlib
import json
import subprocess
import sys

FLAG = "ZERODTE_RESEARCH_ENABLED"


class Refusal(Exception):
    """A reason the proof cannot be given now; `transient` = waiting may cure it (terminating / pending pods)."""

    def __init__(self, reason, transient=False):
        super().__init__(reason)
        self.transient = transient


class Unreadable(Exception):
    pass


# ----------------------------------------------------------------------------------------------- the API, read-only
class Api:
    def __init__(self, namespace):
        self.ns = namespace
        self.cache = {}

    def _run(self, args, what):
        try:
            p = subprocess.run(["kubectl", "-n", self.ns] + args, capture_output=True, text=True)
        except OSError as e:
            raise Unreadable("%s: kubectl could not be run (%s)" % (what, e.__class__.__name__))
        if p.returncode != 0:
            if "NotFound" in p.stderr or "not found" in p.stderr:
                return None
            raise Unreadable("%s could not be read (kubectl exit %d)" % (what, p.returncode))
        return p.stdout

    def list(self, kind):
        out = self._run(["get", kind, "-o", "json"], "the %s list" % kind)
        if out is None:
            raise Unreadable("the %s list could not be read" % kind)
        try:
            items = json.loads(out).get("items")
        except ValueError:
            raise Unreadable("the %s list is not JSON" % kind)
        if not isinstance(items, list):
            raise Unreadable("the %s list carries no items" % kind)
        return items

    def configmap(self, name):
        key = ("configmap", name)
        if key not in self.cache:
            out = self._run(["get", "configmap", name, "-o", "json"], "ConfigMap %s" % name)
            self.cache[key] = None if out is None else json.loads(out)
        return self.cache[key]

    def secret_keys(self, name):
        """The KEY NAMES of a Secret, never its values (a go-template that prints keys only)."""
        key = ("secret", name)
        if key not in self.cache:
            out = self._run(["get", "secret", name, "-o", "go-template={{range $k, $_ := .data}}{{$k}}{{\"\\n\"}}{{end}}"], "Secret %s" % name)
            self.cache[key] = None if out is None else set(l for l in out.split("\n") if l)
        return self.cache[key]


# ----------------------------------------------------------------------------------------------- images
def repository_of(ref):
    """The repository name (the last path segment, without tag or digest) of an image reference; '' for a bare digest."""
    if ref.startswith("docker-pullable://"):
        ref = ref[len("docker-pullable://"):]
    if ref.startswith("sha256:"):
        return ""
    ref = ref.split("@", 1)[0]
    last = ref.rsplit("/", 1)[-1]
    return last.split(":", 1)[0]


def digest_of(image_id):
    if image_id.startswith("docker-pullable://"):
        image_id = image_id[len("docker-pullable://"):]
    if image_id.startswith("sha256:"):
        return image_id
    return image_id.split("@", 1)[1] if "@" in image_id else ""


# ----------------------------------------------------------------------------------------------- the effective flag
def effective_flag(container, api, where):
    """The value ZERODTE_RESEARCH_ENABLED has inside this container (None = unset), resolved as the kubelet resolves it; a Refusal when it cannot be known."""
    entries = [e for e in container.get("env") or [] if e.get("name") == FLAG]
    if len(entries) > 1:
        raise Refusal("%s names %s more than once in env — ambiguous" % (where, FLAG))
    if entries:
        e = entries[0]
        vf = e.get("valueFrom")
        if vf:
            ref = vf.get("configMapKeyRef")
            if ref is None or len(vf) != 1:
                raise Refusal("%s reads %s from a %s — the flag must be a literal or a ConfigMap key to be judged" % (where, FLAG, ", ".join(sorted(vf.keys()))))
            cm = api.configmap(ref["name"])
            if cm is None:
                if ref.get("optional") is True:
                    return None
                raise Refusal("%s reads %s from ConfigMap %s, which does not exist" % (where, FLAG, ref["name"]))
            if ref.get("key") in (cm.get("binaryData") or {}):
                raise Refusal("%s reads %s from a binaryData key of ConfigMap %s" % (where, FLAG, ref["name"]))
            data = cm.get("data") or {}
            if ref.get("key") not in data:
                if ref.get("optional") is True:
                    return None
                raise Refusal("%s reads %s from ConfigMap %s key %s, which is absent" % (where, FLAG, ref["name"], ref.get("key")))
            return data[ref["key"]]
        value = e.get("value", "")
        if "$(" in value:
            raise Refusal("%s sets %s to an expansion (%r) — not judged" % (where, FLAG, value))
        return value
    value = None
    for src in container.get("envFrom") or []:
        prefix = src.get("prefix") or ""
        if not FLAG.startswith(prefix):
            continue
        key = FLAG[len(prefix):]
        cm_ref, sec_ref = src.get("configMapRef"), src.get("secretRef")
        if cm_ref:
            cm = api.configmap(cm_ref["name"])
            if cm is None:
                if cm_ref.get("optional") is True:
                    continue
                raise Refusal("%s imports env from ConfigMap %s, which does not exist" % (where, cm_ref["name"]))
            if key in (cm.get("binaryData") or {}):
                raise Refusal("%s imports %s from a binaryData key of ConfigMap %s" % (where, FLAG, cm_ref["name"]))
            data = cm.get("data") or {}
            if key in data:
                value = data[key]
        elif sec_ref:
            keys = api.secret_keys(sec_ref["name"])
            if keys is None:
                if sec_ref.get("optional") is True:
                    continue
                raise Refusal("%s imports env from Secret %s, which does not exist" % (where, sec_ref["name"]))
            if key in keys:
                raise Refusal("%s would take %s from Secret %s — a flag that lives in a Secret cannot be judged here" % (where, FLAG, sec_ref["name"]))
        else:
            raise Refusal("%s has an envFrom source that is neither a ConfigMap nor a Secret" % where)
    return value


def flag_on(value):
    return value is not None and value.lower() == "true"


# ----------------------------------------------------------------------------------------------- pods
def pod_containers(spec):
    for kind in ("initContainers", "containers", "ephemeralContainers"):
        for c in spec.get(kind) or []:
            yield kind, c


def pod_statuses(status):
    for kind in ("initContainerStatuses", "containerStatuses", "ephemeralContainerStatuses"):
        for s in status.get(kind) or []:
            yield kind, s


def runs_repo(pod, repo):
    spec, status = pod.get("spec") or {}, pod.get("status") or {}
    if any(repository_of(c.get("image", "")) == repo for _, c in pod_containers(spec)):
        return True
    return any(repository_of(s.get("imageID", "")) == repo for _, s in pod_statuses(status))


def exempt(pod, exempt_labels):
    label = ((pod.get("metadata") or {}).get("labels") or {}).get("app.kubernetes.io/name")
    owners = (pod.get("metadata") or {}).get("ownerReferences") or []
    return label in exempt_labels and any(o.get("kind") == "Job" for o in owners)


def judge_pod(pod, repo, digest, api):
    meta, spec, status = pod.get("metadata") or {}, pod.get("spec") or {}, pod.get("status") or {}
    name = meta.get("name", "?")
    phase = status.get("phase")
    if meta.get("deletionTimestamp"):
        raise Refusal("pod %s is terminating (it may still be writing until it is gone)" % name, transient=True)
    if phase in ("Succeeded", "Failed"):
        return
    if phase != "Running":
        raise Refusal("pod %s is %s — its containers are not yet verifiable" % (name, phase or "of unknown phase"), transient=True)
    statuses = {(k.replace("Statuses", "s"), s.get("name")): s for k, s in pod_statuses(status)}
    for kind, c in pod_containers(spec):
        by_spec = repository_of(c.get("image", "")) == repo
        st = statuses.get((kind, c.get("name")))
        by_status = st is not None and repository_of(st.get("imageID", "")) == repo
        if not (by_spec or by_status):
            continue
        where = "pod %s container %s" % (name, c.get("name"))
        if st is None or not st.get("imageID"):
            raise Refusal("%s runs the service image but reports no imageID yet" % where, transient=True)
        if digest_of(st["imageID"]) != digest:
            raise Refusal("%s runs another build of the service image (%s)" % (where, st["imageID"]))
        if flag_on(effective_flag(c, api, where)):
            raise Refusal("%s has %s=true" % (where, FLAG))
    # a status entry for a container the spec does not list (impossible by the API, judged anyway: fail closed)
    for kind, s in pod_statuses(status):
        if repository_of(s.get("imageID", "")) == repo and (kind.replace("Statuses", "s"), s.get("name")) not in {(k, c.get("name")) for k, c in pod_containers(spec)}:
            raise Refusal("pod %s reports a container %s on the service image that its spec does not declare" % (name, s.get("name")))


def pod_set_digest(pods):
    lines = []
    for p in pods:
        meta, status = p.get("metadata") or {}, p.get("status") or {}
        lines.append("|".join([meta.get("uid", ""), status.get("phase", ""), ",".join(sorted(s.get("imageID", "") for _, s in pod_statuses(status))), "T" if meta.get("deletionTimestamp") else "-"]))
    return hashlib.sha256("\n".join(sorted(lines)).encode("utf-8")).hexdigest()


# ----------------------------------------------------------------------------------------------- controllers
def template_of(kind, obj):
    spec = obj.get("spec") or {}
    if kind == "cronjobs":
        return ((spec.get("jobTemplate") or {}).get("spec") or {}).get("template") or {}
    return spec.get("template") or {}


def template_runs_repo(template, repo):
    return any(repository_of(c.get("image", "")) == repo for _, c in pod_containers(template.get("spec") or {}))


def judge_controllers(api, repo, digest, writers, exempt_labels):
    deployments = api.list("deployments")
    declared_uids = {}
    for d in deployments:
        meta = d.get("metadata") or {}
        if not template_runs_repo(template_of("deployments", d), repo):
            continue
        if meta.get("name") not in writers:
            raise Refusal("Deployment %s runs the service image but is not a declared legacy-writer workload" % meta.get("name"))
        declared_uids[meta.get("uid")] = meta.get("name")
        judge_declared_deployment(d, repo, digest, api)
    for kind in ("statefulsets", "daemonsets"):
        for obj in api.list(kind):
            if template_runs_repo(template_of(kind, obj), repo):
                raise Refusal("%s %s runs the service image — not a declared workload" % (kind[:-1], (obj.get("metadata") or {}).get("name")))
    for rs in api.list("replicasets"):
        if not template_runs_repo(template_of("replicasets", rs), repo):
            continue
        owners = (rs.get("metadata") or {}).get("ownerReferences") or []
        if not any(o.get("kind") == "Deployment" and o.get("uid") in declared_uids for o in owners):
            raise Refusal("ReplicaSet %s runs the service image and is not owned by a declared Deployment" % (rs.get("metadata") or {}).get("name"))
    for job in api.list("jobs"):
        if not template_runs_repo(template_of("jobs", job), repo):
            continue
        meta = job.get("metadata") or {}
        if (meta.get("labels") or {}).get("app.kubernetes.io/name") not in exempt_labels:
            raise Refusal("Job %s runs the service image and is not an exempt maintenance Job" % meta.get("name"))
    for cj in api.list("cronjobs"):
        if template_runs_repo(template_of("cronjobs", cj), repo):
            raise Refusal("CronJob %s runs the service image — it could create a writer pod at any moment" % (cj.get("metadata") or {}).get("name"))
    return sorted(set(writers) - set(declared_uids.values()))


def judge_declared_deployment(d, repo, digest, api):
    meta, spec, status = d.get("metadata") or {}, d.get("spec") or {}, d.get("status") or {}
    name = meta.get("name")
    for _, c in pod_containers((template_of("deployments", d)).get("spec") or {}):
        if repository_of(c.get("image", "")) != repo:
            continue
        where = "Deployment %s template container %s" % (name, c.get("name"))
        if digest_of(c.get("image", "")) != digest:
            raise Refusal("%s is not pinned to the digest this Job runs (%s)" % (where, c.get("image")))
        if flag_on(effective_flag(c, api, where)):
            raise Refusal("%s has %s=true — the next pod would write" % (where, FLAG))
    desired = spec.get("replicas", 1)
    if status.get("observedGeneration") != meta.get("generation"):
        raise Refusal("Deployment %s has not observed its latest generation — a rollout is in flight" % name)
    if desired == 0:
        if status.get("replicas", 0) != 0:
            raise Refusal("Deployment %s is scaled to zero but still reports %s replicas" % (name, status.get("replicas")))
        return
    if status.get("updatedReplicas", 0) != desired or status.get("replicas", 0) != desired or status.get("availableReplicas", 0) != desired or status.get("unavailableReplicas", 0) != 0:
        raise Refusal("Deployment %s rollout is not settled (desired %s, updated %s, replicas %s, available %s)" % (name, desired, status.get("updatedReplicas", 0), status.get("replicas", 0), status.get("availableReplicas", 0)))


# ----------------------------------------------------------------------------------------------- main
def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--digest", required=True, help="sha256:<64 hex> — the digest the migration Job runs")
    ap.add_argument("--repository", required=True, help="the image repository name whose containers may carry the legacy writer")
    ap.add_argument("--writer-deployments", required=True, help="comma-separated names of the declared writer Deployments")
    ap.add_argument("--exempt-job-labels", default="", help="comma-separated app.kubernetes.io/name values of maintenance Jobs that run the image and never write")
    ap.add_argument("--expect-set", default="", help="the pod-set digest of the earlier proof; anything else is a refusal")
    a = ap.parse_args(argv)
    if not (a.digest.startswith("sha256:") and len(a.digest) == 71 and all(ch in "0123456789abcdef" for ch in a.digest[7:])):
        print("usage: --digest must be sha256:<64 lowercase hex>", file=sys.stderr)
        return 2
    writers = [w for w in a.writer_deployments.split(",") if w]
    exempt_labels = set(l for l in a.exempt_job_labels.split(",") if l)
    if not writers:
        print("usage: --writer-deployments names at least one Deployment", file=sys.stderr)
        return 2
    api = Api(a.namespace)
    try:
        pods = api.list("pods")
        writer_pods = [p for p in pods if runs_repo(p, a.repository) and not exempt(p, exempt_labels)]
        for p in writer_pods:
            judge_pod(p, a.repository, a.digest, api)
        absent = judge_controllers(api, a.repository, a.digest, writers, exempt_labels)
        pod_set = pod_set_digest(writer_pods)
        if a.expect_set and a.expect_set != pod_set:
            raise Refusal("the writer pod set changed since the proof (%s != %s) — judged again from the start" % (pod_set[:12], a.expect_set[:12]))
    except Refusal as r:
        print("%s: %s" % ("TRANSIENT" if r.transient else "NOT_QUIESCENT", r))
        return 5 if r.transient else 3
    except Unreadable as u:
        print("UNREADABLE: %s" % u)
        return 4
    print("QUIESCENT pods=%d writerPods=%d declaredAbsent=%s set=%s" % (len(pods), len(writer_pods), ",".join(absent) or "-", pod_set))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
