# `.74` — the experiment host

`.74` runs **unproven** services so they consume no prod or dev CPU/memory. See
`options-edge-documents/design/DEV-SERVICE-OFFLOAD-TO-74-REQUIREMENT.md` (§13–§15) and
`MAC74-SERVICE-ONBOARDING-RUNBOOK.md`.

Cluster: Docker Desktop, context **`mac74-desktop`**, namespace `options-edge`,
node UID `56a8ee22-3f7a-4e2b-a21a-74e3182801d2` (the fail-closed identity guard — do NOT use the
macOS hardware UUID, which identifies the host, not the cluster).

## Getting an image onto this cluster

`.74` cannot pull a locally built image, and this is not a configuration oversight:

- the storage driver is **overlay2**, not the containerd snapshotter, so the docker daemon's
  images are invisible to the kubelet — `imagePullPolicy: Never` gives `ErrImageNeverPull`;
- every pull is proxied through Docker Desktop's **`registry-mirror:1273`**, which returns **500**
  for a plain-HTTP registry — both `localhost:5001` and `192.168.100.74:5001`;
- the Docker Desktop settings file that would allow an insecure registry is **TCC-protected**, so
  it cannot be edited over SSH. It needs someone at the machine.

So images are imported straight into the node's containerd:

```bash
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
docker save <image>:mac74 -o /tmp/img.tar
kubectl apply -f ctr-import-pod.yaml        # privileged, hostPath / mounted at /hostfs
kubectl cp /tmp/img.tar default/ctr-import:/hostfs/tmp/img.tar
kubectl exec ctr-import -- /hostfs/usr/local/bin/ctr \
  --address /hostfs/run/containerd/containerd.sock -n k8s.io images import /hostfs/tmp/img.tar
kubectl delete pod ctr-import
```

`/Users` is **not** shared into the VM, so a hostPath to a Mac path is empty — the tarball has to
go in with `kubectl cp`. After the import the workload uses `imagePullPolicy: Never`.

## Three env settings that are not obvious

1. `KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092` — a pod's localhost is its own netns and
   `oe-kafka` is a host container.
2. `TOPIC_PREFIX=""` — it defaults to `dev.`, so without this the service subscribes to `dev.*`
   and sits in `UNKNOWN_TOPIC_OR_PARTITION` while reporting healthy. MM1 mirrors cannot rename
   topics, so names here must match the source exactly.
3. `SCHEMA_REGISTRY_URL=http://192.168.100.252:8082` — **prod's**. Mirrored Avro carries prod's
   schema IDs; `.74`'s own registry does not know them and fails to deserialise silently.
