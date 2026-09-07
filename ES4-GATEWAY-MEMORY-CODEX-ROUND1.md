## Findings

- **BLOCKER — [k8s/es4/services/es-feed-gateway.yaml:114](/private/tmp/oe-deploy-mirror/k8s/es4/services/es-feed-gateway.yaml:114)** — Enablement violates G‑R8’s deployment contingency. G‑R8 explicitly requires measurements “from a full ES session with CVD on and footprint off.” The recorded window is a configured market holiday and the manifest documents that the options feed was rejected/inactive that day. The large calculated margins do not waive this prerequisite. Measure through a regular full session, then enable; otherwise split enablement from this PR.

- **BLOCKER — [k8s/es4/services/es-feed-gateway.yaml:208](/private/tmp/oe-deploy-mirror/k8s/es4/services/es-feed-gateway.yaml:208)** — The PR does not record the metrics G‑R8 requires. The design names `jvm_memory_bytes_used{area="heap"}` for `H_peak` and `container_memory_working_set_bytes` for `W_peak`; the commit substitutes cgroup `memory.peak` for both. Bounding resident heap by total resident memory is directionally conservative on a non-swapping node, but it is not the prescribed heap observation and can fail if heap-used pages are swapped/nonresident. Moreover, `memory.peak` is total cgroup usage, not exactly Kubernetes working set. The unexplained `VmHWM=649 MiB > memory.peak=638 MiB` discrepancy further requires reconciliation. Collect the required series or add a reliable in-pod JVM measurement.

- **HIGH — [k8s/es4/services/es-feed-gateway.yaml:114](/private/tmp/oe-deploy-mirror/k8s/es4/services/es-feed-gateway.yaml:114)** — G‑R8 also says the deploy PR “sets both from one value” for `GATEWAY_ES_FOOTPRINT_MAX_RECORD_BYTES` and producer `FOOTPRINT_MAX_RECORD_BYTES`. Neither is explicitly set here or in the current es4 producer manifest; equality currently depends on independent defaults of `262144`. That is configuration drift exposure capable of invalidating the proven bound. Pin both explicitly to the same value before enablement.

- **MEDIUM — [k8s/es4/services/es-feed-gateway.yaml:217](/private/tmp/oe-deploy-mirror/k8s/es4/services/es-feed-gateway.yaml:217)** — The QoS explanation is wrong. The pod remains `Burstable`: memory request is below its limit, CPU has a request but no equal limit, and the init container has no matching requests/limits. Raising the request does improve memory eviction scoring because the observed baseline becomes lower than the request, but it does not change QoS class or categorically prevent being an eviction candidate. “Costs nothing” is also too strong.

## Verified

- The constants match G‑R8 exactly: footprint contribution ≤179.4 MiB rounded to **180 MiB**; requirements are **308 MiB**, **436 MiB**, and **512 MiB**.
- The displayed calculations are correct using the stated 638 MiB proxy:
  - `1536 − 638 = 898 MiB`
  - `2560 − 638 = 1922 MiB`
  - `2560 − 1536 = 1024 MiB`
- All four environment-variable names and values match G‑R2. `GatewaySettings.value()` applies `TOPIC_PREFIX` to every key ending `_TOPIC`; `TOPIC_PREFIX=es.` therefore resolves them to the correct four `es.futures.footprint*` topics.
- Raising the request by 256 MiB poses no evident scheduling-capacity problem given 15,628 MiB requested from roughly 63.6 GiB allocatable. Memory limits are not scheduling constraints. The limit increase does raise declared limit overcommit from roughly 107% to 109%; the supplied data do not establish actual node-pressure safety for other workloads.
- The manifest is legitimately hand-authored: `scripts/ci/validate-es4-render.sh` explicitly includes `es-feed-gateway` in `HAND_AUTHORED`.
- On restart with the flag enabled, the gateway intentionally fails closed for incompatible existing topic configuration or JVM layout. That is correct, but means deployment readiness depends on all existing footprint topics passing the compression/message-size preflight.

VERDICT: REQUEST_CHANGES
