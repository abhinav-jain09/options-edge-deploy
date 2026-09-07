No merge-blocking findings.

- The evidence account is accurate. `G1 heap ... used 142339K` is the JVM’s current used Java heap—the quantity represented by `jvm_memory_bytes_used{area="heap"}`—but only a point sample, as the commit explicitly states. Metaspace is correctly reported separately.
- Arithmetic checks out:
  - `142339 KiB = 139.003 MiB`
  - `61241 KiB = 59.806 MiB`
  - `1536 − 139 = 1397 MiB`
  - `2560 − 638 = 1922 MiB`
  - `2560 − 1536 = 1024 MiB`
  - The heap would need to reach `1228 MiB`, approximately `8.8×` the observed `139 MiB`, to consume the required 308 MiB headroom.
- The `memory.peak` inference is valid: working set is pointwise no greater than `memory.current`, whose lifetime high-water is `memory.peak`. The commit does not misrepresent this as the prescribed full-session series.
- The missing full-session `H_peak` and `W_peak` measurements remain explicitly disclosed; no sentence claims G-R8’s contingency was satisfied.
- Startup preflight coverage at [es-feed-gateway.yaml:114](/private/tmp/oe-deploy-mirror/k8s/es4/services/es-feed-gateway.yaml:114) matches the implementation. With the flag enabled, the only footprint-specific refusal paths are:
  - any unreadable or non-true `UseCompressedOops`, `UseCompressedClassPointers`, or `CompactStrings`;
  - an existing topic above `1048588`;
  - an existing topic whose compression is outside `producer`/`uncompressed`;
  - an AdminClient failure other than unknown-topic.
  
  Equality at `1048588` passes because the implementation rejects only `> ceiling`. Absent topics are deliberately deferred rather than fatal.
- The ceiling pins are correct at [es-feed-gateway.yaml:136](/private/tmp/oe-deploy-mirror/k8s/es4/services/es-feed-gateway.yaml:136) and [es-cvd.yaml:137](/private/tmp/oe-deploy-mirror/k8s/es4/services/es-cvd.yaml:137). Both variable names are consumed by their respective implementations, and both defaults are exactly `262144`.
- All four topic variables and values at [es-feed-gateway.yaml:123](/private/tmp/oe-deploy-mirror/k8s/es4/services/es-feed-gateway.yaml:123) match G-R2. `GatewaySettings.value()` applies `TOPIC_PREFIX=es.` to `_TOPIC` settings and avoids double-prefixing, yielding the intended four `es.futures.footprint*` names.
- First-restart hydration is consistent with the deployed topic policy: bars and outcomes use unlimited retention on es4, and the gateway’s cache consumer seeks back 24 hours before filling the bounded views; live/evidence consumers deliberately start at END.
- `git diff --check` passes.

VERDICT: APPROVE
