MEDIUM — `k8s/es4/services/es-feed-gateway.yaml:204-207` — “whichever counter is the better proxy: W >= 649 MiB” is unsupported. Neither `VmHWM` nor `memory.peak` is `container_memory_working_set_bytes`; choosing the larger proxy does not prove a lower bound on `W`. Consequently, `limit - W = 1911 MiB` and `Xmx - W = 887 MiB` are not established measurements—at most calculations using a conservative proxy. The resource change itself is valid with the relay off, but the remaining evidentiary overstatement violates the stated review bar.

VERDICT: REQUEST_CHANGES
