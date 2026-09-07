MEDIUM — `k8s/es4/services/es-feed-gateway.yaml:205` — `VmHWM` bounds peak resident heap pages, but G-R8 defines `H_peak` as peak `jvm_memory_bytes_used{area="heap"}`. Used heap can include nonresident pages, so process RSS does not upper-bound that metric. Consequently `H_peak <= 649 MiB` and `Xmx - H_peak >= 887 MiB` at lines 206 and 211 are unsupported. The commit message repeats the same conflation. The `W_peak <= 638 MiB` derivation and its resulting `limit - W_peak >= 1922 MiB` inequality are valid and conservatively rounded.

VERDICT: REQUEST_CHANGES
