# Stopped Short Premium container images

The existing CVD reader image job accepts `short-premium-agent` as a separate
Python service. Its two-agent workload is distinct from the native Agent A and
Agent B launchers. Only existing dev/prod deployments at replicas exactly zero
are accepted, even during server dry run. Nothing is started and no native
launcher, schedule, book, configuration or secret is read or changed.

The newest normal image build for the environment must have completed SUCCESS
and published its fixed-profile architecture without source/tag/registry
overrides or native deployment. Its immutable image lock must match current
Short Premium main, current contracts main and the exact SDK bytes used by the
newest successful private Linux qualification. A newer failed or running image
or qualification never falls back. The private gate's CPU, RSS, latency and
cadence evidence must retain its fixed coverage and 10 percent regression
limits. This is same-source qualification evidence, not proof that a stopped
workload is running or of real-market history when retained forecasts are empty.

One atomic JSON patch tests UID, resource version, replicas and both old
container identities/images before replacing the `agent-a` and `agent-b` images.
Whole-spec verification preserves environment, backend selection, volume and
book settings, resources and all other fields. Namespace, deployer and registry
checks reuse the existing image-only rollout. No readiness claim is made for
replicas zero. Source changes require fresh image and qualification evidence.

Fixture tests use actual Jenkins builds 12 and 13 and qualification 11 from
2026-10-18. They test provenance and the two-image CAS without cluster writes.
The normal image pipeline has separately completed both architecture builds and
its in-image conformance tests. Real server dry runs still gate each update.
