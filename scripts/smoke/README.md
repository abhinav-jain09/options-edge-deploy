# Smoke Tests

`check-k8s-services.sh` verifies that extracted Kubernetes services rolled out and expose `/health/live`.
It checks raw-to-display, volume pace, directional pressure, volume sandwich, raw Postgres writer,
pressure Postgres writer, and the integration-test service. The final integration-test call verifies
that the UI/data path is receiving rows.

Run after deployment:

```bash
export KUBECONFIG=/home/options-edge/config/kubeconfig
scripts/smoke/check-k8s-services.sh
```
