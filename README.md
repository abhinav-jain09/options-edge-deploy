# OptionsEdge Deploy

Kubernetes deployment repo for OptionsEdge services.

Owns:
- Kubernetes manifests
- environment overlays
- Kafka topic operations
- smoke-test scripts
- image tag manifests
- deployment Jenkins pipeline

This repo deploys applications after `options-edge-infra` has prepared Docker/Kubernetes on the remote server.
