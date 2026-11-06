# Dev public-asset PVC scope for the existing infra job

Assigned to OP-P5-01, still IMPLEMENTING under [reconciliation PR1546](https://github.com/abhinav-jain09/options-edge/pull/1546). Storage manifests landed through PR1509, but common-infra dry run95 also showed two unrelated Databento carry-config additions. No apply was submitted.

The existing common-infra-deploy job will gain a default-false WEB_PUBLIC_ASSETS_PVC_ONLY option. Within the existing render/diff/apply stages, it selects exactly PersistentVolumeClaim/options-edge/web-immutable-assets. It is valid only for dev, excludes order-initialization/auth modes, preserves the existing create-only/class-mismatch gates, and performs no mutable apply, auth reconciliation, Kafka operation or workload rollout. Missing or duplicate target claims fail closed. The ordinary all-infra mode remains unchanged. No new Jenkins job or stage is added.

Required validation includes dry-run and apply command traces against simulated kubectl, existing-claim/class mismatch, missing-claim and invalid-mode cases, default full-mode behavior, Claude and Codex reviews, and actual staged existing-Jenkins execution. This document describes the assigned implementation scope; code, tests, review and rollout remain pending.
