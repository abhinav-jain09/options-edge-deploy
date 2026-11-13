# Replay Backend Plane

This Ansible package deploys a second OptionsEdge backend plane for historical
Databento replay. The replay plane runs next to the live plane in Kubernetes but
uses separate Kubernetes names, topics, Kafka Streams application IDs, and state
directories.

The intent is:

- live services keep handling market hours traffic
- replay services consume replay topics only
- the UI can point to the replay gateway when a user selects replay mode
- every release can deploy the same image tag to both live and replay planes

## What Gets Deployed

The playbook renders one manifest at `.ansible-rendered/replay-stack-<env>.yaml`.
It creates:

- `Namespace`: default `options-edge-replay`
- `ConfigMap`: `options-edge-replay-config`
- `Deployment`: one `*-replay` deployment per replay service
- `Service`: replay gateway and mission-control services

Default replay services:

- `feed-gateway-service-replay`
- `databento-volume-aggregator-replay`
- `raw-to-display-databento-service-replay`
- `volume-pace-databento-service-replay`
- `directional-pressure-databento-service-replay`
- `strike-flow-classifier-service-replay`
- `volume-sandwich-databento-service-replay`
- `databento-mission-pace-service-replay`
- `databento-mission-pressure-service-replay`
- `databento-mission-sandwich-service-replay`
- `spx-mission-control-service-replay`
- `hpsf-stage-a-service-replay`
- `hpsf-stage-b-service-replay`

Live-only services such as live IBKR ingestion, live Unusual Whales polling, and
Postgres writers are disabled by default for replay. Enable writers only with a
replay-only database.

## Safety Rules

The playbook fails before apply if:

- replay namespace equals live namespace unless `allow_same_namespace=true`
- replay topic prefix does not contain `replay`
- replay app-id suffix does not contain `replay`
- replay state directory does not contain `replay`
- replay gateway NodePort equals the live gateway NodePort
- production config points Kafka, Schema Registry, or Postgres at localhost
- production config uses a `dev.` topic prefix
- Postgres is enabled without a replay/dev/test JDBC URL

The playbook always renders and validates the manifest first. It only applies to
the cluster when `confirm_replay_deploy=true` is passed.

## Required Variables

Install Ansible on the Mac/Jenkins agent:

```bash
brew install ansible
```

Start from:

```bash
cp ansible/group_vars/replay_stack.example.yml /secure/path/replay-stack-dev.yml
```

Set at minimum:

- `deploy_environment`
- `app_profile`
- `live_namespace`
- `replay_namespace`
- `image_registry`
- `image_tag`
- `kafka_bootstrap_servers`
- `schema_registry_url`
- `replay_topic_prefix`
- `replay_app_id_suffix`
- `replay_gateway_node_port`

Registry expectations:

- dev/local Mac registry: `host.docker.internal:5001`
- production registry: `192.168.100.252:5000`

## Dry Run

Render and validate without applying:

```bash
ansible-playbook ansible/replay-stack.yml \
  -e @/secure/path/replay-stack-dev.yml \
  -e confirm_replay_deploy=false
```

This writes:

```text
.ansible-rendered/replay-stack-dev.yaml
```

## Deploy

Apply after validation:

```bash
ansible-playbook ansible/replay-stack.yml \
  -e @/secure/path/replay-stack-dev.yml \
  -e confirm_replay_deploy=true
```

Production example:

```bash
ansible-playbook ansible/replay-stack.yml \
  -e @/secure/path/replay-stack-prod.yml \
  -e deploy_environment=prod \
  -e app_profile=prod \
  -e image_registry=192.168.100.252:5000 \
  -e image_tag="$IMAGE_TAG" \
  -e replay_topic_prefix=prod.replay. \
  -e replay_app_id_suffix=-replay-prod \
  -e confirm_replay_deploy=true
```

## Jenkins Release Stage

After the normal live deploy stage succeeds, add a replay deploy stage:

```groovy
stage('Deploy replay backend plane') {
  steps {
    sh '''
      ansible-playbook ansible/replay-stack.yml \
        -e @/secure/path/replay-stack-${ENVIRONMENT}.yml \
        -e image_tag=${IMAGE_TAG} \
        -e confirm_replay_deploy=true
    '''
  }
}
```

This makes every release deploy the same build twice:

1. live plane through the existing Kubernetes manifests
2. replay plane through `ansible/replay-stack.yml`

## UI Routing

The UI should point to the live gateway for live mode and the replay gateway for
replay mode.

Default replay gateway:

```text
feed-gateway-service-replay.options-edge-replay.svc.cluster.local:8092
```

Default replay NodePort:

```text
30098
```

## Rollback

Rollback the replay plane independently:

```bash
kubectl -n options-edge-replay rollout undo deployment/feed-gateway-service-replay
kubectl -n options-edge-replay rollout undo deployment/raw-to-display-databento-service-replay
```

Remove the replay plane:

```bash
kubectl delete namespace options-edge-replay
```

Deleting the replay namespace does not delete the live namespace.
