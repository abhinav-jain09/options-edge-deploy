# Jenkins Image Platform Policy

Jenkins runs on Mac ARM, but the remote production Kubernetes nodes run CentOS
amd64. Images deployed to production must therefore be published with a
`linux/amd64` manifest.

## Defaults

The main `options-edge-deploy` pipeline resolves `BUILD_PLATFORM` into
`EFFECTIVE_BUILD_PLATFORM` by environment:

- `dev`: `linux/arm64`
- `production`: `linux/amd64`

For production, Jenkins sets `EFFECTIVE_BUILD_PLATFORM` to `linux/amd64`; an
ARM platform request fails before any deploy step runs.

The HPSF replay gate defaults `BUILD_PLATFORM` to `linux/amd64` because replay
pods run on the remote Kubernetes nodes.

## Build Requirements

Jenkins image builds must use Docker Buildx:

```sh
docker buildx build --platform "$EFFECTIVE_BUILD_PLATFORM" -t "$IMAGE" --push <context>
```

Do not use plain `docker build` followed by `docker push` for images that will
run on remote Kubernetes. On Mac ARM, a plain build can publish an ARM image
that CentOS amd64 nodes cannot run.

## Manifest Validation

After pushing an image, validate the remote manifest with:

```sh
docker buildx imagetools inspect "$IMAGE"
```

Before production deployment, Jenkins runs `docker buildx imagetools
inspect` for every requested runtime image and fails before Kubernetes changes
if `linux/amd64` is not present.

## Dry Deploy Validation

Use `DEPLOY_DRY_RUN=true` for production validation. Jenkins still
renders manifests, resolves images, validates image manifests, and runs
server-side Kubernetes dry-run apply, but it does not mutate runtime resources.
Dry-run builds skip Kafka topic mutations, runtime scaling, Prometheus scrape
updates, and HPSF smoke state changes.

## Web Smoke URL

Jenkins runs from the remote host, so `WEB_PUBLIC_URL` defaults to
`http://192.168.100.252:8090`. Override the parameter only when testing a
different reachable OptionsEdge web endpoint.

## Kafka Bootstrap Default

Jenkins also runs against the remote Kafka brokers by default:
`192.168.100.252:9092`. Do not use
`host.docker.internal` defaults in this remote Jenkins pipeline because the
CentOS Jenkins host cannot resolve that Mac Docker hostname.

## Image Registry Default

All deploy environments default to the remote registry
`192.168.100.252:5000`. Jenkins should not resolve deploy images through
`host.docker.internal:5001`; that hostname is local to Docker Desktop and is not
reachable from the remote CentOS Jenkins host.
