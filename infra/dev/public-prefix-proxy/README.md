# Public `/dev` prefix proxy

This Nginx proxy runs on the dev host and exposes the dev web app at the host root on port `8098`.
It forwards to the existing dev web service on port `8090`, forwards WebSockets to
the dev gateway on port `8091`, and rewrites the dev Keycloak URL to
`https://dev-auth.bleadingoptions.com`. The named dev-Mac Cloudflare tunnel
routes both public hostnames directly to this Mac.

Run it as a persistent Docker container:

```sh
docker run -d --name options-edge-dev-prefix-proxy --restart unless-stopped \
  -p 8098:8098 -v "$PWD/nginx.conf:/etc/nginx/nginx.conf:ro" nginx:1.27-alpine
```
