# Public `/dev` prefix proxy

This Nginx proxy runs on the dev host and exposes only `/dev/` on port `8098`.
It strips the prefix before forwarding to the existing dev web service on port
`8090`, and rewrites root-relative browser resources into the prefix. The
production Cloudflare tunnel routes the matching public path to this port.

Run it as a persistent Docker container:

```sh
docker run -d --name options-edge-dev-prefix-proxy --restart unless-stopped \
  -p 8098:8098 -v "$PWD/nginx.conf:/etc/nginx/nginx.conf:ro" nginx:1.27-alpine
```
