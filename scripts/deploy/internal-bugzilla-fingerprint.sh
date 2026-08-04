#!/usr/bin/env bash
# Fingerprint the INTERNAL Bugzilla stack (REQ-6 of docs/req-portal-bugzilla-keycloak-sso.md).
#
# REQ-6 makes "the internal instance is untouched" a requirement, not a hope. The portal deploy job
# takes this fingerprint before and after and fails the build on any difference — so a mistake that
# disturbed the internal bug workflow could never be discovered later, from a user report.
#
# Read-only by construction: it inspects and hashes, and writes nothing to the host.
set -euo pipefail

PROD_HOST=${PROD_HOST:-192.168.100.252}
PROD_SSH=${PROD_SSH:-abhinav@${PROD_HOST}}

ssh -o BatchMode=yes -o ConnectTimeout=20 "$PROD_SSH" bash -s <<'REMOTE'
set -uo pipefail
echo "# internal Bugzilla fingerprint (REQ-6)"

for c in options-edge-bugzilla-web options-edge-bugzilla-db; do
  if docker inspect "$c" >/dev/null 2>&1; then
    # Image digest + mount sources + restart policy: the things that would actually differ if the
    # stack had been rebuilt, re-pointed, or had its data relocated.
    echo "container=$c $(docker inspect -f 'image={{.Image}} restart={{.HostConfig.RestartPolicy.Name}} mounts={{range .Mounts}}{{.Source}}->{{.Destination}};{{end}}' "$c")"
  else
    echo "container=$c ABSENT"
  fi
done

# Compose file and the Bugzilla params file are hashed rather than printed: params.json holds
# operational configuration, and the point is to detect change, not to copy content into a CI log.
for f in /home/options-edge/deploy/bugzilla/docker-compose.yml \
         /home/options-edge/config/bugzilla.env \
         /home/options-edge/data/bugzilla/data/params.json; do
  if [ -r "$f" ]; then
    echo "sha256=$(sha256sum "$f" | cut -d' ' -f1) $f"
  else
    echo "sha256=UNREADABLE $f"
  fi
done

# Functional check: the internal instance still answers. A 200/302 from its login page proves the
# workflow the rule.md bug process depends on is alive, not merely that a container exists.
echo "http_8092=$(curl -s -m 8 -o /dev/null -w '%{http_code}' http://127.0.0.1:8092/ || echo FAILED)"
REMOTE
