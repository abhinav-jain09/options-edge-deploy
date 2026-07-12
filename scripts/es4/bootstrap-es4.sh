#!/usr/bin/env bash
# bootstrap-es4.sh — idempotent provisioning of the es4 box (192.168.100.4).
#
# Runs ON the es4 box (rsync'd there by the es4-deploy Jenkins job, INFRA_SYNC action).
# Everything here is safe to re-run: each step checks before it changes.
#
# What it provisions (Codex decisions D1/D2, 2026-07-12):
#   1. single-node k3s (unique CIDRs; traefik off) — the app runtime
#   2. containerd insecure-registry mapping for the prod registry 192.168.100.252:5000
#   3. the es4 infra compose stack (Kafka/SR/Postgres/Redis/MM2) under /home/es4/infra
#   4. core es.* topics (create-es-topics.sh)
#   5. options-edge namespace + es4-postgres secret in k3s
#
# Prereqs on the box: docker + compose plugin (present), passwordless sudo (present).

set -euo pipefail

ES4_HOME=/home/es4
INFRA_DIR="$ES4_HOME/infra"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAN_IP=192.168.100.4
PROD_REGISTRY=192.168.100.252:5000

log() { printf '\n=== %s ===\n' "$*"; }

# ---------------------------------------------------------------- 1. k3s
if ! systemctl is-active --quiet k3s; then
  log "installing k3s (single node, unique CIDRs, no traefik)"
  curl -sfL https://get.k3s.io | sudo INSTALL_K3S_EXEC="server \
    --cluster-cidr=10.44.0.0/16 --service-cidr=10.45.0.0/16 \
    --disable=traefik --node-ip=${LAN_IP} --tls-san=${LAN_IP}" sh -
else
  log "k3s already active"
fi

# ------------------------------------------------- 2. insecure registry map
REG_FILE=/etc/rancher/k3s/registries.yaml
if ! sudo test -f "$REG_FILE" || ! sudo grep -q "$PROD_REGISTRY" "$REG_FILE"; then
  log "writing $REG_FILE for $PROD_REGISTRY (plain-http prod registry)"
  sudo mkdir -p /etc/rancher/k3s
  sudo tee "$REG_FILE" >/dev/null <<EOF
mirrors:
  "${PROD_REGISTRY}":
    endpoint:
      - "http://${PROD_REGISTRY}"
EOF
  sudo systemctl restart k3s
else
  log "registries.yaml already maps $PROD_REGISTRY"
fi

# --------------------------------------------------------- 3. infra stack
log "syncing compose stack into $INFRA_DIR"
sudo mkdir -p "$INFRA_DIR" /home/es4/volumes/{kafka,postgres,redis}
sudo chown -R "$(id -u):$(id -g)" "$ES4_HOME"
cp -f "$SCRIPT_DIR/../../infra/es4/docker-compose.yml" "$INFRA_DIR/docker-compose.yml"
mkdir -p "$INFRA_DIR/mm2"
cp -f "$SCRIPT_DIR/../../infra/es4/mm2/mm2.properties" "$INFRA_DIR/mm2/mm2.properties"

if [ ! -f "$INFRA_DIR/.env" ]; then
  log "minting es4 postgres password (once; never committed)"
  printf 'ES4_POSTGRES_PASSWORD=%s\n' "$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 24)" > "$INFRA_DIR/.env"
  chmod 600 "$INFRA_DIR/.env"
fi

log "docker compose up -d (kafka/schema-registry/postgres/redis/mm2)"
(cd "$INFRA_DIR" && docker compose up -d)

log "waiting for kafka + schema-registry health"
for i in $(seq 1 40); do
  ok_k=$(docker inspect -f '{{.State.Health.Status}}' es4-kafka 2>/dev/null || echo starting)
  ok_s=$(docker inspect -f '{{.State.Health.Status}}' es4-schema-registry 2>/dev/null || echo starting)
  [ "$ok_k" = healthy ] && [ "$ok_s" = healthy ] && break
  sleep 5
done
docker ps --format '{{.Names}}\t{{.Status}}' | sed 's/^/  /'

# ------------------------------------------------------------ 4. topics
log "creating core es.* topics"
bash "$SCRIPT_DIR/create-es-topics.sh"

# ------------------------------------------- 5. namespace + pg secret in k3s
log "ensuring options-edge namespace + es4-runtime-secrets"
sudo k3s kubectl get ns options-edge >/dev/null 2>&1 || sudo k3s kubectl create ns options-edge
PGPW=$(grep '^ES4_POSTGRES_PASSWORD=' "$INFRA_DIR/.env" | cut -d= -f2)
# name MUST match the manifests' envFrom secretRef (render_es4_manifests.py)
sudo k3s kubectl -n options-edge create secret generic es4-runtime-secrets \
  --from-literal=POSTGRES_USER=options_edge_es \
  --from-literal=POSTGRES_PASSWORD="$PGPW" \
  --dry-run=client -o yaml | sudo k3s kubectl apply -f -

# ------------------------------------------ 6. remote-ready kubeconfig
log "writing remote-ready kubeconfig to $ES4_HOME/es4.kubeconfig"
sudo cat /etc/rancher/k3s/k3s.yaml \
  | sed "s|https://127.0.0.1:6443|https://${LAN_IP}:6443|" > "$ES4_HOME/es4.kubeconfig"
chmod 600 "$ES4_HOME/es4.kubeconfig"

log "bootstrap complete — scp ${LAN_IP}:$ES4_HOME/es4.kubeconfig ~/.kube/es4.kubeconfig on the Mac"
