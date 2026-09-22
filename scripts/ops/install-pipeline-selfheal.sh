#!/usr/bin/env bash
# Install (or update) the pipeline self-heal on a prod host, from this checkout:
#
#   scripts/ops/oe-pipeline-selfheal.sh            -> /usr/local/sbin/oe-pipeline-selfheal.sh
#   infra/prod/systemd/oe-pipeline-selfheal.service -> /etc/systemd/system/
#   infra/prod/systemd/oe-pipeline-selfheal.timer   -> /etc/systemd/system/
#   infra/prod/systemd/oe-nas-mount-at-boot.service.d/override.conf -> /etc/systemd/system/.../override.conf
#   scripts/ops/oe-boot-bringup.sh                  -> /usr/local/sbin/oe-boot-bringup.sh (the hook that starts the unit)
#
# then `systemctl daemon-reload` and `systemctl enable --now oe-pipeline-selfheal.timer`. Idempotent:
# re-running it after a change ships the change. Run as root ON the host, from the checkout:
#
#   sudo scripts/ops/install-pipeline-selfheal.sh
#
# The same host paths the estate already uses for oe-boot-bringup (Documentation= in its unit).
# Nothing here is removed on failure half-way: every copy is checked and the script stops at the
# first failure, saying which file, so a partial install is visible rather than silent.
set -euo pipefail
here=$(cd "$(dirname "$0")/../.." && pwd)
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
for f in scripts/ops/oe-pipeline-selfheal.sh scripts/ops/oe-boot-bringup.sh infra/prod/systemd/oe-pipeline-selfheal.service \
         infra/prod/systemd/oe-pipeline-selfheal.timer infra/prod/systemd/oe-nas-mount-at-boot.service.d/override.conf; do
  [ -r "$here/$f" ] || { echo "missing $here/$f" >&2; exit 1; }
done
bash -n "$here/scripts/ops/oe-pipeline-selfheal.sh"
bash -n "$here/scripts/ops/oe-boot-bringup.sh"
install -m 0755 "$here/scripts/ops/oe-pipeline-selfheal.sh" /usr/local/sbin/oe-pipeline-selfheal.sh
install -m 0755 "$here/scripts/ops/oe-boot-bringup.sh"      /usr/local/sbin/oe-boot-bringup.sh
install -m 0644 "$here/infra/prod/systemd/oe-pipeline-selfheal.service" /etc/systemd/system/oe-pipeline-selfheal.service
install -m 0644 "$here/infra/prod/systemd/oe-pipeline-selfheal.timer"   /etc/systemd/system/oe-pipeline-selfheal.timer
install -d -m 0755 /etc/systemd/system/oe-nas-mount-at-boot.service.d
install -m 0644 "$here/infra/prod/systemd/oe-nas-mount-at-boot.service.d/override.conf" /etc/systemd/system/oe-nas-mount-at-boot.service.d/override.conf
install -d -m 0755 /var/lib/oe-selfheal /etc/oe-selfheal
[ -e /etc/oe-selfheal/groups.map ] || printf '# group deployment  — explicit group→deployment mappings for oe-pipeline-selfheal (one per line)\n' > /etc/oe-selfheal/groups.map
systemctl daemon-reload
systemctl enable --now oe-pipeline-selfheal.timer
echo "installed: $(sha256sum /usr/local/sbin/oe-pipeline-selfheal.sh | cut -c1-12)…  timer: $(systemctl is-enabled oe-pipeline-selfheal.timer)/$(systemctl is-active oe-pipeline-selfheal.timer)"
