#!/usr/bin/env bash
# oe-nas-mount-at-boot.sh — mount the NAS (/mnt/nas) after boot and keep trying until it answers.
#
# WHY: the boot-time mount runs ~1 s after network-online, before the route to 192.168.100.100 works
# (2026-09-16: "mount error(113): could not connect"), and a failed mount unit is never retried. The
# fstab entry is an x-systemd.automount, which only mounts when something ACCESSES /mnt/nas. This
# service is that access, repeated every NAS_MOUNT_RETRY_INTERVAL seconds until the CIFS mount is
# really there (findmnt -t cifs, not `mountpoint`, which is always true on an automount point), or
# NAS_MOUNT_RETRY_SECONDS have passed (the NAS may itself be off after a power cut).
set -uo pipefail
INTERVAL="${NAS_MOUNT_RETRY_INTERVAL:-30}"
DEADLINE=$(( $(date +%s) + ${NAS_MOUNT_RETRY_SECONDS:-3600} ))
n=0
while :; do
  n=$((n + 1))
  if ! systemctl is-active -q mnt-nas.automount; then
    mount /mnt/nas 2>&1 | head -1               # automount unit absent: mount directly
  fi
  ls /mnt/nas/optionsedge >/dev/null 2>&1        # triggers the automount
  if findmnt -n -t cifs /mnt/nas >/dev/null 2>&1; then
    echo "NAS mounted (attempt $n): $(findmnt -n -t cifs -o SOURCE /mnt/nas)"
    exit 0
  fi
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then
    echo "NAS still NOT mounted after $n attempts — giving up (the next access to /mnt/nas will retry)"
    exit 1
  fi
  echo "NAS not reachable yet (attempt $n) — retrying in ${INTERVAL}s"
  sleep "$INTERVAL"
done
