# Prod NAS mount (/mnt/nas) — survives reboots

The prod host (192.168.100.252) archives Kafka to the Synology share `//192.168.100.100/database`.

## Why a plain fstab mount was not enough
At boot the mount ran ~1 s after `network-online.target`, before the route to the NAS worked
(2026-09-16: `mount error(113): could not connect to 192.168.100.100`). A failed mount unit is never
retried, so the share stayed unmounted after every reboot until someone mounted it by hand
(also after the 2026-09-14 power cut, when the NAS itself was off).

## What is installed
1. `/etc/fstab` entry (options end with the systemd automount):
   ```
   //192.168.100.100/database /mnt/nas cifs credentials=/etc/cifs/optionsedge.cred,uid=1000,gid=1001,file_mode=0644,dir_mode=0755,vers=3.0,_netdev,nofail,x-systemd.automount,x-systemd.mount-timeout=30,x-systemd.idle-timeout=0 0 0
   ```
   Any access to `/mnt/nas` mounts it; a failed attempt is retried on the next access. Nothing can be
   written to the root disk under `/mnt/nas` while the share is down.
2. `oe-nas-mount-at-boot.service` (`infra/prod/systemd/`) running `scripts/ops/oe-nas-mount-at-boot.sh`
   (installed to `/usr/local/sbin/`): after boot it accesses `/mnt/nas` every 30 s until the CIFS mount
   is really present (up to 1 h), so the share comes back without waiting for an archive job to touch it.
   `Type=simple` — boot never waits on the NAS.

Install: copy the script to `/usr/local/sbin/`, the unit to `/etc/systemd/system/`, then
`systemctl daemon-reload && systemctl enable oe-nas-mount-at-boot.service`.

## Checking it
- `findmnt -t cifs /mnt/nas` — the real test. `mountpoint -q /mnt/nas` is ALWAYS true on an automount
  point, even with the NAS down.
- `journalctl -b -u oe-nas-mount-at-boot` — attempts and the final "NAS mounted".
- The archive scripts guard on the sentinel `/mnt/nas/optionsedge/.oe_nas_sentinel`, which only exists on the share.
