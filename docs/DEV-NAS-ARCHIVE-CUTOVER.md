# Dev NAS archive cutover — 2026-10-27

The owner requested that Kafka archival stop on production and run directly on the dev Mac, retaining the existing NAS mount. Production's user crontab was backed up at `/home/abhinav/oe-ops/crontab.before-dev-archive-cutover-20261026T231925Z`; eleven archive and dependent reporting schedules were commented out. The NAS mount, system backup timer, retention job, and existing archive files were left intact.

The dev runtime lives at `~/Library/Application Support/OptionsEdge/dev-nas-archive`. Its source is `scripts/ops/archive/dev-archive.py` and `install-dev-archive.py`. It reads only dev Kafka at localhost:19092 and dev Schema Registry at 192.168.100.102:8082; output is `/Volumes/database/optionsedge/kafka/dev`. The topic policy matches the prior production spot/heavy sets. Spot runs every ten minutes; heavy archival starts after 17:10 New York time on weekdays, and verification after 20:00. Failures retry after thirty seconds. Successful daily runs are marked only after the archive command succeeds. Kafka offsets advance only after a verified file is committed. Retained backlog capture can take multiple runs.

The Mac's Confluent 7 CLI needs --property rather than --formatter-property, and its BSD zcat is replaced by gzip -dc. GNU coreutils and Bash are already installed. The installer requires the existing `options-edge-contracts.jar` and `canonical-archive-reader.jar` alongside its sources; these were copied from the production archive unit, not rebuilt or changed. They are runtime dependencies and are not committed to Git.

Verification: thirteen spot decoder/wiring tests passed. A direct dev spot run wrote 74,538 records in three files with failed=0. Databento raw archive files also reached the NAS. The compacted spot reader now stops at an exclusive offset boundary instead of waiting for an offset-span count of readable records.

## Incomplete restart recovery

A KeepAlive launchd supervisor was installed and tested, but the background process receives Operation not permitted when writing the network volume. The same runtime launched directly can write successfully. The Mac was locked when System Settings was opened, preventing inspection of its network-volume privacy permission. The failing launchd job was unloaded and a temporary direct scheduler restored with `install-dev-archive.py --direct`. Consumer failures retry, but restart after user logout or computer reboot is not verified. Do not claim this cutover is restart-proof until the background NAS permission is resolved and a stop/restart test writes a new committed archive file.

The source supports normal launchd installation by running the installer without --direct. Review the required network-volume permission after unlocking the Mac, install the supervisor, then kill a worker and the supervisor separately and verify replacement processes resume from saved offsets. Do not disconnect the NAS mount or delete old production archives.
