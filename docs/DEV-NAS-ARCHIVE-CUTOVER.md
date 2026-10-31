# Dev NAS archive cutover — 2026-10-31

## Verified target and current state

The archive runs on **192.168.100.102**, verified over SSH as MacBook-Pro-2.local / MacBook Pro (2), with its interface reporting 192.168.100.102. Both the installer and every worker reject a different hostname or IP before doing archive work. All correction, installation, and restart-test operations were performed through SSH to .102. No archive runtime remains installed on the local Mac from the mistaken initial attempt. Docker is not used.

The source checkout is /private/tmp/oe-archive-dev-cutover-102 on .102. The runtime is ~/Library/Application Support/OptionsEdge/dev-nas-archive. Existing changes in .102's separate workspace repository, including infra/dev/public-prefix-proxy/nginx.conf, were left untouched.

Three independent user launch agents supervise spot, daily, and verify workers:
- com.optionsedge.dev-nas-archive-spot
- com.optionsedge.dev-nas-archive-daily
- com.optionsedge.dev-nas-archive-verify

Each has RunAtLoad=true, StartInterval=600, KeepAlive.SuccessfulExit=false, ThrottleInterval=30, ExitTimeOut=15, and AbandonProcessGroup=false. They start when this user's GUI login session starts, run on the ten-minute schedule, and restart after worker failure. Before-login boot operation was not claimed or tested. The daily worker performs heavy archival after 17:10 New York time on weekdays; verification starts after 20:00. Successful daily capture is marked only after the archiver exits successfully; a failed capture remains retryable.

## NAS permission fix

The former launcher exec'd into the Python CLI, changing its macOS responsible-process identity. TCC logs then denied that Python executable Network Volumes access even after the user enabled Bash. The fixed launcher keeps the specifically authorized Homebrew Bash executable as parent and waits for the Python worker. A launchd background write probe succeeded with that parent. No TCC database, Full Disk Access setting, NAS mount, or NAS credentials were altered.

The extra Python scheduler has been removed. Each worker runs directly under launchd. Capture children stay in the worker process group: a small GNU timeout wrapper uses --foreground. macOS then cleans up that entire group when its launchd worker dies, avoiding orphan consumers retaining checkpoint locks.

## Capture and restart evidence on .102

Seventeen tests passed: target-host rejection, production-override rejection, weekend gating, failure retry/success-marker behavior, and the existing SPX decoder/wiring tests.

Live fault injection killed the spot worker (PID 10390); launchd replaced it with PID 12906 and the previous process group was gone. The daily worker was also killed (PID 10393), replaced with PID 15318, and its previous group was gone. A transient lock-contention exit recovered through the configured retry policy.

A capture after the spot restart committed **36,216 records** in spx.basis.state. A separate background capture committed **45,364 IV/RV records**. Both archive files passed SHA-256, gzip, and manifest-record-count verification. These are retained-record captures, not claims that all past sessions are complete. Their storage date is 2026-10-30; records can carry older session dates. Detailed proof is saved on .102 at runtime/state/recovery-proof.json. The temporary verification launch job was removed.

The raw-topic backlog is still being copied. Interrupted test captures can leave .partial files; those are never counted as completed archives and their offsets are not checkpointed. The independent verifier reports incomplete coverage until required data has landed. Existing topic recreation was detected by TopicId, recorded as discontinuities, and rebaselined to retained offsets; expired history is not recovered by this cutover.

## Inputs, outputs and dependencies

The policy matches the prior spot/heavy topic sets in oe-topics.env. Kafka is localhost:19092 ON .102; its registry is 192.168.100.102:8082. The destination is /Volumes/database/optionsedge/kafka/dev. The existing SMB mount and .oe_nas_sentinel are required; local-disk fallback is refused.

The installer uses .102's existing GNU Bash/coreutils, JDK 21 and Confluent CLI. It requires options-edge-contracts.jar and canonical-archive-reader.jar alongside its sources; those were copied from the existing production archive unit and are not committed. Formatter arguments are adapted to Confluent 7 and gzip -dc replaces BSD zcat. Compacted SPX capture stops at its exclusive offset endpoint.

Production Kafka archival was disabled earlier with its crontab backed up at /home/abhinav/oe-ops/crontab.before-dev-archive-cutover-20261026T231925Z on .252. Production was unreachable during the latest read-only check, so its current state was not reverified. No production change was made during this permission fix. NAS mounts and existing shared archives were preserved.
