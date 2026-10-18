# Breakout audit capture (DEV Mac)

Schedules `breakout-audit-service capture` (processing #1155) every 30 minutes on weekdays, one dataset directory per New York date
under `~/oe-ops/breakout-audit/data/<date>/<topic>/`. Dev only; nothing here touches production.

Install (host action):

    mkdir -p ~/oe-ops/breakout-audit
    cp oe-breakout-audit-capture.sh ~/oe-ops/breakout-audit/
    cp <processing>/breakout-audit-service/target/options-edge-breakout-audit-*-SNAPSHOT.jar ~/oe-ops/breakout-audit/options-edge-breakout-audit.jar
    cp com.optionsedge.breakout-audit-capture.plist ~/Library/LaunchAgents/
    launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.optionsedge.breakout-audit-capture.plist

The nightly dev clean recreates topics, so a day's data is complete only if the capture ran before 17:20 ET; the next day starts a new directory.
Enumerate a day with `java -jar ... enumerate --dir <data>/<date> ... --policy-file <policy.json>` (see the processing RUNBOOK).
