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

## ES slices

After 16:30 New York the same job also runs `capture-slice` (processing breakout-audit-service) for `es.futures.cvd.bars`, `es.futures.footprint.strike`,
`options.es-strike-intel-spx-aligned` (whole, 08:30-16:30 ET) and `options.es-gex-spx-aligned` (the same window, one record per 2 s of broker time:
the topic republishes a 16 KB book several times a second). A slice is `<topic>.slice.<fromTs>-<toTs>.jsonl.gz` plus a `.json` naming the
window, the sampling and the per-partition scan range; it is research input for the pattern study, not a completeness-proven audit dataset.
`underlying.es.price` is not captured: it is empty on dev since 25 Oct.
