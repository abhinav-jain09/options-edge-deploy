# ES replay consumer preparation

The processing-source audit uses approved commit `329afe7a2b5c73ba1fdbd99f4d5aaf121237f7a1` of `options-edge-processing`. It does not claim every running consumer image is byte-identical to that tree. This change deploys feed code, not processing workloads.

| Consumer | Treatment of time | Source anchor in options-edge-processing |
|---|---|---|
| CVD | Uses input event milliseconds and second buckets; committed intervals can reject late input. | `es-cvd-service/src/main/java/com/optionsedge/processing/escvd/EsCvdEngine.java:198`, `:220` |
| Auction / AMT | Applies input in event-time/sequence order. | `es-amt-service/src/main/java/com/optionsedge/processing/esamt/AuctionEngine.java:13` |
| Footprint strike | Consumes footprint bars carrying their original barStartMs; it does not pour replay ticks into an arrival-time candle. | `es-footprint-strike-service/src/main/java/com/optionsedge/processing/esfootprintstrike/BarInput.java:10` |
| Context Tape | Uses event-time admission and raw event-time identity. | `context-tape-service/src/main/java/com/optionsedge/processing/contexttape/direction/EsParticipationEngine.java:12`, `context-tape-service/src/main/java/com/optionsedge/processing/contexttape/direction/candle/AdmissionPipeline.java:1` |
| Indicator | Per-partition event-time lateness and sealed-horizon rejection. | `indicator-service/src/main/java/com/optionsedge/processing/indicator/engine/AdmissionPipeline.java:16`, `:191`, `:978` |
| Close direction | Reorders by event time; impact buckets use the trade's event minute. | `close-direction-service/src/main/java/com/optionsedge/processing/closedirection/CanonicalMerge.java:18`, `close-direction-service/src/main/java/com/optionsedge/processing/closedirection/engine/ImpactEstimator.java:73` |
| Tape zones | Event-time release buffer and session buckets; no wall clock in ordering. | `tape-zones-service/src/main/java/com/optionsedge/processing/tapezones/ReleaseBuffer.java:11`, `tape-zones-service/src/main/java/com/optionsedge/processing/tapezones/SessionGrid.java:46` |
| Aggressor | Parses the source eventTime into its print identity. | `es-aggressor-flow-service/src/main/java/com/optionsedge/processing/esaggressor/EsAggressorFlowRuntime.java:369`, `:459` |

The recorded opening-hour probe supplied 111,075 persisted rows to the actual CVD, aggressor, AMT identity, Context Tape and indicator admission code, then repeated them and supplied distinct shifted prints. All five rejected the duplicate pass, admitted distinct input and passed the bounded burst probe. This is an offline engine test, not an actual peak-load runtime replay. Some first-pass indicator records were rejected under existing admission rules; no completeness repair is claimed.

At preparation, ES4 has eight ready consumer deployments. Production and dev Kubernetes linearizers are paused at replicas zero, as are their indicator/Context Tape deployments; this task does not activate them. The earlier production linearizer image includes the 700-print resume fix, but it is not exposed as a running consumer in this rollout. The acceptance gate checks unchanged ready ES4 consumer pods/restart counts and zero lag for active Kafka group members on the source topic. Manually assigned consumers without membership are covered by pod health, not committed-group lag (`scripts/es4/refresh-es-replay.py:107`).

Late replay prints retain their exchange event time and may remain excluded from finalized output. This task enables bounded source reconnect recovery; it does not rebuild historical VWAP or clear completeness latches.
