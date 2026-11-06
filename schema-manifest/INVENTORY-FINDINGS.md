# Schema inventory findings (2026-11-03)

Generated with the baseline inventory (`scripts/ci/schema-manifest-init.py`) from the live dev, prod and es4 registries.

## Subjects whose id differs between registries (87) — now MIRRORED with per-environment ids (R3f)

Each holds the SAME schema content in every registry that has it but a DIFFERENT registry id. They were `ENV_LOCAL` until the mirror rule (R3f)
existed; they are now `MIRRORED` with the dev id as the manifest id and `idByEnvironment` for every registry whose id differs, so sync and the
drift check use the id each registry really holds. That is safe only while no byte-copying mirror carries one of these subjects between
registries whose ids differ: the validator checks it against `activeMirrors` (generated from the mirror jobs by `scripts/ci/schema-mirrors-generate.py`)
and `scripts/ci/schema-mirror-evidence.py` samples the live topics nightly. The table is the history of the divergence, kept for the record.

### Mirror findings (measured 2026-11-03)

| mirror | transport | payload | note |
|---|---|---|---|
| CVD + footprint (8 topics, es4 -> dev, es4 -> prod) | reencode | Avro | all 15 running launchd units use the canonical registry-aware jar; the legacy byte-copy `install` of `Jenkinsfile.es-cvd-mirror` is now refused, because ids differ (cvd-value dev 2030 / prod 2027 / es4 2015) |
| es.futures.aggressor-flow (es4 -> dev, es4 -> prod) | bytes | Avro id 2701 `EsAggressorFlowSnapshot` | **FINDING, fixed here:** prod's registry did not hold id 2701 (404), so the prod mirror's frames named an id prod could not resolve. The manifest now lists prod for it; the prod registration is done through the governed engine after merge |
| es.options.indicators.* (6 topics, es4 -> dev/prod) | bytes | Avro 2001-2007, 2501-2506 | the install asserts identical ids on both registries; snapshot.current showed 2001 when sampled, the others had no record, and the two studies topics do not exist on es4 (reported ABSENT; no studies mirror unit runs here) |
| es.tape-zones.board, es.underlying.es.trades, options.databento.opra.definition.enumeration | bytes | JSON (record starts `{"schemaVers`) | declared unframed; sampled |
| underlying.vix.price (prod -> es4, MM2) | bytes | **Avro since 2026-11-06**: `com.optionsedge.contracts.hpsf.VixUnderlyingPrice`, FIXED id 3061 in every registry (it was sampled as JSON on 2026-11-03) | declared framed with that subject; production's registry is registered at its own cutover (docs/VIX-UNDERLYING-PRICE-AVRO-CUTOVER.md) |
| es.futures.auction (es4 -> prod only), es.strike-intelligence-by-strike | bytes | JSON per the job text / not registry-framed | declared unframed; UNMEASURED (no record on es4 today) |
| underlying.es.price, underlying.spx.price paths | reencode | Avro | `mac74` (dev -> .74) is not a governed registry and is listed as `ungovernedPaths` |

What this does not prove: the subjects of a mirror are declared in its job header and sampled by the newest record only; a copier outside the
deploy repo's mirror jobs is not seen (spec R3f).

| subject | versions | dev | prod | es4 |
|---|---|---|---|---|
| app.options.ibkr.gex.IbkrPreOpenBaseline | 2 | v1=2008, v2=2074 | v1=2008, v2=2058 | - |
| app.options.ibkr.gex.IbkrPreOpenGeneration | 2 | v1=2010, v2=2068 | v1=2010, v2=2047 | - |
| app.options.ibkr.gex.IbkrPreOpenHeartbeat | 2 | v1=2011, v2=2070 | v1=2011, v2=2049 | - |
| app.options.ibkr.gex.IbkrPreOpenManifest | 2 | v1=2012, v2=2072 | v1=2012, v2=2056 | - |
| app.options.ibkr.gex.IbkrPreOpenOwnership | 2 | v1=2013, v2=2067 | v1=2013, v2=2046 | - |
| app.options.ibkr.gex.IbkrPreOpenPathFreeze | 2 | v1=2014, v2=2076 | v1=2014, v2=2059 | - |
| app.options.ibkr.gex.IbkrPreOpenPathStatus | 2 | v1=2015, v2=2069 | v1=2015, v2=2048 | - |
| app.options.ibkr.gex.IbkrPreOpenStrikeStatus | 2 | v1=2016, v2=2073 | v1=2016, v2=2057 | - |
| com.optionsedge.contracts.deltaflow.DeltaFlowAcceptedWatermark | 1 | v1=3004 | v1=2504 | v1=2703 |
| com.optionsedge.contracts.deltaflow.DeltaFlowCalibrationRecord | 1 | v1=3005 | v1=2505 | v1=2704 |
| com.optionsedge.contracts.deltaflow.DeltaFlowCorrectionEvent | 1 | v1=3006 | v1=2506 | v1=2705 |
| com.optionsedge.contracts.deltaflow.DeltaFlowEvent | 1 | v1=3007 | v1=2507 | v1=2706 |
| com.optionsedge.contracts.deltaflow.DeltaFlowGreekStoreValue | 1 | v1=3008 | v1=2508 | v1=2707 |
| com.optionsedge.contracts.deltaflow.DeltaFlowIndexMarker | 1 | v1=3009 | v1=2509 | v1=2708 |
| com.optionsedge.contracts.deltaflow.DeltaFlowIntradayState | 1 | v1=3010 | v1=2510 | v1=2709 |
| com.optionsedge.contracts.deltaflow.DeltaFlowOiState | 1 | v1=3011 | v1=2511 | v1=2710 |
| com.optionsedge.contracts.deltaflow.DeltaFlowPositioningSnapshot | 1 | v1=3012 | v1=2512 | v1=2711 |
| com.optionsedge.contracts.deltaflow.DeltaFlowPositioningState | 1 | v1=3013 | v1=2513 | v1=2712 |
| com.optionsedge.contracts.deltaflow.DeltaFlowQuoteState | 1 | v1=3014 | v1=2514 | v1=2713 |
| com.optionsedge.contracts.deltaflow.DeltaFlowReversibleContribution | 1 | v1=3015 | v1=2515 | v1=2714 |
| com.optionsedge.contracts.deltaflow.DeltaFlowRollingState | 1 | v1=3016 | v1=2516 | v1=2715 |
| com.optionsedge.contracts.deltaflow.DeltaFlowSnapshot | 1 | v1=3017 | v1=2517 | v1=2716 |
| com.optionsedge.contracts.deltaflow.DeltaFlowStrikeSnapshot | 1 | v1=3018 | v1=2518 | v1=2717 |
| com.optionsedge.contracts.deltaflow.DeltaFlowStrikeStoreValue | 1 | v1=3019 | v1=2519 | v1=2718 |
| com.optionsedge.contracts.deltaflow.DeltaFlowTradeInput | 1 | v1=3020 | v1=2520 | v1=2719 |
| com.optionsedge.contracts.greekmoveauth.BaselineState | 1 | v1=3021 | v1=2521 | v1=2720 |
| com.optionsedge.contracts.greekmoveauth.DeltaFrameEvent | 1 | v1=3022 | v1=2522 | v1=2721 |
| com.optionsedge.contracts.greekmoveauth.DeltaSampleWindow | 1 | v1=3023 | v1=2523 | v1=2722 |
| com.optionsedge.contracts.greekmoveauth.EngineCarryState | 1 | v1=3053 | v1=2524 | v1=2723 |
| com.optionsedge.contracts.greekmoveauth.GexFrameEvent | 1 | v1=3054 | v1=2525 | v1=2724 |
| com.optionsedge.contracts.greekmoveauth.LastVerdict | 1 | v1=3055 | v1=2526 | v1=2725 |
| com.optionsedge.contracts.greekmoveauth.MoveAuthenticityVerdict | 1 | v1=3056 | v1=2527 | v1=2726 |
| com.optionsedge.contracts.greekmoveauth.SampleWindow | 1 | v1=3057 | v1=2528 | v1=2727 |
| com.optionsedge.contracts.greekmoveauth.SpotFrameEvent | 1 | v1=3058 | v1=2529 | v1=2728 |
| com.optionsedge.contracts.hpsf.EsUnderlyingPrice | 1 | v1=3060 | - | v1=2752 |
| com.optionsedge.contracts.hpsf.UnderlyingPrice | 1 | v1=2056 | v1=2045 | v1=2029 |
| com.optionsedge.contracts.multileg.MultilegObservation | 1 | v1=3050 | v1=2556 | - |
| com.optionsedge.contracts.multileg.MultilegSrLevels | 1 | v1=3052 | v1=2558 | - |
| com.optionsedge.contracts.multileg.MultilegWindowStatus | 1 | v1=3051 | v1=2557 | - |
| com.optionsedge.contracts.optionpressure.L1Contribution | 1 | v1=2047 | v1=2055 | - |
| com.optionsedge.contracts.optionpressure.PressureFrame | 1 | v1=2046 | v1=2054 | - |
| com.optionsedge.contracts.spotvolregime.SpotVolRegimeLastFrameState | 1 | v1=3049 | v1=2555 | - |
| com.optionsedge.contracts.spotvolregime.SpotVolRegimeSnapshot | 1 | v1=3047 | v1=2553 | - |
| com.optionsedge.contracts.spotvolregime.SpotVolRegimeWindowState | 1 | v1=3048 | v1=2554 | - |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionBehaviorEvidence | 1 | v1=3041 | v1=2547 | v1=2746 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventCandidateInvalidated | 1 | v1=3029 | v1=2535 | v1=2734 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventDefenseBroken | 1 | v1=3035 | v1=2541 | v1=2740 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventInvasion | 1 | v1=3033 | v1=2539 | v1=2738 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventNonFiniteSpotDropped | 1 | v1=3025 | v1=2531 | v1=2730 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventNonFiniteStrikeDropped | 1 | v1=3027 | v1=2533 | v1=2732 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventOutOfOrderSpotDropped | 1 | v1=3026 | v1=2532 | v1=2731 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventSessionReset | 1 | v1=3028 | v1=2534 | v1=2733 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventStage1 | 1 | v1=3032 | v1=2538 | v1=2737 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventStage1Retired | 1 | v1=3030 | v1=2536 | v1=2735 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventStage2Timeout | 1 | v1=3034 | v1=2540 | v1=2739 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventStage2Verdict | 1 | v1=3036 | v1=2542 | v1=2741 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionEventState | 1 | v1=3031 | v1=2537 | v1=2736 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionFlowEvidence | 1 | v1=3040 | v1=2546 | v1=2745 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionGexEvidence | 1 | v1=3039 | v1=2545 | v1=2744 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionPaceEvidence | 1 | v1=3042 | v1=2548 | v1=2747 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionPaceFrame | 1 | v1=3043 | v1=2549 | v1=2748 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionSnapshot | 1 | v1=3024 | v1=2530 | v1=2729 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionSpotEvidence | 1 | v1=3037 | v1=2543 | v1=2742 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionSrEvidence | 1 | v1=3038 | v1=2544 | v1=2743 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionWallFrame | 1 | v1=3044 | v1=2550 | v1=2749 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionWallMeta | 1 | v1=3046 | v1=2552 | v1=2751 |
| com.optionsedge.contracts.strikeinvasion.StrikeInvasionWallRow | 1 | v1=3045 | v1=2551 | v1=2750 |
| com.optionsedge.processing.esspxalign.state.Basis | 1 | v1=2059 | v1=2041 | - |
| com.optionsedge.processing.esspxalign.state.Book | 1 | v1=2060 | v1=2042 | - |
| com.optionsedge.processing.esspxalign.state.Bridge | 1 | v1=2061 | v1=2043 | - |
| com.optionsedge.processing.esspxalign.state.Emitted | 1 | v1=2062 | v1=2044 | - |
| es.futures.compression-expansion.current-value | 1 | v1=2028 | v1=2050 | - |
| es.futures.compression-expansion.events-value | 1 | v1=2029 | v1=2051 | - |
| es.futures.cvd-value | 1 | v1=2030 | v1=2027 | v1=2015 |
| es.futures.cvd.bars-value | 1 | v1=2031 | v1=2028 | v1=2016 |
| es.futures.cvd.levels-value | 1 | v1=2032 | v1=2029 | v1=2017 |
| es.futures.footprint-value | 1 | v1=2033 | v1=2030 | v1=2018 |
| es.futures.footprint.bars-value | 1 | v1=2034 | v1=2031 | v1=2019 |
| es.futures.footprint.evidence-value | 1 | v1=2035 | v1=2032 | v1=2020 |
| es.futures.footprint.outcomes-value | 1 | v1=2036 | v1=2033 | v1=2021 |
| es.futures.footprint.strike-value | 1 | v1=2037 | v1=2034 | v1=2022 |
| es.open-direction.forecast-value | 1 | v1=2038 | v1=2035 | v1=2023 |
| es.open-direction.outcome-value | 1 | v1=2039 | v1=2036 | v1=2024 |
| es.open-direction.status-value | 1 | v1=2040 | v1=2037 | v1=2025 |
| options.es-cvd-spx-levels-value | 1 | v1=2071 | v1=2038 | - |
| options.es-gex-spx-aligned-value | 1 | v1=2043 | v1=2039 | - |
| options.es-strike-intel-spx-aligned-value | 1 | v1=2044 | v1=2040 | - |
