# Dev native liquidity activation — pending release prerequisites

This branch prepares configuration only. Do not merge or roll it out until the reviewed gateway PR302 and web PR1583 images have passed their existing Jenkins Linux/build gates and their immutable identities are available. No runtime configuration has been changed by this branch. Production and ES4 collectors are untouched.

The dev gateway currently uses WS_AUTH_ENABLED=true, GATEWAY_AUTH_ENABLED unset, approval role replay-admin, source DATABENTO, symbol SPX and IB_EXPIRY=AUTO. Its existing issuer/client/JWKS and explicit dev-only ticket/Kafka relaxations are already configured. Native full-book history requires the existing AppSession authority, so this change enables GATEWAY_AUTH_ENABLED and explicitly preserves the current options-edge-web audience. This enables per-session routing and approval/expiry enforcement; it must be checked for ordinary SPX quotes and existing selections during runtime acceptance. Do not bypass those controls to make history work.

GATEWAY_RITHMIC_SYSTEM becomes Rithmic Paper Trading, matching the current native Edge UI's source report (747 bids/893 asks at the read-only baseline). The browser uses the matching HEATMAP_RITHMIC_SYSTEM and HEATMAP_NATIVE_HISTORY_ENABLED=true. RuntimeProfileConfig already publishes HEATMAP-prefixed keys, with explicit native-flag/system tests added in the web change. No credentials are added to manifests or the browser.

CONTEXT_TAPE_COMPILED_ASSETS=true activates the prepared content-addressed ES-module graph, including the native worker, Brotli/gzip representations and ETags. The existing public-assets PVC and digest-pinned init publisher stay in use. Both monolithic and standalone web patches are kept identical; the gateway service slice is regenerated from the monolithic dev overlay.

Local checks: generated service slices; scoped validate-services for web/dev and feed-gateway/dev both pass (image binding, blast radius and mirror). Only four dev files change. No production or experiment manifest changes.

Release sequence: finish actual Claude and independent Codex reviews for application code and this configuration; merge application code; build exact approved main SHAs through existing Jenkins jobs; obtain exact image digests and asset manifests; then merge this configuration and roll only feed-gateway/web through the existing service jobs. Feature branches are not runtime deployment sources. Record any SCM-triggered build separately from a manual rollout. No new Jenkins stage or deployment path is introduced.

Runtime acceptance must use authenticated native Edge on the actual dev URL: current subject bootstrap, matching READY source, changing live book, complete supported source span, displayed quantities, no stale consumed current levels, retained data during resize/pan/return/reconnect, smooth unsent-draft zone moves, no visible clears, and unchanged regular SPX quote/selection behaviour. Measure quiet/loaded whole-page frame/heap/graphics/LAN budgets before closing the six-phase reconciliation. A small synthetic fixture is not that proof.

If activation breaks the existing data view, restore the prior dev flags/auth mode by a reviewed revert through the same Jenkins service path; never hand-edit a running pod or weaken auth. Application source and this configuration remain separately reviewable.
