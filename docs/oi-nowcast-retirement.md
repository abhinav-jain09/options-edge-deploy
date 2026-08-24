# OI nowcast — retired 2026-08-10

Turned off on dev and production. Not deleted: the code, the harness, the
coefficients and the evidence are all in git, and the anchor it depends on is
exact. This note exists so a future revival starts from what was measured rather
than repeating it.

## What it did

Took the exchange's settled open-interest print and carried it forward through
the session as `OI_morning + a x session_volume`, so GEX would place dealer
hedging walls where positions had accumulated rather than where they stood at
09:30.

## Why it was retired

Not because it broke. Because of what the measurement turned out to cover.

**What it can validate.** End-of-day 1DTE OI against the next morning's print,
over 178 walk-forward sessions with a 2-session embargo:

| | mean | p50 | p90 | worst |
|---|---|---|---|---|
| frozen (no service) | 0.474 | 0.536 | 0.622 | 0.759 |
| service | 0.290 | 0.290 | 0.383 | 0.725 |

Beats frozen on 169/178 sessions; 38.8% mean error reduction; captures ~40% of
the real overnight change. Chain-wide (DTE 0-7) the equivalent figures are 24.2%
-> 17.5%.

**What it cannot validate — and this is why it is off.**

1. *The intraday path.* Only the endpoint was ever scored. What the service
   shows at 13:00 assumes OI accrues in proportion to volume through the day.
   That assumption was never tested and cannot be: OPRA publishes open interest
   ONCE, 06:30 ET, one print per contract (verified directly against the raw
   statistics stream, 17,116 records, one per symbol). There is no intraday open
   interest anywhere to check against — it is a clearing quantity established
   overnight, so no "OI at 13:00" exists even in principle.

2. *0DTE.* The panel's target is the next session's settled print, joined inner.
   A contract expiring today has no next-day row, so it is dropped: across 251
   sessions the DTE-0 fit has 808 rows from a single session and a = 0.0000.
   There is also no trend to extrapolate along — per-DTE coefficients are noise
   around 0.5 (1: 0.5673, 2: 0.3623, 3: 0.5586, 4: 0.5155, 5: 0.5214, 6: 0.5071,
   7: 0.6418) with no structure as expiry approaches.

The desk trades 0DTE. So the part that is measured is the part that is not
traded, and the part that is traded cannot be measured. That is the whole reason
this is off.

## The failure mode to remember

`a x volume` assumes volume means positions OPENING. It cannot see closing flow.
2026-06-17, strike 7500 put: 9,228 contracts traded and OI barely moved
(4,569 -> 4,518). Frozen was off by 51; the service was off by 3,973. Same
session, strike 7600 call: 30,214 traded, OI 9,317 -> 24,682, frozen off by
15,365 and the service off by 2,524. It helps where flow opens and hurts where
flow closes, and nothing in OPRA distinguishes the two — not even a perfect
aggressor classifier, since buy/sell and open/close are different latent
variables.

## What is worth keeping

- **The anchor.** The morning settled print, reconciled against an independent
  definition universe: 2534/2534 exact. It is what GEX uses now, and it is
  right.
- **The harness** (`gex-backtest/oi-study-harness`) and its 251-session cache.
- **The coefficients**, fitted on the service's own cells, 190+ independent
  sessions each. Cells and fit finally agree (contracts#62, processing#605).

## If this is ever revived

Do not start by rebuilding the pipeline. Start by choosing a target that exists
for the contracts actually traded. For 0DTE that cannot be open interest; the
only observable worth scoring against is what price did — whether a level the
signal names actually holds, pins or rejects. Score the decision, not the input,
and do not call the result OI.
