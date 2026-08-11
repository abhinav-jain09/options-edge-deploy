"""Pivot-confirmation geometry of the reversal hunt, ported for offline replay.

A port of `HuntPivotTracker.onValidBucket` / `onPivotLow` as built into the
DEPLOYED production image — options-edge-processing
`f9ee2aca103f34a2c4117e11f733ef2644569be7` (see ../../../docs/evidence/
reversal-hunt-28t31b/README.md for the source map and its provenance):

  * 2 s buckets, OHLC from trades; a bucket with no trades is INVALID
  * pivot candidate sits k buckets back (k = pivotWingBuckets = 15)
  * every wing bucket must be VALID, else no confirmation (conservative)
  * lowOk  : no left bucket's low  < pivot low, no right bucket's low  <= pivot low
    highOk : no left bucket's high > pivot high, no right bucket's high >= pivot high
  * on a confirmed pivot low: depthTicks = round(dir * (swingBefore - price) / tickSize),
    declineBuckets = (pivotBucket - swingBeforeBucket) / BUCKET_MS
  * counter-swing older than swingLookbackBuckets (150) ages out
  * eligible when depth >= minTicks AND declineBuckets >= minBuckets
  * trendResumed (close already beyond the swing) is reported but never CALLed

dir=+1 hunts bottoms, dir=-1 mirrors for tops.

This module deliberately contains NO tape loading: buckets are built by the
caller (calibrate.py) in an order-independent way. Anything that folded trades
here would duplicate that logic and could diverge from it.
"""

BUCKET_MS = 2000
TICK = 0.25
WING = 15            # REVERSAL_PIVOT_WING_BUCKETS default
LOOKBACK = 150       # REVERSAL_SWING_LOOKBACK_BUCKETS default

# bucket layout, shared with calibrate.py
O, HI, LO, C, N, OK, CK = range(7)


class Tracker:
    """One direction. dir=+1 -> bottoms, dir=-1 -> tops."""

    def __init__(self, buckets, direction, min_ticks, min_buckets):
        self.b = buckets
        self.d = direction
        self.min_ticks = min_ticks
        self.min_buckets = min_buckets
        self.swing_fav = None        # lastSwingHigh for dir=+1
        self.swing_fav_bucket = None

    def adverse(self, bar):
        return bar[LO] if self.d > 0 else bar[HI]

    def favorable(self, bar):
        return bar[HI] if self.d > 0 else bar[LO]

    def step(self, now):
        j = now - WING * BUCKET_MS
        pivot = self.b.get(j)
        if pivot is None:
            return None
        low_ok = high_ok = True
        for i in range(1, WING + 1):
            left = self.b.get(j - i * BUCKET_MS)
            right = self.b.get(j + i * BUCKET_MS)
            if left is None or right is None:
                return None                          # invalid wing blocks
            if (self.d * (self.adverse(left) - self.adverse(pivot)) < 0
                    or self.d * (self.adverse(right) - self.adverse(pivot)) <= 0):
                low_ok = False
            if (self.d * (self.favorable(left) - self.favorable(pivot)) > 0
                    or self.d * (self.favorable(right) - self.favorable(pivot)) >= 0):
                high_ok = False
            if not low_ok and not high_ok:
                return None
        out = None
        if low_ok:
            out = self._pivot(self.adverse(pivot), j, now)
        if high_ok:
            self.swing_fav = self.favorable(pivot)
            self.swing_fav_bucket = j
        return out

    def _pivot(self, price, pivot_bucket, confirm_bucket):
        swing, swing_bucket = self.swing_fav, self.swing_fav_bucket
        if swing is None or swing_bucket >= pivot_bucket:
            return None
        decline_buckets = (pivot_bucket - swing_bucket) // BUCKET_MS
        if decline_buckets > LOOKBACK:
            return None
        depth = round(self.d * (swing - price) / TICK)
        if depth < self.min_ticks or decline_buckets < self.min_buckets:
            return None
        bar = self.b[confirm_bucket]
        trend_resumed = self.d * (bar[C] - swing) > 0
        return dict(anchor=price, anchor_bucket=pivot_bucket,
                    swing=swing, confirm_bucket=confirm_bucket,
                    depth_ticks=depth, decline_buckets=decline_buckets,
                    trend_resumed=trend_resumed)
