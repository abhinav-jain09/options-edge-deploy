# Evidence — reversal hunt production thresholds 40t/60s → 28t/62s

Supporting evidence for the two production values changed in this PR. Kept in
this repository, beside the change, so the numbers below cannot drift away from
the config they justify.

| | |
|---|---|
| Change | `REVERSAL_HUNT_MIN_DECLINE_TICKS` unset (default 40) → `28`; `REVERSAL_HUNT_MIN_DECLINE_BUCKETS` `30` → `31` |
| Env | production only — `k8s/overlays/production/kustomization.yaml` and the generated `k8s/services/reversal-confirmation/overlays/production/manifest.yaml` |
| Passive engine | **unchanged** — `REVERSAL_MIN_DECLINE_TICKS=28`, `REVERSAL_MIN_DECLINE_BUCKETS=30` |
| Scope of the claim | notification **volume** only. Nothing here claims the hunt calls better bottoms or tops. |

## 1. Why 28t/31b and not shallower

`HuntConfig.validateAgainst(passive)` throws `IllegalArgumentException` at boot
unless `huntTicks >= passiveTicks` **and** `huntBuckets >= passiveBuckets` **and**
at least one is strictly greater. Against the passive `28t/30b`:

| candidate | admissible | why |
|---|---|---|
| `24t/30b` (originally requested) | **no** | `24 < 28` — would crash-loop the pod at boot |
| `28t/30b` | **no** | neither dimension strictly greater |
| **`28t/31b`** | **yes** | shallowest admissible pair **by depth** |
| `29t/30b` | yes | the other minimal pair, along **duration** instead |

`28t/31b` was chosen over `29t/30b` because the complaint being answered was that
the hunt says nothing, and depth is the dimension that was binding.

## 2. Source provenance

The replay is a port of the geometry and voice guard **as built into the image
production is running**, not of whatever is on a branch today.

| link in the chain | value |
|---|---|
| deployed image | `192.168.100.252:5000/options-edge-reversal-confirmation@sha256:5d1835c4ef72282fd9276b53cb9908c943c6b336fbb16328076df7a8fbfc35c8` |
| image config created | `2026-08-07T22:19:58Z` |
| image label `options-edge.jenkins-build` | `options-edge-processing` build **1331** |
| image label `org.opencontainers.image.revision` | `f9ee2aca103f34a2c4117e11f733ef2644569be7` |
| Jenkins 1331 `BuildData` for `refs/remotes/origin/main` | `f9ee2aca103f34a2c4117e11f733ef2644569be7` |
| Jenkins 1331 `lastBuiltRevision.SHA1` (branch `main`) | `5f246070cfdeec035ffc9b9b771cb924d382c5d8` — **does not agree**, see below |

**The discrepancy, recorded rather than smoothed over.** Jenkins reports two
different revisions for the same build. `5f246070…` is not present in the
repository at all (`git cat-file` fails; no branch contains it), so it cannot be
resolved or diffed. `f9ee2aca…` is a real commit on `main`
("Continuous auto-hunt: the service keeps both hunt modes armed all session
(#589)"), is reported by the SCM plugin for `refs/remotes/origin/main`, and is
what the image itself carries. The image label is the stronger witness because
the pinned `Jenkinsfile` sets it from the build's own checkout —
`GIT_COMMIT_FULL="$(git rev-parse HEAD)"` at line 264, applied as
`--label org.opencontainers.image.revision=$GIT_COMMIT_FULL` at line 446 — so it
records the tree that was actually compiled. The most likely explanation for
`5f246070…` is a Jenkins-local merge commit that was never pushed, but that is an
inference, not something established here.

**Pinned revision: options-edge-processing `f9ee2aca103f34a2c4117e11f733ef2644569be7`.**
Every line reference below resolves at that SHA. The five files the port depends
on are additionally **blob-identical** between `f9ee2aca` and
`89590abc21f79139b26fdd4921ff7141e0808c44` (`origin/main` at the time of
writing), so the port is equally valid against current main:
`HuntPivotTracker.java`, `HuntEngine.java`, `HuntSettings.java`,
`HuntConfig.java`, `engine/EngineConfig.java`.

Note the image's other `org.opencontainers.image.*` labels (`title: ubuntu`,
`version: 26.04`, the Canonical description) are inherited from the base image
and say nothing about this service; only `revision`, `source` and the
`options-edge.*` labels are set by our build.

### Source map

Paths relative to
`reversal-confirmation-service/src/main/java/com/optionsedge/processing/reversalconfirmation/`
at `f9ee2aca`:

| what | source | value / formula |
|---|---|---|
| depth | `hunt/HuntPivotTracker.java:123` | `depthTicks = round(dir × (swingBefore − price) / tickSize)`, `dir` = +1 bottom, −1 top |
| trend-resumed | `hunt/HuntPivotTracker.java:127` | `dir × (confirmationBar.close − swingBefore) > 0` — reported, **never CALLed** |
| voice guard | `hunt/HuntEngine.java:957` | `voiceEligible = !lateRecovery && !lateEmission` |
| late fraction | `hunt/HuntSettings.java:70` | `REVERSAL_HUNT_LATE_VOICE_FRACTION` default `0.5` |
| hunt depth default | `hunt/HuntSettings.java:68` | `REVERSAL_HUNT_MIN_DECLINE_TICKS` default `40` |
| strict-subset gate | `hunt/HuntConfig.java:43` | `validateAgainst(passive)` |
| CONFIG_CHANGED on restore | `hunt/HuntEngine.java:172` | `restoreTerminated(snap, HuntTerminalReason.CONFIG_CHANGED, true)` |
| bucket width | `engine/EngineConfig.java:91` | `BUCKET_MS = 2000` |

Contract fields used by the rollback gate in §5, cited at options-edge-contracts
`71d52fa6dfa56e8b64725a9be0b3833d034a16b2`,
`src/main/java/com/optionsedge/contracts/reversalhunt/`.

**This is a reference pin, not an artifact pin.** `71d52fa6` was committed after
the image was built, so it does not establish the contracts jar compiled into it.
What makes the citations valid for the running service is that all three cited
files are **blob-identical** between `71d52fa6` and `241e4e12ce9e022b366570af7273a9359dcbd081`,
the last `main` commit on options-edge-contracts before the image's creation
timestamp. The gate also reads these fields from the **live topic**, so it is
verifiable operationally at the moment it is used.

| what | source | value |
|---|---|---|
| alert record | `ReversalHuntTypes.java:333` | `HuntAlert(… huntId, huntMode, callSeq, stage, … voiceEligible, … bucketStartMs, emittedAtMs)` |
| alert stages | `ReversalHuntEnums.java:42` | `CALLED / STRENGTHENING / STANDING / INVALIDATED / CONFIRMED / SUPERSEDED / DISARMED / SESSION_END / CONFIG_CHANGED` |
| topic + retention | `ReversalHuntTopics.java:68,123` | `es.reversal.hunt.alerts`, DELETE, **7 days**, key `huntId` |

The 7-day retention is what makes a three-session observation window
reconstructible from the alerts topic alone.

## 3. What the replay does and does not model

Implemented: 2 s buckets, wing `k=15` with every wing bucket required VALID,
counter-swings aged out past `swingLookbackBuckets=150`,
`declineBuckets = (pivotBucket − swingBeforeBucket) / 2000 ms`, the depth and
trend-resumed formulas above, and the voice guard
(`lateRecovery = denom > 0 && recovered/denom >= 0.5`, with
`denom = dir × (counterSwing − anchor)` and
`recovered = dir × (confirmationBar.close − anchor)`). The replay has no
restore/catch-up path, so `lateEmission` is false throughout.

**Not implemented:** standing-call suppression, `INVALIDATED`/`CONFIRMED`
termination, re-arm cadence, and the feed's own record validation, quarantine,
de-duplication and late-event handling. Each changes how many *distinct* calls a
live session reaches, in either direction.

For that reason the rows in §4 are **voice-eligible pivot candidates under a
partial replay**, not production call counts, and not a bound on them in either
direction. They are a like-for-like comparison of two threshold settings under
identical assumptions — which is exactly what a threshold decision needs, and no
more.

Wall join and translation freshness are *not* a source of divergence:
`voiceEligible` depends solely on `lateRecovery` and `lateEmission`
(`HuntEngine.java:957`); a missing wall or translation leaves metadata null and
cannot suppress a voice.

### Determinism

`sequence` is the **venue** message sequence, not a trade counter, so several
trades legitimately share one `(eventTime, sequence)` at the same millisecond —
this occurs on both tapes. Buckets are therefore folded under the total key
`(eventTime, sequence, file index in the manifest, line index in that file)`,
which for a per-partition archive is offset order.

Be precise about what that buys. Given a **fixed manifest**, the result does not
depend on the order records are *read* — the key is assigned from the manifest
position, not from arrival — so the replay is reproducible. It does **not** mean
any file ordering yields the same OHLC: reversing the manifest reassigns the
`file index` component and can therefore change open/close inside a bucket whose
first or last trades share a venue sequence across partitions.

Production has no total order at all: the service folds trades as they arrive
across 4 partitions, so *its* bucket close depends on consumption interleaving,
which no offline replay can reproduce. What the replay does instead is a
**two-order sensitivity check**: every run replays each session a second time
with the manifest file order reversed and aborts unless every reported row —
mode, confirmation time, anchor, depth, duration and voiced flag — is identical.
On these two sessions it is. That is evidence the reported numbers are not an
artefact of the chosen order; it is not proof that every possible production
interleaving would agree.

## 4. Result — voice-eligible candidates per session

Reproduce (verifies every input hash first, runs the self-tests, then replays):

```
scripts/evidence/reversal-hunt-28t31b/calibrate.py \
    --manifest docs/evidence/reversal-hunt-28t31b/inputs.tsv \
    --tapes <dir containing the .jsonl.gz files>
```

and `diff` the output against `expected-output.txt` in this directory. Inputs are
enumerated exactly — path, byte size, sha256, record count — in `inputs.tsv`;
they live read-only on the NAS under
`/mnt/nas/optionsedge/kafka/es4/es.underlying.es.trades/` and are not committed
(≈100–143 MB raw each). `calibrate.py --self-test` runs the unit checks alone;
the reporting path refuses to print numbers if they fail.

| session | setting | voice-eligible | FIND_BOTTOM | FIND_TOP |
|---|---|---|---|---|
| 2026-08-10 | 40t/60s (before) | **1** | 1 | 0 |
| 2026-08-10 | 28t/62s (after) | **5** | 5 | 0 |
| 2026-08-11 | 40t/60s (before) | **1** | 1 | 0 |
| 2026-08-11 | 28t/62s (after) | **4** | 2 | 2 |
| **both** | 40t/60s (before) | **2** — 1.0/session | | |
| **both** | 28t/62s (after) | **9** — 4.5/session | | |

Every eligible candidate in both sessions was voice-eligible: the late-recovery
guard suppressed none at either setting.

On 08-10 the previous setting produced its single candidate at 09:35 and none for
the remaining six hours. On 08-11 it produced one, at 14:30:24, whose anchor
(ES 7738.00) equals the minimum print in the supplied tape for that session.

Two things follow, and only two. The previous setting is not blind to the
session's extreme — so **this change is not a fix for a missed bottom on
08-11**. And the number of candidates the geometry admits rises from 1 to 4.5
per session. Whether either setting *emitted* a call at those instants, and what
happened afterwards, is outside what this replay models (§3) and is not claimed.

### The 28t/62s candidates

| session | time ET | mode | ES anchor | depth / duration |
|---|---|---|---|---|
| 08-10 | 09:35:30 | FIND_BOTTOM | 7771.25 | 12.25p / 96s |
| 08-10 | 09:52:46 | FIND_BOTTOM | 7771.25 | 9.75p / 96s |
| 08-10 | 15:52:22 | FIND_BOTTOM | 7771.25 | 7.50p / 118s |
| 08-10 | 15:53:40 | FIND_BOTTOM | 7770.50 | 8.25p / 196s |
| 08-10 | 15:54:28 | FIND_BOTTOM | 7771.75 | 7.00p / 244s |
| 08-11 | 09:42:48 | FIND_TOP | 7783.50 | 8.75p / 166s |
| 08-11 | 14:29:34 | FIND_BOTTOM | 7743.50 | 7.50p / 160s |
| 08-11 | 14:30:24 | FIND_BOTTOM | 7738.00 | 13.00p / 210s |
| 08-11 | 14:32:36 | FIND_TOP | 7745.75 | 7.75p / 132s |

## 5. Rollout, observation and rollback

### Rollout behaviour

Both hunt thresholds feed the `HuntSettings` fingerprint, so a hunt armed across
the rollout is terminated `CONFIG_CHANGED` (`HuntEngine.java:172`) rather than
restored under thresholds it was not armed with; auto-arm then replaces each
unsuppressed terminal on the first finalized live in-session bucket. State is not
stranded, and there is an intentional, specified gap during restore/catch-up.

**Deploy outside RTH.** A rollout mid-session drops whatever is armed; if it has
to happen mid-session, expect the affected modes to re-arm within a few buckets
and treat that session as not counting toward the window below.

### Observation window

The **first three full RTH sessions** after rollout, each 09:30–16:00 ET, counted
per session. A session interrupted by a deploy, a service restart, or an es4
outage does not count; extend the window instead.

A voiced call is a record on `es.reversal.hunt.alerts` with `stage = CALLED` and
`voiceEligible = true`, **de-duplicated by `(huntId, callSeq)`** — the topic is
keyed by `huntId` and a call can be re-emitted.

### Rollback triggers

Roll back if either fires:

- voiced calls exceed **12 in a single qualifying session** (≈3× the 4.5/session
  seen here), or
- voiced calls exceed **8 per session averaged over the three sessions**.

If the alert records needed to evaluate a trigger are unavailable for a session —
topic gap, retention loss, broker outage — the gate **fails closed**: restore the
previous thresholds rather than record a pass.

There is deliberately **no quality trigger**. The replay measures volume; it does
not measure whether a call was right, and the two sessions here cannot support a
quality tolerance that would mean anything. `INVALIDATED`/`ANCHOR_BREAK` is not a
substitute — it fires on any trade back through the anchor and would need its own
baseline before any threshold on it could be defended. Establishing that baseline
is follow-up work, not a precondition for a two-value threshold change to a
voice-only layer whose passive engine is untouched.

### Rollback procedure

Owner: whoever deploys this change.

1. In `k8s/overlays/production/kustomization.yaml`, delete the
   `REVERSAL_HUNT_MIN_DECLINE_TICKS` patch entry and set
   `REVERSAL_HUNT_MIN_DECLINE_BUCKETS` back to `"30"`. Leave both
   `REVERSAL_MIN_DECLINE_*` passive values alone.
2. `scripts/deploy/generate-service-slices.sh` and commit the regenerated slice.
3. `scripts/ci/validate-services.sh` must pass.
4. Deploy via the `service-deploy` Jenkins job, `SERVICE=reversal-confirmation`,
   `ENVIRONMENT=production`. Never `kubectl apply` the slice directly.
5. Confirm on the running pod that `REVERSAL_HUNT_MIN_DECLINE_TICKS` is absent
   and `REVERSAL_HUNT_MIN_DECLINE_BUCKETS=30`.

## 6. Limitations

- **Two sessions, consecutive, one regime.** Nine candidates fixes the volume
  expectation only roughly; the trigger thresholds carry a 3× and ~2× margin for
  that reason.
- **The archive job did not run for 2026-08-11.** Only `dt=2026-08-10` was
  produced by the job; the 08-11 tape exists because it was copied to the NAS by
  hand for this calibration, hence its distinct filename in `inputs.tsv`. The gap
  is an open defect: until it is fixed, sessions are lost for calibration and this
  evidence base cannot grow.
- The replay is a **port**, not the service binary — see §3 for exactly what it
  omits.
- No claim is made here about call quality. See §5.
