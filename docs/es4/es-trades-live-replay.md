# Shared ES futures replay refresh

`es4-deploy` action `refresh-es-replay` builds the native Rust feed from the exact reviewed `main` commit in `FEED_PERMITTED_SHA`, on the Mac CI agent. The canonical guard also binds this deployment repository to `PERMITTED_SHA`. Other actions retain their previous behavior (`Jenkinsfile.es4-deploy:104`).

Before image publication, the action runs Rust and rollout tests, then a read-only actual vendor replay comparison against the saved rolling hour. The helper reads only the API key from the existing ES4 Secret into process memory, passes it through the child environment and redacts it from captured output. No Kafka producer, group commit, DB write or Kubernetes mutation occurs in this diagnostic. Failure prevents rollout (`scripts/es4/run-es-replay-shadow.py:1`).

Only the active singleton Recreate ES feed is refreshed. Cluster identities and exclusive placement are checked. The running template is copied; only its immutable image and explicit `DATABENTO_ES_TRADES_REPLAY_START_MINUTES=60` override change. It serves both dev and production consumers through the shared source. No ConfigMap or replica-count apply occurs (`scripts/es4/refresh-es-replay.py:119`).

Acceptance requires the exact verified Linux amd64 image/source, one ready pod with zero restarts, a 60-minute subscription, a completed replay report matching every unique saved identity, and at least 60 seconds of stable observation. Reconnect loops fail. Only after these checks does the action promote the exact verified manifest bytes to the canonical `prod` tag (`scripts/es4/refresh-es-replay.py:39`).

Failure or SIGTERM restores this action's previous template with bounded optimistic-concurrency retries, without overwriting a competing rollout. A hard kill or host failure cannot guarantee recovery; inspect aborted builds. Restoring the template does not remove already published records or reopen sealed indicators. Existing incomplete VWAP sessions require a separate validated rebuild. The action's global timeout is 180 minutes, build stage 120 minutes, rollout stage 45 minutes (`Jenkinsfile.es4-deploy:63`).
