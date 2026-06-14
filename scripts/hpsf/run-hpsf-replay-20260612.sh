#!/usr/bin/env bash
set -euo pipefail

DRY_RUN=false
FIXTURE_MODE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --fixture-mode) FIXTURE_MODE=true ;;
    *) echo "Unknown argument: $arg" >&2; exit 2 ;;
  esac
done

BUILD_DIR="${HPSF_REPLAY_BUILD_DIR:-build/hpsf-replay-20260612}"
ARTIFACT_DIR="${HPSF_REPLAY_ARTIFACT_DIR:-artifacts}"
REPORT="${HPSF_REPLAY_REPORT:-$ARTIFACT_DIR/hpsf-replay-report-20260612.md}"
EVIDENCE="$BUILD_DIR/evidence.json"
mkdir -p "$BUILD_DIR" "$ARTIFACT_DIR" "$BUILD_DIR/logs" "$ARTIFACT_DIR/logs"

jenkins_build_url() {
  local build_url="${BUILD_URL:-}"
  if [[ -z "$build_url" && -n "${JENKINS_PUBLIC_URL:-}" && -n "${JOB_NAME:-}" && -n "${BUILD_NUMBER:-}" ]]; then
    local job_path
    job_path="$(printf '%s' "$JOB_NAME" | sed 's#/#/job/#g')"
    build_url="${JENKINS_PUBLIC_URL%/}/job/${job_path}/${BUILD_NUMBER}/"
  fi
  echo "$build_url"
}

write_fail_report() {
  local reason="$1"
  local evidence_mode="REAL"
  local json_reason
  if [[ "$FIXTURE_MODE" == "true" && "$DRY_RUN" == "true" ]]; then
    evidence_mode="FIXTURE_DRY_RUN"
  elif [[ "$FIXTURE_MODE" == "true" ]]; then
    evidence_mode="FIXTURE"
  elif [[ "$DRY_RUN" == "true" ]]; then
    evidence_mode="DRY_RUN"
  fi
  json_reason="$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$reason")"
  cat > "$EVIDENCE" <<JSON
{
  "evidenceMode": "$evidence_mode",
  "jenkins": {
    "buildUrl": "$(jenkins_build_url)",
    "buildNumber": "${BUILD_NUMBER:-}",
    "commitSha": "${GIT_COMMIT:-${CODE_GIT_SHA:-}}",
    "jobName": "${JOB_NAME:-}"
  },
  "replay": {
    "date": "${REPLAY_DATE:-2026-06-12}",
    "start": "${REPLAY_START:-2026-06-12T13:30:00Z}",
    "end": "${REPLAY_END:-2026-06-12T20:00:00Z}",
    "topicPrefix": "${TOPIC_PREFIX:-options.replay.20260612}",
    "opraDataset": "${OPRA_DATASET:-OPRA.PILLAR}",
    "opraSchema": "${OPRA_SCHEMA:-tcbbo}",
    "esDataset": "${ES_DATASET:-GLBX.MDP3}",
    "esSchema": "${ES_SCHEMA:-trades}",
    "spxSpotSource": "${SPX_SPOT_SOURCE:-}"
  },
  "counts": {},
  "esSelection": {},
  "stageA": {"started": false, "startupLog": ""},
  "stageB": {"started": false, "startupLog": ""},
  "keyValidation": {"signalKeyValid": false, "latestSignalKeyValid": false, "auditKeyValid": false},
  "samples": {},
  "missingScenarioNotes": [$json_reason],
  "orderInstructionEnabledTrueFound": false
}
JSON
  scripts/hpsf/generate-hpsf-replay-report-20260612.py --input "$EVIDENCE" --output "$REPORT" >/dev/null || true
  cp "$EVIDENCE" "$ARTIFACT_DIR/hpsf-replay-summary.json"
  cat > "$ARTIFACT_DIR/replay-validation-result.json" <<JSON
{
  "validationResult": "FAIL",
  "failureReasons": ["JENKINS_REPLAY_PIPELINE_FAILED"],
  "details": [$json_reason]
}
JSON
}

if [[ "$DRY_RUN" == "true" || "$FIXTURE_MODE" == "true" ]]; then
  write_fail_report "${HPSF_REPLAY_FAILURE_REASON:-Dry-run/fixture replay is not valid HPSF-82A release evidence. Run Jenkins with real Databento replay evidence.}"
  echo "HPSF replay gate failed closed; report=$REPORT" >&2
  exit 1
fi

if [[ -n "${HPSF_REPLAY_EVIDENCE_JSON:-}" ]]; then
  scripts/hpsf/validate-replay-output.sh
  scripts/hpsf/generate-replay-report.sh
else
  scripts/hpsf/create-replay-topics-20260612.sh > "$BUILD_DIR/create-replay-topics.log"
  scripts/hpsf/download-databento-history.sh
  scripts/hpsf/publish-historical-replay.sh
  scripts/hpsf/validate-replay-output.sh --final
  scripts/hpsf/generate-replay-report.sh
fi

cp -R "$BUILD_DIR/logs/." "$ARTIFACT_DIR/logs/" 2>/dev/null || true

grep -F 'Final PASS/FAIL: PASS' "$REPORT" >/dev/null
grep -F 'orderInstruction.enabled=true ever appeared: NO' "$REPORT" >/dev/null

echo "HPSF replay gate passed; report=$REPORT"
