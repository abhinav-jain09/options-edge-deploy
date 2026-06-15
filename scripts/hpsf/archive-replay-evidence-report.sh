#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR="${HPSF_REPLAY_ARTIFACT_DIR:-artifacts}"
if [[ -z "${REPLAY_DATE:-}" ]]; then
  if [[ "${HPSF_REPLAY_DATE_ID:-}" =~ ^[0-9]{8}$ ]]; then
    REPLAY_DATE="${HPSF_REPLAY_DATE_ID:0:4}-${HPSF_REPLAY_DATE_ID:4:2}-${HPSF_REPLAY_DATE_ID:6:2}"
  else
    REPLAY_DATE="unknown"
  fi
fi
COMPACT_DATE="${REPLAY_DATE//-/}"
REPLAY_RUN_NAME="${HPSF_REPLAY_RUN_NAME:-hpsf-historical-replay-$COMPACT_DATE}"
default_project_evidence_dir() {
  local build_number="${BUILD_NUMBER:-manual}"
  if [[ -d "../options-edge" ]]; then
    printf '../options-edge/evidence-report/%s/%s\n' "$REPLAY_RUN_NAME" "$build_number"
  elif [[ -d ".replay/options-edge" ]]; then
    printf '.replay/options-edge/evidence-report/%s/%s\n' "$REPLAY_RUN_NAME" "$build_number"
  else
    printf 'evidence-report/%s/%s\n' "$REPLAY_RUN_NAME" "$build_number"
  fi
}
EVIDENCE_DIR="${HPSF_REPLAY_EVIDENCE_DIR:-$(default_project_evidence_dir)}"
JENKINS_ARCHIVE_EVIDENCE_DIR=".replay/options-edge/evidence-report/${REPLAY_RUN_NAME}/${BUILD_NUMBER:-manual}"
MANIFEST="${HPSF_REPLAY_ARTIFACT_MANIFEST:-artifact-manifest.json}"
ARCHIVER=".replay/options-edge/scripts/hpsf/archive_replay_evidence.py"
VALIDATION_FILE="$ARTIFACT_DIR/replay-validation-result.json"

mkdir -p "$ARTIFACT_DIR/logs" "$EVIDENCE_DIR"
rm -f "$EVIDENCE_DIR"/*.md "$EVIDENCE_DIR"/*.json

local_evidence_mirror_dir() {
  local root="${HPSF_REPLAY_EVIDENCE_LOCAL_ROOT:-}"
  local build_number="${BUILD_NUMBER:-manual}"
  if [[ -n "$root" ]]; then
    printf '%s/%s/%s\n' "${root%/}" "$REPLAY_RUN_NAME" "$build_number"
  fi
}

copy_canonical_artifacts_to_evidence_dir() {
  for artifact in \
    "${HPSF_REPLAY_REPORT:-$ARTIFACT_DIR/hpsf-replay-report-$COMPACT_DATE.md}" \
    "$ARTIFACT_DIR/hpsf-replay-summary.json" \
    "$ARTIFACT_DIR/replay-validation-result.json"; do
    if [[ -s "$artifact" ]]; then
      cp "$artifact" "$EVIDENCE_DIR/"
    fi
  done
}

mirror_evidence_dir() {
  local target
  for target in "$JENKINS_ARCHIVE_EVIDENCE_DIR" "${HPSF_REPLAY_EVIDENCE_MIRROR_DIR:-}" "$(local_evidence_mirror_dir)"; do
    if [[ -z "$target" || "$target" == "$EVIDENCE_DIR" ]]; then
      continue
    fi
    if [[ "$target" == "$JENKINS_ARCHIVE_EVIDENCE_DIR" && ! -d ".replay/options-edge" ]]; then
      continue
    fi
    if ! mkdir -p "$target" 2>/dev/null; then
      echo "Skipping replay evidence mirror; cannot create target: $target"
      continue
    fi
    rm -f "$target"/*.md "$target"/*.json 2>/dev/null || true
    cp "$EVIDENCE_DIR"/*.md "$EVIDENCE_DIR"/*.json "$target"/ 2>/dev/null || true
    echo "Mirrored replay evidence report to: $target"
  done
}

python3 - <<'PY'
import json
import os
from pathlib import Path

artifact_dir = Path(os.environ.get("HPSF_REPLAY_ARTIFACT_DIR", "artifacts"))
manifest_path = Path(os.environ.get("HPSF_REPLAY_ARTIFACT_MANIFEST", "artifact-manifest.json"))
artifacts = []
if artifact_dir.exists():
    for path in sorted(artifact_dir.rglob("*")):
        if path.is_file():
            artifacts.append({
                "fileName": path.name,
                "relativePath": str(path),
            })

job_name = os.environ.get("JOB_NAME", "hpsf-historical-replay")
replay_date = os.environ.get("REPLAY_DATE")
if not replay_date:
    replay_date_id = os.environ.get("HPSF_REPLAY_DATE_ID", "")
    if len(replay_date_id) == 8 and replay_date_id.isdigit():
        replay_date = f"{replay_date_id[0:4]}-{replay_date_id[4:6]}-{replay_date_id[6:8]}"
    else:
        replay_date = "unknown"
compact_date = replay_date.replace("-", "")
replay_run_name = os.environ.get("HPSF_REPLAY_RUN_NAME", f"hpsf-historical-replay-{compact_date}")
build_number = os.environ.get("BUILD_NUMBER", "manual")
jenkins_public_url = os.environ.get("JENKINS_PUBLIC_URL", "http://192.168.100.252:8085").rstrip("/")
job_path = job_name.replace("/", "/job/")
build_url = os.environ.get("BUILD_URL") or f"{jenkins_public_url}/job/{job_path}/{build_number}/"
commit_sha = os.environ.get("GIT_COMMIT") or os.environ.get("CODE_GIT_SHA")
if not commit_sha and Path("build-git-sha.txt").exists():
    commit_sha = Path("build-git-sha.txt").read_text(encoding="utf-8").strip()

manifest = {
    "jenkins": {
        "buildNumber": build_number,
        "buildUrl": build_url,
        "commitSha": commit_sha or "unknown",
        "jobName": job_name,
        "replayRunName": replay_run_name,
    },
    "replayDate": replay_date,
    "replayRunName": replay_run_name,
    "artifacts": artifacts,
}
manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

if [[ ! -f "$ARCHIVER" ]]; then
  cat > "$EVIDENCE_DIR/hpsf-replay-evidence-${REPLAY_DATE:-unknown}-build-${BUILD_NUMBER:-manual}-FAIL.md" <<EOF
# HPSF Replay Evidence Report

Final result: FAIL

## Replay Metadata

- Replay date: \`${REPLAY_DATE:-unknown}\`
- Jenkins build: \`build-${BUILD_NUMBER:-manual}\`
- Source: \`$PWD\`

## Validation Outcome

- Evidence bucket: \`FAIL\`
- Validation failure reasons: \`MISSING_OPTIONS_EDGE_ARCHIVER\`
EOF
  copy_canonical_artifacts_to_evidence_dir
  mirror_evidence_dir
  echo "Replay evidence report generated without archiver: $EVIDENCE_DIR"
  exit 1
fi

set +e
python3 "$ARCHIVER" . "$MANIFEST" "$VALIDATION_FILE" "$EVIDENCE_DIR"
archive_status=$?
set -e
copy_canonical_artifacts_to_evidence_dir
mirror_evidence_dir
exit "$archive_status"
