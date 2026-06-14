#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR="${HPSF_REPLAY_ARTIFACT_DIR:-artifacts}"
EVIDENCE_DIR="${HPSF_REPLAY_EVIDENCE_DIR:-evidence-report}"
MANIFEST="${HPSF_REPLAY_ARTIFACT_MANIFEST:-artifact-manifest.json}"
ARCHIVER=".replay/options-edge/scripts/hpsf/archive_replay_evidence.py"
VALIDATION_FILE="$ARTIFACT_DIR/replay-validation-result.json"

mkdir -p "$ARTIFACT_DIR/logs" "$EVIDENCE_DIR"
rm -f "$EVIDENCE_DIR"/*.md

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

job_name = os.environ.get("JOB_NAME", "hpsf-historical-replay-20260612")
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
    },
    "replayDate": os.environ.get("REPLAY_DATE", "2026-06-12"),
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
  echo "Replay evidence report generated without archiver: $EVIDENCE_DIR"
  exit 1
fi

python3 "$ARCHIVER" . "$MANIFEST" "$VALIDATION_FILE" "$EVIDENCE_DIR"
