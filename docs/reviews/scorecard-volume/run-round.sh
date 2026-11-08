#!/bin/bash
# usage: run-round.sh ROUND  (reads codex-rROUND.prompt, writes .log and .verdict) -- Codex gpt-5.6-terra, report-only
set -u
cd "$(dirname "$0")"
R=$1
codex exec -m gpt-5.6-terra -s read-only -c model_reasoning_effort=medium --skip-git-repo-check -o codex-r$R.verdict - < codex-r$R.prompt > codex-r$R.log 2>&1
echo done $R
