#!/usr/bin/env bash
# jenkins-job-path.sh <simple-child-job>
#   — THE resolver of a downstream job's identity, shared by every script that addresses a child the
#     caller schedules with `build job: '<simple-child-job>'`.
#
# Jenkins resolves a simple job name in `build job:` RELATIVE TO THE CALLER'S FOLDER: from
# `folder/caller`, `build job: 'child'` schedules `folder/child`. So everything that inspects the
# child — its registered parameters, its SCM definition (require-guarded-downstream.sh), the artifact
# of the build it produced (fetch-permitted-image-lock.sh) — must address that same job, or it would
# judge one job and act on another. One implementation, so the two can never disagree.
#
# stdout, two lines:
#   full=<folder/child>          the job's full name
#   path=/job/<folder>/job/<child>   its URL path under JENKINS_URL
# Refuses (non-zero) a child that is not a simple name, and any name segment outside [A-Za-z0-9._-]
# (a segment needing URL encoding is refused, never guessed).
set -euo pipefail
child="${1:?usage: jenkins-job-path.sh <simple-child-job>}"
case "$child" in
  *[!A-Za-z0-9._-]*|.|..)
    echo "jenkins-job-path: REFUSED — child job '$child' must be a simple job name ([A-Za-z0-9._-]); it is resolved relative to this job's folder, as build job: does" >&2
    exit 1 ;;
esac
folder=""
case "${JOB_NAME:-}" in
  */*) folder="${JOB_NAME%/*}" ;;
esac
full="${folder:+$folder/}$child"
path=""
IFS='/' read -r -a segs <<< "$full"
for s in "${segs[@]}"; do
  case "$s" in
    ''|*[!A-Za-z0-9._-]*|.|..)
      echo "jenkins-job-path: REFUSED — folder segment '$s' of '$full' is not a plain name" >&2
      exit 1 ;;
  esac
  path="$path/job/$s"
done
printf 'full=%s\npath=%s\n' "$full" "$path"
