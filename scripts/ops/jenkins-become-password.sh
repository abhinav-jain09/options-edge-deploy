#!/usr/bin/env bash
# Executed by Ansible as a password-file helper inside Jenkins withCredentials.
set -euo pipefail
printf '%s\n' "${BECOME_PASSWORD:?Jenkins credential missing}"
