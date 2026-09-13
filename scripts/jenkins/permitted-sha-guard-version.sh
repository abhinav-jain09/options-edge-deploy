#!/usr/bin/env bash
# Prints the sha256 of the shared guard script next to this file — the value every guarded job
# declares as the default of its PERMITTED_SHA_GUARD_VERSION parameter, and the value the guard
# compares itself against at run time. One implementation of the hash for the guard, the
# downstream compatibility check, the validators and the tests.
set -euo pipefail
f="${1:-$(cd "$(dirname "$0")" && pwd)/permitted-sha-guard.sh}"
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$f" | cut -d' ' -f1
else
  shasum -a 256 "$f" | cut -d' ' -f1
fi
