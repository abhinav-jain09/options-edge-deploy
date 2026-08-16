#!/usr/bin/env bash
# U16 (CL-R10 L4/O3): promtool check + test over the declared paging rules. Runs anywhere —
# uses promtool from PATH when present, otherwise downloads the pinned prometheus release
# (sha256-verified) into a cache dir. This is what lets the same script be a mandatory step on
# GitHub-hosted CI runners AND run on a workstation with nothing preinstalled.
set -euo pipefail
cd "$(dirname "$0")/../.."

PROM_VERSION="2.55.1"
resolve_promtool() {
  if command -v promtool >/dev/null 2>&1; then
    echo "promtool"
    return
  fi
  local os arch sha
  case "$(uname -s)" in
    Linux) os="linux" ;;
    Darwin) os="darwin" ;;
    *) echo "unsupported OS $(uname -s): install promtool on PATH" >&2; exit 1 ;;
  esac
  case "$(uname -m)" in
    x86_64) arch="amd64" ;;
    arm64|aarch64) arch="arm64" ;;
    *) echo "unsupported arch $(uname -m): install promtool on PATH" >&2; exit 1 ;;
  esac
  case "$os-$arch" in
    linux-amd64)  sha="19700bdd42ec31ee162e4079ebda4cd0a44432df4daa637141bdbea4b1cd8927" ;;
    darwin-arm64) sha="551d557d8b4e5e5ed4fdbfea9622034f6e9ee760986d876736ced885b764bae9" ;;
    darwin-amd64) sha="ba915f45b680566646fc824f2b9793dbcd2741c157a5a599be2cb4665b38b498" ;;
    *) echo "no pinned checksum for $os-$arch: install promtool on PATH" >&2; exit 1 ;;
  esac
  local cache="${PROMTOOL_CACHE_DIR:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}/promtool-$PROM_VERSION-$os-$arch}"
  local bin="$cache/promtool"
  if [[ ! -x "$bin" ]]; then
    mkdir -p "$cache"
    local dist="prometheus-$PROM_VERSION.$os-$arch"
    local tarball="$cache/$dist.tar.gz"
    curl -fsSL --retry 3 -o "$tarball" \
      "https://github.com/prometheus/prometheus/releases/download/v$PROM_VERSION/$dist.tar.gz"
    echo "$sha  $tarball" | shasum -a 256 -c - >/dev/null
    tar -xzf "$tarball" -C "$cache" --strip-components=1 "$dist/promtool"
    rm -f "$tarball"
  fi
  echo "$bin"
}

PROMTOOL="$(resolve_promtool)"
"$PROMTOOL" --version >/dev/null

cd scripts/monitoring
"$PROMTOOL" check rules cvd-spx-levels-alerts.yaml
"$PROMTOOL" test rules cvd-spx-levels-alerts-test.yaml
echo "=== validate-cvd-levels-alerts: OK ==="
