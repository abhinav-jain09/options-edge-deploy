#!/usr/bin/env bash
# prod-cleanup.sh — THE one prod cleanup script. Reclaims disk on the small 70 GB root (/) of .252 —
# the exact thing that caused the 2026-06-30 DiskPressure outage. SAFE + non-destructive:
#   * trims Kafka's log4j logs at /opt/kafka/.../logs (server.log/controller.log grow unbounded on /)
#   * removes any rotated daily log archives older than a day
#   * clears the PackageKit + dnf caches in /var/cache (pure cache, re-downloaded on demand)
# 2026-07-19: the third bullet used to be documented here but was NEVER implemented — the remote script
# only trimmed the log dirs, so /var/cache/PackageKit silently reached 11G (7.7G of it stale cuda-rhel8
# repo metadata) on the 70 GB root while nightly runs reported "0 -> 0" and looked clean. Now real.
# 2026-09-16: the Kafka DATA dir /home/kafka/kraft-combined-logs lives on its OWN filesystem
# (/dev/nvme0n1p1, xfs, 1.9 TB, mounted at /home/kafka) — NOT on /home. It hit 100% that day and the
# broker crash-looped ("all log dirs have failed", ENOSPC) while this script's BEFORE/AFTER lines and
# Discord post only ever showed root(/) and /home, so nobody saw it coming. The script now REPORTS the
# Kafka log dir's own mount (resolved with `findmnt -T`, never assumed) and prints a loud WARN line +
# Discord mention at >= 80 % used. Reporting ONLY: it never deletes Kafka data — retention is an owner
# policy decision (see session memory prod-kafka-disk-full-0916.md).
# Does NOT touch Kafka topic DATA, Streams state, Keycloak, or any k8s workload. Posts result to the
# prod Discord webhook. Kafka logs are owned by the 'kafka' user, so the file work runs as root via
# su (abhinav has no sudo) — this FREES the root disk (removes files from /), per the home-dir rule.
#
# This file is the SAME file as ~/oe-ops/prod-cleanup.sh on the Mac (launchd runs the Mac copy every
# 15 min with `auto`). Keep the two identical; the SSH password and the Discord webhook are read from
# 0600 files next to the Mac copy, never written here.
set -uo pipefail
export PATH=/opt/homebrew/bin:/usr/local/bin:$PATH
OPS=/Users/abhinav/oe-ops
HOST=192.168.100.252
RUSER=abhinav
PW=$(cat "$OPS/.prod-ssh-pw" 2>/dev/null | tr -d '\n')
WEBHOOK=$(cat "$OPS/.prod-discord-webhook" 2>/dev/null)
if [ -z "$PW" ]; then echo "prod-cleanup: $OPS/.prod-ssh-pw missing or empty — cannot reach .252" >&2; exit 2; fi
SSH="sshpass -p $PW ssh -o StrictHostKeyChecking=no -o ConnectTimeout=15 ${RUSER}@${HOST}"

# ---- nightly AUTO gate: launchd fires this every 15 min; run ONCE in the 20:30-20:59 ET window (market
#      closed). Manual `prod-cleanup` (no arg) skips the gate and runs immediately. ET-computed = TZ-proof.
if [ "${1:-}" = "auto" ]; then
  HH=$((10#$(TZ=America/New_York date '+%H%M')))
  MARK=/tmp/.prod-cleanup-$(TZ=America/New_York date '+%Y%m%d')
  { [ "$HH" -ge 2030 ] && [ "$HH" -le 2059 ] && [ ! -f "$MARK" ]; } || exit 0
  : > "$MARK"; export NOTIFY=onchange
fi

# 1. push the remote cleanup script (written under /home/abhinav — allowed by the home-dir rule)
$SSH 'cat > /home/abhinav/.prod-cleanup-remote.sh' <<'REMOTE'
#!/usr/bin/env bash
# Kafka log4j dirs to clean: the stale pre-fix one on ROOT (/opt) + the ACTIVE one on /home.
# Both are kafka-owned; we run as root. Data (/home/kafka/kraft-combined-logs) is NEVER touched.
LOGDIRS="/opt/kafka/kafka_2.13-4.3.0/logs /home/kafka-logs"
OUT=/home/abhinav/.prod-cleanup.out
# Kafka DATA dir — reported, never cleaned. Its filesystem is resolved at run time with findmnt so the
# report follows wherever the dir is actually mounted (own NVMe since 2026-09-16 — see header).
KAFKA_DATA=/home/kafka/kraft-combined-logs
KAFKA_WARN_PCT=80
clean_dir() {
  d="$1"; [ -d "$d" ] || return
  # (a) any current (undated) *.log > 50 MB -> keep last 10 MB, in place (preserves broker's open fd)
  for f in "$d"/*.log; do
    [ -f "$f" ] || continue
    sz=$(stat -c%s "$f" 2>/dev/null || echo 0)
    if [ "$sz" -gt $((50*1024*1024)) ]; then
      b=$(du -h "$f" | cut -f1)
      tail -c $((10*1024*1024)) "$f" > "$f.tmp" 2>/dev/null && cat "$f.tmp" > "$f" && rm -f "$f.tmp"
      echo "trim   $(basename "$f")  ${b} -> $(du -h "$f" | cut -f1)"
    fi
  done
  # (b) delete rotated daily/hourly archives NOT from today (keeps today's incl. the active file) -> 1-day retention
  TODAY=$(date +%Y-%m-%d)
  before=$(du -sh "$d" 2>/dev/null | cut -f1)
  find "$d" -maxdepth 1 -name '*.log.20*' -type f ! -name "*${TODAY}*" -delete 2>/dev/null
  echo "purge  $d  ${before} -> $(du -sh "$d" 2>/dev/null | cut -f1)"
}
# Package caches on ROOT. Both regenerate on demand — no package is installed, removed, or upgraded,
# and nothing in use is touched. PackageKit is a plain cache dir (its metadata is what grows: 11G by
# 2026-07-19); dnf gets cleaned with its own tool. Same "X -> Y" output shape as clean_dir so the
# caller's NOTIFY=onchange reclaim detection sees it.
clean_pkgcache() {
  d=/var/cache/PackageKit
  if [ -d "$d" ]; then
    before=$(du -sh "$d" 2>/dev/null | cut -f1)
    rm -rf "${d:?}"/* 2>/dev/null                      # :? guards against an empty $d -> rm -rf /*
    echo "purge  $d  ${before} -> $(du -sh "$d" 2>/dev/null | cut -f1)"
  fi
  d=/var/cache/dnf
  if [ -d "$d" ]; then
    before=$(du -sh "$d" 2>/dev/null | cut -f1)
    dnf clean all >/dev/null 2>&1
    echo "purge  $d  ${before} -> $(du -sh "$d" 2>/dev/null | cut -f1)"
  fi
}

# The journal is PERSISTENT on prod as of 2026-08-17 (Storage=persistent) so a post-mortem
# survives a reboot — the CPU-saturation outage that day could not be explained because the
# previous boot's journal was gone. Persistence must not become the next disk incident, so
# trim it here too; journald's SystemMaxUse=2G is the hard cap, this is the routine floor.
# Vacuuming never touches the CURRENT boot's active journal, so today's evidence is kept.
clean_journal() {
  command -v journalctl >/dev/null 2>&1 || return 0
  before=$(journalctl --disk-usage 2>/dev/null | grep -oE '[0-9.]+[KMG]' | tail -1)
  journalctl --vacuum-time=14d >/dev/null 2>&1
  journalctl --vacuum-size=2G  >/dev/null 2>&1
  echo "purge  journald  ${before:-?} -> $(journalctl --disk-usage 2>/dev/null | grep -oE '[0-9.]+[KMG]' | tail -1)"
}

# The image builds' staging directories. Production image builds rsync the whole workspace to
# $HOME/ci/remote-builds/<job>-<build>.XXXXXXXX on this host and build there. They used to clear their
# own directory over ssh; that delete was removed (options-edge-processing: a caller-supplied path and a
# same-account rename race made it unsafe from the build side), so the directories now accumulate here —
# full workspaces, on a host that ran out of disk on 2026-09-16. This is the host-owned half of that
# decision: a fixed root, no caller input, age-based, and it never follows a symlink out of the root.
BUILD_STAGING="$HOME/ci/remote-builds"
BUILD_STAGING_KEEP_DAYS=3
clean_build_staging() {
  root="$BUILD_STAGING"
  [ -d "$root" ] || return 0
  if [ -L "$root" ]; then echo "WARN    build staging $root is a symlink — not pruning"; return 0; fi
  root_p=$(cd "$root" 2>/dev/null && pwd -P) || return 0
  case "$root_p" in
    "$(cd "$HOME" && pwd -P)"/ci/remote-builds) : ;;
    *) echo "WARN    build staging resolves to $root_p, outside \$HOME — not pruning"; return 0 ;;
  esac
  before=$(du -sh "$root_p" 2>/dev/null | cut -f1)
  n=0
  while IFS= read -r d; do
    [ -n "$d" ] || continue
    [ -L "$d" ] && continue                      # never follow a symlink out of the root
    case "$d" in "$root_p"/?*) : ;; *) continue ;; esac
    case "${d#"$root_p"/}" in */*) continue ;; esac   # exactly one level down
    OE_ALLOW_RECURSIVE_RM=1 rm -rf -- "$d" && n=$((n+1))
  done <<EOF
$(find "$root_p" -mindepth 1 -maxdepth 1 -type d -mtime +$BUILD_STAGING_KEEP_DAYS 2>/dev/null)
EOF
  echo "purge  build-staging  ${before:-?} -> $(du -sh "$root_p" 2>/dev/null | cut -f1) (${n} dir(s) older than ${BUILD_STAGING_KEEP_DAYS}d)"
}

# ---- disk REPORTING (no cleanup below this line) -------------------------------------------------
# usage_of PATH -> "used/size pct%" of the filesystem PATH lives on. -P keeps df on one line even
# when the device name is long (the bare `df -h` wraps and NR==2 would then print garbage).
usage_of() { df -hP "$1" 2>/dev/null | awk 'NR==2{print $3"/"$2" "$5}'; }
# pct_of PATH -> integer Use% (no sign) of the filesystem PATH lives on; empty if df fails.
pct_of() { df -P "$1" 2>/dev/null | awk 'NR==2{sub("%","",$5); print $5}'; }
# kafka_mount -> the mount point the Kafka data dir actually lives on, via findmnt -T (falls back to
# df's "Mounted on" column when findmnt is unavailable). Empty when the dir does not exist at all.
kafka_mount() {
  [ -e "$KAFKA_DATA" ] || return 0
  m=$(findmnt -T "$KAFKA_DATA" -n -o TARGET 2>/dev/null | head -1)
  [ -n "$m" ] || m=$(df -P "$KAFKA_DATA" 2>/dev/null | awk 'NR==2{print $6}')
  printf '%s' "$m"
}
# report_disks BEFORE|AFTER -> one line: root(/), home, AND the Kafka data mount (labelled by its
# resolved mount point so a remount elsewhere is visible in the report, not silently folded into /home).
report_disks() {
  km=$(kafka_mount)
  if [ -n "$km" ]; then kafka="kafka($km) $(usage_of "$km")"; else kafka="kafka(?) MISSING $KAFKA_DATA"; fi
  printf '%-7s root(/) %s   home %s   %s\n' "$1" "$(usage_of /)" "$(usage_of /home)" "$kafka"
}
# warn_kafka -> prints a loud WARN line when the Kafka data mount is >= KAFKA_WARN_PCT % used (or is
# missing). Reporting only — nothing is deleted here; the Mac side turns a WARN into a Discord mention.
warn_kafka() {
  km=$(kafka_mount)
  if [ -z "$km" ]; then
    echo "WARN    kafka data dir $KAFKA_DATA is MISSING — broker cannot be running from it (unmounted?)"
    return 0
  fi
  p=$(pct_of "$km")
  case "$p" in ''|*[!0-9]*) echo "WARN    kafka($km) usage unreadable (df gave '${p:-}')"; return 0;; esac
  if [ "$p" -ge "$KAFKA_WARN_PCT" ]; then
    echo "WARN    kafka($km) ${p}% used >= ${KAFKA_WARN_PCT}% — broker crash-loops at 100% (2026-09-16); NOT cleaned here, retention is an owner decision"
  fi
}
# Run the cleanup only when executed (nohup bash .prod-cleanup-remote.sh); `source` just loads the
# functions so the reporting can be unit-tested with a fake df/findmnt on PATH.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
{
  echo "=== prod cleanup $(date '+%F %T %Z') ==="
  report_disks BEFORE
  for d in $LOGDIRS; do clean_dir "$d"; done
  clean_pkgcache
  clean_journal
  clean_build_staging
  warn_kafka
  report_disks AFTER
} > "$OUT" 2>&1
chmod 644 "$OUT"
fi
REMOTE

# 2. FIRE the cleanup as ROOT (su via expect pty — abhinav has no sudo), launched in the background so
#    we never depend on fragile prompt-matching for completion. Clear the old result file first.
$SSH 'rm -f /home/abhinav/.prod-cleanup.out' >/dev/null 2>&1
PW="$PW" HOST="$HOST" RUSER="$RUSER" expect <<'EXP' >/dev/null 2>&1
set timeout 15
set pw $env(PW); set host $env(HOST); set ruser $env(RUSER)
spawn sshpass -p $pw ssh -tt -o StrictHostKeyChecking=no $ruser@$host
expect { -re {\$ $} {} -re {# $} {} timeout {} }
send "su -\r"
expect { -nocase assword {} timeout {} }
send "$pw\r"
expect { -re {# $} {} timeout {} }
send "nohup bash /home/abhinav/.prod-cleanup-remote.sh >/dev/null 2>&1 & disown\r"
expect { -re {# $} {} timeout {} }
send "exit\r"; expect { -re {\$ $} {} timeout {} eof {} }
send "exit\r"; expect { eof {} timeout {} }
EXP

# 3. POLL for the result file (written at the END of the remote script -> contains an 'AFTER' line)
RESULT=""
for i in $(seq 1 20); do
  sleep 3
  RESULT=$($SSH 'cat /home/abhinav/.prod-cleanup.out 2>/dev/null')
  printf '%s\n' "$RESULT" | grep -q '^AFTER' && break
done
[ -z "$RESULT" ] && RESULT="(no output captured — check .252 manually)"
echo "$RESULT"

# 4. post to prod Discord. NOTIFY=onchange (set by the nightly launchd) posts ONLY when something was
#    actually reclaimed; default (manual runs) always posts. A line "reclaimed" if its  X -> Y  differ.
#    A WARN line (Kafka data mount >= 80 % / missing) ALWAYS posts, with an @here mention and an orange
#    embed — the 2026-09-16 disk-full went unseen precisely because the nightly post was silent.
CHANGED=$(printf '%s\n' "$RESULT" | grep ' -> ' | while read -r ln; do
  b=$(printf '%s' "$ln" | sed 's/.* \([^ ]*\) -> .*/\1/'); a=$(printf '%s' "$ln" | sed 's/.* -> \([^ ]*\).*/\1/')
  [ "$b" != "$a" ] && echo X
done | grep -c X)
WARNS=$(printf '%s\n' "$RESULT" | grep '^WARN')
if [ "${NOTIFY:-always}" = "onchange" ] && [ "$CHANGED" -eq 0 ] && [ -z "$WARNS" ]; then
  echo "nothing reclaimed, no WARN — skipping Discord post (NOTIFY=onchange)"
else
  if [ -n "$WARNS" ]; then
    TITLE="⚠️ PROD cleanup (.252) — Kafka disk WARN"; COLOR=15105570; CONTENT="@here $WARNS"
    echo "$WARNS" >&2
  else
    TITLE="🧹 PROD cleanup (.252)"; COLOR=3066993; CONTENT=""
  fi
  json=$(BODY="$RESULT" TITLE="$TITLE" COLOR="$COLOR" CONTENT="$CONTENT" python3 -c 'import json,os;d={"username":"OptionsEdge PROD Cleanup","embeds":[{"title":os.environ["TITLE"],"description":os.environ["BODY"][:3900],"color":int(os.environ["COLOR"])}]};c=os.environ["CONTENT"].strip();c and d.update(content=c[:1900]);print(json.dumps(d))')
  [ -n "$WEBHOOK" ] && curl -s -m10 -H 'Content-Type: application/json' -d "$json" "$WEBHOOK" >/dev/null 2>&1 && echo "posted to prod Discord"
fi
