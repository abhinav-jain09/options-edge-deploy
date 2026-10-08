#!/usr/bin/env bash
# Which MirrorMaker agents are LOADED RIGHT NOW and producing into a given broker — asked of launchd,
# not of the plist files.
#
#   loaded_mirror_agents_for <bootstrap-host:port>
#     prints one label per line, and returns
#       0  the answer is complete (possibly empty)
#       1  it could not be established — then the output is NOT an answer and the caller must refuse
#
# WHY NOT THE PLISTS. scripts/ops/prod-clean-slate.sh discovers agents by reading
# ~/Library/LaunchAgents/com.optionsedge.*.plist and checking each one with `launchctl list`. That is
# the right way to RECORD what to resume later (the ledger needs the plist path), and the wrong way to
# ask "is anything still producing into the broker I am about to wipe": a job stays loaded when its
# plist is moved or edited, a plist that fails to parse was silently skipped, and a `launchctl list`
# that failed read as "not loaded" (deploy Codex round 8). All three turn a live mirror into a clear
# gate.
#
# So this asks launchd: `launchctl list` for the loaded labels, then `launchctl list <label>` for each
# one, which prints the job as loaded -- including the Program it actually runs. The unit directory is
# that program's directory, exactly as the plist-based discovery derives it, and the agent is producing
# into this broker when the producer.properties beside it says so.
#
# FAILS CLOSED, and the distinction matters: a loaded com.optionsedge.* job whose unit directory has NO
# producer.properties is NOT a mirror (that is how a mirror is identified at all), while one that HAS
# an unreadable producer.properties, or whose program cannot be read out of launchd, is a job this
# cannot classify -- and that is a refusal, not an empty answer.

_lma_launchctl() { command launchctl "$@"; }   # indirection so a test can stub `launchctl`

loaded_mirror_agents_for() {
  local bs="${1-}" table labels label job prog dir props rc
  [ -n "$bs" ] || { echo "loaded_mirror_agents_for: need <bootstrap-host:port>" >&2; return 1; }

  table="$(_lma_launchctl list 2>/dev/null)"; rc=$?
  [ "$rc" -eq 0 ] || { echo "loaded_mirror_agents_for: \`launchctl list\` failed (status $rc)" >&2; return 1; }

  # The table is "PID Status Label"; the label is the last field. Only our own namespace is considered.
  labels="$(printf '%s\n' "$table" | awk 'NF >= 3 { print $NF }' | grep '^com\.optionsedge\.' || true)"
  [ -n "$labels" ] && : || return 0

  while IFS= read -r label; do
    [ -n "$label" ] || continue
    job="$(_lma_launchctl list "$label" 2>/dev/null)"; rc=$?
    if [ "$rc" -ne 0 ]; then
      echo "loaded_mirror_agents_for: cannot read the loaded job $label (status $rc)" >&2
      return 1
    fi
    # `"Program" = "/path";` or the first entry of `"ProgramArguments" = ( "/path"; ... )`.
    prog="$(printf '%s\n' "$job" | sed -nE 's/^[[:space:]]*"Program"[[:space:]]*=[[:space:]]*"(.*)";[[:space:]]*$/\1/p' | head -1)"
    if [ -z "$prog" ]; then
      prog="$(printf '%s\n' "$job" | awk '/"ProgramArguments"/{f=1;next} f&&/"/{gsub(/^[[:space:]]*"|";?[[:space:]]*$/,"");print;exit}')"
    fi
    if [ -z "$prog" ]; then
      echo "loaded_mirror_agents_for: $label is loaded but launchd reports no program for it" >&2
      return 1
    fi
    case "$prog" in /*) : ;; *) echo "loaded_mirror_agents_for: $label runs a non-absolute program ($prog)" >&2; return 1 ;; esac
    dir="$(dirname -- "$prog")"
    props="$dir/producer.properties"
    # Not a mirror at all: no producer config beside the program. (This is the same test the plist-based
    # discovery uses to decide what IS a mirror.)
    [ -e "$props" ] || continue
    if [ ! -r "$props" ]; then
      echo "loaded_mirror_agents_for: $label has an unreadable $props, so where it produces is unknown" >&2
      return 1
    fi
    if grep -qE "^[[:space:]]*bootstrap\.servers[[:space:]]*=[[:space:]]*$(printf '%s' "$bs" | sed 's/[.[\*^$()+?{|]/\\&/g')[[:space:]]*$" "$props"; then
      printf '%s\n' "$label"
    fi
  done <<< "$labels"
}
