#!/usr/bin/env bash
# validate-archive-unit-completeness.sh — every script the archive crontab invokes, and every helper
# those scripts source, must be in the repo AND in the deploy job's UNIT.
#
# WHY: oe-archive-verify.sh — the completeness safety net, written after four prod sessions were lost
# in August 2026 — ran on .252 for a month with no repo copy and no place in UNIT (Bugzilla 356). It
# could not be reviewed, a deploy could not update it, and a hand edit on the host was drift that the
# job's own drift check structurally could not see, because a file outside UNIT is outside every check
# the job makes. The sweep that found it also found oe-trading-day.sh, oe-archive-daily.sh,
# oe-archive-dev.sh and ibkr-raw-retention.sh in the same state — one of which made the verifier exit 0
# having verified nothing.
#
# A missing file here is not a style problem. It is a job that silently does not do its job.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 2
DIR=scripts/ops/archive
JF=Jenkinsfile.archive-scripts-deploy
fails=0
UNIT_LINE="$(grep -m1 '^    UNIT ' "$JF")"

# 1. everything the crontab invokes by path
wanted="$(grep -oE '/home/abhinav/oe-ops/[A-Za-z0-9._-]+\.sh' "$DIR/oe-archive.crontab" | sed 's|.*/||' | sort -u)"
# 2. everything the unit's own scripts source
# The filter used to accept ^oe- and not ^oe_, so oe_corpus_reader.py — the one file BOTH halves import
# — was silently excluded, and the guard passed with it missing from UNIT. A guard that quietly drops the
# thing it is guarding is worse than none. .env files are named too: oe-topics.env and
# calibration-targets.env are read by the unit and must be installed with it.
# `vol-premium` is in the list because of a hole this guard had: oe-vol-premium-open-capture.sh RUNS
# vol-premium-open-reference-capture.py, and that name matched none of the prefixes, so the guard
# reported OK while never considering the one file the new cron entry cannot work without. A guard
# whose subject depends on the spelling of a filename is a guard with a gap per naming convention.
sourced="$(grep -hoE '[A-Za-z0-9._-]+\.(sh|py|env)' "$DIR"/*.sh 2>/dev/null \
           | sed 's|.*/||' | grep -E '^(oe[-_]|calibration|market_|push-validation|test-archive|vol-premium)' | sort -u)"
# 3. every Java source the unit runs through the JDK source launcher (StrikeArchiveReader.java, run by
#    oe-archive-kafka.sh for OE_COMMITTED_READ_TOPICS) — named by a unit script, or simply living in the
#    unit's directory. There is no build step to notice a missing one: the archiver would find no file on
#    the host and fail every committed-read capture, every night.
java_named="$(grep -hoE '[A-Za-z0-9_]+\.java' "$DIR"/*.sh 2>/dev/null | sort -u)"
java_present="$(cd "$DIR" && ls -1 ./*.java 2>/dev/null | sed 's|^\./||' | sort -u)"
sourced="$(printf '%s\n' $sourced $java_named $java_present | awk 'NF' | sort -u)"
# DELIBERATE exclusions, each with the reason it is one. A list like this is only honest if every entry
# has to earn its place — an unexplained name here is how a real gap gets waved through.
#
#   oe-ops.env                    host-only, holds credentials; must never be in the repo
#   calibration-progress-watch.sh runs on the DEV MAC by design (A5.7: a different host and a different
#                                 schedule from the reporter), so it is deliberately not installed on
#                                 .252 — a watchdog sharing its subject's host shares its failures.
#                                 An exemption here is now an OBLIGATION elsewhere: this name was
#                                 excused by a true reason and then checked by nothing, which is how
#                                 the watchdog reached "in git and installed nowhere" (r19 #2). Its
#                                 install path, plist and sourced files are asserted by
#                                 scripts/ci/validate-dev-mac-watchdog.sh, and the deploy job runs it.
#   install-dev-mac-watchdog.sh   the installer for the above, and equally a dev-Mac host action
#   install-dev-mac-watchdog-test.sh  its test; runs in CI, never on .252
EXEMPT="oe-ops.env calibration-progress-watch.sh install-dev-mac-watchdog.sh install-dev-mac-watchdog-test.sh"

# An exemption that no other guard picks up is a hole with a comment on it. Assert the successor
# exists, here, where the excuse is made.
for _guard in scripts/ci/validate-dev-mac-watchdog.sh scripts/ci/verify-archive-unit-staged.sh; do
  [ -f "$_guard" ] || { echo "MISSING GUARD: $_guard — calibration-progress-watch.sh is exempted here on the promise that $_guard checks it" >&2; exit 1; }
done

# AN EXEMPTION THAT NO OTHER GUARD PICKS UP IS A HOLE WITH A COMMENT ON IT, and the same applies to
# a delegation: the staging check above defers the real work to verify-archive-unit-staged.sh, so the
# job must actually run it. Without this, removing that line from the job would leave the staging
# unchecked by anything while both files still looked accounted for.
case "$(tr '\n' ' ' < "$JF")" in
  *"bash scripts/ci/verify-archive-unit-staged.sh"*) : ;;
  *) echo "NOT RUN BY THE DEPLOY JOB: scripts/ci/verify-archive-unit-staged.sh — the staging check here defers to it, so without it in $JF nothing checks that the staged copies are the committed sources" >&2
     fails=$((fails+1)) ;;
esac

for f in $wanted $sourced; do
  case " $EXEMPT " in *" $f "*) continue ;; esac
  # THREE PLACES A UNIT DEPENDENCY MAY LIVE, each because something else owns it there: $DIR for the
  # unit's own files, scripts/jenkins for market_calendar.py (the close-chain jobs own the calendar,
  # and a second committed copy is how two calendars disagree about a holiday), and scripts/ops for
  # vol-premium-open-reference-capture.py (ops tooling, tested from there by
  # tests/test_vol_premium_open_reference_capture.py).
  #
  # A FILE OUTSIDE $DIR MUST BE STAGED INTO IT BY THE DEPLOY JOB, AND THAT IS CHECKED HERE.
  # Allowing another directory without asserting the staging was a real weakening of this guard
  # (review of PR #1125): a future scripts/ops/foo.py named by a unit script would satisfy both the
  # repo test and the UNIT test while the job copied nothing.
  #
  # WHAT THE STAGING IS FOR, precisely, because the guard must not claim more than it checks: the
  # job runs test-archive-reset.sh in a container with ONLY scripts/ops/archive mounted, so a
  # dependency that is not staged there is absent from the suite that is supposed to exercise it —
  # the suite would pass by never running the case. The SHIP is a separate matter and a separate
  # check: the scp sends the committed source paths, and tests/test_jenkins_permitted_sha_guard.py
  # (test_the_archive_ship_is_exactly_the_unit) asserts that list is exactly UNIT.
  # The job stages with a literal `cp <source> scripts/ops/archive/<name>`, so that is what is
  # required to exist, per file, by name.
  # THE COMMITTED LOCATIONS ARE CONSULTED FIRST, and the unit directory LAST. The deploy job stages
  # market_calendar.py and vol-premium-open-reference-capture.py INTO $DIR, and both staged copies
  # are gitignored there — so on a reused CI workspace, or on any machine where the job or the
  # suite has been run once, $DIR holds a copy. Looking there first attributed the file to $DIR,
  # concluded it was a unit file, and skipped the staging requirement entirely: the check was
  # absent exactly where a previous build had left evidence. Found by this guard's own tests, which
  # copy the repository and could not make a removed staging command fail.
  home=""
  for _where in scripts/jenkins scripts/ops "$DIR"; do
    [ -f "$_where/$f" ] && { home="$_where"; break; }
  done
  if [ -z "$home" ]; then
    echo "MISSING FROM THE REPO: $f — the crontab or a unit script names it and nothing tracks it" >&2
    fails=$((fails+1))
    continue
  fi
  if [ "$home" != "$DIR" ]; then
    # THE COPY MUST BE A COMMAND, AND IT MUST RUN BEFORE THE SUITE. Matching the flattened file for
    # the text `cp <source> <dest>` was syntactic and the reviewer was right to refuse it: the same
    # characters in a comment, in a string, or in a stage that runs afterwards would have satisfied
    # it (review round 2). So comment lines are removed first, continuations are joined, and the cp
    # is required to appear BEFORE the docker run that mounts the unit directory into the suite
    # container — which is the ordering the staging exists for. A cp after it stages nothing the
    # suite can see.
    # THE COPY MUST BE THE WHOLE STATEMENT, not text that contains it. Matching `cp <src> <dst>`
    # anywhere on a non-comment line passed for `echo "cp scripts/jenkins/market_calendar.py …"` —
    # the reviewer demonstrated it — and for a Groovy string holding the same words. So the file is
    # normalised to one statement per record, split on newlines and `;` only, and a record must BE
    # the copy.
    #
    # `&&`, `||` and `|` are deliberately NOT separators here, so `false && cp <src> <dst>` is one
    # statement that does not start with cp and is refused. That also refuses a legitimate
    # `something && cp <src> <dst>`, which the job does not use — and if it ever does, this says so
    # and someone updates it, which is the direction to fail in.
    #
    # WHAT THIS CANNOT DO, said plainly because the reviewer was right to press on it twice: prove
    # the copy EXECUTES. `if false; then cp <src> <dst>; fi` satisfies any text rule, a `cp` inside
    # a multiline Groovy string or a heredoc body reads as a statement to the normaliser above, and
    # a workspace reused between builds can hold a copy an earlier build left behind — so even an
    # absent staging step can look like a present one.
    #
    # THE CONTROL IS scripts/ci/verify-archive-unit-staged.sh, which the job runs on the agent after
    # staging and before the container: it asserts each file the suite will read exists and is
    # byte-identical to its committed source. That is the property that matters — a stale copy equal
    # to its source is harmless, and one that differs or is absent fails there — and it is checked
    # by effect in tests/test_archive_unit_completeness_validator.py. This guard is the fast,
    # naming check: it says WHICH file a new dependency forgot, before the build gets that far.
    #
    # The one thing this guard adds that the verifier cannot: the verifier knows the two
    # dependencies it was written for, while this notices a THIRD arriving in the crontab or a unit
    # script, and refuses until the job stages it and the verifier covers it.
    staged=$(awk -v src="$home/$f" -v dst="$DIR/$f" -v mount="$DIR:/w:ro" '
      # Strip Groovy and shell comments, but only where the line STARTS with one: a trailing # inside
      # a quoted string is not a comment, and cutting there would corrupt real commands.
      { line = $0 }
      line ~ /^[[:space:]]*(\/\/|#)/ { next }
      # A backslash continuation belongs to the SAME statement as the line it continues; everything
      # else starts a new one. Appending a newline for both split the two-line cp of the capture
      # into two statements, neither of which was the copy.
      # Keep this program free of apostrophes: it is single-quoted, so one ends it mid-comment.
      {
        continued = (line ~ /\\[[:space:]]*$/)
        gsub(/\\[[:space:]]*$/, "", line)
        joined = joined (carry ? " " : "\n") line
        carry = continued
      }
      END {
        # One statement per record. Only `;` joins the newlines already there.
        gsub(/;/, "\n", joined)
        n = split(joined, statement, "\n")
        want = "cp " src " " dst
        cp_at = 0
        for (i = 1; i <= n; i++) {
          line = statement[i]
          gsub(/[[:space:]]+/, " ", line)
          sub(/^ /, "", line); sub(/ $/, "", line)
          if (line == want && cp_at == 0) { cp_at = i }
          if (index(line, mount) > 0 && mount_at == 0) { mount_at = i }
        }
        if (cp_at == 0)       { print "absent"; exit }
        if (mount_at == 0)    { print "no-suite-mount"; exit }
        if (cp_at > mount_at) { print "too-late"; exit }
        # A staged copy is gitignored where it lands, and verify-permitted-tree.sh refuses any
        # ignored path it was not told to expect. Staging a file without declaring it stops the
        # first REAL install — never a tests-only run, which skips that stage.
        if (index(joined, "--allow-ignored " dst) == 0) { print "ok-but-undeclared"; exit }
        print "ok"
      }' "$JF")
    case "$staged" in
      ok) : ;;
      too-late)
        echo "STAGED TOO LATE: $f lives in $home/ and $JF copies it only AFTER the docker run that mounts $DIR into the suite container — the suite would run without it" >&2
        fails=$((fails+1)) ;;
      ok-but-undeclared)
        echo "STAGED BUT NOT DECLARED: $f is copied into $DIR by $JF, where it is gitignored, but no '--allow-ignored $DIR/$f' is passed to verify-permitted-tree.sh — that verifier refuses any ignored path it was not told to expect, so the first real install would stop at it (and a tests-only run would not, because it skips that stage)" >&2
        fails=$((fails+1)) ;;
      no-suite-mount)
        echo "CANNOT CHECK STAGING: $JF has no docker run mounting $DIR:/w:ro, so the ordering this guard relies on no longer exists — update the guard with the job" >&2
        fails=$((fails+1)) ;;
      *)
        echo "NOT STAGED FOR THE SUITE: $f lives in $home/ and $JF has no 'cp $home/$f $DIR/$f' as a command — the containerised suite mounts only $DIR, so every case that needs $f would be skipped or green for the wrong reason" >&2
        fails=$((fails+1)) ;;
    esac
  fi
  # The first entry is preceded by a quote, not a space, so a bare substring test misses it — strip the
  # quotes and pad both sides before matching, or the very first unit member reads as absent.
  unit_names=" $(printf '%s' "$UNIT_LINE" | tr -d '"' | sed 's/^.*UNIT *= *//') "
  case "$unit_names" in
    *" $f "*) : ;;
    *) echo "NOT IN UNIT: $f — it is in the repo but the deploy job will not install or update it" >&2
       fails=$((fails+1)) ;;
  esac
done
if [ "$fails" -ne 0 ]; then
  echo "=== validate-archive-unit-completeness: $fails problem(s) ===" >&2
  exit 1
fi
echo "checked $(printf '%s\n' $wanted $sourced | sort -u | wc -l | tr -d ' ') archive-unit dependencies"
echo "=== validate-archive-unit-completeness: OK ==="
