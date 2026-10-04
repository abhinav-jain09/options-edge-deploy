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
    # it (review round 2). So lines that BEGIN with a comment marker are removed, continuations are joined, and the cp
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
    # WHAT A TEXT CHECK CANNOT DO, now that six review rounds have each found another way to write
    # something that looks like the step and is not it: decide whether a line EXECUTES. Line-leading
    # comments, shell operators, redirections, an echo of the verifier, an argument quoted around a
    # space and a heredoc body are each handled, and each was a different form rather than a better
    # version of the same one. A line of identical text in a construct nothing here models would
    # still be read as the step. Only a Groovy-and-shell parser could settle that, and shipping one
    # as a preflight is out of proportion to what its false negative costs.
    #
    # WHAT THIS CHECK IS, in proportion: a PREFLIGHT, and it is the THIRD guard on this step rather
    # than the only one. It exists so a missing declaration is named here, by file, instead of
    # appearing as a refused install.
    #
    # The other two are what make the step itself trustworthy, and they are worth naming because a
    # false negative here is bounded by them rather than by anything in this file:
    #   * scripts/jenkins/validate-jenkinsfile-guard.py — the repository-wide rule that a
    #     source-consuming effect must be immediately preceded by a DEDICATED verify step with
    #     nothing between, and that the step is the verify command and nothing else. It refuses
    #     `sh \047exit 0; <the verifier>\047` — review raised exactly that, where the verifier never
    #     runs, the stage succeeds and the ship proceeds, which would be the gate SKIPPED rather
    #     than refused. tests/test_archive_unit_completeness_validator.py asserts that refusal
    #     against the real job, so the claim here rests on an effect rather than on this comment.
    #   * verify-permitted-tree.sh itself, at install time, on the TREE — which no reading of the
    #     job definition can talk round.
    #
    # So what a false negative in THIS file costs is a declaration that goes unnamed until the
    # install refuses and says why.
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
      # HEREDOC BODIES ARE NOT CODE. A `cat <<"EOF"` whose body holds the whole verifier command
      # printed the text and ran nothing, and this read it as the step (review round 6 of #1128).
      # The opener names its terminator; everything up to that line is skipped.
      heredoc != "" {
        if (line ~ "^[[:space:]]*" heredoc "[[:space:]]*$") { heredoc = "" }
        next
      }
      line ~ /<<-?[[:space:]]*[\047"]?[A-Za-z_][A-Za-z0-9_]*[\047"]?/ {
        tag = line
        sub(/^.*<<-?[[:space:]]*/, "", tag)
        gsub(/[\047"]/, "", tag)
        sub(/[^A-Za-z0-9_].*$/, "", tag)
        if (tag != "") { heredoc = tag }
      }
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
        #
        # THE DECLARATION MUST BE AN ARGUMENT TO THAT VERIFIER, in the same statement that runs it.
        # Looking for the text anywhere in the file was the first version of this and review was
        # right to refuse it: `echo --allow-ignored <path>` satisfied it while the real invocation
        # went without, so the guard could pass and the install still stop. The statement list built
        # above is reused, so the same normalisation applies — comments gone, continuations joined.
        # THE VERIFIER MUST BE THE COMMAND WORD, not a filename appearing somewhere in a statement:
        # `echo scripts/jenkins/verify-permitted-tree.sh --allow-ignored <path>` contains both the
        # name and the declaration and runs no verifier, and that satisfied the previous version
        # (review round 2 of #1128). So each statement is tokenised, a leading `sh`, any
        # VAR=VALUE assignments and a `bash` are stepped over, and what follows has to BE the
        # verifier. Quotes are REMOVED first — not turned into spaces, which split
        # `PERMITTED_SHA="${PERMITTED_SHA:-}"` in two and made this refuse the committed job — so
        # neither the Groovy wrapper nor the assignment quoting hides the command word.
        verifier_at = 0
        for (i = 1; i <= n; i++) {
          bounded = statement[i] " "
          gsub(/[\047"]/, "", bounded)
          gsub(/[[:space:]]+/, " ", bounded)
          sub(/^ /, "", bounded)
          words = split(bounded, word, " ")
          at = 1
          if (words >= 1 && word[at] == "sh") { at++ }
          while (at <= words && word[at] ~ /^[A-Za-z_][A-Za-z0-9_]*=/) { at++ }
          if (at <= words && word[at] == "bash") { at++ }
          if (at > words || word[at] !~ /(^|\/)verify-permitted-tree\.sh$/) { continue }
          verifier_at = i
          # THE ARGUMENT LIST ENDS AT THE FIRST SHELL OPERATOR, and searching past it was the last
          # way round this: `… verify-permitted-tree.sh --dir . && echo --allow-ignored <path>`
          # leaves the verifier undeclared while the text sits in the same statement, and an inline
          # `#` comment does the same (review round 3 of #1128). Only `;` and the newline separate
          # statements above, so the rest is handled here.
          #
          # And the flag and its value must be ADJACENT TOKENS rather than a substring of the line.
          # That is what "an argument to the verifier" means.
          # THE ARGUMENT LIST IS READ WITH THE QUOTES LEFT IN. Removing them is right for finding the
          # command word — it keeps `PERMITTED_SHA="${PERMITTED_SHA:-}"` in one piece — and WRONG
          # here, because it makes
          #   --allow-ignored "scripts/ops/archive/<path> harmless"
          # look like the flag followed by the path, when the shell passes ONE argument containing a
          # space that the real verifier then rejects (review round 4 of #1128). With the quotes
          # kept, that splits into two tokens and neither is the path.
          #
          # The sh step\047s own wrapping quote is removed first, since it is glued to the last
          # argument — `… <path>\047` — and is not part of it. Whatever quoting remains inside an
          # argument is the author\047s, so a value counts only if it IS the path, or the path
          # wrapped in a matched pair. In practice that means the unquoted form the job uses or a
          # DOUBLE-quoted one, since the step is `sh \047…\047` and a single-quoted argument cannot
          # appear inside it; the single-quote case is tolerated here for a step written with
          # triple quotes, and is not something the suite can construct.
          kept = statement[i]
          sub(/^[[:space:]]*/, "", kept)
          if (kept ~ /^sh[[:space:]]+[\047"]/) {
            sub(/^sh[[:space:]]+[\047"]/, "", kept)
            sub(/[\047"][[:space:]]*$/, "", kept)
          }
          gsub(/[[:space:]]+/, " ", kept)
          sub(/^ /, "", kept)
          qwords = split(kept, qword, " ")
          qat = 0
          for (j = 1; j <= qwords; j++) {
            scrubbed = qword[j]
            gsub(/[\047"]/, "", scrubbed)
            if (scrubbed ~ /(^|\/)verify-permitted-tree\.sh$/) { qat = j; break }
          }
          declared = 0
          for (j = qat + 1; qat > 0 && j <= qwords; j++) {
            if (qword[j] == "&&" || qword[j] == "||" || qword[j] == "#" ||
                qword[j] ~ /^\|/ || qword[j] ~ /^[0-9]*>>?/ || qword[j] ~ /^</) { break }
            if (qword[j] != "--allow-ignored" || j >= qwords) { continue }
            value = qword[j + 1]
            if (value == dst) { declared = 1 }
            if (value == "\"" dst "\"") { declared = 1 }
            if (value == "\047" dst "\047") { declared = 1 }
          }
          if (declared) { print "ok"; exit }
        }
        if (verifier_at == 0) { print "no-verifier"; exit }
        print "ok-but-undeclared"
      }' "$JF")
    case "$staged" in
      ok) : ;;
      too-late)
        echo "STAGED TOO LATE: $f lives in $home/ and $JF copies it only AFTER the docker run that mounts $DIR into the suite container — the suite would run without it" >&2
        fails=$((fails+1)) ;;
      ok-but-undeclared)
        echo "STAGED BUT NOT DECLARED: $f is copied into $DIR by $JF, where it is gitignored, but no '--allow-ignored $DIR/$f' is passed to verify-permitted-tree.sh — that verifier refuses any ignored path it was not told to expect, so the first real install would stop at it (and a tests-only run would not, because it skips that stage)" >&2
        fails=$((fails+1)) ;;
      no-verifier)
        echo "CANNOT CHECK THE DECLARATION: $JF has no statement running verify-permitted-tree.sh, so the gate this guard defers to no longer exists — update the guard with the job" >&2
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
