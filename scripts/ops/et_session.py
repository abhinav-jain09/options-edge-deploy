#!/usr/bin/env python3
"""WHICH SESSION a close-chain run is for, decided from the wall clock in New York.

Prints one line: ``<HH> <YYYY-MM-DD>`` — the hour in New York, and the session to freeze.

BEFORE NOON IN NEW YORK a run is the morning CATCH-UP (the second cron in
Jenkinsfile.stockgex-close-board), healing a night the evening chain lost: today's close has not
happened and cannot be frozen, so the session is YESTERDAY'S. From noon on it is today's.

WHY THIS IS PYTHON AND NOT ``date``. It was two lines of shell using ``date -v-1d``, the BSD
spelling, on the reasoning that "the agents are Macs". They are — but what a Mac's ``$PATH``
resolves ``date`` to is not fixed: with GNU coreutils on the path (this agent has it, and the
repo's own apply.sh already relies on the GNU ``date -d``) the BSD flag is a hard error. The
morning catch-up of 2026-09-01 died on exactly that, in build #33, one second in:

    date: invalid option -- 'v'

The failure mode is the expensive kind: the evening run is unaffected, so the chain looks healthy
until the one morning it is actually needed. Nothing in the shell can be trusted to say
"yesterday" portably, so the arithmetic moved to python3 — already a hard dependency of the
wrapper, which validates the session date with it two lines further down.

``--now`` exists ONLY for the tests: an ISO-8601 local instant to answer as if the New York clock
read that. Tests that cannot move the clock end up asserting the implementation back to itself.
"""
from __future__ import annotations

import argparse
import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# Noon: late enough that every catch-up cron (01:00 ET) and any hand re-run of a lost night lands
# before it, early enough that no evening run (16:10 ET) ever does.
CATCHUP_BEFORE_HOUR = 12


def session_for(now: datetime.datetime) -> datetime.date:
    """The session a run happening at ``now`` (New York) is responsible for freezing."""
    if now.hour < CATCHUP_BEFORE_HOUR:
        return now.date() - datetime.timedelta(days=1)
    return now.date()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--now", help="ISO-8601 instant, read as New York local time (tests only)")
    args = ap.parse_args()
    now = (datetime.datetime.fromisoformat(args.now).replace(tzinfo=ET)
           if args.now else datetime.datetime.now(ET))
    print(f"{now.hour:02d} {session_for(now).isoformat()}")


if __name__ == "__main__":
    main()
