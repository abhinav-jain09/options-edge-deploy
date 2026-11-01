#!/bin/bash
# Proves market.sh agrees with the code's own calendar: compiles app.kafka.SpxSessionCalendar from the processing repo, asks it
# optionsEntryDate() for ~2,100 instants (every open/close second +-60 s, holidays and half days Nov 2026-Jan 2027, 700 random
# instants to Dec 2027, and three past the table), asks market.sh the same with no lead/lag, and compares. Needs java, python3.
# usage: PROCESSING_DIR=/path/to/options-edge-processing ./market-test.sh
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"; : "${PROCESSING_DIR:?set PROCESSING_DIR to a checkout of options-edge-processing}"
T="$(mktemp -d)"; mkdir -p "$T/src" "$T/out/app/kafka"
cp "$PROCESSING_DIR/processing-common/src/main/java/app/kafka/SpxSessionCalendar.java" "$T/src/"
cp "$PROCESSING_DIR/processing-common/src/main/resources/app/kafka/spx-session-calendar.csv" "$T/out/app/kafka/"
cmp -s "$T/out/app/kafka/spx-session-calendar.csv" "$HERE/spx-session-calendar.csv" || { echo "the table copied next to market.sh differs from the code's table: re-copy it"; exit 1; }
cat > "$T/src/Cmp.java" <<'JAVA'
import app.kafka.SpxSessionCalendar; import java.io.*;
public class Cmp { public static void main(String[] a) throws Exception { BufferedReader r=new BufferedReader(new InputStreamReader(System.in)); String l; while((l=r.readLine())!=null){ long ms=Long.parseLong(l.trim())*1000L; boolean sup=SpxSessionCalendar.isSupported(java.time.Instant.ofEpochMilli(ms).atZone(SpxSessionCalendar.ET).toLocalDate()); System.out.println(l.trim()+" "+(!sup?"U":SpxSessionCalendar.optionsEntryDate(ms)!=null?"1":"0")); } } }
JAVA
javac -d "$T/out" "$T/src/SpxSessionCalendar.java" "$T/src/Cmp.java"
python3 - "$T/pts.txt" <<'PY'
import sys,random,datetime as dt
from zoneinfo import ZoneInfo
ET=ZoneInfo('America/New_York'); random.seed(7); pts=set()
at=lambda d,h,m:int(dt.datetime(d.year,d.month,d.day,h,m,tzinfo=ET).timestamp())
d=dt.date(2026,11,15)
while d<=dt.date(2027,1,20):
    for h,m in ((20,15),(9,30),(13,0),(16,0),(12,0),(3,0),(23,59)):
        for off in (-60,0,60): pts.add(at(d,h,m)+off)
    d+=dt.timedelta(days=1)
lo=int(dt.datetime(2026,10,1,tzinfo=ET).timestamp()); hi=int(dt.datetime(2027,12,31,23,tzinfo=ET).timestamp())
for _ in range(700): pts.add(random.randint(lo,hi))
for d in (dt.date(2027,12,30),dt.date(2027,12,31),dt.date(2028,1,2)):
    for h in (3,12,20): pts.add(at(d,h,0))
open(sys.argv[1],'w').write('\n'.join(map(str,sorted(pts)))+'\n')
PY
java -cp "$T/out" Cmp < "$T/pts.txt" > "$T/java.txt"
( export MKT_LEAD_MIN=0 MKT_LAG_MIN=0; . "$HERE/market.sh"
  while read -r e; do market_eval "$e"; case $MKT_STATE in open) r=1;; closed) r=0;; *) r=U;; esac; echo "$e $r"; done < "$T/pts.txt" ) > "$T/bash.txt"
paste -d' ' "$T/java.txt" "$T/bash.txt" | awk '{ if ($2!=$4 || $1!=$3) {bad++; if (bad<=10) print "MISMATCH epoch",$1,"java",$2,"market.sh",$4} n++ } END { print n" instants compared, "(bad+0)" mismatches"; exit (bad>0) }'
