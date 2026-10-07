#!/usr/bin/env bash
# ansible/vars/es-mirrors.yml and ansible/templates/* are a SECOND generator for files the mirror
# Jenkinsfiles already generate. Two generators for one artifact drift — silently, because the
# Ansible path is used when nobody is watching a Jenkins console. This gate fails the build when they
# stop agreeing on the mechanical facts:
#
#   1. the producer stanza, BYTE for byte (the template is the pipelines' stanza with only the
#      bootstrap line templated) — and that line's DIRECTION: the producer must publish to the
#      target and the consumer must read the source, a swap a line-dropping comparison cannot see
#   2. per pipeline: the consumer group.id formula, auto.offset.reset, and isolation.level
#   3. per pipeline: the topic allow-list
#   4. per pipeline: whether the unit name carries the topic
#   5. the generated run-mirror.sh, BYTE for byte after substituting $MDIR/$KBIN/'$RE' — arguments,
#      not just flag names, because --num.streams 2 or a broadened --whitelist keeps the sequence
#      while changing what is replicated — including the --offset.commit.interval.ms each pipeline
#      passes (auction and tape-zones pass 5000, the others none, and MM1's default is 60000)
#   6. log4j.properties, byte for byte
#   7. the whole generated consumer.properties as a key/value MAP, so a key added or dropped on
#      either side fails — not only the three somebody thought to check
#   8. the launchd plist, byte for byte after substituting $LABEL and $MDIR
#
# What it does NOT check, stated rather than implied, and this is the whole list: the per-end SHAPE
# assertions. Those live in each
# pipeline as shell, each written differently (an exact-string compare here, an effective-value read
# there, a reconcile somewhere else), and the table records them per end with a comment naming the
# pipeline's own behaviour. A reviewer changing a pipeline's shape assertion must update the table by
# hand; this gate cannot see that one.
set -euo pipefail
cd "$(dirname "$0")/../.."
python3 - "$@" <<'PY'
import re, sys, pathlib

fail = []
tbl = pathlib.Path("ansible/vars/es-mirrors.yml").read_text()
tpl = pathlib.Path("ansible/templates/mirror-producer.properties.j2").read_text()
cons_tpl = pathlib.Path("ansible/templates/mirror-consumer.properties.j2").read_text()
run_tpl = pathlib.Path("ansible/templates/run-mirror.sh.j2").read_text()
log4j_tpl = pathlib.Path("ansible/templates/mirror-log4j.properties.j2").read_text()
plist_tpl = pathlib.Path("ansible/templates/mirror.plist.j2").read_text()

# ---- 1a. the two CONDITIONS, literally ----
# The checks below strip Jinja tags symbolically rather than rendering them, so a weakened condition
# — {% if item.isolation_level and false %}, or the same trick on the commit interval — would pass
# every comparison while the rendered unit silently LOSES that setting. Both conditions are therefore
# pinned as exact text; changing one has to be a deliberate edit here too.
EXPECT_COND = {
    "ansible/templates/mirror-consumer.properties.j2": "{% if item.isolation_level | string | length > 0 %}",
    "ansible/templates/run-mirror.sh.j2": "{% if item.commit_interval | string | length > 0 %}",
}
for path, cond in EXPECT_COND.items():
    text = pathlib.Path(path).read_text()
    found = re.findall(r'\{%\s*if[^%]*%\}', text)
    if found != [cond]:
        fail.append(f"{path}: expected exactly one condition, {cond!r} — a weakened or extra "
                    f"condition drops a live setting while every other check still passes. Found {found!r}")

# ---- 1b. the DIRECTION of both bootstrap lines ----
if tpl.split("\n")[0] != "bootstrap.servers={{ item.target }}":
    fail.append("ansible/templates/mirror-producer.properties.j2: line 1 must be "
                "'bootstrap.servers={{ item.target }}' — the producer PUBLISHES to the target; "
                f"found {tpl.split(chr(10))[0]!r}")
if cons_tpl.split("\n")[0] != "bootstrap.servers={{ item.source }}":
    fail.append("ansible/templates/mirror-consumer.properties.j2: line 1 must be "
                "'bootstrap.servers={{ item.source }}' — the consumer READS the source; "
                f"found {cons_tpl.split(chr(10))[0]!r}")
for want in ("group.id={{ item.group_id }}", "auto.offset.reset={{ item.offset_reset }}",
             "isolation.level={{ item.isolation_level }}"):
    if want not in cons_tpl:
        fail.append(f"ansible/templates/mirror-consumer.properties.j2 does not carry {want!r}")

PIPELINES = {
    "es-cvd-mirror": "Jenkinsfile.es-cvd-mirror",
    "es-indicator-mirror": "Jenkinsfile.es-indicator-mirror",
    "es-auction-mirror": "Jenkinsfile.es-auction-mirror",
    "es-tape-zones-mirror": "Jenkinsfile.es-tape-zones-mirror",
    "es-futures-flow-mirror": "Jenkinsfile.es-futures-flow-mirror",
    "es-strike-intel-mirror": "Jenkinsfile.es-strike-intel-mirror",
}

def stanza(text, name, term):
    m = re.search(r'cat > "\$MDIR/%s" <<%s\n(.*?)\n%s\n' % (re.escape(name), term, term), text, re.S)
    return m.group(1).split("\n") if m else None

# ---- 1. the producer stanza, byte for byte ----
for pipe, jf in PIPELINES.items():
    body = stanza(pathlib.Path(jf).read_text(), "producer.properties", "P")
    if body is None:
        fail.append(f"{jf}: no producer.properties heredoc — this gate cannot compare it")
        continue
    want = body[1:]                                   # drop bootstrap.servers=$TGT
    got = tpl.rstrip("\n").split("\n")[1:]            # drop the templated bootstrap line
    if got != want:
        fail.append(f"{jf}: the producer stanza differs from ansible/templates/mirror-producer.properties.j2")
        for i, (a, b) in enumerate(zip(want, got)):
            if a != b:
                fail.append(f"    first difference at line {i+2}: jenkinsfile={a!r} template={b!r}")
                break
        if len(want) != len(got):
            fail.append(f"    lengths differ: jenkinsfile={len(want)} template={len(got)}")

# ---- the table, parsed from the fields this gate needs ----
blocks = re.split(r'\n  - pipeline: ', "\n" + tbl)[1:]
if len(blocks) != len(PIPELINES):
    fail.append(f"ansible/vars/es-mirrors.yml: parsed {len(blocks)} pipeline block(s), expected {len(PIPELINES)} — "
                "the table's shape changed and this gate can no longer read it")

for blk in blocks:
    pipe = blk.split("\n", 1)[0].strip()
    jf = PIPELINES.get(pipe)
    if not jf:
        fail.append(f"ansible/vars/es-mirrors.yml declares pipeline {pipe!r}, which this gate does not know")
        continue
    j = pathlib.Path(jf).read_text()

    def field(key):
        m = re.search(r'^\s{4}%s:\s*(.+?)\s*$' % key, blk, re.M)
        return m.group(1).strip().strip('"\'') if m else None

    parts = field("group_parts")
    offset = field("offset_reset")
    iso = field("isolation_level") or ""
    slug_with_topic = (field("slug_with_topic") == "true")
    topics = re.findall(r'^\s+- name: (\S+)\s*$', blk, re.M)

    # ---- 2. group.id / auto.offset.reset / isolation.level, from the pipeline's own heredoc ----
    cons = stanza(j, "consumer.properties", "C")
    if cons is None:
        fail.append(f"{jf}: no consumer.properties heredoc — this gate cannot compare it")
        continue
    cmap = {}
    for line in cons:
        if line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cmap[k.strip()] = v.strip()

    # the DIRECTION on the Jenkins side too: asserting only the template's fixed first line left a
    # pipeline free to publish into its own source, which the template could not contradict
    prod = stanza(j, "producer.properties", "P")
    if prod is None or prod[0] != "bootstrap.servers=$TGT":
        fail.append(f"{jf}: the producer stanza's first line must be 'bootstrap.servers=$TGT' — a "
                    f"producer pointed at the SOURCE mirrors a topic into itself; found "
                    f"{(prod[0] if prod else None)!r}")
    if cmap.get("bootstrap.servers") != "$SRC":
        fail.append(f"{jf}: the consumer's bootstrap.servers must be '$SRC' — a consumer pointed at "
                    f"the TARGET mirrors the target back onto itself; found "
                    f"{cmap.get('bootstrap.servers')!r}")

    tokens = {"ip": "${TARGET_SPX_CLUSTER_IP}", "port": "${TARGET_PORT}", "topic": "${TOPIC}"}
    want_gid = pipe + "-" + "-".join(tokens[t] for t in re.findall(r'\w+', parts or ""))
    if cmap.get("group.id") != want_gid:
        fail.append(f"{jf}: group.id is {cmap.get('group.id')!r} but the table's group_parts {parts} "
                    f"compose {want_gid!r} — a wrong group is a NEW mirror that re-reads from "
                    f"auto.offset.reset")
    # the WHOLE consumer map, not three spot checks: the template's expressions are resolved
    # symbolically to what the pipeline writes, and then the two maps must be equal — so a key added
    # or dropped on either side fails, not just the three somebody thought to check
    resolved_cons = (cons_tpl
                     .replace("{{ item.source }}", "$SRC")
                     .replace("{{ item.group_id }}", want_gid)
                     .replace("{{ item.offset_reset }}", offset or "")
                     .replace("{{ item.isolation_level }}", iso))
    tmap = {}
    for line in resolved_cons.split("\n"):
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("{%") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        tmap[k.strip()] = v.strip()
    if iso == "":
        tmap.pop("isolation.level", None)
    if tmap != cmap:
        only_j = {k: v for k, v in cmap.items() if tmap.get(k) != v}
        only_t = {k: v for k, v in tmap.items() if cmap.get(k) != v}
        fail.append(f"{jf}: the generated consumer.properties does not match the template: "
                    f"jenkinsfile-only/differing={only_j} template-only/differing={only_t}")

    # ---- 3. the topic allow-list ----
    m = re.search(r"choice\(name: 'TOPIC', choices: \[(.*?)\]", j, re.S)
    if not m:
        fail.append(f"{jf}: no TOPIC choice parameter — this gate cannot compare the allow-list")
    else:
        want_topics = re.findall(r"'([^']+)'", m.group(1))
        if sorted(want_topics) != sorted(topics):
            fail.append(f"{jf}: TOPIC choices {sorted(want_topics)} != table topics {sorted(topics)}")

    # ---- 5. the generated run-mirror.sh, byte for byte after token substitution ----
    # Flag NAMES are not the contract: --num.streams 2 or a broadened --whitelist keeps the same
    # sequence while changing what gets replicated. So the pipeline's generated line is reconstructed
    # (its heredoc is UNQUOTED, so the shell removes each backslash-newline and writes ONE physical
    # line), its $MDIR/$KBIN are replaced by the template's expressions and its '$RE' by the
    # template's whitelist expression, and the whole text is compared.
    runner = stanza(j, "run-mirror.sh", "S")
    if runner is None:
        fail.append(f"{jf}: no run-mirror.sh heredoc — this gate cannot compare the runner")
    else:
        joined = "\n".join(runner).replace("\\\\\n", "")
        commit = field("commit_interval_ms")
        m2 = re.search(r'--offset\.commit\.interval\.ms (\d+)', joined)
        want_commit = m2.group(1) if m2 else None
        if (commit or None) != want_commit:
            fail.append(f"{jf}: passes --offset.commit.interval.ms {want_commit!r} but the table says "
                        f"commit_interval_ms={commit!r} — MM1's default is 60000, so a dropped value "
                        f"installs a silently slower commit and a wrong one changes the install proof")
        # the template, with its conditional arm resolved the way this pipeline uses it
        tpl_with = re.sub(r'{%[^%]*%}', '', run_tpl)
        tpl_without = re.sub(r'{% if item\.commit_interval.*?{% endif %}', '', run_tpl, flags=re.S)
        if tpl_with == tpl_without:
            fail.append("ansible/templates/run-mirror.sh.j2: --offset.commit.interval.ms must stay "
                        "inside an {% if item.commit_interval %} block — hardcoded it reaches "
                        "pipelines that pass none, deleted it is lost where they do")
        resolved = (tpl_with if want_commit else tpl_without)
        if want_commit:
            resolved = resolved.replace("{{ item.commit_interval }}", want_commit)
        # The whitelist is PINNED, not substituted away. The comparison below replaces the
        # pipeline's '$RE' with the template's token so the rest of the line can be compared — which
        # by construction makes any template whitelist match, so broadening it to 'es\.futures\..*'
        # (every topic on the source into this one target) passed. The token must therefore equal the
        # single-topic escape exactly, and that is asserted here, separately.
        EXPECT_WL = "'{{ item.topic | regex_replace('\\.', '\\\\.') }}'"
        wl = re.search(r"--whitelist (.*?) --num\.streams", resolved, re.S)
        if not wl or wl.group(1) != EXPECT_WL:
            fail.append("ansible/templates/run-mirror.sh.j2: --whitelist must be exactly "
                        f"{EXPECT_WL} — one topic, dots escaped. A broader pattern mirrors topics "
                        f"this unit was never installed for. Found {(wl.group(1) if wl else None)!r}")
        want = (joined
                .replace("$MDIR", "{{ item.mdir }}")
                .replace("$KBIN", "{{ oe_kbin }}"))
        if wl:
            want = re.sub(r"--whitelist '\$RE'", "--whitelist " + wl.group(1).replace("\\", "\\\\"), want)
        else:
            fail.append("ansible/templates/run-mirror.sh.j2: no --whitelist ... --num.streams in the runner")
        if want.strip() != resolved.strip():
            fail.append(f"{jf}: the generated run-mirror.sh differs from "
                        f"ansible/templates/run-mirror.sh.j2 (arguments, not just flag names)")
            for i, (x, y) in enumerate(zip(want.strip().split("\n"), resolved.strip().split("\n"))):
                if x != y:
                    fail.append(f"    first difference at line {i+1}:\n      jenkinsfile={x!r}\n      template={y!r}")
                    break

    # ---- 5b. log4j.properties, byte for byte ----
    l4 = stanza(j, "log4j.properties", "L")
    if l4 is None:
        fail.append(f"{jf}: no log4j.properties heredoc — this gate cannot compare it")
    elif "\n".join(l4).strip() != log4j_tpl.strip():
        fail.append(f"{jf}: log4j.properties differs from ansible/templates/mirror-log4j.properties.j2")

    # ---- 6. the plist, byte for byte after token substitution ----
    pl = stanza(j, "", "PL")
    if pl is None:
        m3 = re.search(r'cat > "\$PLIST" <<PL\n(.*?)\nPL\n', j, re.S)
        pl = m3.group(1).split("\n") if m3 else None
    if pl is None:
        fail.append(f"{jf}: no plist heredoc — this gate cannot compare it")
    else:
        want_pl = "\n".join(pl).replace("$LABEL", "{{ item.label }}").replace("$MDIR", "{{ item.mdir }}")
        if want_pl.strip() != plist_tpl.strip():
            fail.append(f"{jf}: the plist differs from ansible/templates/mirror.plist.j2")
            for i, (x, y) in enumerate(zip(want_pl.split("\n"), plist_tpl.split("\n"))):
                if x != y:
                    fail.append(f"    first difference at line {i+1}: jenkinsfile={x!r} template={y!r}")
                    break

    # ---- 4. does the unit name carry the topic? ----
    m = re.search(r'^\s+MDIR\s+=\s+"(.*)"\s*$', j, re.M)
    if not m:
        fail.append(f"{jf}: no MDIR definition — this gate cannot compare the unit name")
    else:
        jf_has_topic = "params.TOPIC" in m.group(1)
        if jf_has_topic != slug_with_topic:
            fail.append(f"{jf}: MDIR {'carries' if jf_has_topic else 'does not carry'} the topic, "
                        f"table says slug_with_topic={str(slug_with_topic).lower()} — the unit, label and "
                        f"plist names would not match the installed ones")

if fail:
    print("=== validate-ansible-mirror-parity: FAILED ===")
    for f in fail:
        print(f)
    sys.exit(1)
print(f"=== validate-ansible-mirror-parity: OK === {len(PIPELINES)} pipelines agree with ansible/vars/es-mirrors.yml "
      "on both bootstrap DIRECTIONS at both ends, all four generated files (producer, consumer, log4j "
      "and the runner, with its arguments), the plist, the topic allow-list and unit naming")
PY
