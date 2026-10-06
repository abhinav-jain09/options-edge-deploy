#!/usr/bin/env bash
# ansible/vars/es-mirrors.yml and ansible/templates/* are a SECOND generator for files the mirror
# Jenkinsfiles already generate. Two generators for one artifact drift — silently, because the
# Ansible path is used when nobody is watching a Jenkins console. This gate fails the build when they
# stop agreeing on the mechanical facts:
#
#   1. the producer stanza, BYTE for byte (the template is the pipelines' stanza with only the
#      bootstrap line templated)
#   2. per pipeline: the consumer group.id formula, auto.offset.reset, and isolation.level
#   3. per pipeline: the topic allow-list
#   4. per pipeline: whether the unit name carries the topic
#
# What it does NOT check, stated rather than implied: the per-end SHAPE assertions. Those live in each
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

    tokens = {"ip": "${TARGET_SPX_CLUSTER_IP}", "port": "${TARGET_PORT}", "topic": "${TOPIC}"}
    want_gid = pipe + "-" + "-".join(tokens[t] for t in re.findall(r'\w+', parts or ""))
    if cmap.get("group.id") != want_gid:
        fail.append(f"{jf}: group.id is {cmap.get('group.id')!r} but the table's group_parts {parts} "
                    f"compose {want_gid!r} — a wrong group is a NEW mirror that re-reads from "
                    f"auto.offset.reset")
    if cmap.get("auto.offset.reset") != offset:
        fail.append(f"{jf}: auto.offset.reset={cmap.get('auto.offset.reset')!r}, table says {offset!r}")
    if cmap.get("isolation.level", "") != iso:
        fail.append(f"{jf}: isolation.level={cmap.get('isolation.level', '')!r}, table says {iso!r}")

    # ---- 3. the topic allow-list ----
    m = re.search(r"choice\(name: 'TOPIC', choices: \[(.*?)\]", j, re.S)
    if not m:
        fail.append(f"{jf}: no TOPIC choice parameter — this gate cannot compare the allow-list")
    else:
        want_topics = re.findall(r"'([^']+)'", m.group(1))
        if sorted(want_topics) != sorted(topics):
            fail.append(f"{jf}: TOPIC choices {sorted(want_topics)} != table topics {sorted(topics)}")

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
      "on the producer stanza, group.id, offset reset, isolation level, topic allow-list and unit naming")
PY
