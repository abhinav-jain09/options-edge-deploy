#!/usr/bin/env python3
"""The 0DTE VIRGIN attestation and provisioning files, judged with the Job's own rules (options-edge-processing increment 7a:
VirginAttestation / ProvisioningFile), in stdlib Python — so a PR is refused BEFORE the Job would refuse it, and an append-only
change is enforced where the Job cannot (it only reads the file).

The chain hash is sha256 over the canonical TLV encoding of the COMPLETE preceding entry (design §7.1 / increment 7 Q5):
    MAP frame  = 0x07 | len(8 bytes big-endian) | INT frame(count) | for each key in UTF-8 byte order: STRING frame(key) STRING frame(value)
    INT frame  = 0x02 | len | decimal ASCII;   STRING frame = 0x04 | len | UTF-8
This port is held to the Java implementation by the golden vectors in scripts/ci/fixtures/zerodte/golden.tsv (generated from
VirginAttestation.Entry.hash()); `--vectors` fails when a hash differs.

Usage:
  zerodte_attestation.py verify <attestation.yaml> [--base <ref-or-file>]      structure, chain, one-shot, lineages; append-only vs base
  zerodte_attestation.py tail <attestation.yaml>                               the prevEntryHash a new entry must carry
  zerodte_attestation.py entry-hash <symbol> <lineage> <ledgerTopicId> <clusterId> <createdAt> <operator> <prevEntryHash>
  zerodte_attestation.py provisioning <provisioning.yaml>                      the declaration's rules
  zerodte_attestation.py --vectors                                             the golden vectors
  zerodte_attestation.py --corpus [dir]                                        the shared YAML-subset corpus (accept/reject verdicts + corpus.sha256)
  zerodte_attestation.py --corpus-manifest [dir]                               regenerate the corpus's corpus.sha256 (after any corpus change)
Every refusal prints `REFUSED: <reason>` and exits 1.
"""
import datetime
import hashlib
import json
import os
import re
import struct
import subprocess
import sys

UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
HEX32 = re.compile(r"^[0-9a-f]{32}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
TEXT = re.compile(r"^[\x20-\x7e]{1,128}$")
# A person's name as the review records it, ASCII by POLICY (reviewer identities in this repository are ASCII; an accented name is
# written in its ASCII form): words of letters joined by single spaces, a word may carry an inner . ' or - between letters and a
# trailing . for an initial; 2..128 characters. The SAME grammar is VirginAttestation.APPROVER in the Job.
APPROVER = re.compile(r"^(?=.{2,128}$)[A-Za-z]+(?:[.'-][A-Za-z]+)*\.?(?: [A-Za-z]+(?:[.'-][A-Za-z]+)*\.?)*$")
MAX_CODE_POINTS = 1 << 16   # the subset's size cap, counted over the RAW text (a CR counts); the Job checks the same number before SnakeYAML sees the text
# SnakeYAML's printable code points (YAML 1.1 c-printable): TAB, LF, CR, 0x20–0x7E, NEL, 0xA0–0xD7FF, 0xE000–0xFFFD, 0x10000–0x10FFFF.
# Anything else — a C0 control, DEL — is refused ANYWHERE in the text, a comment included, as the Job refuses it.
_PRINTABLE = re.compile("^[\t\n\r\x20-\x7e\x85\xa0-\ud7ff\ue000-\ufffd\U00010000-\U0010ffff]*$")
UNAPPROVED = "UNAPPROVED"
OPERATOR = re.compile(r"^[\x20-\x7e]{1,64}$")
SYMBOL = re.compile(r"^[A-Z0-9]{1,16}$")
TOPIC = re.compile(r"^[a-zA-Z0-9._-]{1,249}$")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_INSTANT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?Z$")
FIRST_PREV = "0" * 64
ENTRY_KEYS = ["symbol", "environmentLineageId", "ledgerTopicId", "clusterId", "createdAt", "operator", "prevEntryHash"]
LINEAGE_KEYS = ["id", "name", "parent", "approvedBy", "date"]
QUOTED_ENTRY_KEYS = {"ledgerTopicId", "clusterId", "prevEntryHash"}
ROLES = ["FRAMES", "HEAD", "CURRENT", "PULSE", "DEPLOYMENTS"]
MODES = {"RAW", "EMBEDDED"}
POLICIES = {"DELETE", "COMPACT", "COMPACT_DELETE"}
INPUT_TOPICS_MAX = 8


class Refused(Exception):
    pass


# ------------------------------------------------------------------------------------------------ a strict YAML subset
# The files are written in a small, fixed shape: block maps, block lists of maps or scalars, flow `[]`/`null`, scalars plain or
# quoted. This reader accepts exactly that — anything else is refused, as the Job's loader refuses tags, anchors, aliases and
# duplicate keys. A scalar keeps whether it was QUOTED (the Job demands quoting for the hex identities).

class Scalar:
    __slots__ = ("text", "quoted")

    def __init__(self, text, quoted):
        self.text, self.quoted = text, quoted

    def __repr__(self):
        return ("'%s'" if self.quoted else "%s") % self.text


def _strip_comment(line):
    out, quote = [], None
    i = 0
    while i < len(line):
        c = line[i]
        if quote:
            out.append(c)
            if c == quote:
                quote = None
        elif c in "\"'":
            quote = c
            out.append(c)
        elif c == "#" and (i == 0 or line[i - 1] in " \t"):
            break
        else:
            out.append(c)
        i += 1
    return "".join(out).rstrip()


def _scalar(text):
    text = text.strip()
    if text.startswith('"'):   # a scalar that OPENS a quote must be a complete, valid scalar of that style (SnakeYAML refuses the rest)
        if not (text.endswith('"') and len(text) >= 2):
            raise Refused("an unterminated double-quoted scalar: %s" % text)
        body = text[1:-1]
        if "\\" in body or '"' in body:
            raise Refused("an escape inside a double-quoted scalar is not accepted: %s" % text)
        return Scalar(body, True)
    if text.startswith("'"):
        if not (text.endswith("'") and len(text) >= 2):
            raise Refused("an unterminated single-quoted scalar: %s" % text)
        body = text[1:-1].replace("''", "\x00")   # YAML's one single-quoted escape: '' is one apostrophe (SnakeYAML decodes it too)
        if "'" in body:
            raise Refused("a lone quote inside a single-quoted scalar is not accepted: %s" % text)
        return Scalar(body.replace("\x00", "'"), True)
    if text == "[]":
        return []
    if text.startswith("[") or text.startswith("{") or text.startswith("&") or text.startswith("*") or text.startswith("!") or text.startswith("|") or text.startswith(">"):
        raise Refused("flow collections, anchors, aliases, tags and block scalars are not accepted: %s" % text)
    if text in ("", "~", "null", "Null", "NULL"):
        return None
    if ": " in text or text.endswith(":"):
        raise Refused("a plain scalar cannot contain ': ' (YAML reads a mapping there): %s" % text)
    return Scalar(text, False)


def _separated(rest):
    """A colon is a mapping indicator only when YAML separation follows it: a space, or the end of the line (`key:1` is a plain scalar)."""
    return rest == "" or rest.startswith(" ")


def load(text):
    if text.startswith("\ufeff"):
        raise Refused("a byte-order mark is not accepted")
    if len(text) > MAX_CODE_POINTS:
        raise Refused("the file exceeds %d code points" % MAX_CODE_POINTS)
    if not _PRINTABLE.match(text):
        raise Refused("a non-printable character is not accepted anywhere in the text (a comment included)")
    lines = []
    for raw in text.split("\n"):
        if raw.startswith("---") or raw.startswith("...") or raw.startswith("%"):
            raise Refused("document markers (--- ...) and directives (%) are not accepted")
        s = _strip_comment(raw)
        if s.strip() == "":
            continue
        leading = re.match(r"^[ \t]*", s).group(0)
        if "\t" in leading:
            raise Refused("tabs in indentation")
        indent = len(leading)
        lines.append((indent, s.strip()))
    pos = [0]

    def parse_block(indent):
        if pos[0] >= len(lines):
            raise Refused("an empty block")
        ind, content = lines[pos[0]]
        if ind != indent:
            raise Refused("unexpected indentation at: %s" % content)
        if content.startswith("- "):
            return parse_list(indent)
        return parse_map(indent)

    def parse_map(indent):
        m = {}
        while pos[0] < len(lines):
            ind, content = lines[pos[0]]
            if ind < indent:
                break
            if ind > indent:
                raise Refused("unexpected indentation at: %s" % content)
            if content.startswith("- "):
                raise Refused("a list item where a map entry was expected: %s" % content)
            key, sep, rest = content.partition(":")
            if not sep or not key or key != key.strip() or not _separated(rest):
                raise Refused("not a map entry (the separator is ': ' or a colon ending the line): %s" % content)
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
                raise Refused("a key is a plain identifier (never quoted): %s" % key)
            if key in m:
                raise Refused("duplicate key %s" % key)
            pos[0] += 1
            if rest.strip() == "":
                if pos[0] < len(lines) and lines[pos[0]][0] > indent:
                    m[key] = parse_block(lines[pos[0]][0])
                elif pos[0] < len(lines) and lines[pos[0]][0] == indent and lines[pos[0]][1].startswith("- "):
                    m[key] = parse_list(indent)
                else:
                    m[key] = None
            else:
                m[key] = _scalar(rest)
        return m

    def parse_list(indent):
        items = []
        while pos[0] < len(lines):
            ind, content = lines[pos[0]]
            if ind < indent or not content.startswith("- "):
                break
            if ind != indent:
                raise Refused("unexpected list indentation at: %s" % content)
            first = content[2:].strip()
            if ":" in first and not (first.startswith('"') or first.startswith("'")) and _separated(first.partition(":")[2]):
                key, _, rest = first.partition(":")
                pos[0] += 1
                item = {}
                if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
                    raise Refused("a key is a plain identifier: %s" % key)
                item[key] = _scalar(rest) if rest.strip() != "" else None
                child = indent + 2
                while pos[0] < len(lines) and lines[pos[0]][0] == child and not lines[pos[0]][1].startswith("- "):
                    k, sep, r = lines[pos[0]][1].partition(":")
                    if not sep or not _separated(r) or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", k):
                        raise Refused("not a map entry (the separator is ': ' or a colon ending the line): %s" % lines[pos[0]][1])
                    if k in item:
                        raise Refused("duplicate key %s" % k)
                    pos[0] += 1
                    item[k] = _scalar(r) if r.strip() != "" else None
                items.append(item)
            else:
                pos[0] += 1
                items.append(_scalar(first))
        return items

    root = parse_block(lines[0][0]) if lines else None
    if pos[0] != len(lines):
        raise Refused("trailing content at: %s" % lines[pos[0]][1])
    if not isinstance(root, dict):
        raise Refused("the file is a map")
    return root


# ------------------------------------------------------------------------------------------------ TLV and the chain

# ------------------------------------------------------------------------------------------------ the shared corpus
# MANIFEST DISCIPLINE (Codex 7b r2): expected.tsv has exactly three columns (name, kind in {attestation, provisioning}, verdict in
# {accept, reject}), no duplicate name, and names EXACTLY the *.yaml files beside it — one to one. corpus.sha256 holds the sha256 of
# expected.tsv and of every fixture; both runners (this port and the Job's YamlSubsetCorpusTest) refuse a corpus the manifest does not
# describe, so the two copies cannot drift silently. Regenerate with `zerodte_attestation.py --corpus-manifest`.

def corpus_cases(d):
    cases, names = [], set()
    for line in _read(os.path.join(d, "expected.tsv")).splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            raise Refused("expected.tsv: a case has three tab-separated columns: %r" % line)
        name, kind, verdict = parts
        if not re.match(r"^[a-z0-9-]+$", name):
            raise Refused("expected.tsv: a case name is lowercase-kebab: %r" % name)
        if kind not in ("attestation", "provisioning"):
            raise Refused("expected.tsv: kind is attestation or provisioning: %r" % line)
        if verdict not in ("accept", "reject"):
            raise Refused("expected.tsv: verdict is accept or reject: %r" % line)
        if name in names:
            raise Refused("expected.tsv: duplicate case %s" % name)
        names.add(name)
        cases.append((name, kind, verdict))
    files = {f[:-5] for f in os.listdir(d) if f.endswith(".yaml")}
    if files != names:
        raise Refused("the corpus and expected.tsv disagree: only in the directory %s; only in the manifest %s" % (sorted(files - names), sorted(names - files)))
    return cases


# ONE canonical corpus (Codex 7b r3): corpus.sha256 is regenerated from the fixtures, so by itself it only proves a copy is self-consistent.
# CORPUS_DIGEST — the sha256 of corpus.sha256 — is a LITERAL pinned here AND in the Job's YamlSubsetCorpusTest: a change to the corpus must
# change the literal in BOTH repositories (printed by --corpus-manifest), so a copy that drifted from the pinned version fails its runner.
CORPUS_DIGEST = "55b5bd59864f8c5502fcd1914299990d1bef3bbff275ff74c6d8bcd677f52181"


def corpus_manifest(d, cases):
    out = []
    for f in ["expected.tsv"] + sorted(name + ".yaml" for name, _, _ in cases):
        with open(os.path.join(d, f), "rb") as fh:
            out.append("%s  %s\n" % (hashlib.sha256(fh.read()).hexdigest(), f))
    return "".join(out)


def _frame(tag, payload):
    return bytes([tag]) + struct.pack(">q", len(payload)) + payload


def tlv_string(s):
    return _frame(0x04, s.encode("utf-8"))


def tlv_map(entries):
    body = _frame(0x02, str(len(entries)).encode("ascii"))
    for k in sorted(entries, key=lambda x: x.encode("utf-8")):
        body += tlv_string(k) + tlv_string(entries[k])
    return _frame(0x07, body)


def entry_hash(e):
    return hashlib.sha256(tlv_map({k: e[k] for k in ENTRY_KEYS})).hexdigest()


def _date(v, what):
    t = _text(v, what, ISO_DATE)
    try:
        datetime.date.fromisoformat(t)
    except ValueError:
        raise Refused("%s is not a real calendar date: %s" % (what, t))
    return t


def _instant(v, what):
    t = _text(v, what, ISO_INSTANT)
    try:
        datetime.datetime.strptime(t[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        raise Refused("%s is not a real instant: %s" % (what, t))
    return t


def _text(v, what, pattern=None, quoted=None):
    if not isinstance(v, Scalar):
        raise Refused("%s is a string" % what)
    if quoted and not v.quoted:
        raise Refused("%s is a QUOTED string (YAML reads digits as a number)" % what)
    if pattern and not pattern.match(v.text):
        raise Refused("%s is not in its domain: %s" % (what, v.text))
    return v.text


def _exact_keys(m, keys, what):
    if not isinstance(m, dict):
        raise Refused("%s is a map" % what)
    for k in m:
        if k not in keys:
            raise Refused("%s has an unknown key: %s" % (what, k))
    for k in keys:
        if k not in m:
            raise Refused("%s lacks the key: %s" % (what, k))


def parse_attestation(text):
    root = load(text)
    _exact_keys(root, ["lineages", "entries"], "the attestation file")
    lineages, entries = [], []
    if not isinstance(root["lineages"], list) or not isinstance(root["entries"], list):
        raise Refused("lineages and entries are lists (empty lists when there are none)")
    ids = set()
    for l in root["lineages"]:
        _exact_keys(l, LINEAGE_KEYS, "lineages[]")
        row = {
            "id": _text(l["id"], "lineages[].id", UUID),
            "name": _text(l["name"], "lineages[].name", TEXT),
            "parent": None if l["parent"] is None else _text(l["parent"], "lineages[].parent", UUID),
            "approvedBy": _text(l["approvedBy"], "lineages[].approvedBy", APPROVER),
            "date": _date(l["date"], "lineages[].date"),
        }
        if row["approvedBy"].upper() == UNAPPROVED:
            raise Refused("lineage %s carries the %s marker: a lineage is usable only with the OWNER's recorded approval (approvedBy = the owner's name)" % (row["id"], UNAPPROVED))
        if row["id"] in ids:
            raise Refused("a lineage id occurs once: %s" % row["id"])
        ids.add(row["id"])
        lineages.append(row)
    for l in lineages:
        if l["parent"] is not None and l["parent"] not in ids:
            raise Refused("a cloned lineage names a KNOWN parent: %s" % l["id"])
        if l["parent"] == l["id"]:
            raise Refused("a lineage is not its own parent: %s" % l["id"])
    seen = set()
    expected = FIRST_PREV
    for i, e in enumerate(root["entries"]):
        _exact_keys(e, ENTRY_KEYS, "entries[]")
        row = {
            "symbol": _text(e["symbol"], "entries[].symbol", SYMBOL),
            "environmentLineageId": _text(e["environmentLineageId"], "entries[].environmentLineageId", UUID),
            "ledgerTopicId": _text(e["ledgerTopicId"], "entries[].ledgerTopicId", HEX32, quoted=True),
            "clusterId": _text(e["clusterId"], "entries[].clusterId", TEXT, quoted=True),
            "createdAt": _instant(e["createdAt"], "entries[].createdAt"),
            "operator": _text(e["operator"], "entries[].operator", OPERATOR),
            "prevEntryHash": _text(e["prevEntryHash"], "entries[].prevEntryHash", HEX64, quoted=True),
        }
        if row["environmentLineageId"] not in ids:
            raise Refused("entries[%d] names an unknown lineage %s" % (i, row["environmentLineageId"]))
        key = (row["symbol"], row["environmentLineageId"])
        if key in seen:
            raise Refused("entries[%d] is a SECOND attestation for (%s, %s): the attestation is one-shot" % (i, key[0], key[1]))
        seen.add(key)
        if row["prevEntryHash"] != expected:
            raise Refused("entries[%d].prevEntryHash breaks the chain: the preceding entry was altered or removed" % i)
        expected = entry_hash(row)
        entries.append(row)
    return {"lineages": lineages, "entries": entries, "tail": expected}


def append_only(base, now):
    """Everything the base held is still there, unchanged, in the same order; the chain of what follows is already verified."""
    if len(now["lineages"]) < len(base["lineages"]) or now["lineages"][: len(base["lineages"])] != base["lineages"]:
        raise Refused("a lineage of the base version was removed or altered (lineages are append-only)")
    if len(now["entries"]) < len(base["entries"]) or now["entries"][: len(base["entries"])] != base["entries"]:
        raise Refused("an entry of the base version was removed or altered (entries are append-only)")


def parse_provisioning(text):
    root = load(text)
    keys = ["schemaVersion", "symbol", "environmentLineageId", "generation", "bootstrapKind", "migration", "eraId", "inputs", "outputs", "recreatedTopics", "modeChange", "operator"]
    if not isinstance(root, dict):
        raise Refused("the provisioning file is a map")
    for k in root:
        if k not in keys and k != "previousGeneration":
            raise Refused("the provisioning file has an unknown key: %s" % k)
    for k in keys:
        if k not in root:
            raise Refused("the provisioning file lacks the key: %s" % k)

    def integer(v, what, lo, hi):
        if not isinstance(v, Scalar) or v.quoted or not re.match(r"^[-+]?[0-9]+$", v.text):
            raise Refused("%s is an integer" % what)
        n = int(v.text)
        if n < lo or n > hi:
            raise Refused("%s is in [%d, %d]" % (what, lo, hi))
        return n

    def boolean(v, what):
        if not isinstance(v, Scalar) or v.quoted or v.text not in ("true", "false"):
            raise Refused("%s is true or false" % what)
        return v.text == "true"

    def enum(v, what, domain):
        t = _text(v, what)
        if t not in domain:
            raise Refused("%s is one of %s" % (what, sorted(domain)))
        return t

    if integer(root["schemaVersion"], "schemaVersion", 1, 1 << 31) != 1:
        raise Refused("schemaVersion is 1")
    symbol = _text(root["symbol"], "symbol", SYMBOL)
    lineage = _text(root["environmentLineageId"], "environmentLineageId", UUID)
    generation = integer(root["generation"], "generation", 1, 1 << 62)
    kind = enum(root["bootstrapKind"], "bootstrapKind", {"VIRGIN", "RECOVERY"})
    migration = boolean(root["migration"], "migration")
    previous = integer(root["previousGeneration"], "previousGeneration", 1, 1 << 62) if "previousGeneration" in root else None
    if migration != (previous is not None):
        raise Refused("previousGeneration is present exactly when migration is true")
    if migration and previous != generation - 1:
        raise Refused("a migration names its predecessor: previousGeneration = generation - 1")
    era = integer(root["eraId"], "eraId", 1, 1 << 62)
    inputs = root["inputs"]
    if not isinstance(inputs, list) or not 1 <= len(inputs) <= INPUT_TOPICS_MAX:
        raise Refused("inputs names 1-%d topics" % INPUT_TOPICS_MAX)
    names, mode_of = set(), {}
    for i in inputs:
        _exact_keys(i, ["topic", "dependencyMode"], "inputs[]")
        t = _text(i["topic"], "inputs[].topic", TOPIC)
        if t in names:
            raise Refused("a topic name occurs once: %s" % t)
        names.add(t)
        mode_of[t] = enum(i["dependencyMode"], "inputs[].dependencyMode", MODES)
    outputs = root["outputs"]
    _exact_keys(outputs, ROLES, "outputs")
    out = {}
    for r in ROLES:
        o = outputs[r]
        _exact_keys(o, ["topic", "cleanupPolicy", "partitions", "retentionMs"], "outputs.%s" % r)
        t = _text(o["topic"], "outputs.%s.topic" % r, TOPIC)
        if t in names:
            raise Refused("a topic name occurs once: %s" % t)
        names.add(t)
        out[r] = {"topic": t, "cleanupPolicy": enum(o["cleanupPolicy"], "outputs.%s.cleanupPolicy" % r, POLICIES),
                  "partitions": integer(o["partitions"], "outputs.%s.partitions" % r, 1, 100), "retentionMs": integer(o["retentionMs"], "outputs.%s.retentionMs" % r, -1, 1 << 62)}
    # the output CONTRACT (CheckpointRecord.requireOutputContract): one partition for FRAMES/HEAD/DEPLOYMENTS; the pinned policies
    for r in ("FRAMES", "HEAD", "DEPLOYMENTS"):
        if out[r]["partitions"] != 1:
            raise Refused("%s has exactly one partition (offsets order the chain)" % r)
    for r, p in (("FRAMES", "DELETE"), ("HEAD", "COMPACT"), ("PULSE", "COMPACT_DELETE"), ("DEPLOYMENTS", "DELETE")):
        if out[r]["cleanupPolicy"] != p:
            raise Refused("%s has cleanup.policy %s" % (r, p))
    if out["PULSE"]["retentionMs"] != 86400000:
        raise Refused("PULSE retention is exactly one day (86400000 ms)")
    if out["DEPLOYMENTS"]["retentionMs"] != -1:
        raise Refused("DEPLOYMENTS retention is unlimited (-1): history is never erased")
    if out["CURRENT"]["cleanupPolicy"] == "DELETE":
        raise Refused("CURRENT is a compacted topic")
    recreated = root["recreatedTopics"]
    if not isinstance(recreated, list):
        raise Refused("recreatedTopics is a list")
    rec = set()
    for t in recreated:
        n = _text(t, "recreatedTopics[]", TOPIC)
        if n not in names:
            raise Refused("a recreated topic is a topic of THIS generation: %s" % n)
        if n in rec:
            raise Refused("recreatedTopics lists a name once: %s" % n)
        rec.add(n)
    changes = root["modeChange"]
    if not isinstance(changes, list):
        raise Refused("modeChange is a list")
    changed = set()
    for c in changes:
        _exact_keys(c, ["topic", "from", "to"], "modeChange[]")
        t = _text(c["topic"], "modeChange[].topic", TOPIC)
        f, to = enum(c["from"], "modeChange[].from", MODES), enum(c["to"], "modeChange[].to", MODES)
        if f == to:
            raise Refused("modeChange[] names two DIFFERENT modes (from, to)")
        if t not in mode_of:
            raise Refused("a modeChange row names an input topic of this generation: %s" % t)
        if mode_of[t] != to:
            raise Refused("a modeChange's toMode is this generation's declared mode: %s" % t)
        if t in rec:
            raise Refused("a recreated topic has no modeChange row: %s" % t)
        if t in changed:
            raise Refused("one modeChange row per topic: %s" % t)
        changed.add(t)
    if not migration and (rec or changed):
        raise Refused("recreatedTopics and modeChange describe a MIGRATION: empty unless migration is true")
    operator = _text(root["operator"], "operator", OPERATOR)
    return {"symbol": symbol, "environmentLineageId": lineage, "generation": generation, "bootstrapKind": kind, "migration": migration,
            "previousGeneration": previous, "eraId": era, "inputs": [{"topic": t, "dependencyMode": mode_of[t]} for t in mode_of], "outputs": out,
            "recreatedTopics": sorted(rec), "modeChange": changes and [{"topic": _text(c["topic"], "t"), "from": _text(c["from"], "f"), "to": _text(c["to"], "t")} for c in changes] or [],
            "operator": operator}


# ------------------------------------------------------------------------------------------------ commands

def _read(path):
    with open(path, encoding="utf-8", newline="") as f:   # newline="": the RAW line endings reach the reader — a CR is a code point the cap counts
        return f.read()


def _base_text(base, path):
    """The file at a git ref (`<ref>:<path>` through git show) or at a file path; None when the ref has no such file (a new file)."""
    if base is None:
        return None
    try:
        return _read(base)
    except OSError:
        pass
    r = subprocess.run(["git", "show", "%s:%s" % (base, path)], capture_output=True)   # bytes: the base's RAW line endings, like _read
    if r.returncode != 0:
        err = r.stderr.decode("utf-8", "replace")
        if "does not exist" in err or "exists on disk, but not in" in err:
            return None
        raise Refused("the base version could not be read from git (%s): %s" % (base, err.strip()))
    return r.stdout.decode("utf-8")


def main(argv):
    if not argv:
        print(__doc__)
        return 2
    cmd = argv[0]
    try:
        if cmd == "--vectors":
            n = 0
            for line in _read("scripts/ci/fixtures/zerodte/golden.tsv").splitlines():
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) != 8:
                    raise Refused("a golden vector has 8 fields")
                e = dict(zip(ENTRY_KEYS, parts[:7]))
                got = entry_hash(e)
                if got != parts[7]:
                    raise Refused("golden vector %d: this port hashes %s, Java hashes %s" % (n + 1, got, parts[7]))
                n += 1
            if n < 2:
                raise Refused("fewer than two golden vectors")
            print("OK: %d golden vectors agree with VirginAttestation.Entry.hash()" % n)
            return 0
        if cmd in ("--corpus", "--corpus-manifest"):
            d = argv[1] if len(argv) > 1 else "scripts/ci/fixtures/zerodte/corpus"
            cases = corpus_cases(d)
            if cmd == "--corpus-manifest":
                manifest = corpus_manifest(d, cases)
                with open(os.path.join(d, "corpus.sha256"), "w", encoding="utf-8") as f:
                    f.write(manifest)
                print("wrote %s/corpus.sha256 (%d files); CORPUS_DIGEST = %s — pin this literal in zerodte_attestation.py AND in the Job's YamlSubsetCorpusTest, and copy the corpus verbatim" % (d, len(cases) + 1, hashlib.sha256(manifest.encode("utf-8")).hexdigest()))
                return 0
            want = _read(os.path.join(d, "corpus.sha256"))
            got = corpus_manifest(d, cases)
            if want != got:
                raise Refused("corpus.sha256 does not describe the corpus (a file changed, was added or removed without regenerating the manifest with --corpus-manifest; the Java copy verifies the SAME manifest)")
            digest = hashlib.sha256(want.encode("utf-8")).hexdigest()
            if digest != CORPUS_DIGEST:
                raise Refused("the corpus is not the pinned canonical version: corpus.sha256 hashes to %s, CORPUS_DIGEST pins %s (a corpus change must update the literal in BOTH repositories)" % (digest, CORPUS_DIGEST))
            n = 0
            for name, kind, verdict in cases:
                text = _read(os.path.join(d, name + ".yaml"))
                try:
                    (parse_attestation if kind == "attestation" else parse_provisioning)(text)
                    verdict_got = "accept"
                except Refused:
                    verdict_got = "reject"
                if verdict_got != verdict:
                    raise Refused("corpus %s: this port says %s, the corpus expects %s" % (name, verdict_got, verdict))
                n += 1
            if n < 30:
                raise Refused("the corpus has only %d cases" % n)
            print("OK: %d corpus cases give the expected verdict; corpus.sha256 agrees" % n)
            return 0
        if cmd == "verify":
            path = argv[1]
            base = None
            if len(argv) > 2:
                if argv[2] != "--base" or len(argv) < 4:
                    raise Refused("usage: verify <file> [--base <ref-or-file>]")
                base = argv[3]
            now = parse_attestation(_read(path))
            text = _base_text(base, path)
            if text is not None:
                append_only(parse_attestation(text), now)
                print("append-only against %s: OK" % base)
            print("OK: %d lineages, %d entries, chain intact, tail=%s" % (len(now["lineages"]), len(now["entries"]), now["tail"]))
            return 0
        if cmd == "tail":
            print(parse_attestation(_read(argv[1]))["tail"])
            return 0
        if cmd == "entry-hash":
            if len(argv) != 8:
                raise Refused("entry-hash takes the seven fields")
            print(entry_hash(dict(zip(ENTRY_KEYS, argv[1:8]))))
            return 0
        if cmd == "provisioning":
            d = parse_provisioning(_read(argv[1]))
            print(json.dumps({k: d[k] for k in ("symbol", "environmentLineageId", "generation", "bootstrapKind", "migration", "previousGeneration", "eraId", "operator")}, sort_keys=True))
            return 0
        raise Refused("unknown command %s" % cmd)
    except Refused as r:
        print("REFUSED: %s" % r)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
