#!/usr/bin/env python3
"""R6 ratchet (docs/schema-governance): processing modules that use Avro but have no src/main/resources/schema-requirements.json.

  python3 scripts/ci/schema-r6-backlog.py <processing-checkout> [--backlog schema-manifest/r6-backlog.txt] [--write]

"Uses Avro" is an OVER-approximation by design: main code that imports com.optionsedge.contracts.*Avro* or io.confluent.kafka.serializers /
streams.serdes. A module that merely touches a DTO from the kit is listed too; the cost of a false entry is one requirements file, the cost of a
missed module is an ungoverned reader. The committed backlog may only SHRINK: a module that uses Avro, lacks the file and is not on the list
FAILS (a new Avro service must arrive with its requirements file); a listed module that now has the file, or no longer exists or uses Avro,
FAILS until it is removed from the list (so the list stays true). Prints the count that R6 requires to reach zero.
"""
import argparse, os, re, sys

PAT = re.compile(r"com\.optionsedge\.contracts\.[A-Za-z.]*Avro|io\.confluent\.kafka\.(serializers|streams\.serdes)")


def modules(root):
    use, have = set(), set()
    for d in sorted(os.listdir(root)):
        main = os.path.join(root, d, "src", "main")
        if not os.path.isdir(main):
            continue
        if os.path.exists(os.path.join(main, "resources", "schema-requirements.json")):
            have.add(d)
        hit = False
        for dp, _, fs in os.walk(os.path.join(main, "java")):
            for f in fs:
                if f.endswith(".java") and PAT.search(open(os.path.join(dp, f), errors="replace").read()):
                    hit = True; break
            if hit: break
        if hit:
            use.add(d)
    return use, have


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("root"); ap.add_argument("--backlog", default="schema-manifest/r6-backlog.txt"); ap.add_argument("--write", action="store_true")
    a = ap.parse_args()
    use, have = modules(a.root)
    missing = sorted(use - have)
    if a.write:
        open(a.backlog, "w").write("# R6 backlog: Avro-using processing modules without schema-requirements.json (may only shrink; scripts/ci/schema-r6-backlog.py)\n" + "".join(m + "\n" for m in missing))
        print("wrote %d module(s)" % len(missing)); return
    listed = {l.strip() for l in open(a.backlog) if l.strip() and not l.startswith("#")}
    rc = 0
    for m in sorted(set(missing) - listed):
        print("FAIL: %s uses Avro and has no schema-requirements.json, and is not on the R6 backlog (a new Avro service must declare its requirements)" % m); rc = 1
    for m in sorted(listed - set(missing)):
        why = "now has schema-requirements.json" if m in have else "no longer exists or no longer uses Avro"
        print("FAIL: %s is on the R6 backlog but %s; remove it from %s" % (m, why, a.backlog)); rc = 1
    print("R6 backlog: %d of %d Avro-using module(s) still have no schema-requirements.json (must reach zero)" % (len(missing), len(use)))
    sys.exit(rc)


if __name__ == "__main__":
    main()
