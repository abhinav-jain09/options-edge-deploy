"""The -Xmx-vs-memory-limit ratio of every rendered container. See
scripts/ci/validate-heap-leaves-container-headroom.sh for why this gate exists."""
import glob
import re
import sys

import yaml


def to_mib(value):
    m = re.fullmatch(r"(\d+)(Mi|Gi|M|G|Ki|K)?", str(value).strip())
    if not m:
        return None
    n, unit = int(m.group(1)), (m.group(2) or "")
    if unit in ("Gi", "G"):
        return n * 1024
    if unit in ("Ki", "K"):
        return n // 1024
    return n


def xmx_mib(container):
    """The -Xmx of this container, from any env value that carries one (JAVA_TOOL_OPTIONS,
    JAVA_OPTS, JDK_JAVA_OPTIONS, a bespoke name — the flag is what matters, not the variable)."""
    for entry in container.get("env") or []:
        m = re.search(r"-Xmx(\d+)([kKmMgG])", str(entry.get("value") or ""))
        if m:
            n, unit = int(m.group(1)), m.group(2).lower()
            return n * 1024 if unit == "g" else (n // 1024 if unit == "k" else n)
    return None


def containers(doc):
    spec = ((doc.get("spec") or {}).get("template") or {}).get("spec") or {}
    if not spec:  # CronJob nests one level deeper
        job = ((doc.get("spec") or {}).get("jobTemplate") or {}).get("spec") or {}
        spec = (job.get("template") or {}).get("spec") or {}
    for key in ("containers", "initContainers"):
        for c in spec.get(key) or []:
            yield c


def main():
    max_pct = int(sys.argv[1])
    files = sorted(glob.glob("k8s/services/*/overlays/*/manifest.yaml"))
    if not files:
        sys.exit("FAIL: no rendered overlays found — the glob and the repo layout have diverged")

    failures, checked = [], 0
    for path in files:
        with open(path, encoding="utf-8") as fh:
            docs = [d for d in yaml.safe_load_all(fh) if isinstance(d, dict)]
        for doc in docs:
            for c in containers(doc):
                heap = xmx_mib(c)
                if heap is None:
                    continue
                limit_raw = ((c.get("resources") or {}).get("limits") or {}).get("memory")
                if limit_raw is None:
                    failures.append(
                        f"{path}: {doc.get('kind')}/{(doc.get('metadata') or {}).get('name')} "
                        f"container {c.get('name')} pins -Xmx{heap}Mi with NO memory limit — an "
                        f"unbounded container with a pinned heap hides the ratio this gate checks")
                    continue
                limit = to_mib(limit_raw)
                if limit is None or limit == 0:
                    continue
                checked += 1
                pct = heap * 100 // limit
                if pct > max_pct:
                    failures.append(
                        f"{path}: container {c.get('name')} has -Xmx{heap}Mi against a {limit}Mi "
                        f"limit ({pct}%). The heap must leave the container room for RocksDB, "
                        f"metaspace, thread stacks, code cache and direct buffers; over {max_pct}% "
                        f"is OOMKilled (exit 137) with no OutOfMemoryError to read. Lower -Xmx or "
                        f"raise the limit.")

    if checked == 0:
        sys.exit("FAIL: no container with both a pinned -Xmx and a memory limit was found — this "
                 "gate would pass vacuously, which is worse than not having it")
    for f in failures:
        print("FAIL: " + f)
    if failures:
        sys.exit(1)
    print(f"checked {checked} heap-pinned container(s) across {len(files)} rendered overlay(s); "
          f"all at or below {max_pct}% of their limit")


if __name__ == "__main__":
    main()
