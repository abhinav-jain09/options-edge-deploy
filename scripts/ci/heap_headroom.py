"""The -Xmx-vs-memory-limit ratio of every rendered container. See
scripts/ci/validate-heap-leaves-container-headroom.sh for why this gate exists."""
import glob
import re
import sys

import yaml


class Unparseable(ValueError):
    """A quantity this gate could not read. It is a FAILURE, never a skip: a limit nobody can
    parse is a limit nobody is checking, and the first version silently passed those."""


# Kubernetes quantities come in two families and they are NOT interchangeable: the binary
# suffixes are powers of 1024, the bare SI ones powers of 1000. Reading "1G" as 1024Mi
# overstates the limit by 7% and makes a borderline container look compliant.
_BINARY = {"Ki": 1024 ** 1, "Mi": 1024 ** 2, "Gi": 1024 ** 3, "Ti": 1024 ** 4,
           "Pi": 1024 ** 5, "Ei": 1024 ** 6}
_DECIMAL = {"m": 1e-3, "": 1, "k": 1e3, "K": 1e3, "M": 1e6, "G": 1e9,
            "T": 1e12, "P": 1e15, "E": 1e18}


def to_mib(value):
    """A Kubernetes memory quantity in MiB. Fractions ("0.5Gi") and exponent form ("3e9") are
    valid quantities and are accepted; anything unreadable raises."""
    text = str(value).strip()
    m = re.fullmatch(r"(\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)([A-Za-z]{1,2})?", text)
    if not m:
        raise Unparseable(text)
    number, suffix = m.group(1), (m.group(2) or "")
    try:
        number = float(number)
    except ValueError as exc:
        raise Unparseable(text) from exc
    if suffix in _BINARY:
        scale = _BINARY[suffix]
    elif suffix in _DECIMAL:
        scale = _DECIMAL[suffix]
    else:
        raise Unparseable(text)
    return number * scale / (1024 ** 2)


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
    # Both trees, because both deploy containers. The es4 tree is NOT rendered from k8s/base —
    # it carried its own copies of the same 100% heaps (databento-maxpain, greek-move-authenticity,
    # option-truth-engine), so a gate on the overlays alone would have reported those fixed while
    # es4 kept shipping the fault.
    files = sorted(glob.glob("k8s/services/*/overlays/*/manifest.yaml")
                   + glob.glob("k8s/es4/services/*.yaml"))
    if not files:
        sys.exit("FAIL: no manifests found — the globs and the repo layout have diverged")

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
                where = (f"{path}: {doc.get('kind')}/"
                         f"{(doc.get('metadata') or {}).get('name')} container {c.get('name')}")
                if limit_raw is None:
                    failures.append(
                        f"{where} pins -Xmx{heap}Mi with NO memory limit — an unbounded container "
                        f"with a pinned heap hides the ratio this gate checks")
                    continue
                try:
                    limit = to_mib(limit_raw)
                except Unparseable:
                    # Fail CLOSED. Skipping here is how a gate reports coverage it does not have.
                    failures.append(
                        f"{where} has a memory limit this gate cannot read: {limit_raw!r}. Teach "
                        f"to_mib() the quantity rather than leaving the container unchecked")
                    continue
                if limit <= 0:
                    failures.append(
                        f"{where} pins -Xmx{heap}Mi against a memory limit of {limit_raw!r}, "
                        f"which is zero or negative")
                    continue
                checked += 1
                pct = int(heap * 100 // limit)
                if pct > max_pct:
                    failures.append(
                        f"{where} has -Xmx{heap}Mi against a {limit:.0f}Mi limit ({pct}%). The heap "
                        f"must leave the container room for RocksDB, metaspace, thread stacks, code "
                        f"cache and direct buffers; over {max_pct}% is OOMKilled (exit 137) with no "
                        f"OutOfMemoryError to read. Lower -Xmx or raise the limit.")

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
