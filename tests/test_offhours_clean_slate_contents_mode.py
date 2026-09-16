"""STATE_RESET_MODE=contents — offhours-clean-slate.sh empties a Streams-state PVC's local-path directory
in place instead of deleting the claim (prod rule: PVCs are never deleted).

The reset function is extracted between its markers and run against fake kubectl/kcr binaries, so the
refusal paths (wrong PV path, unbound claim, missing dir) are exercised for real."""
import os, re, stat, subprocess, tempfile, textwrap, unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/ops/offhours-clean-slate.sh"


def _block():
    t = SCRIPT.read_text()
    m = re.search(r"# ---- B-contents-begin ----\n(.*?)# ---- B-contents-end ----", t, re.S)
    assert m, "contents block markers missing"
    return m.group(1)


class ContentsModeStatic(unittest.TestCase):
    def test_default_mode_is_recreate_so_dev_behaviour_is_unchanged(self):
        self.assertIn('STATE_RESET_MODE="${STATE_RESET_MODE:-recreate}"', SCRIPT.read_text())

    def test_contents_mode_never_calls_delete_pvc(self):
        self.assertNotIn("delete pvc", _block())

    def test_recreate_summary_line_is_inside_the_recreate_branch(self):
        t = SCRIPT.read_text()
        i = t.index('log "streams-state PVCs: recreated=')
        j = t.index('log "streams-state PVCs: emptied in place=')
        self.assertLess(j, i)
        # the recreate log sits before the closing fi of the elif branch
        self.assertRegex(t[i:i + 120], r'recreated=\$pvc_ok failed=\$pvc_fail"\nfi\n')

    def test_dry_run_describes_the_active_mode(self):
        self.assertIn('EMPTY its local-path directory in place (PVC kept; STATE_RESET_MODE=contents)', SCRIPT.read_text())


class ContentsModeBehaviour(unittest.TestCase):
    def run_reset(self, pv_path, claim="svc-streams-state", bound=True, make_dir=True):
        tmp = Path(tempfile.mkdtemp())
        bindir = tmp / "bin"; bindir.mkdir()
        hp = tmp / "storage" / f"pvc-abc_options-edge_{claim}" if pv_path is None else Path(pv_path)
        if make_dir:
            hp.mkdir(parents=True, exist_ok=True)
            (hp / "rocksdb").mkdir(exist_ok=True); (hp / "rocksdb" / "000001.sst").write_text("x")
            (hp / ".checkpoint").write_text("y")
        fake_kubectl = textwrap.dedent(f"""\
            #!/bin/bash
            # fake kubectl: 'get pv <name> -o jsonpath=...' -> the PV path; 'get pvc' handled by kcr below
            if [ "$1" = get ] && [ "$2" = pv ]; then printf '%s' "{hp}"; exit 0; fi
            exit 1
            """)
        (bindir / "kubectl").write_text(fake_kubectl); (bindir / "kubectl").chmod(0o755)
        harness = textwrap.dedent(f"""\
            #!/bin/bash
            NS=options-edge
            log() {{ echo "[log] $*"; }}
            kcr() {{ if [ "$1" = get ] && [ "$2" = pvc ]; then printf '%s' "{'pv-1' if bound else ''}"; return 0; fi; return 1; }}
            {_block()}
            reset_pvc_contents "{claim}"; echo "rc=$?"
            """)
        h = tmp / "h.sh"; h.write_text(harness)
        env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}")
        out = subprocess.run(["bash", str(h)], capture_output=True, text=True, env=env).stdout
        return out, hp

    def test_empties_the_claims_own_directory_but_keeps_the_directory(self):
        out, hp = self.run_reset(None)
        self.assertIn("rc=0", out)
        self.assertTrue(hp.is_dir())
        self.assertEqual(os.listdir(hp), [])

    def test_refuses_a_pv_path_that_is_not_this_claims_directory(self):
        other = Path(tempfile.mkdtemp()) / "pvc-zzz_options-edge_OTHER-streams-state"
        out, hp = self.run_reset(str(other))
        self.assertIn("REFUSING", out); self.assertIn("rc=1", out)
        self.assertTrue((hp / ".checkpoint").exists(), "foreign directory must be untouched")

    def test_refuses_an_unbound_claim(self):
        out, hp = self.run_reset(None, bound=False)
        self.assertIn("not bound", out); self.assertIn("rc=1", out)
        self.assertTrue((hp / ".checkpoint").exists())

    def test_missing_directory_is_skipped_not_created(self):
        out, hp = self.run_reset(None, make_dir=False)
        self.assertIn("not a directory", out); self.assertIn("rc=1", out)
        self.assertFalse(hp.exists())


if __name__ == "__main__":
    unittest.main()


class ScaleExempt(unittest.TestCase):
    """Keycloak never rides the pipeline scale-down: the snapshot that drives scale-to-0 and restore excludes it."""

    def test_default_exempts_keycloak_only(self):
        self.assertIn('SCALE_EXEMPT_RE="${SCALE_EXEMPT_RE:-^oe-keycloak$}"', SCRIPT.read_text())

    def test_snapshot_filter_drops_exempt_names_and_keeps_replica_counts(self):
        snap = "oe-keycloak 1\nfeed-gateway-service 1\nindicator-service 0\n"
        out = subprocess.run(["bash", "-c", 'printf "%s" "$1" | awk -v re="$2" \'$1 !~ re\'', "_", snap, "^oe-keycloak$"],
                             capture_output=True, text=True).stdout
        self.assertEqual(out, "feed-gateway-service 1\nindicator-service 0\n")

    def test_ready_wait_ignores_exempt_deployments(self):
        t = SCRIPT.read_text()
        self.assertIn("awk -v re=\"${SCALE_EXEMPT_RE:-^$}\" '$1 !~ re {s+=$2} END{print s+0}'", t)
