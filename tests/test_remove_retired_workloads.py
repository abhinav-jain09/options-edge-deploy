# CI-REQUIRED
#
# This module's assertions MUST run in CI. scripts/ci/validate-tests-are-collectable.sh - a separate,
# already-wired step of .github/workflows/deploy-validation.yml - fails the build unless a mandatory step
# of that workflow invokes this module as a `python3 -m unittest` target, with no continue-on-error, no
# `if:` and no shell construct that would swallow its failure.
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class RemoveRetiredWorkloadsTest(unittest.TestCase):
    def test_service_deploy_runs_retired_workload_cleanup_before_apply(self):
        jenkinsfile = (ROOT / "Jenkinsfile.service-deploy").read_text()
        cleanup = "bash scripts/deploy/remove-retired-workloads.sh"
        deploy = "bash scripts/deploy/service-deploy.sh"

        assert cleanup in jenkinsfile
        assert jenkinsfile.index(cleanup) < jenkinsfile.index(deploy)

    def test_cleanup_is_narrowly_scoped_to_dev_feed_gateway(self):
        script = (ROOT / "scripts/deploy/remove-retired-workloads.sh").read_text()

        assert '[ "$ENVIRONMENT" = "dev" ] && [ "$SERVICE" = "feed-gateway" ]' in script
        assert "delete deployment es-synthetic-feed --ignore-not-found=true" in script
        assert "DEPLOY_DRY_RUN" in script


if __name__ == "__main__":
    unittest.main()
