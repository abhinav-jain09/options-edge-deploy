# Dev Rithmic collector transport

Both mirrored web dev env patches point to the private, authenticated collector through host.docker.internal:17432/depth. The collector's exact-revision Jenkins deployment creates namespace options-edge Secret rithmic-depth-bridge, key token, before web rollout. The web receives only this transport token; Rithmic account credentials remain on the owner Mac in Keychain.

No production manifest changes. Only the web service is rolled by web-service-deploy, with exact main revisions for both the web image and deployment repository. Validation: VALIDATE_ONLY_SERVICE=web VALIDATE_ONLY_ENV=dev bash scripts/ci/validate-services.sh passes. Missing collector or token makes the feature unavailable rather than falling back to another source.

Bugzilla change tracking is currently blocked: both the LAN endpoint and a request from its host to localhost:8092 refused connection on 2026-11-02. Keep this evidence and record the line-level changes and actual Jenkins results once Bugzilla is reachable. Do not close the associated work without runtime acceptance.
