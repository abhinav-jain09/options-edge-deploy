# CVD schema registration through Jenkins

`Jenkinsfile.es4-deploy` action `register-cvd-schemas` registers the released external
contracts for existing CVD-related topics. Run with `DEPLOY_DRY_RUN=true` first;
false performs registration. The action resolves current contracts main and its
successful Jenkins artifact, with no version selector or older-release fallback
(`scripts/jenkins/fetch-canonical-contracts.py:72`). It verifies packaged resources
against that checkout before importing the package.

The explicit31-entry inventory in `scripts/kafka/register-cvd-schemas.py:22` covers
CVD/footprint, strike, Direction, regime, Open Direction, aligned levels/GEX/strike,
compression/expansion, absorption, EAV and Short Premium external outputs. It admits
the exact default name or one existing es4 prefix. Kafka metadata selects only
existing topics; progress/manual-line and unrelated Home GEX are excluded. Missing
Kafka topics are not created. Inactive Align internal state schemas are outside
this action and must be registered before any future activation of that service.

The three fixed broker/registry pairs are at `register-cvd-schemas.py:17`. All
metadata inventories and released resources are acquired before registration.
Each subject uses TopicNameStrategy's existing `topic-value` name. Registry IDs
are assigned independently; registry-aware mirrors reserialize for destinations.
The action does not force IDs, modify compatibility/modes, remove earlier versions,
or change Kafka data, topics, groups, offsets, replicas or service configuration.

`register-cvd-schemas.py:108` performs exact schema lookup before an optional
registration. Both subject lookup and schema-ID readback must match the released
schema without references, rules or metadata transformations. A lookup miss first checks all subject history, including deleted versions, and
refuses if any exists. Every subject is previewed across all clusters before the
first registration, and history is rechecked at the write boundary. Conflicts and
registry failures stop the run; completed per-subject receipts remain inspectable
and a later reviewed run rolls forward. The receipt records source, SDK provenance,
registry IDs, schema hashes and dry-run status. It does not prove reader closure,
retained-data compatibility or readiness to activate a writer.
