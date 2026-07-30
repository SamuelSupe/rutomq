# Roadmap

rutomq's goal is broad Kafka client and administration compatibility without
introducing Agent-local durable state. The project advertises only implemented
wire versions and treats documented boundaries as part of the contract.

This roadmap communicates direction rather than dates or guaranteed scope.

## v0.1 — public compatibility preview

- Direct immutable object-storage Produce path with PostgreSQL metadata commit.
- Stateless multi-Agent Fetch, groups, transactions, retention, and compaction.
- Kafka Java, librdkafka, franz-go, Flink, and Kafka Streams acceptance.
- TLS, SCRAM, delegation tokens, ACLs, quotas, metrics, and Helm deployment.

The validated envelope is recorded in
[`docs/compatibility-result-2026-07-30.md`](docs/compatibility-result-2026-07-30.md).

## v0.2 — compatibility closure

- Continue API-by-API comparison against the current Apache Kafka reference
  broker.
- Close observable semantic gaps found by real clients and malformed-wire
  testing.
- Add safe non-empty classic/consumer protocol migration or explicitly retain
  the boundary with stronger fencing tests.
- Expand client-version, compression, transaction, group, and authorization
  matrices.
- Publish repeatable compatibility reports for every release.

## v0.3 — operational hardening

- Establish repeatable throughput, latency, object-request, and PostgreSQL
  baselines across one and three Agents.
- Add upgrade and schema-migration compatibility windows across releases.
- Expand failure injection for dependency throttling, long outages, and
  high-cardinality coordinator workloads.
- Improve operator-facing diagnostics, capacity guidance, and recovery drills.
- Evaluate signed multi-architecture container images.

## v1.0 — stable compatibility contract

- Stable storage format and migration policy.
- Documented protocol and client support window with release-level regression
  evidence.
- Production operations guide, SLO-oriented dashboards, and disaster-recovery
  procedures.
- Security review and stable vulnerability-response policy.
- No known acknowledged-message-loss defect inside the supported durability
  envelope.

## Longer-term candidates

- Multiple virtual clusters and tenant isolation.
- Cross-region replication and disaster-recovery orchestration.
- Kubernetes Operator for lifecycle automation.
- Additional OpenDAL backends where their consistency model satisfies the
  immutable-object contract.

Schema Registry and Kafka Connect management are separate services and are not
required for Kafka clients to connect to rutomq.
