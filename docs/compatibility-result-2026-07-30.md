# rutomq compatibility result — 2026-07-30

## Result

**PASS for the compatibility envelope described below.**

This checkpoint records the v0.1.0 release candidate as a dated result. The
broad acceptance baseline was established on 2026-07-29. After the release
candidate's dependency and security updates, the Rust workspace, exact
OpenDAL/MinIO data path, Flink 2.2.1 and 2.3.0 matrices, Kafka Java 3.9.2
compatibility path, Kafka Streams scenarios, and TLS/SCRAM security suite were
rerun successfully on 2026-07-30. It does not claim complete semantic parity
with every Apache Kafka deployment or every possible client configuration.

The API-by-API contract remains the source of truth for exact versions and
behavior: [Kafka compatibility matrix](compatibility.md).

## Snapshot

| Item | Result |
| --- | --- |
| Wire baseline | Types generated from the official Kafka 4.3.0 schemas through `kafka-protocol 0.17.0-kafka.4.3.0` |
| Advertised surface | 74 API keys; `ApiVersions` advertises only handled versions, apart from Kafka 4.3's required Produce 0-13 negotiation range while removed Produce v0-v2 remain rejected |
| Backward compatibility | Kafka Java 3.9.2 AdminClient, Kafka Java 4.2.0 clients, librdkafka 2.15.0, and franz-go 1.21.5 passed |
| Flink | Flink 2.2.1 with Kafka connector 5.0.0-2.2 passed source, sink, checkpoint-offset recovery, and exact resume |
| Kafka Streams | Kafka Streams 4.2.0 passed classic and Streams group protocols, `exactly_once_v2`, state restore, repartition, and standby promotion |
| Persistence | PostgreSQL metadata plus immutable OpenDAL S3/MinIO objects; no Agent-local WAL, PVC, or data volume |
| Acknowledgement boundary | Produce succeeds only after object storage succeeds and the PostgreSQL metadata transaction commits |
| Deployment | Stateless multi-Agent and Helm/Kubernetes rolling replacement plus scale-out/scale-in passed |

## Validated functionality

| Area | Current result |
| --- | --- |
| Produce and Fetch | Idempotent production, compression, timestamp and record validation, immutable object batching, exact range fetch, checksum verification, sparse offsets, fetch sessions, `read_committed`, and `read_uncommitted` passed |
| Consumer groups | Classic groups and the Kafka 4 consumer protocol passed membership, static membership, rebalances, offset recovery, assignment epochs, response-loss recovery, and PostgreSQL-backed coordinator replacement |
| Group conversion and assignment | Empty classic and consumer-protocol groups convert in both directions while preserving offsets and GROUP config; consumer assignors are `uniform` and `range`; the share assignor is `simple` |
| Regex subscriptions | The 10-second metadata guard and configurable maximum regex refresh interval passed paused-time, PostgreSQL reconnect, cross-Agent, and full-client validation |
| Share groups | Share membership, acquire/acknowledge/release/redelivery, duration reset, lag, Admin APIs, and share coordinator state APIs passed |
| Transactions | Idempotent retry, commit/abort, transactional offset commits, fencing, KIP-939 two-phase recovery, KIP-1228 marker epochs, expiry, and `read_committed` visibility passed |
| Admin and configuration | Topic lifecycle, partitions, configs, ACLs, quotas, telemetry, logical feature metadata, virtual log directories, leader/reassignment compatibility, and SCRAM administration passed |
| Security | TLS, SCRAM-SHA-256, SCRAM-SHA-512, delegation-token authentication, Topic/Group/Cluster/TransactionalId ACLs, and authorization filtering passed with Kafka 4.3.1 clients |
| Stateless durability | Three Agents passed ambiguous acknowledgement retry, SIGKILL/restart, S3 outage, PostgreSQL commit failure, orphan GC, retention deletion recovery, and exact object/span convergence |
| Kubernetes | A stable Service retained 1,600 unique records at contiguous offsets through an image rollout and replica changes `3 -> 5 -> 2`, with zero Agent PVCs |

## Explicit compatibility boundaries

- Non-empty classic-to-consumer or consumer-to-classic online migration is not
  implemented or advertised. Mixed-protocol rolling membership remains
  outside this result.
- Server-side assignors are the implemented Rust-native choices only:
  consumer `uniform`/`range` and share `simple`. Java assignor classes or
  arbitrary assignor plugins are not loaded by the Agent.
- The topology is one logical Kafka cluster with a virtual broker,
  controller, leader, and ISR member. APIs that require physical replica logs,
  follower reads, local log directories, or real KRaft controller state are
  virtualized, constrained to the one-broker model, or omitted as documented
  in the API matrix.
- PostgreSQL and S3-compatible object storage are required for durable
  progress. If either durable dependency is unavailable, Produce is not
  acknowledged and the client must retry.
- This result does not include a UI, cross-region replication, a Schema
  Registry service, Kafka Connect management, a Kubernetes Operator, or
  multi-virtual-cluster isolation.
- Unknown or future API versions are rejected with Kafka protocol errors; they
  are not treated as successful compatibility.

## Validation evidence

The release-candidate regression gates ran in OrbStack with Rust 1.88 and
isolated PostgreSQL and MinIO dependencies. The broader baseline rows were
completed against the immediately preceding implementation checkpoint.

| Gate | Last run | Evidence |
| --- | --- | --- |
| Rust workspace | 2026-07-30 | Formatting, strict all-target/all-feature Clippy, all workspace tests, fresh PostgreSQL integrations, release build, and RustSec audit passed |
| OpenDAL/MinIO | 2026-07-30 | Multipart immutable write, exact range fetch, and a 10+ MiB Kafka object fetch passed against the pinned OpenDAL release-candidate dependency graph |
| Flink/Java/Streams | 2026-07-30 | Flink 2.2.1 and 2.3.0, Kafka connector 5.0.0-2.2, Java Kafka 4.2.0, Kafka Streams 4.2.0, and Kafka 3.9.2 AdminClient acceptance passed |
| Security | 2026-07-30 | Kafka 4.3.1 TLS/SCRAM/ACL/delegation-token acceptance passed after the certificate and private-key parser update |
| Retention | 2026-07-29 baseline | Shared-object safety, durable deletion retry, replacement-Agent recovery, and zero remaining objects passed |
| Native clients | 2026-07-29 baseline | librdkafka 2.15.0 and franz-go 1.21.5 acceptance passed |
| Multi-Agent | 2026-07-29 baseline | 900 unique records at contiguous offsets, 378 committed objects, format-v1 integrity, and zero orphan objects passed |
| Kubernetes | 2026-07-29 baseline | 1,600 unique records at contiguous offsets across rollout and scaling, with zero PVCs, passed |

The executable acceptance gates are kept under `tests/`; each
`run-orbstack.sh` entry point recreates its isolated dependencies and
assertions.

## Conclusion

The current workspace is a validated Kafka-compatible, stateless,
object-storage-backed broker for the tested client and deployment envelope.
Flink runs correctly within that envelope. The explicit boundaries above are
part of the result; this checkpoint should not be described as complete Kafka
semantic parity.
