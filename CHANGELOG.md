# Changelog

All notable changes to rutomq are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Native Debian packages for `amd64` and `arm64`, plus RPM packages for
  `x86_64` and `aarch64`, with a hardened systemd service and external
  dependency configuration template.
- A responsive GitHub Pages project site with architecture, compatibility, and
  package-installation guidance.

## [0.1.0] - 2026-07-30

### Added

- Stateless Rust Agent with no local WAL, partition data, or persistent volume.
- OpenDAL-backed immutable S3/MinIO objects, multipart writes, range reads,
  checksum verification, orphan-object GC, retention, and log compaction.
- PostgreSQL control plane for topics, ordered offsets and spans, producers,
  transactions, consumer groups, ACLs, quotas, credentials, and maintenance.
- Kafka 4.3 generated wire baseline with 74 advertised API keys and backward
  client acceptance for Kafka Java 3.9/4.2, librdkafka, and franz-go.
- Idempotent and transactional Produce, `read_committed` Fetch, classic and
  Kafka 4 consumer groups, share groups, and Kafka Streams group protocol.
- Flink 2.2.1 source, sink, checkpoint-offset recovery, and exact-resume
  acceptance through the Kafka connector.
- TLS, SCRAM-SHA-256/512, delegation tokens, Kafka ACLs, and client quotas.
- Docker Compose development stack and Helm chart for stateless Kubernetes
  deployment.
- Prometheus metrics, health probes, graceful shutdown, and multi-Agent failure
  acceptance.

### Fixed

- Classic group rebalance deadlines are evaluated by PostgreSQL, avoiding
  premature generations when Agent and database clocks differ.

### Security

- Upgraded the Prometheus exporter to `0.14`, the Kafka 3 compatibility fixture
  to `kafka-clients 3.9.2`, and OpenDAL's XML parser to its upstream `0.41`
  security fix; replaced the unmaintained `rustls-pemfile` parser.

### Known boundaries

- `0.1.0` is a public preview, not a claim of complete Apache Kafka semantic
  parity.
- Non-empty online migration between classic and consumer group protocols is
  not implemented.
- Physical replica logs, follower reads, local log directories, and KRaft state
  are virtualized or excluded by the stateless architecture.
- Cross-region replication, Schema Registry, Kafka Connect management, a
  Kubernetes Operator, and multi-virtual-cluster isolation are not included.

[Unreleased]: https://github.com/SamuelSupe/rutomq/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/SamuelSupe/rutomq/releases/tag/v0.1.0
