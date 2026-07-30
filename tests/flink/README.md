# Flink compatibility acceptance

The default matrix entry runs the official Flink 2.2.1 image with
`flink-connector-kafka:5.0.0-2.2` and its Kafka 4.2 client against rutomq.
The same harness also supports the Flink 2.3.0 runtime.
The connector's official parent POM fixes `kafka-clients` at 4.2.0. The
acceptance harness deliberately preserves that binary contract: forcing 4.3.0
causes the connector's transactional sink to call a removed internal
`TransactionalRequestResult.await()` signature. Kafka 4.3 protocol coverage is
validated separately by `tests/security/run-orbstack.sh`, including an exact
`DescribeLogDirs` v5 request.

It verifies:

- a two-partition exactly-once `KafkaSink`;
- a bounded `KafkaSource` using earliest/latest offset discovery;
- checkpoint-driven consumer offset commits;
- a source-to-sink exactly-once transfer;
- recovery after an injected failure following a completed checkpoint;
- `read_committed` output with no missing or duplicate records.
- Kafka 4 consumer protocol (`group.protocol=consumer`) with two members,
  server-side assignment, cooperative rebalance, offset commit, and restart.
- Kafka 4.3 KIP-1263 assignment batching for consumer, share, and Streams
  groups, including delayed assignment epochs, persisted recovery, and the
  exact Streams status-code-5 assignment-delay message observed by a real
  Kafka Streams client.
- Kafka 4.3 KIP-1251 consumer assignment epochs through Kafka 4.2 generated
  wire: one stale OffsetCommit accepts the retained partition and returns
  `STALE_MEMBER_EPOCH` only for the partition that moved.
- Kafka Admin OffsetCommit/OffsetFetch preserves offset metadata and committed
  leader epoch `0` through PostgreSQL.
- Kafka 4.3 KIP-1211 topic-creation defaults with the acceptance Agent set to
  three partitions: real AdminClient `-1` creation and generated Metadata v13
  auto-creation must both return the same topology.
- Kafka 4.3 KIP-1257 partition retention pressure through the Agent's
  Prometheus endpoint, including one zero-valued series per partition for
  unlimited `retention.bytes`.
- generated StreamsGroupHeartbeat internal-topic creation with replication
  factor `0`, one-replica virtual-default resolution, dynamic config
  provenance, acceptance of Kafka Streams' range-validated no-op
  `segment.bytes` repartition hint, and non-mutating `MISSING_INTERNAL_TOPICS`
  failures for an explicit factor of two or unsupported physical-log
  configuration.
- Kafka classic static membership replacement with `group.instance.id`, old
  member fencing, Admin description, and batched member removal.
- topic-level post-compression `max.message.bytes`, CreateTime rejection, and
  LogAppendTime Producer/Consumer timestamp agreement.
- topic-level `compression.type` for producer/uncompressed/gzip/snappy/lz4/zstd,
  exact Java consumer round trips, final-size rejection, and DELETE to default.
- Kafka Streams 4.2 with two stream threads under both
  `group.protocol=classic` and `group.protocol=streams`, `exactly_once_v2`,
  stable Streams-group Admin description, cooperative sticky task assignment,
  materialized changelog restore, repartition-topic creation/partition
  propagation, dynamic GROUP configs, standby promotion, and restart without
  replay.
- Kafka 4.2 modern Admin APIs for cluster description, filtered group listing,
  classic-group description, empty consumer/classic group deletion,
  validate-only and committed partition expansion, and topic-partition
  description;
- `DeleteRecords` low-watermark advancement and `OffsetDelete` committed-offset
  removal.
- Kafka 4.2 client-quota validate-only/commit/remove operations, strict and
  non-strict filters, default and IP entities, numeric validation, and
  observable Produce/Fetch throttling with successful Fetch retry.
- Kafka 4.2 Admin feature description and update against Kafka 4.3
  `metadata.version` support, including supported/finalized
  ranges, validate-only, atomic failure, invalid direction, and epoch changes.
- Kafka 4.2 generated-wire legacy Group/ClientMetrics full replacement and
  Kafka 3.9 AdminClient legacy ClientMetrics replacement, including
  validate-only, omitted-key deletion, and empty-resource removal.
- two Kafka 4.2 `KafkaShareConsumer` members with explicit accept/release,
  redelivery, duplicate-free acceptance, group description, and Admin
  share-offset query/reset/delete.
- Kafka 4.2 `share.auto.offset.reset=by_duration:PT5M` configured through
  Admin, with record-level timestamp selection and persisted SPSO verified by
  a real `KafkaShareConsumer`.
- Kafka 4.2 compacted-topic creation, sparse offset preservation, idempotent
  producer sequence continuity, unkeyed records, and tombstone expiry.
- Kafka 4.2 `InitProducerId` v6 KIP-939 recovery, including distinct prepared
  and recovery producer identities, survival beyond the original transaction
  timeout, completion after fencing, and `read_committed` visibility.
- Kafka 4.2 generated-wire InitProducerId identity-pair validation and legacy
  `INVALID_PRODUCER_EPOCH`/`PRODUCER_FENCED` boundaries for InitProducerId,
  AddOffsetsToTxn, and EndTxn.
- Kafka 4.2 generated-wire `FindCoordinator` v4-v6 batching, duplicate-key
  cardinality, coordinator-type validation, share-key validation, and
  non-routable error-node semantics.
- Kafka 4.2 generated-wire `ListTransactions` unknown-state and full-ID regex
  filters, `INVALID_REGULAR_EXPRESSION`, and `DescribeTransactions` empty-ID
  validation.

Flink commits source offsets asynchronously after checkpoint completion. The
verifier waits up to 15 seconds for the final bounded-source offsets to reach
the exact expected value; it never accepts a partial commit.

Validated on 2026-07-30:

| Flink runtime | Connector | Kafka client | Records | Result |
| --- | --- | --- | ---: | --- |
| 2.2.1 | 5.0.0-2.2 | 4.2.0 | 1,000 | passed, including KIP-1263 assignment batching, FindCoordinator v4-v6 and transaction-query semantics, six-codec topic compression, record admission, classic static fencing, stateful Streams/standby, share duration reset, compaction, and KIP-939 recovery |
| 2.3.0 | 5.0.0-2.2 | 4.2.0 | 1,000 | passed |

Run with OrbStack:

```bash
tests/flink/run-orbstack.sh
```

Set `KEEP_CLUSTER=1` to preserve the compose project after a run.

Run the Flink 2.3.0 matrix entry with:

```bash
RUTOMQ_FLINK_VERSION=2.3.0 \
RUTOMQ_FLINK_COMPOSE_PROJECT=rutomq-flink-23 \
  tests/flink/run-orbstack.sh
```

`RUTOMQ_FLINK_RECORDS` changes the generated record count.
`RUTOMQ_FLINK_KAFKA_VERSION` overrides the connector artifact and
`FLINK_IMAGE` overrides the runtime image.
