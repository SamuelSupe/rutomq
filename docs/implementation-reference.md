# rutomq implementation reference

This document preserves the detailed implementation, configuration, security,
and acceptance notes for the `v0.1.0` checkpoint. Start with the
[project overview](../README.md), then use the
[compatibility matrix](compatibility.md) for the API-by-API contract.

Rust-based, Kafka-compatible, diskless message queue.

The data plane is stateless: acknowledged records are persisted to an S3-compatible object store and indexed by PostgreSQL. Agents do not use a local WAL or persistent volume.

The current Kafka wire coverage is recorded in
[`compatibility.md`](compatibility.md). The dated validation result
for the current checkpoint is
[`compatibility-result-2026-07-30.md`](compatibility-result-2026-07-30.md).
Produce is acknowledged only after the OpenDAL object write and the PostgreSQL
metadata transaction both succeed. A bounded in-memory batch uses a 250 ms window or 8 MiB limit;
topic `flush.messages` and `flush.ms` can only shorten that window for the
partitions present in the batch. An agent crash drops only unacknowledged
requests, which clients can retry.
An unsupported future `ApiVersions` request receives Kafka's version-zero
`UNSUPPORTED_VERSION` response and the complete supported API table, allowing
the client to retry negotiation on the same connection.
Matching Kafka 4.3's KAFKA-18659 librdkafka workaround, that table advertises
Produce 0-13 even though removed Produce schemas 0-2 remain rejected and the
actual implementation baseline is version 3.
Produce never creates topic metadata: after Topic Write authorization, an
authorized missing topic or out-of-range partition returns
`UNKNOWN_TOPIC_OR_PARTITION`, while Metadata remains the explicit auto-create
path. Valid partitions in the same request continue independently.
Metadata v0 empty lists, v10/v11 identity validation, v12+ topic-ID-first
selection, unknown-ID privacy, Describe/Create authorization ordering, invalid
names, and requested authorized-operation bitfields follow Kafka 4.2.
Idempotent producer retries reuse their original offset. Transactional records
remain invisible to `read_committed` consumers until `EndTxn`, and staged group
offsets are applied in the same PostgreSQL transaction. For an idempotent-only
`InitProducerId` request, Kafka defines `transaction_timeout_ms` as irrelevant;
rutomq therefore accepts the zero value sent by librdkafka while continuing to
validate positive timeouts for producers with a transactional ID.
Kafka's broker-listener `WriteTxnMarkers` v1-v2 is also supported. Since Agents
have no replica logs, a marker atomically updates the matching PostgreSQL
transaction and object-span visibility after validating complete partition
coverage. Legacy marker epochs may equal the transaction's data epoch, while
KIP-1228 transaction-version 2 markers must use a strictly greater epoch.
Accepted producer/coordinator marker epochs persist across Agents, exact
completed retries are idempotent, and late producer or coordinator epochs are
fenced before visibility changes.
New object spans persist a format version and SHA-256 checksum. Fetch verifies
each OpenDAL range before decoding or caching it; corruption returns a Kafka
storage error. Pre-migration version-zero spans remain readable during rolling
upgrades.
Kafka 4.2 `InitProducerId` v6 supports KIP-939 prepared-transaction recovery:
the old producer is fenced without aborting its pending transaction, 2PC
transactions do not expire automatically, and a PostgreSQL-backed recovery
session can only commit or abort the preserved work. TransactionalId `WRITE`
is required for every transactional producer. The Kafka-compatible broker gate
`transaction.two.phase.commit.enable` defaults to false and is controlled by
`RUTOMQ_TRANSACTION_TWO_PHASE_COMMIT_ENABLE`; once enabled, v6 requests with
`enable_2_pc=true` additionally require Kafka's dedicated
`TWO_PHASE_COMMIT(15)` ACL and may exceed the ordinary transaction timeout
ceiling. Other transactional producers are bounded by
`transaction.max.timeout.ms`, which defaults to 900000 ms.
`CreateAcls` validates Kafka UNKNOWN/ANY/MATCH enum values for the complete
request before any PostgreSQL mutation; semantic resource errors remain
independent per creation.
Kafka Admin clients can describe and incrementally change topic retention
settings; legacy full-replacement `AlterConfigs` is also supported. The
three topic-config mutation APIs reject settings that require Kafka's local
segmented log, physical replica machinery, or broker tiered-storage subsystem
with `INVALID_CONFIG`. Rejection is atomic per topic, validate-only reports the
same error, and an invalid topic does not block an unrelated valid resource in
the request. The
topic `compression.type` policy preserves producer compression by default or
re-encodes immutable batches as uncompressed/gzip/snappy/lz4/zstd before
applying `max.message.bytes`; `compression.gzip.level`,
`compression.lz4.level`, and `compression.zstd.level` use Kafka's defaults,
ranges, and actual codec encoders rather than config-only values. Fetch offset
rewriting preserves that codec. Producer-supplied Zstandard requires Produce
v7 or newer; v3-v6 return `UNSUPPORTED_COMPRESSION_TYPE`. An older request
carrying another source codec may still be rewritten to topic Zstandard by the
broker. The retention worker advances
`log_start_offset` and keeps shared objects while any span still references
them. Topic `file.delete.delay.ms` (default: 60000 ms) persists the physical
deletion deadline contributed by retention, compaction, `DeleteRecords`, or
topic deletion. For an object shared by multiple topics, the largest deadline
wins; deletion starts only after the final span is gone and both that deadline
and the Agent safety grace have elapsed. PostgreSQL keeps the committed object
row as a durable deletion obligation until OpenDAL deletion succeeds. A failed
delete, Agent crash, or crash between physical deletion and metadata completion
is retried idempotently by any replacement Agent without a local retry journal.
Kafka topic `flush.messages` (default: `Long.MAX_VALUE`) counts admitted
records per partition, while `flush.ms` (default: `Long.MAX_VALUE`) contributes
the earliest partition deadline. Reaching either configured threshold commits
the whole current cross-topic batch through immutable OpenDAL storage and the
PostgreSQL metadata transaction; these settings never introduce a local WAL.
`CreateTopics` preserves Kafka's `-1` partition/replication defaults and
validates the one-broker virtual topology explicitly. KIP-1211 topic creation
uses one virtual-controller default for Admin requests and Metadata
auto-creation; `RUTOMQ_NUM_PARTITIONS` selects the positive partition count,
while `RUTOMQ_DEFAULT_REPLICATION_FACTOR` must remain one. Automatic placement
only accepts replication factor one; manual placement requires zero-based
consecutive partition indexes, `num_partitions=-1`,
`replication_factor=-1`, and exactly `[0]` as every replica list.
Kafka topic-name length, legal-character, `.`/`..`, and dot/underscore
collision rules are enforced in both memory and PostgreSQL. A PostgreSQL
transaction-level namespace lock makes colliding creates safe across Agents;
existing topics fail during validate-only as well as committed creation, and
duplicate names in one request cannot partially create that topic.
Kafka v5+ creation responses return the 19 supported effective settings in
name order with dynamic/default source metadata. Partition count, virtual
replication factor, and v7 topic ID are exposed through Kafka Admin; a missing
Topic/DescribeConfigs permission hides those settings without rolling back a
successfully authorized topic creation.
Requests whose upper-bound total exceeds Kafka's 10,000-partition batch limit
fail every topic with `POLICY_VIOLATION` before ACL checks or metadata
mutation, so a rejected batch cannot create a partial topic set.
`DeleteTopics` v6 accepts names or topic IDs, validates duplicate/conflicting
identities before mutation, and deletes the resolved immutable ID in
PostgreSQL so concurrent name reuse cannot redirect the operation. Topic-name
existence is checked only after Describe authorization, a resolved ID's name
is hidden without Describe, and name/ID alias conflicts are reported only
after deletion authorization so the mapping cannot be probed.
Topic config source names are persisted with the values. `DescribeConfigs`
therefore reports explicit create/SET/APPEND/SUBTRACT values as
`DYNAMIC_TOPIC_CONFIG`, while DELETE and untouched values remain
`DEFAULT_CONFIG`, including after another Agent reconnects to PostgreSQL.
When Kafka Admin requests synonyms, dynamic topic values are followed by the
mapped virtual broker default; inherited values return that default directly.
GROUP and CLIENT_METRICS resources expose their same-name effective values
with Kafka 4.2 source IDs 8 and 7.
For topics using `cleanup.policy=compact`, a leased stateless worker rewrites
immutable objects while preserving sparse Kafka offsets. It keeps the latest
record per key, preserves historical unkeyed records written before compaction
is enabled, respects
`min.compaction.lag.ms`, waits for `min.cleanable.dirty.ratio` unless the
oldest dirty span reaches `max.compaction.lag.ms`, rechecks tombstones after
`delete.retention.ms`, and uses a PostgreSQL compare-and-swap before replacing
span references. Once the cleanup policy contains `compact`, new client records
without a key are rejected with `INVALID_RECORD` before offset allocation.
The virtual data topology has one honest ISR member. Topic
`min.insync.replicas` is persisted and enforced for Produce: `acks=all`
returns `NOT_ENOUGH_REPLICAS` when the configured minimum exceeds one,
`acks=1` still uses the durable object-plus-metadata commit path, and invalid
required-acks values are rejected. rutomq does not invent physical replicas or
a local replication log.
Kafka 4's consumer group protocol is supported with PostgreSQL-persisted
member epochs, `uniform` and `range` server-side assignment, cooperative
partition transfer, regex subscriptions, and `ConsumerGroupDescribe`.
The consumer protocol heartbeat defaults to 5000 ms within
`group.consumer.min.heartbeat.interval.ms=5000` and
`group.consumer.max.heartbeat.interval.ms=15000`; its session timeout defaults
to 45000 ms within `group.consumer.min.session.timeout.ms=45000` and
`group.consumer.max.session.timeout.ms=60000`. These read-only broker bounds
also validate per-group `consumer.heartbeat.interval.ms` and
`consumer.session.timeout.ms` Admin updates.
Share groups use the same 5000..15000 ms heartbeat and 45000..60000 ms session
ranges through `group.share.min.*`/`group.share.max.*`; per-group
`share.heartbeat.interval.ms` and `share.session.timeout.ms` overrides are
bounded before they reach the durable coordinator.
Share acquisition locks default to 30000 ms and group-level
`share.record.lock.duration.ms` values are bounded to 15000..60000 ms.
Streams groups use the corresponding `group.streams.min.*` and
`group.streams.max.*` broker bounds; per-group
`streams.heartbeat.interval.ms` and `streams.session.timeout.ms` values are
validated before persistence, while legacy persisted values are clamped when
read. `streams.num.standby.replicas` is likewise limited by the read-only
`group.streams.max.standby.replicas=2` broker setting. The effective
`group.streams.initial.rebalance.delay.ms=3000` value is also reported as a
read-only broker config and delays the first assignment of a new Streams group.
Consumer, share, and Streams members also persist their previous epoch so an
exact heartbeat retry after a lost response is idempotent across Agent
restarts. Consumer/Streams recovery fences ownership claims outside the
persisted assignment. Consumer static epoch `-2` retains the assignment while
temporarily leaving and permits a fresh member ID to reconnect; no coordinator
recovery state is Agent-local.
`FindCoordinator` routes group, transaction, and Kafka 4.2 share-partition
keys to the virtual endpoint while preserving batched request cardinality.
Invalid, unauthorized, and backend-failed lookups return Kafka's non-routable
no-node identity instead of leaking a usable broker endpoint.
Classic JoinGroup generations use a PostgreSQL-backed barrier with a shared
rebalance ID, deadlines, and per-member join tokens. Responses wait for the
configured `group.initial.rebalance.delay.ms` and required rejoins; any Agent
can finish the generation exactly once. Old process members are reaped at
session timeout even when a longer rebalance timeout is active, and a
LeaveGroup during the barrier does not double-increment the generation.
Classic members must request a session timeout within
`group.min.session.timeout.ms=6000` and
`group.max.session.timeout.ms=1800000`; the Helm/env overrides are
`RUTOMQ_GROUP_MIN_SESSION_TIMEOUT_MS` and
`RUTOMQ_GROUP_MAX_SESSION_TIMEOUT_MS`. Out-of-range JoinGroup requests return
Kafka's `INVALID_SESSION_TIMEOUT` without creating group state.
`RUTOMQ_GROUP_MAX_SIZE` controls the read-only `group.max.size` setting
(default: 2147483647). After expired members are removed, a new classic member
at capacity receives `GROUP_MAX_SIZE_REACHED`; existing rejoins, valid pending
member-ID retries, and static identity replacement do not consume another
slot.
Empty classic and Kafka 4 consumer-protocol groups can take over the same
group ID in either direction. Conversion expires stale dynamic members,
invalidates pending classic member IDs, and preserves committed offsets and
GROUP configuration. Active members and consumer static members at temporary
epoch `-2` still block conversion. Non-empty rolling migration remains
disabled until `group.consumer.migration.policy` is implemented.
Kafka Streams 4.2's broker-driven group protocol is also supported through
`StreamsGroupHeartbeat` and `StreamsGroupDescribe`. Topology, process/member
epochs, current and target task assignments, interactive-query endpoints,
task offsets, and liveness live in PostgreSQL. The coordinator creates
authorized changelog/repartition topics, propagates repartition partition
counts, resolves Streams replication factor `0` through
`default.replication.factor`, and applies the same supported configuration
validation and provenance as CreateTopics. Kafka Streams' standard
`segment.bytes` repartition hint is range-validated but has no local-log effect
on immutable object packing. Failed creation leaves the group
`NOT_READY` with `MISSING_INTERNAL_TOPICS`; an existing internal topic's
configuration mismatch remains non-blocking. The coordinator preserves sticky
active tasks and places stateful standbys on different processes. Incoming
topologies reject Kafka internal topics and invalid Kafka topic names with
`STREAMS_INVALID_TOPOLOGY` before topic authorization, internal-topic
creation, or group mutation. Stateful
two-thread applications have passed
`group.protocol=streams`, `exactly_once_v2`, changelog restore after local
state deletion, repartition, standby promotion, and restart-without-replay
acceptance.
Kafka Admin clients can list classic and Kafka 4 consumer groups, describe
classic members, delete empty groups, and describe the virtual cluster.
Raw `DeleteGroups` requests are stably deduplicated before authorization and
mutation, so each distinct group is deleted and reported exactly once.
The virtual BROKER and BROKER_LOGGER resources expose read-only listener,
protocol-limit, group-runtime, object-batch, cache, no-local-WAL, and tracing
filter values without synthesizing local Kafka log or replica settings.
Topic partition counts can be increased atomically through `CreatePartitions`.
Duplicate topic resources are rejected before authorization or mutation while
unrelated topics continue independently; `DescribeTopicPartitions` exposes the
virtual topology with Kafka-compatible partition pagination. Explicit names
are authorized before existence lookup, denied names receive a zero topic ID,
all-topic queries omit denied names, and invalid cursors fail the request.
`DeleteRecords` advances the logical log start without renumbering offsets, and
atomically rebases or removes partition-local producer retry state from the
retained spans. It stops before pending transactional data so an administrative
truncation cannot invalidate an open transaction.
`OffsetDelete` protects offsets for topics that an active group still
subscribes to.
Kafka 4.2 OffsetCommit/OffsetFetch v10 resolve topic IDs through PostgreSQL
while retaining the earlier name-based wire versions. OffsetCommit v6+ and
TxnOffsetCommit v2+ preserve the supplied committed leader epoch through
memory or PostgreSQL, including transactional staging, and OffsetFetch v5+
returns it exactly; earlier fetch versions retain the protocol default `-1`.
Kafka 4.3 KIP-1251 consumer groups persist an assignment epoch per assigned or
pending-revocation partition. OffsetCommit v9+ can therefore accept a harmless
older member epoch for partitions the member retained while returning
`STALE_MEMBER_EPOCH` only for partitions that moved. Validation and accepted
offset upserts share one memory/SQL transaction. TxnOffsetCommit applies the
same partition proof atomically: any moved partition returns
`ILLEGAL_GENERATION` for the request and stages no partial offsets. Static
temporary leave resets retained assignment epochs to zero, and PostgreSQL
reconnect preserves all fencing decisions.
Fetch v7+ uses a bounded broker-local session cache for Kafka's full and
incremental response optimization. Sessions are recoverable protocol state and
may be recreated after Agent loss; acknowledged records and offsets remain in
OpenDAL/PostgreSQL. Consumer Fetch resolves topic IDs and completes one batched
Topic Read authorization preflight before metadata or object reads. Name-based
requests authorize before existence lookup, authorization-backend failures use
Kafka's versioned request-wide response, and mixed responses place fetched
partitions before accumulated denied or missing partitions.
`DescribeLogDirs` exposes one `/rutomq/object-store` virtual directory with
per-partition object-span bytes and unknown remote capacity.
`AlterReplicaLogDirs` accepts placement into that virtual directory as a no-op;
it never creates a local log directory, WAL, or persistent volume.
`ElectLeaders` and partition reassignment APIs expose the same fixed virtual
topology: broker 0 is already leader and replica placement is always `[0]`.
`DescribeQuorum` exposes the PostgreSQL control plane as one honest virtual
KRaft metadata quorum with controller/voter 0; it does not imply a local Raft
log or physical ISR.
Kafka 4.0 client-metrics resource discovery and Kafka 4.1+ filtered config
resource discovery expose existing topics plus virtual broker resources.
Kafka Admin client quotas support `user`, `client-id`, combined
`user`/`client-id`, and `ip` entities, including defaults, validate-only,
strict filters, and PostgreSQL persistence. Agents enforce producer/consumer
bandwidth, request percentage, controller mutation, and connection creation
rates without a local WAL.
Kafka Admin feature negotiation reports PostgreSQL-backed finalized
`metadata.version` 29 with Kafka 4.3 level 30 supported,
`transaction.version` 0-2, and
`group.version`/`share.version`/`streams.version` 0-1 levels. Level 30 is
finalized only through an explicit `UpdateFeatures` request after rolling all
Agents; its metadata-changing transition cannot be downgraded.
`UpdateFeatures` otherwise supports validate-only and atomic
upgrades/downgrades.
Disabling a group feature returns typed `UNSUPPORTED_VERSION` errors from its
protocol rather than a fake success. Physical KRaft and eligible-leader feature
ranges are intentionally not advertised.
Kafka transaction v2 uses Produce v12+ and TxnOffsetCommit v5+ server-side
participant registration. EndTxn v5 atomically commits or aborts, bumps the
producer epoch exactly once, and returns the same bumped identity to exact
retries across Agent replacement.
Client AddPartitionsToTxn requests authorize the complete transaction and
validate every partition before one PostgreSQL mutation; mixed failures return
`OPERATION_NOT_ATTEMPTED` for otherwise valid partitions and never register a
partial participant set. Kafka v4+ broker-form requests use Cluster
`CLUSTER_ACTION` and preserve the same per-transaction atomicity.
Multi-producer object commits and the transaction-timeout sweep lock producer
rows in ascending producer-ID order before partition or transaction mutation.
This keeps concurrent Agents, EndTxn, and the stateless expiry worker on one
PostgreSQL lock order instead of surfacing a deadlock to Kafka producers.
Produce, compaction, and retention likewise lock partitions by topic name and
partition index, rather than mixing name order with random topic-UUID order.
Regular OffsetCommit, transactional offset publication, OffsetDelete, and
empty-group deletion mutate PostgreSQL consumer-offset rows in
`(group ID, topic ID, partition)` order. Topic rows are protected before
offset rows when a commit may create a foreign-key reference, so reverse
partition requests and concurrent topic deletion cannot form an inverse lock
cycle.
InitProducerId rejects half-present producer identities. Its v0-v3 fencing
errors, plus AddOffsetsToTxn and EndTxn v0-v1 fencing errors, are
down-converted to `INVALID_PRODUCER_EPOCH`; newer versions return
`PRODUCER_FENCED`.
Kafka 4.2/4.3 share consumers use PostgreSQL-backed membership, fetch sessions,
record acquisition locks, delivery counts, and acknowledgements. ShareFetch
v2 supports batch-aligned and strict record-limit acquisition;
ShareAcknowledge v2 supports lock renewal and returns the lock timeout. Real
Admin clients can query KIP-1226 lag, reset, and delete empty-group share
offsets through APIs 90-92. Kafka 4.3 broker-listener APIs 83-87 initialize,
read, write, summarize, and delete the same Share Coordinator partition state.
The state is range-normalized in PostgreSQL, is observed by subsequent
ShareFetch/ShareAcknowledge operations, and never relies on a local
`__share_group_state` log or Agent WAL. Kafka 4.2 GROUP configs include
`share.auto.offset.reset=by_duration:<ISO-8601>`; rutomq performs one exact
record-timestamp lookup and persists the resulting SPSO. Kafka 4.3 KIP-1240
adds per-group delivery-count and per-partition record-lock limits plus a renew
acknowledgement switch; persisted values are bounded by the described broker
limits, and disabled renewals return `INVALID_RECORD_STATE`. No share state is
durable on an Agent. Kafka 4.3 KIP-1263 batches assignment recomputation for
consumer, share, and Streams groups. Membership changes advance the group
epoch immediately, while the target assignment remains stable until the
configured assignment interval elapses. The default `1000 ms` interval, its
`0..15000 ms` bounds, per-group overrides, and the last-assignment timestamp
are PostgreSQL-backed, so Agent replacement cannot bypass the delay. The three
cluster-wide defaults can be changed through the empty Kafka BROKER config
resource; a `0 ms` effective value removes batching delay. Assignment
computation is also offloaded by default to a bounded pool of two dedicated
workers. A new member first receives epoch 1 and an empty assignment; a
completed target is published at epoch 2 on a later heartbeat. PostgreSQL
group locks provide cross-Agent single-flight, and each task fences on the
persisted group epoch, assignment epoch, and prior computation timestamp so a
late result cannot overwrite newer membership. The broker-wide
`group.*.assignor.offload.enable` settings and corresponding per-group
`*.assignor.offload.enable` overrides can disable offload protocol by protocol.
Share groups remain reconciling until every member acknowledges its current
epoch, preventing a newly published assignment from being reported stable
before consumers can observe it. Stable Streams groups do not report an
assignment-interval delay during periodic metadata probes unless a newer group
epoch still needs assignment.
Kafka client telemetry implements KIP-714 `GetTelemetrySubscriptions` and
`PushTelemetry`. Admin clients manage `CLIENT_METRICS` resources in
PostgreSQL, Agents match subscriptions using client and connection attributes,
and accepted compressed telemetry is exposed through Prometheus counters.

## Development

The project targets Rust 1.88. The host toolchain is not required; the supported development path uses the repository's Rust container and OrbStack.

```bash
docker compose -f deploy/compose.dev.yml up -d postgres minio
docker compose -f deploy/compose.dev.yml run --rm migrate
docker compose -f deploy/compose.dev.yml up --build rutomq
```

The Kafka listener defaults to `127.0.0.1:9092`. Health and metrics are exposed on `127.0.0.1:8080`.

`RUTOMQ_NUM_PARTITIONS` defaults to `1` and drives every unspecified
CreateTopics and Metadata auto-creation partition count.
`RUTOMQ_DEFAULT_REPLICATION_FACTOR` defaults to and must remain `1` because
rutomq exposes one virtual service replica.
`RUTOMQ_AUTO_CREATE_TOPICS_ENABLE` defaults to `true`; setting it to `false`
disables Metadata auto-creation without affecting explicit CreateTopics.
All three are read-only Kafka broker settings and should be identical across
the stateless Agent Deployment.
Retention runs every 60 seconds by default. `RUTOMQ_RETENTION_INTERVAL_MS`
changes the sweep interval and `RUTOMQ_OBJECT_DELETE_GRACE_MS` controls the
Agent-wide safety floor between removing the final object-span reference and
deleting the object (default: 300000 ms). The effective physical deletion
delay is the larger of this floor and every removed span's persisted topic
`file.delete.delay.ms`; Agent replacement cannot shorten it.
`RUTOMQ_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS` and
`RUTOMQ_GROUP_CONSUMER_SESSION_TIMEOUT_MS` control the Kafka 4 consumer
protocol defaults (5000 ms and 45000 ms).
`RUTOMQ_GROUP_CONSUMER_MAX_SIZE` controls
`group.consumer.max.size` (default: 2147483647). New members are rejected with
`GROUP_MAX_SIZE_REACHED` only after expired members are removed; existing
member retries and static replacement do not consume another slot.
`RUTOMQ_GROUP_CONSUMER_ASSIGNORS` controls the ordered
`group.consumer.assignors` list (default: `uniform,range`). The first entry is
used when the first member omits `server_assignor`; an explicit request for a
built-in assignor outside the configured list receives
`UNSUPPORTED_ASSIGNOR`. Only the Rust-native `uniform` and `range` algorithms
are accepted.
`RUTOMQ_GROUP_CONSUMER_REGEX_REFRESH_INTERVAL_MS` controls
`group.consumer.regex.refresh.interval.ms` (default: 600000 ms, minimum:
10000 ms). A newly matching topic is incorporated after Kafka's 10-second
minimum refresh guard, while the configured interval forces periodic
re-resolution even when topic metadata has not changed. Pending refresh state
is PostgreSQL-backed and survives Agent replacement.
`RUTOMQ_GROUP_SHARE_ASSIGNORS` controls `group.share.assignors` and must
contain exactly one entry. The supported and default value is `simple`, which
selects the Rust-native assignor used by every Share group. Custom Java class
names and multiple entries are rejected at Agent startup instead of being
reported as supported.
`RUTOMQ_GROUP_CONSUMER_ASSIGNMENT_INTERVAL_MS`,
`RUTOMQ_GROUP_CONSUMER_MIN_ASSIGNMENT_INTERVAL_MS`, and
`RUTOMQ_GROUP_CONSUMER_MAX_ASSIGNMENT_INTERVAL_MS` control the consumer
assignment default and bounds (1000 ms, 0 ms, and 15000 ms).
`RUTOMQ_GROUP_COORDINATOR_BACKGROUND_THREADS` controls the fixed assignment
worker count (default: 2). `RUTOMQ_GROUP_CONSUMER_ASSIGNOR_OFFLOAD_ENABLE`,
`RUTOMQ_GROUP_SHARE_ASSIGNOR_OFFLOAD_ENABLE`, and
`RUTOMQ_GROUP_STREAMS_ASSIGNOR_OFFLOAD_ENABLE` provide the static fallbacks
for the three dynamic Kafka broker settings and default to `true`.
`RUTOMQ_GROUP_COORDINATOR_CACHED_BUFFER_MAX_BYTES` and
`RUTOMQ_SHARE_COORDINATOR_CACHED_BUFFER_MAX_BYTES` provide the KIP-1196 static
fallbacks (default: 1048588, minimum: 524288). Kafka Admin can override either
cluster-wide; rutomq persists the values in PostgreSQL but retains no
coordinator append buffers.
`RUTOMQ_STREAMS_GROUP_HEARTBEAT_INTERVAL_MS`,
`RUTOMQ_STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS`,
`RUTOMQ_STREAMS_GROUP_MAX_HEARTBEAT_INTERVAL_MS`,
`RUTOMQ_STREAMS_GROUP_SESSION_TIMEOUT_MS`,
`RUTOMQ_STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS`,
`RUTOMQ_STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS`,
`RUTOMQ_GROUP_STREAMS_MAX_SIZE`,
`RUTOMQ_STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS`,
`RUTOMQ_STREAMS_GROUP_NUM_STANDBY_REPLICAS`,
`RUTOMQ_STREAMS_GROUP_MAX_STANDBY_REPLICAS`,
`RUTOMQ_STREAMS_ACCEPTABLE_RECOVERY_LAG`, and
`RUTOMQ_STREAMS_TASK_OFFSET_INTERVAL_MS` control the Streams protocol defaults
and timeout ranges (5000 ms heartbeat within 5000..15000 ms, 45000 ms session
within 45000..60000 ms, a maximum of 2147483647 clients, 3000 ms initial
delay, 0 replicas, 10000 records, and 10000 ms); capacity is checked after
expired members are removed and the standby default is bounded by a
configurable maximum of 2.
`RUTOMQ_STREAMS_GROUP_ASSIGNMENT_INTERVAL_MS`,
`RUTOMQ_STREAMS_GROUP_MIN_ASSIGNMENT_INTERVAL_MS`, and
`RUTOMQ_STREAMS_GROUP_MAX_ASSIGNMENT_INTERVAL_MS` control the Streams
assignment default and bounds (1000 ms, 0 ms, and 15000 ms).
`RUTOMQ_SHARE_GROUP_ASSIGNMENT_INTERVAL_MS`,
`RUTOMQ_SHARE_GROUP_MIN_ASSIGNMENT_INTERVAL_MS`, and
`RUTOMQ_SHARE_GROUP_MAX_ASSIGNMENT_INTERVAL_MS` provide the same defaults and
bounds for share groups.
`RUTOMQ_SHARE_GROUP_MAX_SIZE` controls `group.share.max.size` (default: 200,
valid range: 1..1000). Capacity is checked after expired members are removed
and within the shared PostgreSQL coordinator transaction.
`RUTOMQ_SHARE_RECORD_LOCK_DURATION_MS`,
`RUTOMQ_SHARE_MIN_RECORD_LOCK_DURATION_MS`, and
`RUTOMQ_SHARE_MAX_RECORD_LOCK_DURATION_MS` control the share acquisition-lock
default and group-level range (30000 ms, 15000 ms, and 60000 ms).
`RUTOMQ_TELEMETRY_MAX_BYTES` controls the maximum compressed client telemetry
payload (default: 1048576 bytes).
`RUTOMQ_COMPACTION_INTERVAL_MS`, `RUTOMQ_COMPACTION_LEASE_MS`, and
`RUTOMQ_COMPACTION_MAX_OBJECT_BYTES` default to 60000, 300000, and 67108864.
Compaction objects use the same OpenDAL backend and orphan-object GC as
Produce; no Agent-local WAL or persistent volume is introduced.
`RUTOMQ_PRODUCER_ID_EXPIRATION_MS` defaults to Kafka's 86400000 ms and is
reported as the read-only broker config `producer.id.expiration.ms`.
`RUTOMQ_PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS` controls the bounded
PostgreSQL cleanup sweep (default: 600000 ms).
`RUTOMQ_OFFSET_METADATA_MAX_BYTES` exposes Kafka's read-only
`offset.metadata.max.bytes=4096` default. OffsetCommit and TxnOffsetCommit
match Kafka 4.3's Java `String.length()` check by measuring UTF-16 code units,
then reject only an oversized partition with `OFFSET_METADATA_TOO_LARGE`;
accepted siblings continue and a rejected value is never persisted or
transactionally staged.
`RUTOMQ_OFFSETS_RETENTION_MINUTES` and
`RUTOMQ_OFFSETS_RETENTION_CHECK_INTERVAL_MS` expose Kafka's read-only
`offsets.retention.minutes=10080` and
`offsets.retention.check.interval.ms=600000` defaults. OffsetCommit v2-v4 may
set an explicit per-offset lifetime. Otherwise standalone/manual commits age
from commit time, while group-managed offsets stay protected for active
subscriptions and age after the classic group becomes Empty or stops
subscribing to the topic. Pending transactional offset updates are never
expired. The bounded sweep coordinates through PostgreSQL and introduces no
Agent-local WAL.
`RUTOMQ_TRANSACTIONAL_ID_EXPIRATION_MS` defaults to Kafka's 604800000 ms
(seven days). Empty and completed transactional IDs are detached after that
idle period, while ongoing transactions remain protected.
`RUTOMQ_TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS` controls
the stateless PostgreSQL cleanup sweep and defaults to 3600000 ms.
`RUTOMQ_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS` controls
the first and subsequent timed-out transaction abort scans and defaults to
Kafka's 10000 ms.
`DescribeConfigs` also reports Kafka's read-only
`add.partitions.to.txn.retry.backoff.ms=20` and
`add.partitions.to.txn.retry.backoff.max.ms=100`. The PostgreSQL-backed
AddPartitionsToTxn operation is already serialized and does not use an
Agent-local transaction-log retry loop.
`RUTOMQ_TRANSACTION_PARTITION_VERIFICATION_ENABLE` defaults to `true` and is
the static fallback for Kafka's cluster-wide dynamic
`transaction.partition.verification.enable` setting. Disabling it skips only
the partition-membership check for Produce v11 and below; producer fencing,
transactional-ID ownership, and ongoing transaction validation remain active.
Produce v12+ always performs transaction-v2 server-side partition registration.
`RUTOMQ_TRANSACTION_MAX_TIMEOUT_MS` controls Kafka's read-only
`transaction.max.timeout.ms` broker setting and defaults to 900000 ms.
`RUTOMQ_TRANSACTION_TWO_PHASE_COMMIT_ENABLE` controls Kafka's read-only
`transaction.two.phase.commit.enable` broker setting and defaults to `false`.
Active sequence state keeps
the five newest accepted Kafka record batches and their exact producer sequence
ranges idempotent. Producer sequence increments use Kafka's modulo-`2^31`
arithmetic both within and between batches, so `2147483647` advances to `0`
without losing duplicate detection in memory or PostgreSQL. Matching Kafka
4.2's client append validator, one Produce
partition payload may contain only one magic-v2 record batch; a concatenated
multi-batch payload returns `INVALID_RECORD` before an object or offset is
allocated. The original client header must also use `BaseOffset=0`, report a
positive record count, an offset range matching that count, and a nonnegative
base sequence when a producer ID is present. These checks run before topic
compression or timestamp rewriting, while broker-generated compacted batches
may retain sparse offsets.
CreateTime accepts Kafka's `NO_TIMESTAMP(-1)` sentinel. A client LogAppendTime
batch is validated with its batch-header `MaxTimestamp`, matching Kafka's
record iterator, and an in-range non-sentinel maximum is normalized to
CreateTime. If source and target compression match, normalization preserves
the raw inner timestamp deltas; a codec change rebuilds every record with that
interpreted batch maximum. Other client-set LogAppendTime cases are rejected.
LogAppendTime topics accept client CreateTime batches and assign broker time,
but reject client LogAppendTime batches. Rejected timestamp batches do not
allocate an offset. Timestamp-window comparisons use the same wrapping signed
64-bit subtraction and negation as Kafka's Java validator, including extrema
where a saturating lower/upper bound would disagree. Magic-v2 inner-record
timestamp deltas use Kafka's signed 64-bit Varlong representation, including
deltas beyond `i32::MAX`.
Declared inner-record bodies and the positive declared record-count payload
must also be consumed exactly; valid-CRC padding returns `INVALID_RECORD`
before topic rewriting or offset allocation. A declared batch record count or
record header count larger than its remaining decoded payload is rejected
before allocating declaration-sized vector storage. A stored CRC that does not
match the batch contents is distinguished as `CORRUPT_MESSAGE`, with no offset,
producer-state, or object mutation. Magic-v2 key, value, and header-value
lengths use Kafka's nullable rule: every negative Varint decodes as null, while
header keys and header counts remain non-null and nonnegative.
Offset reconstruction uses Kafka's signed two's-complement wrapping semantics
at `i64` boundaries; accepted Produce data is subsequently rewritten to
broker-owned partition offsets. When topic policy requires a pre-append batch
rewrite, encoding uses the first record as the explicit batch base and computes
ordered offset deltas with the same wrapping semantics rather than numeric
minimum/maximum subtraction. Offset assignment normalizes the batch partition
leader epoch to the virtual current epoch `0`. Record headers remain an ordered
sequence rather than a map, so repeated keys and their values survive
admission, storage, and Fetch without reordering or collapse.
`BaseTimestamp` is the first record timestamp, while `MaxTimestamp` remains the
batch maximum; later older records use signed negative Varlong deltas.
No-producer batches keep `NO_SEQUENCE(-1)` on every record and after sparse
compaction instead of deriving synthetic sequences from offset deltas.
An older retry is rejected as out of order. After state expiration, the next
append starts a fresh duplicate window. A partition with pending transactional
data is never expired or truncated. Retention and `DeleteRecords` rebuild the
five-batch window from retained spans and remove the producer from
`DescribeProducers` once none remain.
`RUTOMQ_ORPHAN_GC_INTERVAL_MS` and `RUTOMQ_ORPHAN_GC_GRACE_MS` default to
60000 and 300000. Before an OpenDAL PUT, an Agent records a PostgreSQL upload
intent. Metadata commit and orphan GC lock that same row, preventing one Agent
from deleting another Agent's in-flight object. A winning GC claim remains in
PostgreSQL and permanently fences metadata commit until OpenDAL deletion
succeeds and the intent is completed; delete failure or Agent replacement
therefore cannot lose the retry obligation.
When an idempotent retry follows a lost success response, PostgreSQL returns
the original span and offset. The retry's redundant immutable upload keeps its
uncommitted intent until orphan GC deletes it; it never becomes a second
visible append or an untracked object.
`RUTOMQ_SHUTDOWN_GRACE_MS` defaults to 25000. SIGTERM and Ctrl-C first remove
the Agent from readiness, stop new Produce admission, flush queued in-memory
batches, and then close active connections within that bound.
`RUTOMQ_FETCH_CACHE_BYTES` bounds the Agent-local immutable object-range cache
(default: 268435456). Setting it to zero disables caching. The cache is
recoverable, carries no acknowledged state, and disappears with the Agent.
`RUTOMQ_MAX_REQUEST_PARTITION_SIZE_LIMIT` defaults to Kafka's 2000 and bounds
each `DescribeTopicPartitions` page; client requests are clamped to at least
one partition.
Prometheus reports cache hit/miss/eviction/resident bytes, OpenDAL
request/error/byte/latency by operation, and Produce flush/metadata-commit
latency. `rutomq_object_integrity_failures_total` reports rejected checksum or
format metadata.
`rutomq_expired_consumer_offsets_total` counts offsets removed by the shared
expiration sweep, and `rutomq_consumer_offset_maintenance_errors_total`
reports failed sweeps.
Assignment offload exports queue and processing histograms by protocol,
queued/active worker gauges, an instantaneous idle ratio, and completion
counts by outcome under the `rutomq_group_assignment_background_*` metric
family.
`rutomq_coordinator_batch_buffer_cache_bytes` and
`rutomq_coordinator_batch_buffer_cache_discards_total` expose KIP-1196 group
and share coordinator labels. Both remain zero because the direct PostgreSQL
control path has no reusable coordinator-log append buffer.
Every Agent also refreshes a bounded snapshot of shared PostgreSQL state.
`rutomq_group_members`, `rutomq_group_rebalance_epoch`,
`rutomq_transactions_by_state`, `rutomq_consumer_group_lag`, and
`rutomq_partition_retention_size_percent` report group membership/state,
persisted generation or assignment epochs, latest transactional-ID states,
committed offset lag, and Kafka 4.3 KIP-1257 integer retention pressure.
Partition pressure uses each partition's currently referenced logical
object-span bytes divided by `retention.bytes`; unlimited retention reports
zero, delayed cleanup may exceed 100, and no local-retention metric is exposed
because Agents have no local log tier. The interval defaults to 15 seconds;
`RUTOMQ_OBSERVABILITY_MAX_GROUPS` (1000),
`RUTOMQ_CONSUMER_LAG_MAX_SERIES` (10000), and
`RUTOMQ_PARTITION_RETENTION_MAX_SERIES` (10000) cap label cardinality.
Collection errors and truncation are explicit metrics, and a failed refresh
keeps the previous snapshot. These are cluster-wide gauges repeated by each
Agent, so Prometheus queries should group by Agent instead of summing replicas.
`RUTOMQ_OBJECT_WRITE_CHUNK_BYTES` and
`RUTOMQ_OBJECT_WRITE_CONCURRENCY` configure OpenDAL multipart writes (defaults:
8388608 bytes and 4). S3-compatible chunk sizes must be at least 5 MiB.
Small objects use one conditional PUT. Larger writes atomically reserve the
final key before multipart upload, so another Agent cannot replace a committed
object even when an S3-compatible backend does not enforce multipart-complete
preconditions.

To exercise PostgreSQL sequence and transaction semantics explicitly:

```bash
RUTOMQ_TEST_PG_URL=postgres://rutomq:rutomq@host.docker.internal:5432/rutomq \
  cargo test -p rutomq-control --test postgres_transactions
```

The PostgreSQL suite includes a deterministic inverse-order race between one
multi-producer object commit and a timeout sweep over twelve transactions.

Real non-Java client compatibility is exercised by
[`tests/clients/run-orbstack.sh`](../tests/clients/run-orbstack.sh). The isolated
OrbStack gate runs confluent-kafka/librdkafka 2.15.0 and franz-go 1.21.5
through Admin, idempotent multi-partition Produce/Fetch, consumer-group
commit/restart, and committed/aborted transaction isolation.

`tests/compat/KafkaTransactionSmoke.java` is a dependency-free harness for the
Kafka client JARs shipped in the Apache Kafka 4.0 container. It verifies topic
creation/configuration, transaction list/describe, transaction initialization,
idempotent produce, transactional offset commit, abort filtering, and
`read_committed` fetch against a running agent.

Flink compatibility is exercised by [`tests/flink/run-orbstack.sh`](../tests/flink/run-orbstack.sh).
The OrbStack harness runs a real JobManager and TaskManager against PostgreSQL,
MinIO, and rutomq, then verifies checkpointed offsets, exactly-once KafkaSource
to KafkaSink transfer, injected-failure recovery, and duplicate-free
`read_committed` output. Flink connector 5.0.0-2.2 officially fixes its Kafka
client dependency at 4.2.0, so the harness preserves that supported binary
combination; Kafka 4.3 wire negotiation is covered by the separate security
suite. It also runs a two-member Kafka 4.2 consumer group
using `group.protocol=consumer`, including cooperative rebalance, offset
commit, describe, and restart recovery. The same Kafka 4.2 client verifies
cluster description plus filtered list/describe/delete operations for both
consumer and classic groups, raw duplicate-name `DeleteGroups` semantics,
validate-only and committed topic expansion, and Kafka 4.2 topic-partition
description. A dedicated classic static-member path
replaces the same `group.instance.id`, requires fencing of the old client, and
removes the surviving identity through Kafka Admin. A raw-heartbeat path sends
Kafka 4.2 consumer/share/streams requests to verify
previous-epoch response-loss recovery, owned-assignment fencing, and consumer
static temporary leave/reconnect. A raw JoinGroup v9 path holds the first
request through the initial rebalance delay, joins a second member through
shared PostgreSQL state, and requires one generation, one leader, and the full
two-member leader list. Raw StreamsGroupHeartbeat requests also verify exact
Kafka 4.2 internal/invalid topology topic-name rejection, default internal-topic
replication, the standard no-op repartition `segment.bytes` hint, unsupported
virtual replication, and unsupported configuration failures without metadata
mutation. A record-admission path also verifies
compressed batch limits, CreateTime bounds and `NO_TIMESTAMP`,
client timestamp-type rejection/normalization, and exact LogAppendTime
Producer/Consumer timestamps. The Kafka 4.2 Java producer/consumer path also
requires virtual leader epoch `0`, a same-batch timestamp delta beyond
`i32::MAX`, and two same-key headers in their original order. A six-codec path
verifies topic
final compression, exact payload round trips, final-size rejection, and config
DELETE back to `producer`. It also verifies gzip/LZ4/Zstandard level
Create/Describe, invalid-value rejection, payload round trips, and DELETE back
to Kafka defaults. The same Admin path creates, describes, incrementally
changes, validates, and DELETEs `file.delete.delay.ms` back to Kafka's 60000 ms
default. It creates, describes, incrementally changes, validates, and DELETEs
`flush.messages` and `flush.ms` back to Kafka's `Long.MAX_VALUE` defaults, and
the Rust object-store path proves that both policies trigger durable commits
before the Agent-wide window. A dedicated Kafka 4.2 Admin path verifies
default and explicit `CreateTopics` topology, manual virtual assignments,
validate-only, existing-topic/name/collision behavior, and exact
invalid-partition/replication/assignment exceptions. The returned
`CreateTopicsResult` must also expose effective configs with correct
dynamic/default sources, partition count, replication factor, and topic ID.
It also verifies log-start truncation, offset reset, producer state, topic
configuration, consumer-group offset deletion, and virtual object-store
log-directory APIs.
Preferred leader election and partition reassignment are also exercised through
the Kafka 4.2 `Admin` client, together with virtual metadata-quorum
description, config-resource discovery, client-quota CRUD, and observable
Produce/Fetch throttling. GROUP config-resource discovery must include
dynamically configured groups and exclude offset-only groups with no dynamic
configuration. Feature describe/update, atomic validation, and
feature-epoch persistence are covered by the same real Admin client. It also
creates and describes a CLIENT_METRICS resource, obtains a client instance ID,
and verifies an automatic Kafka 4.2 telemetry push through Prometheus.
While a real two-member Kafka 4.2 consumer group remains stable, the harness
also requires its PostgreSQL-derived member count, rebalance epoch, exact
per-partition zero lag, and committed transaction count to appear in
Prometheus.
Two Kafka 4.2 `KafkaShareConsumer` workers additionally verify shared
`record_limit` acquisition, explicit acknowledgement, release/redelivery,
renewal, lock-timeout propagation, Admin offset/lag query/reset/delete, and
record-level `by_duration` SPSO initialization through a dynamic GROUP config.
The consumer/Admin path requires observed OffsetCommit/OffsetFetch v10 request
counters, so topic-ID compatibility is proven by negotiated client traffic.
The Kafka 4.2 producer/consumer path creates a delete-only topic through
`CreateTopics`, writes a historical keyless record, enables compaction, then
verifies sparse offsets and idempotent producer sequence continuity, rejects
new keyless records, retains the historical record, and waits for tombstone
expiry.
It round-trips `min.insync.replicas=2`, proves a real `acks=all` producer gets
`NOT_ENOUGH_REPLICAS` without allocating an offset, rejects raw required
acks `2`, accepts `acks=1`, and verifies DELETE-to-default recovery for
`acks=all`.
It additionally creates a pending transaction with the Kafka 4.2 producer,
recovers it through the official v6 generated request after the producer is
fenced, waits beyond its original timeout, commits through the recovery
session, and verifies `read_committed` visibility.
Finally, it runs Kafka Streams 4.2 twice: once with the classic group protocol
and once with `group.protocol=streams`. Both use two stream threads and
`exactly_once_v2`; the Streams-protocol path additionally requires a stable
two-member `StreamsGroupDescribe`, type-filtered `ListGroups`, materialized
changelog recovery, repartition partition propagation, dynamic GROUP configs,
standby failover, PostgreSQL recovery, and exact output after local state
deletion and restart.

Multi-Agent durability is exercised by
[`tests/multi-agent/run-orbstack.sh`](../tests/multi-agent/run-orbstack.sh). Three
Agents share PostgreSQL and MinIO behind one endpoint while producers write to
one partition. The gate first drops a success response after durable commit,
removes that Agent, and retries the exact idempotent producer sequence through
another Agent, requiring the original offset and one visible span/object. It
then kills an Agent, restarts every Agent, interrupts S3, injects a PostgreSQL
metadata-commit failure after upload, waits for orphan GC, and requires
contiguous unique records plus an exact match between committed metadata rows
and physical objects.

Retention deletion recovery is exercised independently by
[`tests/retention/run-orbstack.sh`](../tests/retention/run-orbstack.sh). A Kafka
4.2 generated request puts two topics in one immutable object, expires them in
stages to prove shared-object safety, interrupts MinIO during physical deletion,
removes the first Agent, and requires a replacement Agent to converge both the
persisted PostgreSQL deletion obligation and MinIO object count to zero.

Kubernetes rollout and scale behavior is exercised by
[`tests/kubernetes/run-orbstack.sh`](../tests/kubernetes/run-orbstack.sh). It
installs the Helm chart on OrbStack Kubernetes, replaces every Agent while a
Kafka 4.2 producer remains active, scales 3 -> 5 -> 2, and requires 1,600
contiguous unique records with no Agent PVC or data volume.

A fixed-resource one/three-Agent benchmark is available at
[`tests/performance/run-orbstack.sh`](../tests/performance/run-orbstack.sh). The
reviewed first baseline and raw measurements are in
[`tests/performance/results/baseline-20260727-orbstack.md`](../tests/performance/results/baseline-20260727-orbstack.md)
and the adjacent JSONL file. These are reproducible point measurements, not a
throughput commitment.

## TLS and SASL

Set both `KAFKA_TLS_CERT_FILE` and `KAFKA_TLS_KEY_FILE` to make the Kafka
listener TLS-only. Set `RUTOMQ_SASL_ENABLED=true` to require SCRAM even when no
startup users are configured. The JSON user map is a bootstrap fallback;
Kafka Admin can persist dynamic credentials in PostgreSQL.

```bash
export KAFKA_TLS_CERT_FILE=/etc/rutomq/tls/tls.crt
export KAFKA_TLS_KEY_FILE=/etc/rutomq/tls/tls.key
export RUTOMQ_SASL_ENABLED=true
export RUTOMQ_SCRAM_USERS_JSON='{"flink":"change-me"}'
export RUTOMQ_SCRAM_ITERATIONS=4096
export RUTOMQ_SASL_MAX_REAUTH_MS=0
export RUTOMQ_DELEGATION_TOKEN_SECRET='replace-with-a-cluster-secret'
export RUTOMQ_DELEGATION_TOKEN_MAX_LIFETIME_MS=604800000
export RUTOMQ_DELEGATION_TOKEN_EXPIRY_MS=86400000
```

`AlterUserScramCredentials` and `DescribeUserScramCredentials` manage
SCRAM-SHA-256/512 credentials through Kafka Admin. Only salt, iterations,
StoredKey, and ServerKey are persisted; changes apply to new connections.
SaslHandshake v0 uses the legacy length-prefixed opaque SCRAM exchange and
returns to Kafka framing after authentication; v1 uses SaslAuthenticate
envelopes. Framed connections support KIP-368 re-authentication on the same
connection, require an unchanged mechanism and principal, and recompute the
session lifetime from delegation-token expiry and
`RUTOMQ_SASL_MAX_REAUTH_MS` (zero leaves ordinary SCRAM sessions unbounded).
Unsupported negotiation and failed framed authentication return their Kafka
error response before the broker closes the connection. The full
Kafka 4.3 `SASL_SSL` acceptance test includes an exact `DescribeLogDirs` v5
exchange plus real AdminClient KIP-1240, KIP-1263, and KIP-1196 GROUP/BROKER
config round trips, including validation and DELETE-to-default, and runs in
OrbStack:

```bash
tests/security/run-orbstack.sh
```

When `RUTOMQ_DELEGATION_TOKEN_SECRET` is set, Kafka Admin clients can create,
renew, expire, and describe delegation tokens. Tokens are stored in PostgreSQL,
authenticate through SCRAM-SHA-256/512 with Kafka's `tokenauth=true` extension,
and assume the token owner's ACL identity. A requester that creates a token for
another owner remains eligible to describe, renew, and expire it, matching
Kafka's owner/requester/renewer rules. In CreateDelegationToken v3, a null or
empty owner name defaults to the authenticated requester even when an owner
type was supplied. User `CREATE_TOKENS` and
`DESCRIBE_TOKENS` ACL resources use the complete owner principal, such as
`User:alice`. Renewer lists are persisted without rewriting their order or
cardinality. Token-authenticated connections cannot invoke delegation-token
management APIs, and their generated-wire error identities and timestamp
sentinels match Kafka 4.2. Keep the token secret identical for every Agent and
inject it from a Kubernetes Secret.

Kafka ACL authorization is optional. When enabled, configure at least one
super user for bootstrap administration. Principals use Kafka's
`PrincipalType:name` form:

```bash
export RUTOMQ_ACL_ENABLED=true
export RUTOMQ_SUPER_USERS='User:admin'
export RUTOMQ_ALLOW_EVERYONE_IF_NO_ACL_FOUND=false
```

`CreateAcls`, `DescribeAcls`, and `DeleteAcls` persist literal or prefixed
rules in PostgreSQL. Topic, Group, Cluster, and TransactionalId authorization
is enforced with explicit deny precedence and Kafka resource-specific error
codes. Kafka 4.2 operation `TWO_PHASE_COMMIT(15)` round-trips through these
Admin APIs and never inherits permission from TransactionalId `WRITE`.
`ListTransactions` filters each result by TransactionalId `DESCRIBE` without
requiring Cluster access; `DescribeTransactions` removes participating topics
without Topic `DESCRIBE`. Transaction state filters preserve unknown-only
requests as empty results, transactional-ID regexes match complete IDs, and
malformed patterns return Kafka's dedicated error. `ListGroups` uses Cluster
`DESCRIBE` as an all-groups fast path before falling back to per-Group
`DESCRIBE`.

## License

Licensed under the [Apache License 2.0](../LICENSE).
