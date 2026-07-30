# Quick start

This guide starts one rutomq Agent with PostgreSQL and MinIO. Docker Compose is
required; the repository is primarily validated with OrbStack.

## Start the stack

```bash
git clone https://github.com/SamuelSupe/rutomq.git
cd rutomq
docker compose -f deploy/compose.dev.yml up --build -d
```

The stack contains:

| Service | Address |
| --- | --- |
| Kafka listener | `127.0.0.1:9092` |
| Health and metrics | `127.0.0.1:8080` |
| PostgreSQL | `127.0.0.1:5432` |
| MinIO S3 API | `127.0.0.1:9000` |
| MinIO console | `127.0.0.1:9001` |

Check startup:

```bash
docker compose -f deploy/compose.dev.yml ps
curl --fail http://127.0.0.1:8080/health/live
curl --fail http://127.0.0.1:8080/health/ready
```

The development credentials in `deploy/compose.dev.yml` are intentionally
local-only defaults and must not be reused in production.

## Produce and consume

With the Apache Kafka command-line tools installed:

```bash
kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --create \
  --topic quickstart \
  --partitions 1 \
  --replication-factor 1

printf 'one\ntwo\nthree\n' | kafka-console-producer.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --topic quickstart

kafka-console-consumer.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --topic quickstart \
  --group quickstart-consumer \
  --from-beginning \
  --max-messages 3
```

The first Produce may wait for the default bounded flush window of up to
`250 ms`. The batch can also flush when it reaches `8 MiB` or a shorter topic
flush policy.

## Observe the broker

```bash
curl --silent http://127.0.0.1:8080/metrics \
  | grep -E '^rutomq_(produce|fetch|committed_objects)'
```

Useful metric families include:

- Produce flush and PostgreSQL metadata-commit latency;
- OpenDAL operation count, bytes, errors, and latency;
- Fetch cache hits, misses, evictions, and bytes;
- transaction states, group membership, and consumer lag;
- retention, compaction, orphan GC, and object-integrity failures.

## Run migrations explicitly

The Agent runs migrations during startup. Operators can run them separately
before a deployment:

```bash
docker compose -f deploy/compose.dev.yml run --rm migrate
```

The production Helm chart provides a migration Job for the same purpose.

## Stop the stack

```bash
docker compose -f deploy/compose.dev.yml down -v
```

Removing the Compose volumes deletes the local PostgreSQL and MinIO data. This
command is for the disposable development stack only.

## Next steps

- Read the [architecture and consistency model](architecture.md).
- Review the [compatibility result](compatibility-result-2026-07-30.md).
- Inspect the [API compatibility matrix](compatibility.md).
- Configure TLS, SASL, ACLs, and production settings in the
  [implementation reference](implementation-reference.md).
