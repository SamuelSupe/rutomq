# Contributing to rutomq

Thank you for helping improve rutomq. The project values observable Kafka
behavior, explicit compatibility boundaries, and simple implementations that
preserve the stateless durability model.

## Before starting

- For a confirmed bug or focused improvement, open an issue or pull request.
- For design questions, compatibility proposals, or early ideas, start a
  [Discussion](https://github.com/SamuelSupe/rutomq/discussions).
- Report security issues privately according to [SECURITY.md](SECURITY.md).
- Read the [compatibility matrix](docs/compatibility.md) before changing a
  Kafka API or advertising a new version.

Large changes should describe:

1. the client-visible behavior being changed;
2. the Kafka version, API, KIP, or reference behavior involved;
3. the PostgreSQL and object-storage consistency impact;
4. the acceptance evidence that will protect the behavior.

## Development environment

The workspace is pinned to Rust `1.88.0`. OrbStack is the primary tested
container and Kubernetes environment, but Docker-compatible runtimes can run
the local development stack.

```bash
docker compose -f deploy/compose.dev.yml up -d postgres minio minio-init
docker compose -f deploy/compose.dev.yml run --rm migrate
```

The repository's `rust-toolchain.toml` installs the expected formatter and
Clippy components when a compatible `rustup` environment is available.

## Fast validation

Run the checks relevant to your change:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo audit
```

PostgreSQL integration tests run when `RUTOMQ_TEST_PG_URL` is set:

```bash
export RUTOMQ_TEST_PG_URL='postgres://rutomq:rutomq@127.0.0.1:5432/rutomq'
cargo test -p rutomq-control --all-features --locked
```

## Acceptance suites

Use the narrowest suite that exercises the observable boundary:

| Area | Command |
| --- | --- |
| OpenDAL S3/MinIO | `tests/storage/run-orbstack.sh` |
| librdkafka and franz-go | `tests/clients/run-orbstack.sh` |
| Flink, Java, Kafka Streams, protocol coverage | `tests/flink/run-orbstack.sh` |
| TLS, SCRAM, ACLs, delegation tokens | `tests/security/run-orbstack.sh` |
| Multi-Agent durability and retry | `tests/multi-agent/run-orbstack.sh` |
| Retention deletion recovery | `tests/retention/run-orbstack.sh` |
| Helm rollout and scaling | `tests/kubernetes/run-orbstack.sh` |

These scripts create isolated dependencies and clean them up. Include the exact
command and result in the pull request.

## Change guidelines

- Keep `ApiVersions` truthful. Never advertise an unhandled version to make a
  compatibility matrix look complete.
- Preserve the acknowledgement boundary: object storage first, PostgreSQL
  metadata commit second, client acknowledgement last.
- Do not introduce an Agent-local WAL, persistent volume, or correctness state
  that cannot survive Agent replacement.
- Preserve Kafka error, authorization, privacy, fencing, and partial-result
  semantics rather than returning a generic success.
- Keep shared-object retention safe: an object cannot be deleted while any
  committed span still references it.
- Prefer focused behavior tests over assertions about internal implementation
  details.
- Update the compatibility matrix and dated evidence when a public claim
  changes.

## Pull requests

Pull requests should be small enough to review as one coherent change. Complete
the repository pull-request template, keep commits descriptive, and avoid
unrelated formatting or refactoring.

By contributing, you agree that your contribution is licensed under the
Apache License 2.0.
