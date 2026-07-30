# Security policy

## Supported versions

| Version | Security updates |
| --- | --- |
| `0.1.x` | Supported |
| Unreleased `main` | Best effort |
| Older versions | Not supported |

`0.1.x` is a public preview. Operators should evaluate the documented
compatibility boundaries, isolate PostgreSQL and object-storage credentials,
enable TLS and authentication, and apply least-privilege Kafka ACLs before any
production trial.

## Report a vulnerability

Do not open a public issue for a suspected vulnerability.

Use
[GitHub's private vulnerability report](https://github.com/SamuelSupe/rutomq/security/advisories/new)
and include:

- the affected version or commit;
- the deployment and authentication configuration;
- reproduction steps or a minimal proof of concept;
- the expected impact and any known mitigations;
- whether the report can be shared with upstream dependencies.

The maintainer will acknowledge a complete report as soon as practical,
coordinate validation and remediation privately, and publish an advisory when
users have a reasonable opportunity to update. Exact timelines depend on
severity and reproducibility.

## Security scope

High-priority boundaries include:

- acknowledgement without durable object and metadata commit;
- cross-topic, group, transactional-ID, or principal authorization bypass;
- producer, transaction, group, or share-state fencing violations;
- unauthenticated access when TLS/SASL is configured;
- credential or token disclosure;
- object-range integrity bypass or unsafe shared-object deletion;
- malformed Kafka frames causing memory-safety or unbounded resource use.

Reports about unsupported behavior that is already explicit in the
[compatibility matrix](docs/compatibility.md) may be handled as feature
requests unless they create a separate security impact.

## Operational security

- Store PostgreSQL, S3, SCRAM bootstrap, delegation-token, and TLS secrets in a
  dedicated secret manager.
- Use TLS for Kafka and encrypted connections to durable dependencies.
- Set `RUTOMQ_ACL_ENABLED=true`, configure a minimal bootstrap super user, and
  disable allow-by-default authorization for shared environments.
- Keep the same delegation-token secret and security configuration on every
  Agent.
- Restrict the health and Prometheus listener to trusted networks.
- Review dependency alerts and release notes before upgrading.
