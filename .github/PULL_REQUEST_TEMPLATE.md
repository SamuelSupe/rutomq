## Summary

<!-- What changed, and why is this the smallest coherent change? -->

## Kafka compatibility impact

<!-- List affected APIs/versions, client-visible errors, KIPs, or state "none". -->

## Durability and operations impact

<!-- Cover PostgreSQL, object storage, Agent statelessness, upgrades, and metrics. -->

## Validation

<!-- Exact commands and observable results. -->

- [ ] `cargo fmt --all -- --check`
- [ ] Relevant Clippy and Rust tests
- [ ] Relevant OrbStack acceptance suite, or an explanation of why it is not needed
- [ ] Compatibility documentation updated when a public claim changed

## Checklist

- [ ] `ApiVersions` remains truthful
- [ ] No Agent-local durable correctness state was introduced
- [ ] New configuration has safe defaults and deployment wiring
- [ ] Logs, examples, and fixtures contain no credentials
