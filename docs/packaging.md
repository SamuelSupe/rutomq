# Linux packages

The GitHub release publishes native system packages for both supported Linux
architectures:

| Distribution family | x86-64 | ARM64 |
| --- | --- | --- |
| Debian, Ubuntu | `rutomq_0.1.0_amd64.deb` | `rutomq_0.1.0_arm64.deb` |
| Fedora, RHEL, Rocky Linux | `rutomq-0.1.0-1.x86_64.rpm` | `rutomq-0.1.0-1.aarch64.rpm` |

## Install

Download the package for the host architecture from the
[v0.1.0 release](https://github.com/SamuelSupe/rutomq/releases/tag/v0.1.0).

Debian or Ubuntu:

```bash
sudo apt install ./rutomq_0.1.0_amd64.deb
```

Fedora, RHEL, or Rocky Linux:

```bash
sudo dnf install ./rutomq-0.1.0-1.x86_64.rpm
```

Use `arm64` for a Debian-family ARM host and `aarch64` for an RPM-family ARM
host.

## Configure and start

Packages install:

- `/usr/bin/rutomq`;
- `/etc/rutomq/rutomq.env`;
- `/usr/lib/systemd/system/rutomq.service`;
- the project README and Apache-2.0 license.

The service is deliberately not started during installation. Configure the
external PostgreSQL and S3-compatible dependencies first:

```bash
sudoedit /etc/rutomq/rutomq.env
sudo systemctl enable --now rutomq
sudo systemctl status rutomq
```

The packaged service runs as the unprivileged `rutomq` system user. It writes
logs to the systemd journal and does not create an Agent data directory, WAL, or
persistent volume.

## Verify

```bash
rutomq --version
curl --fail http://127.0.0.1:8080/health/live
journalctl --unit rutomq --follow
```

Release packages are listed in `SHA256SUMS` and carry GitHub build provenance
attestations.
