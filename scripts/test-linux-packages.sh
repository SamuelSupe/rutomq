#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: $0 <version> <deb-arch> <rpm-arch> [package-dir]" >&2
    exit 2
}

[[ $# -ge 3 && $# -le 4 ]] || usage

version="$1"
deb_arch="$2"
rpm_arch="$3"
package_dir="${4:-dist}"

case "${deb_arch}:${rpm_arch}" in
    amd64:x86_64)
        platform="linux/amd64"
        ;;
    arm64:aarch64)
        platform="linux/arm64"
        ;;
    *)
        echo "unsupported architecture pair: ${deb_arch}:${rpm_arch}" >&2
        exit 2
        ;;
esac

package_dir="$(cd "$package_dir" && pwd)"
deb_name="rutomq_${version}_${deb_arch}.deb"
rpm_name="rutomq-${version}-1.${rpm_arch}.rpm"

[[ -f "$package_dir/$deb_name" ]] || {
    echo "missing package: $package_dir/$deb_name" >&2
    exit 1
}
[[ -f "$package_dir/$rpm_name" ]] || {
    echo "missing package: $package_dir/$rpm_name" >&2
    exit 1
}

docker run --rm \
    --platform "$platform" \
    --env "PACKAGE_NAME=$deb_name" \
    --env "RUTOMQ_VERSION=$version" \
    --volume "$package_dir:/packages:ro" \
    debian:12-slim \
    sh -euxc '
        apt-get update
        apt-get install -y "/packages/$PACKAGE_NAME"
        test "$(rutomq --version)" = "rutomq $RUTOMQ_VERSION"
        test -f /etc/rutomq/rutomq.env
        test -f /usr/lib/systemd/system/rutomq.service
        systemd-analyze verify /usr/lib/systemd/system/rutomq.service
        dpkg-query -W -f="\${Architecture} \${Version}\n" rutomq
        dpkg --remove rutomq
        test ! -e /usr/bin/rutomq
        test -f /etc/rutomq/rutomq.env
    '

docker run --rm \
    --platform "$platform" \
    --env "PACKAGE_NAME=$rpm_name" \
    --env "RUTOMQ_VERSION=$version" \
    --volume "$package_dir:/packages:ro" \
    rockylinux:9 \
    sh -euxc '
        dnf install -y "/packages/$PACKAGE_NAME"
        test "$(rutomq --version)" = "rutomq $RUTOMQ_VERSION"
        test -f /etc/rutomq/rutomq.env
        test -f /usr/lib/systemd/system/rutomq.service
        systemd-analyze verify /usr/lib/systemd/system/rutomq.service
        rpm -q --qf "%{ARCH} %{VERSION}-%{RELEASE}\n" rutomq
        rpm -V rutomq
        rpm -e rutomq
        test ! -e /usr/bin/rutomq
    '
