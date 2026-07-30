#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: $0 <version> <binary> <deb-arch> <rpm-arch> [output-dir]" >&2
    exit 2
}

[[ $# -ge 4 && $# -le 5 ]] || usage

version="$1"
binary="$2"
deb_arch="$3"
rpm_arch="$4"
output_dir="${5:-dist}"

[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z]+)*$ ]] || {
    echo "invalid version: $version" >&2
    exit 2
}
[[ -x "$binary" ]] || {
    echo "binary is missing or not executable: $binary" >&2
    exit 2
}

case "${deb_arch}:${rpm_arch}" in
    amd64:x86_64 | arm64:aarch64) ;;
    *)
        echo "unsupported architecture pair: ${deb_arch}:${rpm_arch}" >&2
        exit 2
        ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="$(mkdir -p "$output_dir" && cd "$output_dir" && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/rutomq-package.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

deb_root="$work_dir/deb"
install -D -m 0755 "$binary" "$deb_root/usr/bin/rutomq"
install -D -m 0644 \
    "$repo_root/packaging/systemd/rutomq.service" \
    "$deb_root/usr/lib/systemd/system/rutomq.service"
install -D -m 0640 \
    "$repo_root/packaging/rutomq.env" \
    "$deb_root/etc/rutomq/rutomq.env"
install -D -m 0644 \
    "$repo_root/README.md" \
    "$deb_root/usr/share/doc/rutomq/README.md"
install -D -m 0644 \
    "$repo_root/LICENSE" \
    "$deb_root/usr/share/doc/rutomq/copyright"
install -d -m 0755 "$deb_root/DEBIAN"

sed \
    -e "s/@VERSION@/$version/g" \
    -e "s/@DEB_ARCH@/$deb_arch/g" \
    "$repo_root/packaging/debian/control.in" >"$deb_root/DEBIAN/control"
printf '%s\n' "/etc/rutomq/rutomq.env" >"$deb_root/DEBIAN/conffiles"

for script in preinst postinst prerm postrm; do
    install -m 0755 \
        "$repo_root/packaging/debian/$script" \
        "$deb_root/DEBIAN/$script"
done

installed_size="$(du -sk "$deb_root/usr" "$deb_root/etc" | awk '{ total += $1 } END { print total }')"
printf 'Installed-Size: %s\n' "$installed_size" >>"$deb_root/DEBIAN/control"

deb_name="rutomq_${version}_${deb_arch}.deb"
dpkg-deb --build --root-owner-group "$deb_root" "$output_dir/$deb_name"

rpm_top="$work_dir/rpmbuild"
install -d \
    "$rpm_top/BUILD" \
    "$rpm_top/BUILDROOT" \
    "$rpm_top/RPMS" \
    "$rpm_top/SOURCES" \
    "$rpm_top/SPECS" \
    "$rpm_top/SRPMS"
install -m 0755 "$binary" "$rpm_top/SOURCES/rutomq"
install -m 0644 \
    "$repo_root/packaging/systemd/rutomq.service" \
    "$rpm_top/SOURCES/rutomq.service"
install -m 0644 \
    "$repo_root/packaging/rutomq.env" \
    "$rpm_top/SOURCES/rutomq.env"
install -m 0644 "$repo_root/README.md" "$rpm_top/SOURCES/README.md"
install -m 0644 "$repo_root/LICENSE" "$rpm_top/SOURCES/LICENSE"

sed \
    -e "s/@VERSION@/$version/g" \
    -e "s/@RPM_ARCH@/$rpm_arch/g" \
    "$repo_root/packaging/rpm/rutomq.spec.in" >"$rpm_top/SPECS/rutomq.spec"

rpmbuild \
    --target "$rpm_arch" \
    --define "_topdir $rpm_top" \
    --define "_build_id_links none" \
    --define "_unitdir /usr/lib/systemd/system" \
    -bb "$rpm_top/SPECS/rutomq.spec"

rpm_path="$(find "$rpm_top/RPMS" -type f -name '*.rpm' -print -quit)"
[[ -n "$rpm_path" ]] || {
    echo "rpmbuild did not produce a package" >&2
    exit 1
}
rpm_name="rutomq-${version}-1.${rpm_arch}.rpm"
install -m 0644 "$rpm_path" "$output_dir/$rpm_name"

sha256sum "$output_dir/$deb_name" "$output_dir/$rpm_name"
