#!/bin/sh
# Install the peql command on Linux or macOS from a GitHub release:
#
#   curl -LsSf https://github.com/griot-cloud/peql/releases/latest/download/install.sh | sh
#
# PEQL_VERSION=0.1.0 picks a release (default: the latest), and PEQL_INSTALL_DIR picks
# where the binary goes (default: ~/.local/bin). The archive's sha256 is checked before
# anything is installed.
set -eu

repo="griot-cloud/peql"
bin="peql"
version="${PEQL_VERSION:-latest}"
dir="${PEQL_INSTALL_DIR:-$HOME/.local/bin}"

fail() { echo "install.sh: $*" >&2; exit 1; }

case "$(uname -s)" in
  Linux) os="unknown-linux-gnu" ;;
  Darwin) os="apple-darwin" ;;
  *) fail "no $bin build for $(uname -s); on Windows use install.ps1" ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) arch="x86_64" ;;
  aarch64 | arm64) arch="aarch64" ;;
  *) fail "no $bin build for $(uname -m)" ;;
esac
target="$arch-$os"
archive="$bin-$target.tar.gz"

if [ "$version" = latest ]; then
  base="https://github.com/$repo/releases/latest/download"
else
  base="https://github.com/$repo/releases/download/v${version#v}"
fi

if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL "$1" -o "$2"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -q "$1" -O "$2"; }
else
  fail "needs curl or wget"
fi
if command -v sha256sum >/dev/null 2>&1; then
  sha() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
  sha() { shasum -a 256 "$1" | cut -d' ' -f1; }
else
  fail "needs sha256sum or shasum"
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "downloading $archive ($version)"
fetch "$base/$archive" "$tmp/$archive" || fail "could not download $base/$archive"
fetch "$base/$archive.sha256" "$tmp/$archive.sha256" || fail "could not download the checksum"
[ "$(sha "$tmp/$archive")" = "$(cut -d' ' -f1 "$tmp/$archive.sha256")" ] || fail "checksum mismatch for $archive"

tar -xzf "$tmp/$archive" -C "$tmp"
mkdir -p "$dir"
cp "$tmp/$bin-$target/$bin" "$dir/$bin"
chmod 755 "$dir/$bin"
echo "installed $("$dir/$bin" --version) to $dir/$bin"

case ":$PATH:" in
  *":$dir:"*) ;;
  *) echo "add $dir to your PATH to run $bin from anywhere" ;;
esac
