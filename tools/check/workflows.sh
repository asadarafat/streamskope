#!/usr/bin/env bash
# Validate GitHub's workflow schema, expressions, and reusable workflow contracts.
set -euo pipefail

version=1.7.12
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    target=linux_amd64
    checksum=8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8
    ;;
  Linux-aarch64 | Linux-arm64)
    target=linux_arm64
    checksum=325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6
    ;;
  Darwin-arm64)
    target=darwin_arm64
    checksum=aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f
    ;;
  Darwin-x86_64)
    target=darwin_amd64
    checksum=5b44c3bc2255115c9b69e30efc0fecdf498fdb63c5d58e17084fd5f16324c644
    ;;
  MINGW*-x86_64 | MSYS*-x86_64)
    target=windows_amd64
    checksum=6e7241b51e6817ea6a047693d8e6fed13b31819c9a0dd6c5a726e1592d22f6e9
    ;;
  *) echo "Unsupported actionlint host: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT
extension=tar.gz
binary=actionlint
if [[ "$target" == windows_* ]]; then
  extension=zip
  binary=actionlint.exe
fi
archive="$temporary/actionlint.$extension"
curl --fail --location --silent --show-error --retry 3 --connect-timeout 10 --max-time 120 \
  "https://github.com/rhysd/actionlint/releases/download/v$version/actionlint_${version}_${target}.$extension" \
  --output "$archive"
if command -v sha256sum >/dev/null 2>&1; then
  printf '%s  %s\n' "$checksum" "$archive" | sha256sum --check
else
  printf '%s  %s\n' "$checksum" "$archive" | shasum -a 256 --check
fi
if [[ "$extension" == zip ]]; then
  unzip -q "$archive" "$binary" -d "$temporary"
else
  tar -xzf "$archive" -C "$temporary" "$binary"
fi
# External shell/Python linters are separate tools; workflow validation is portable.
"$temporary/$binary" -shellcheck= -pyflakes= .github/workflows/*.yml
