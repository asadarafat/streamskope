#!/usr/bin/env bash
# Build a complete unsigned EDA OCI application in a disposable local registry.
set -euo pipefail

version=$(node --input-type=module -e 'import { EDA_CAPTURE_APPLICATION } from "./plugins/eda/contracts/eda-capture-types.ts"; process.stdout.write(EDA_CAPTURE_APPLICATION.version)')
node tools/check/eda-source.mjs --publish "$version"
node --import tsx tools/check/eda-version.ts

builder_version=v26.8.2
root=$(pwd)
temporary=$(mktemp -d)
registry_name="streamskope-eda-build-$(basename "$temporary" | tr '[:upper:]' '[:lower:]')"
cleanup() {
  docker rm -f "$registry_name" >/dev/null 2>&1 || true
  rm -rf "$temporary"
}
trap cleanup EXIT

if [[ -n "${EDABUILDER:-}" ]]; then
  builder=$EDABUILDER
else
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) target=linux-amd64 ;;
    Linux-aarch64 | Linux-arm64) target=linux-arm64 ;;
    *) echo "EDA CI packaging needs Linux x64 or ARM64." >&2; exit 1 ;;
  esac
  asset="edabuilder-$builder_version-$target"
  base="https://github.com/nokia-eda/edabuilder/releases/download/$builder_version"
  curl --fail --location --silent --show-error --output "$temporary/$asset" "$base/$asset"
  curl --fail --location --silent --show-error --output "$temporary/$asset.sha256sum" "$base/$asset.sha256sum"
  (cd "$temporary" && sha256sum --check "$asset.sha256sum")
  chmod 0755 "$temporary/$asset"
  builder="$temporary/$asset"
fi
"$builder" version 2>&1 | tee "$temporary/builder-version"
grep -F "CLI version: $builder_version" "$temporary/builder-version" >/dev/null
docker build --file vendors/streamskope/apps/capture/agent/Dockerfile \
  --tag "localhost/streamskope-eda-app-agent:$version" vendors/streamskope/apps

docker run --detach --rm --name "$registry_name" --publish 127.0.0.1::5000 registry:2.8.3 >/dev/null
port=$(docker port "$registry_name" 5000/tcp | awk -F: '{print $NF}')
registry="http://127.0.0.1:$port"
for attempt in $(seq 1 60); do
  if curl --fail --silent "$registry/v2/" >/dev/null; then break; fi
  if [[ "$attempt" == 60 ]]; then echo "Local EDA build registry did not become ready." >&2; exit 1; fi
  sleep 1
done
export EDABUILDER_AUTH_CONFIG="$temporary/edabuilder-auth.json"
"$builder" login registry "$registry"
(
  cd vendors/streamskope/apps
  "$builder" build-push --sign-data-dir "$temporary/sign" --app "manifest=capture/manifest.yaml,context=.,image=127.0.0.1:$port/streamskope-eda-app:$version"
)
node tools/package/eda-oci.mjs "$registry" "$version" "$temporary/oci"
mkdir -p "$root/dist/eda-package"
cp "$root/dist/ci/eda-version.json" "$root/dist/eda-package/eda-version.json"
name="streamskope-eda-app-$version.oci.tar"
archive="$root/dist/eda-package/$name"
tar -cf "$archive" -C "$temporary/oci" .
(cd "$root/dist/eda-package" && sha256sum "$name" > "$name.sha256")
echo "Built unsigned development EDA application: $archive"
