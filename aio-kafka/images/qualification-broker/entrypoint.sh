#!/usr/bin/env bash
set -euo pipefail

fixture_directory="$1"
cluster_id="$2"
mapfile -t storage_arguments < "$fixture_directory/storage-arguments"
/opt/kafka/bin/kafka-storage.sh format --standalone \
  "--cluster-id=$cluster_id" --config "$fixture_directory/server.properties" \
  "${storage_arguments[@]}"
exec /opt/kafka/bin/kafka-server-start.sh "$fixture_directory/server.properties"
