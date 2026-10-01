#!/bin/bash
set -euo pipefail

echo "[BOOT] Starting internal OAuth token server (if python3 is available)..."
if command -v python3 >/dev/null 2>&1; then
  python3 /opt/oauth/server.py &
else
  echo "[WARN] python3 not found; skipping internal OAuth server"
fi

export ADVERTISED_HOST="${ADVERTISED_HOST:-localhost}"
export ADVERTISED_PORT="${ADVERTISED_PORT:-9093}"
export INTERNAL_HOST="${INTERNAL_HOST:-localhost}"
export OAUTH_HOST="${OAUTH_HOST:-clab-aio-kafka-oauth}"
: "${OAUTH_CLIENT_ID:?OAUTH_CLIENT_ID is required}"
: "${OAUTH_CLIENT_SECRET:?OAUTH_CLIENT_SECRET is required}"
: "${OAUTH_EXPECTED_AUDIENCE:?OAUTH_EXPECTED_AUDIENCE is required}"
: "${OAUTH_EXPECTED_ISSUER:?OAUTH_EXPECTED_ISSUER is required}"
export PRODUCER_ENABLED="${PRODUCER_ENABLED:-true}"
export PRODUCER_TOPIC="${PRODUCER_TOPIC:-test}"
export KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
export KAFKA_CONFIG_DIR="$KAFKA_HOME/config"
PRODUCER_PID=""

# Point Kafka to JAAS and allow the OAuth JWKS + token URLs (Kafka 3.3+/4.0 URL allowlist)
export ALLOWED_JWKS_URL="${ALLOWED_JWKS_URL:-http://clab-aio-kafka-oauth:5000/.well-known/jwks.json}"
export ALLOWED_TOKEN_URL="${ALLOWED_TOKEN_URL:-http://$OAUTH_HOST:5000/rest-gateway/rest/api/v1/auth/token}"
export KAFKA_OPTS="-Djava.security.auth.login.config=$KAFKA_CONFIG_DIR/jaas.conf -Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=$ALLOWED_JWKS_URL,$ALLOWED_TOKEN_URL ${KAFKA_OPTS:-}"

# Render server.properties with ADVERTISED_HOST substitution if mounted at /opt/kafka/server.properties
if [ -f "/opt/kafka/server.properties" ]; then
  echo "[CONF] Rendering server.properties for $ADVERTISED_HOST:$ADVERTISED_PORT"
  awk -v host="$ADVERTISED_HOST" -v internal_host="$INTERNAL_HOST" -v oauth_host="$OAUTH_HOST" -v oauth_audience="$OAUTH_EXPECTED_AUDIENCE" -v oauth_issuer="$OAUTH_EXPECTED_ISSUER" -v port="$ADVERTISED_PORT" \
    '{ gsub(/\$\{ADVERTISED_HOST\}/, host); gsub(/\$\{ADVERTISED_PORT\}/, port); gsub(/\$\{INTERNAL_HOST\}/, internal_host); gsub(/\$\{OAUTH_HOST\}/, oauth_host); gsub(/\$\{OAUTH_EXPECTED_AUDIENCE\}/, oauth_audience); gsub(/\$\{OAUTH_EXPECTED_ISSUER\}/, oauth_issuer); print }' \
    /opt/kafka/server.properties > "$KAFKA_CONFIG_DIR/server.properties"
fi

echo "[BOOT] Starting Kafka from $KAFKA_HOME with $KAFKA_CONFIG_DIR/server.properties"
# Generate a client config for console tools with OAuth token endpoint
cat > "$KAFKA_CONFIG_DIR/consumer.properties" <<EOF
security.protocol=SASL_SSL
ssl.truststore.type=JKS
ssl.truststore.location=/opt/kafka/config/certs/kafka.truststore.jks
ssl.truststore.password=password
ssl.endpoint.identification.algorithm=
sasl.mechanism=OAUTHBEARER
sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
sasl.oauthbearer.token.endpoint.url=http://$OAUTH_HOST:5000/rest-gateway/rest/api/v1/auth/token
sasl.oauthbearer.client.credentials.client.id=$OAUTH_CLIENT_ID
sasl.oauthbearer.client.credentials.client.secret=$OAUTH_CLIENT_SECRET
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
EOF
# Auto-format KRaft storage if not initialized
if [ ! -f "/var/lib/kafka/data/meta.properties" ] && [ -x "$KAFKA_HOME/bin/kafka-storage.sh" ]; then
  echo "[INIT] Formatting KRaft storage"
  CLUSTER_ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_CONFIG_DIR/server.properties"
fi

# Start Kafka in background so we can optionally start a producer
"$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CONFIG_DIR/server.properties" &
KAFKA_PID=$!

# Optional background producer emitting JSON lines to the configured topic
start_producer() {
  echo "[PRODUCER] Waiting for the internal broker listener to be ready..."
  TRIES=0
  until "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 --command-config "$KAFKA_CONFIG_DIR/consumer.properties" >/dev/null 2>&1; do
    TRIES=$((TRIES+1))
    if [ "$TRIES" -gt 60 ]; then echo "[PRODUCER] Broker not ready after 60 tries" >&2; return; fi
    sleep 2
  done
  echo "[PRODUCER] Starting stream to topic '$PRODUCER_TOPIC'"
  (
    i=0
    while true; do
      i=$((i+1))
      printf '{"seq":%d,"ts":"%s","msg":"hello"}\n' "$i" "$(date -u +%FT%TZ)"
      sleep 1
    done
  ) | "$KAFKA_HOME/bin/kafka-console-producer.sh" \
      --bootstrap-server localhost:9092 \
      --producer.config "$KAFKA_CONFIG_DIR/consumer.properties" \
      --topic "$PRODUCER_TOPIC" &
  PRODUCER_PID=$!
}

if [ "$PRODUCER_ENABLED" = "true" ]; then
  start_producer
fi

trap 'echo "[BOOT] Caught signal, shutting down"; kill -TERM $KAFKA_PID 2>/dev/null; [ -n "$PRODUCER_PID" ] && kill -TERM "$PRODUCER_PID" 2>/dev/null; wait $KAFKA_PID' TERM INT
wait $KAFKA_PID
