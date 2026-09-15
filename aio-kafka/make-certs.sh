#!/usr/bin/env bash
set -euo pipefail

# Generates demo JKS keystore/truststore and a PEM CA certificate for clients.
# Requirements: keytool (from a JDK/JRE) available on PATH.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERT_DIR="${CERT_DIR:-${SCRIPT_DIR}/config/kafka-broker/certs}"
mkdir -p "$CERT_DIR"

PASSWORD="${PASSWORD:-password}"
FIXTURE_NAME="${STREAMSKOPE_FIXTURE_NAME:-streamskope-kafka}"
HOST="${HOST:-clab-${FIXTURE_NAME}-broker}"
# Subject Alternative Names for TLS hostname verification (you can add more)
SAN="${SAN:-DNS:${HOST},DNS:localhost,IP:127.0.0.1}"

KEYSTORE="$CERT_DIR/kafka.keystore.jks"
TRUSTSTORE="$CERT_DIR/kafka.truststore.jks"
CA_CERT="$CERT_DIR/ca.pem"

echo "[CERTS] Generating credentials for HOST=$HOST SAN=$SAN"

if ! command -v keytool >/dev/null 2>&1; then
  echo "[CERTS] ERROR: keytool not found on PATH. Install a JDK/JRE providing keytool and retry." >&2
  exit 1
fi

echo "[CERTS] Generating JKS keystore: $KEYSTORE"
keytool -genkeypair \
  -alias kafka \
  -keystore "$KEYSTORE" \
  -storetype JKS \
  -storepass "$PASSWORD" \
  -keypass "$PASSWORD" \
  -keyalg RSA \
  -keysize 2048 \
  -validity 3650 \
  -dname "CN=$HOST, OU=Dev, O=NSP, L=City, S=State, C=US" \
  -ext "SAN=$SAN"

echo "[CERTS] Exporting client CA certificate to $CA_CERT"
keytool -exportcert \
  -alias kafka \
  -keystore "$KEYSTORE" \
  -storepass "$PASSWORD" \
  -rfc \
  -file "$CA_CERT"

echo "[CERTS] Creating truststore: $TRUSTSTORE"
keytool -importcert \
  -alias kafka \
  -file "$CA_CERT" \
  -keystore "$TRUSTSTORE" \
  -storetype JKS \
  -storepass "$PASSWORD" \
  -noprompt

echo "[CERTS] Done. Files:"
ls -l "$KEYSTORE" "$TRUSTSTORE" "$CA_CERT"
echo "[CERTS] Mount path in broker: /opt/kafka/config/certs"
echo "[CERTS] PEM CA for non-Java clients: $CA_CERT"
