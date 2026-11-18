#!/bin/sh
# Idempotently registers (create or update) the S3 sink connector.
# PUT /connectors/<name>/config is an upsert in the Kafka Connect REST API.
set -eu
CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
NAME="${CONNECTOR_NAME:-s3-sink-bronze}"
CONFIG="${CONNECTOR_CONFIG:-/connectors/s3-sink-bronze.json}"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -fs "${CONNECT_URL}/connector-plugins" | grep -q S3SinkConnector; do sleep 3; done

echo "Registering connector '${NAME}'"
curl -fsS -X PUT -H "Content-Type: application/json" \
  --data @"${CONFIG}" "${CONNECT_URL}/connectors/${NAME}/config"
echo
echo "Done."
