#!/bin/bash
set -e

echo "⏳ Waiting for Kafka cluster..."

until kafka-broker-api-versions --bootstrap-server kafka-1:9092 | grep -q kafka-3; do
  sleep 3
done

echo "✅ Kafka cluster is ready"

kafka-topics \
  --bootstrap-server kafka-1:9092 \
  --create \
  --if-not-exists \
  --topic events_raw \
  --partitions 12 \
  --replication-factor 3

echo "🚀 Topic events_raw created (12 partitions, RF=3)"

