#!/bin/bash

# Script to create required Kafka topics
echo "Creating Kafka topics..."

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 10

# Create input topic
echo "Creating input topic 'test'..."
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create \
    --topic test \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists

# Create output topic
echo "Creating output topic 'user-count-output'..."
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create \
    --topic user-count-output \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists

# List all topics
echo "Available topics:"
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

echo "✅ Topics created successfully!"
