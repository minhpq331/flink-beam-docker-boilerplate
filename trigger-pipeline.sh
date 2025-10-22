#!/bin/bash

# Script to trigger the Beam pipeline
echo "=== Triggering Beam Pipeline ==="

# Check if services are running
echo "🔍 Checking if services are running..."
if ! docker compose ps | grep -q "Up"; then
    echo "❌ Services are not running. Starting them..."
    docker compose up -d
    echo "⏳ Waiting for services to be ready..."
    sleep 30
fi

# Create topics if they don't exist
echo "📝 Ensuring topics exist..."
./create-topics.sh

# Send test data
echo "📤 Sending test data to trigger processing..."
echo '{"userId":1,"time":"2024-01-15T10:30:00Z"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic test
echo '{"userId":2,"time":"2024-01-15T10:30:00Z"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic test
echo '{"userId":3,"time":"2024-01-15T10:31:00Z"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic test
echo '{"userId":1,"time":"2024-01-15T10:32:00Z"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic test

echo "✅ Test data sent!"

# Check if beam-app is running
echo "🔍 Checking beam-app status..."
if docker compose ps beam-app | grep -q "Up"; then
    echo "✅ beam-app is running"
    echo "📊 Check Flink Web UI: http://localhost:8081"
    echo "📊 Check AKHQ Web UI: http://localhost:8080"
else
    echo "⚠️  beam-app is not running. Starting it..."
    docker compose up beam-app -d
    echo "⏳ Waiting for beam-app to start..."
    sleep 10
fi

# Show logs
echo "📋 Recent beam-app logs:"
docker logs beam-app --tail 10

echo
echo "=== Pipeline Triggered! ==="
echo "📊 Monitor the pipeline at:"
echo "   - Flink Web UI: http://localhost:8081"
echo "   - AKHQ Kafka UI: http://localhost:8080"
echo
echo "📤 To send more test data:"
echo "   echo '{\"userId\":4,\"time\":\"2024-01-15T10:33:00Z\"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic test"
echo
echo "📥 To check output:"
echo "   docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic user-count-output --from-beginning"
