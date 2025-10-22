# Apache Beam with Flink Runner and Kafka Integration

This boilerplate provides a complete setup for running Apache Beam pipelines with Flink runner and Kafka integration using Docker Compose. The setup uses Kafka in KRaft mode (no Zookeeper required) for simplified deployment.

## Architecture

- **Apache Beam 2.68.0**: Latest version with Java SDK
- **Apache Flink 1.19.0**: Latest Flink runner for Beam
- **Apache Kafka 3.9.0**: Message streaming platform (KRaft mode - no Zookeeper needed)
- **AKHQ 0.25.0**: Kafka UI for monitoring and management
- **Docker Compose**: Local development environment

## Pipeline Overview

The pipeline processes user events from Kafka and produces aggregated results:

### Input Format
```json
{"userId": 1, "time": "2024-01-15T10:30:00Z"}
```

### Output Format
```json
{"bucket": "2024-01-15-10-30", "userCount": 100}
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Java 17+ (for local development)
- Maven 3.6+ (for local development)

### Running the Pipeline

1. **Start the infrastructure:**
   ```bash
   docker compose up -d kafka flink-jobmanager flink-taskmanager
   ```

2. **Build and run the Beam application:**
   ```bash
   docker compose up --build beam-app
   ```

3. **Send test data to Kafka:**
   
   **Option A: Using AKHQ Web UI (Recommended)**
   - Open http://localhost:8080 in your browser
   - Navigate to Topics → test
   - Click "Produce" and send JSON messages
   
   **Option B: Using Command Line**
   ```bash
   # Create a test topic (if not auto-created)
   docker exec kafka kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   
   # Send sample data
   echo '{"userId":1,"time":"2024-01-15T10:30:00Z"}' | docker exec -i kafka kafka-console-producer.sh --topic test --bootstrap-server localhost:9092
   echo '{"userId":2,"time":"2024-01-15T10:30:00Z"}' | docker exec -i kafka kafka-console-producer.sh --topic test --bootstrap-server localhost:9092
   echo '{"userId":3,"time":"2024-01-15T10:31:00Z"}' | docker exec -i kafka kafka-console-producer.sh --topic test --bootstrap-server localhost:9092
   ```

4. **Check the output:**
   
   **Option A: Using AKHQ Web UI (Recommended)**
   - Open http://localhost:8080 in your browser
   - Navigate to Topics → user-count-output
   - View messages in real-time
   
   **Option B: Using Command Line**
   ```bash
   docker exec kafka kafka-console-consumer.sh --topic user-count-output --bootstrap-server localhost:9092 --from-beginning
   ```

### Local Development

1. **Build the project:**
   ```bash
   mvn clean package
   ```

2. **Run locally with DirectRunner (for testing):**
   ```bash
   java -cp target/flink-beam-kafka-boilerplate-1.0.0.jar com.example.beam.UserEventProcessor --runner=DirectRunner
   ```

3. **Run with FlinkRunner:**
   ```bash
   java -cp target/flink-beam-kafka-boilerplate-1.0.0.jar com.example.beam.UserEventProcessor --runner=FlinkRunner --flinkJobManager=localhost:8081
   ```

## Configuration

### Pipeline Options

- `--kafkaBootstrapServers`: Kafka bootstrap servers (default: localhost:9092)
- `--inputTopic`: Input Kafka topic (default: test)
- `--outputTopic`: Output Kafka topic (default: user-count-output)
- `--flinkJobManager`: Flink job manager address (default: localhost:8081)

### Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection string
- `FLINK_JOB_MANAGER`: Flink job manager address
- `INPUT_TOPIC`: Input topic name
- `OUTPUT_TOPIC`: Output topic name

## Monitoring

- **Flink Web UI**: http://localhost:8081 - Monitor Flink jobs and performance
- **AKHQ Kafka UI**: http://localhost:8080 - Monitor Kafka topics, messages, and clusters
- **Kafka Topics**: Use `docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092`

## Project Structure

```
├── src/main/java/com/example/beam/
│   ├── UserEvent.java              # Input data model
│   ├── UserCountResult.java        # Output data model
│   └── UserEventProcessor.java     # Main pipeline
├── docker-compose.yml              # Infrastructure setup
├── Dockerfile                      # Application container
└── pom.xml                         # Maven dependencies
```

## Customization

### Adding New Transformations

1. Create new `DoFn` classes in the processor
2. Add them to the pipeline using `.apply()`
3. Rebuild and redeploy

### Changing Time Buckets

Modify the `CreateTimeBucketFn` class to change the bucket size:
- Current: 1-minute buckets
- Options: seconds, minutes, hours, days

### Adding More Data Processing

Extend the pipeline by adding more transforms between parsing and output:
- Filtering
- Windowing
- Complex aggregations
- External API calls

## Troubleshooting

### Common Issues

1. **Kafka connection issues**: Ensure Kafka is running and accessible
2. **Flink job submission failures**: Check Flink job manager is running
3. **JSON parsing errors**: Verify input data format matches expected schema

### Logs

- **Beam App**: `docker compose logs beam-app`
- **Flink**: `docker compose logs flink-jobmanager flink-taskmanager`
- **Kafka**: `docker compose logs kafka`