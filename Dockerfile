# Runtime stage with Flink CLI tools
FROM eclipse-temurin:17-jre-jammy

# Set working directory
WORKDIR /app

# Copy the built JAR from build stage
COPY target/flink-beam-kafka-boilerplate-1.0.0.jar /app/

# Create log directory with proper permissions
RUN mkdir -p /opt/flink/log && chmod 777 /opt/flink/log

# Default command (can be overridden in docker compose)
CMD ["java", "-jar", "/app/flink-beam-kafka-boilerplate-1.0.0.jar"]
