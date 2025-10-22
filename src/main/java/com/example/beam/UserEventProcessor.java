package com.example.beam;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Apache Beam pipeline that processes user events from Kafka and produces aggregated results
 */
public class UserEventProcessor {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(UserEventProcessor.class);

    public interface UserEventProcessorOptions extends FlinkPipelineOptions {
        @Description("Kafka bootstrap servers")
        @Default.String("localhost:9092")
        String getKafkaBootstrapServers();
        void setKafkaBootstrapServers(String value);

        @Description("Input Kafka topic")
        @Default.String("test")
        String getInputTopic();
        void setInputTopic(String value);

        @Description("Output Kafka topic")
        @Default.String("user-count-output")
        String getOutputTopic();
        void setOutputTopic(String value);
    }

    public static void main(String[] args) {
        UserEventProcessorOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(UserEventProcessorOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        // Read from Kafka
        PCollection<KV<String, String>> kafkaMessages = pipeline
                .apply("Read from Kafka", KafkaIO.<String, String>read()
                        .withBootstrapServers(options.getKafkaBootstrapServers())
                        .withTopic(options.getInputTopic())
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata());

        // Parse JSON messages to UserEvent objects
        PCollection<UserEvent> userEvents = kafkaMessages
                .apply("Extract Values", Values.create())
                .apply("Parse User Events", ParDo.of(new ParseUserEventFn()));

        // Apply windowing for streaming processing
        PCollection<UserEvent> windowedEvents = userEvents
                .apply("Window Events", Window.<UserEvent>into(FixedWindows.of(Duration.standardMinutes(1))));

        // Create time buckets and count users
        PCollection<UserCountResult> userCounts = windowedEvents
                .apply("Create Time Buckets", ParDo.of(new CreateTimeBucketFn()))
                .apply("Count Users by Bucket", Count.perKey())
                .apply("Format Results", ParDo.of(new FormatUserCountFn()));

        // Write results back to Kafka
        userCounts
                .apply("Convert to Kafka Messages", ParDo.of(new ConvertToKafkaMessageFn()))
                .apply("Write to Kafka", KafkaIO.<String, String>write()
                        .withBootstrapServers(options.getKafkaBootstrapServers())
                        .withTopic(options.getOutputTopic())
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));

        pipeline.run().waitUntilFinish();
    }

    /**
     * Parses JSON string to UserEvent object
     */
    static class ParseUserEventFn extends DoFn<String, UserEvent> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                String json = c.element();
                UserEvent userEvent = objectMapper.readValue(json, UserEvent.class);
                c.output(userEvent);
            } catch (Exception e) {
                logger.warn("Failed to parse user event: {}", e.getMessage());
            }
        }
    }

    /**
     * Creates time buckets from user events
     */
    static class CreateTimeBucketFn extends DoFn<UserEvent, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            UserEvent userEvent = c.element();
            try {
                Instant instant = Instant.parse(userEvent.getTime());
                // Create 1-minute buckets
                String bucket = instant.atOffset(ZoneOffset.UTC)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm"));
                c.output(KV.of(bucket, userEvent.getUserId()));
            } catch (Exception e) {
                logger.warn("Failed to create time bucket: {}", e.getMessage());
            }
        }
    }

    /**
     * Formats count results
     */
    static class FormatUserCountFn extends DoFn<KV<String, Long>, UserCountResult> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, Long> kv = c.element();
            UserCountResult result = new UserCountResult(kv.getKey(), kv.getValue());
            c.output(result);
        }
    }

    /**
     * Converts UserCountResult to Kafka message
     */
    static class ConvertToKafkaMessageFn extends DoFn<UserCountResult, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                UserCountResult result = c.element();
                String json = objectMapper.writeValueAsString(result);
                c.output(KV.of(result.getBucket(), json));
            } catch (Exception e) {
                logger.warn("Failed to convert to Kafka message: {}", e.getMessage());
            }
        }
    }
}
