package com.example.beam;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

public class UserEventProcessorTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testUserEventCreation() {
        UserEvent event = new UserEvent();
        event.setUserId(1);
        event.setTime("2024-01-15T10:30:00Z");
        
        assert event.getUserId() == 1;
        assert event.getTime().equals("2024-01-15T10:30:00Z");
    }

    @Test
    public void testUserCountResultCreation() {
        UserCountResult result = new UserCountResult();
        result.setBucket("2024-01-15-10-30");
        result.setUserCount(100L);
        
        assert result.getBucket().equals("2024-01-15-10-30");
        assert result.getUserCount() == 100L;
    }

    @Test
    public void testParseUserEventFn() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        
        // Test valid JSON
        UserEvent expectedEvent = new UserEvent(123, "2024-01-15T10:30:00Z");
        String validJson = mapper.writeValueAsString(expectedEvent);
        
        PCollection<String> input = pipeline.apply(Create.of(validJson));
        PCollection<UserEvent> output = input.apply(ParDo.of(new UserEventProcessor.ParseUserEventFn()));
        
        PAssert.that(output).satisfies(events -> {
            assert events.iterator().hasNext();
            UserEvent actualEvent = events.iterator().next();
            assert actualEvent.equals(expectedEvent);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testParseUserEventFnWithInvalidJson() {
        String invalidJson = "invalid json";
        
        PCollection<String> input = pipeline.apply(Create.of(invalidJson));
        PCollection<UserEvent> output = input.apply(ParDo.of(new UserEventProcessor.ParseUserEventFn()));
        
        // Should not output anything for invalid JSON
        PAssert.that(output).empty();
        pipeline.run();
    }

    @Test
    public void testCreateTimeBucketFn() {
        UserEvent event1 = new UserEvent(1, "2024-01-15T10:30:00Z");
        UserEvent event2 = new UserEvent(2, "2024-01-15T10:30:30Z");
        UserEvent event3 = new UserEvent(3, "2024-01-15T10:31:00Z");
        
        PCollection<UserEvent> input = pipeline.apply(Create.of(event1, event2, event3));
        PCollection<KV<String, Integer>> output = input.apply(ParDo.of(new UserEventProcessor.CreateTimeBucketFn()));
        
        // Events in same minute should have same bucket
        KV<String, Integer> bucket1 = KV.of("2024-01-15-10-30", 1);
        KV<String, Integer> bucket2 = KV.of("2024-01-15-10-30", 2);
        KV<String, Integer> bucket3 = KV.of("2024-01-15-10-31", 3);
        
        PAssert.that(output).satisfies(buckets -> {
            java.util.List<KV<String, Integer>> bucketList = new java.util.ArrayList<>();
            buckets.forEach(bucketList::add);
            assert bucketList.size() == 3;
            assert bucketList.contains(bucket1);
            assert bucketList.contains(bucket2);
            assert bucketList.contains(bucket3);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testCreateTimeBucketFnWithInvalidTimestamp() {
        UserEvent event = new UserEvent(1, "invalid-timestamp");
        
        PCollection<UserEvent> input = pipeline.apply(Create.of(event));
        PCollection<KV<String, Integer>> output = input.apply(ParDo.of(new UserEventProcessor.CreateTimeBucketFn()));
        
        // Should not output anything for invalid timestamp
        PAssert.that(output).empty();
        pipeline.run();
    }

    @Test
    public void testFormatUserCountFn() {
        KV<String, Long> input1 = KV.of("2024-01-15-10-30", 5L);
        KV<String, Long> input2 = KV.of("2024-01-15-10-31", 3L);
        
        PCollection<KV<String, Long>> input = pipeline.apply(Create.of(input1, input2));
        PCollection<UserCountResult> output = input.apply(ParDo.of(new UserEventProcessor.FormatUserCountFn()));
        
        UserCountResult result1 = new UserCountResult("2024-01-15-10-30", 5L);
        UserCountResult result2 = new UserCountResult("2024-01-15-10-31", 3L);
        
        PAssert.that(output).satisfies(results -> {
            java.util.List<UserCountResult> resultList = new java.util.ArrayList<>();
            results.forEach(resultList::add);
            assert resultList.size() == 2;
            assert resultList.contains(result1);
            assert resultList.contains(result2);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testConvertToKafkaMessageFn() throws Exception {
        UserCountResult result = new UserCountResult("2024-01-15-10-30", 5L);
        ObjectMapper mapper = new ObjectMapper();
        String expectedJson = mapper.writeValueAsString(result);
        
        PCollection<UserCountResult> input = pipeline.apply(Create.of(result));
        PCollection<KV<String, String>> output = input.apply(ParDo.of(new UserEventProcessor.ConvertToKafkaMessageFn()));
        
        KV<String, String> expectedKv = KV.of("2024-01-15-10-30", expectedJson);
        PAssert.that(output).satisfies(kvs -> {
            java.util.List<KV<String, String>> kvList = new java.util.ArrayList<>();
            kvs.forEach(kvList::add);
            assert kvList.size() == 1;
            assert kvList.contains(expectedKv);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testCompletePipelineWithWindowing() {
        // Create test data with events in different time windows
        UserEvent event1 = new UserEvent(1, "2024-01-15T10:30:00Z");
        UserEvent event2 = new UserEvent(2, "2024-01-15T10:30:30Z");
        UserEvent event3 = new UserEvent(3, "2024-01-15T10:31:00Z");
        UserEvent event4 = new UserEvent(1, "2024-01-15T10:31:30Z"); // Same user, different window
        
        PCollection<UserEvent> input = pipeline.apply(Create.of(event1, event2, event3, event4));
        
        // Apply windowing
        PCollection<UserEvent> windowedEvents = input.apply(
            Window.<UserEvent>into(FixedWindows.of(Duration.standardMinutes(1)))
        );
        
        // Process through the pipeline steps
        PCollection<UserCountResult> userCounts = windowedEvents
            .apply("Create Time Buckets", ParDo.of(new UserEventProcessor.CreateTimeBucketFn()))
            .apply("Count Users by Bucket", Count.perKey())
            .apply("Format Results", ParDo.of(new UserEventProcessor.FormatUserCountFn()));
        
        // Expected results: 2 users in 10:30 window, 2 users in 10:31 window
        UserCountResult result1 = new UserCountResult("2024-01-15-10-30", 2L);
        UserCountResult result2 = new UserCountResult("2024-01-15-10-31", 2L);
        
        PAssert.that(userCounts).satisfies(results -> {
            java.util.List<UserCountResult> resultList = new java.util.ArrayList<>();
            results.forEach(resultList::add);
            assert resultList.size() == 2;
            assert resultList.contains(result1);
            assert resultList.contains(result2);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testPipelineWithMixedValidAndInvalidData() {
        // Mix of valid and invalid data
        UserEvent validEvent1 = new UserEvent(1, "2024-01-15T10:30:00Z");
        UserEvent validEvent2 = new UserEvent(2, "2024-01-15T10:30:30Z");
        
        PCollection<UserEvent> input = pipeline.apply(Create.of(validEvent1, validEvent2));
        
        // Process through the pipeline
        PCollection<UserCountResult> userCounts = input
            .apply("Create Time Buckets", ParDo.of(new UserEventProcessor.CreateTimeBucketFn()))
            .apply("Count Users by Bucket", Count.perKey())
            .apply("Format Results", ParDo.of(new UserEventProcessor.FormatUserCountFn()));
        
        // Should only process valid events
        UserCountResult expectedResult = new UserCountResult("2024-01-15-10-30", 2L);
        PAssert.that(userCounts).satisfies(results -> {
            java.util.List<UserCountResult> resultList = new java.util.ArrayList<>();
            results.forEach(resultList::add);
            assert resultList.size() == 1;
            assert resultList.contains(expectedResult);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testEmptyInput() {
        // Create an empty collection using Create.empty with coder
        PCollection<UserEvent> input = pipeline.apply(Create.empty(org.apache.beam.sdk.coders.SerializableCoder.of(UserEvent.class)));
        
        PCollection<UserCountResult> userCounts = input
            .apply("Create Time Buckets", ParDo.of(new UserEventProcessor.CreateTimeBucketFn()))
            .apply("Count Users by Bucket", Count.perKey())
            .apply("Format Results", ParDo.of(new UserEventProcessor.FormatUserCountFn()));
        
        PAssert.that(userCounts).empty();
        pipeline.run();
    }

    @Test
    public void testUserEventEqualsAndHashCode() {
        UserEvent event1 = new UserEvent(1, "2024-01-15T10:30:00Z");
        UserEvent event2 = new UserEvent(1, "2024-01-15T10:30:00Z");
        UserEvent event3 = new UserEvent(2, "2024-01-15T10:30:00Z");
        
        assert event1.equals(event2);
        assert event1.hashCode() == event2.hashCode();
        assert !event1.equals(event3);
    }

    @Test
    public void testUserCountResultEqualsAndHashCode() {
        UserCountResult result1 = new UserCountResult("2024-01-15-10-30", 5L);
        UserCountResult result2 = new UserCountResult("2024-01-15-10-30", 5L);
        UserCountResult result3 = new UserCountResult("2024-01-15-10-31", 5L);
        
        assert result1.equals(result2);
        assert result1.hashCode() == result2.hashCode();
        assert !result1.equals(result3);
    }
}
