package com.example.beam;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents aggregated user count data by time bucket
 */
@DefaultCoder(SerializableCoder.class)
public class UserCountResult implements Serializable {
    private static final long serialVersionUID = 1L;
    
    @JsonProperty("bucket")
    private String bucket;
    
    @JsonProperty("userCount")
    private long userCount;

    public UserCountResult() {}

    public UserCountResult(String bucket, long userCount) {
        this.bucket = bucket;
        this.userCount = userCount;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public long getUserCount() {
        return userCount;
    }

    public void setUserCount(long userCount) {
        this.userCount = userCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserCountResult that = (UserCountResult) o;
        return userCount == that.userCount && Objects.equals(bucket, that.bucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, userCount);
    }

    @Override
    public String toString() {
        return "UserCountResult{" +
                "bucket='" + bucket + '\'' +
                ", userCount=" + userCount +
                '}';
    }
}
