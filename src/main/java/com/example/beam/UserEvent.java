package com.example.beam;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a user event with userId and timestamp
 */
@DefaultCoder(SerializableCoder.class)
public class UserEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    
    @JsonProperty("userId")
    private int userId;
    
    @JsonProperty("time")
    private String time;

    public UserEvent() {}

    public UserEvent(int userId, String time) {
        this.userId = userId;
        this.time = time;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserEvent userEvent = (UserEvent) o;
        return userId == userEvent.userId && Objects.equals(time, userEvent.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, time);
    }

    @Override
    public String toString() {
        return "UserEvent{" +
                "userId=" + userId +
                ", time='" + time + '\'' +
                '}';
    }
}
