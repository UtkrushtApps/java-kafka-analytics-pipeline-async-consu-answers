package com.example.analytics.model;

import java.time.Instant;
import java.util.Map;
import java.io.Serializable;

public class AnalyticsEvent implements Serializable {
    private String userId;
    private String eventType;
    private Instant timestamp;
    private Map<String, Object> properties;

    public AnalyticsEvent() {
    }

    public AnalyticsEvent(String userId, String eventType, Instant timestamp, Map<String, Object> properties) {
        this.userId = userId;
        this.eventType = eventType;
        this.timestamp = timestamp;
        this.properties = properties;
    }

    public String getUserId() {
        return userId;
    }
    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventType() {
        return eventType;
    }
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Instant getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }
    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
