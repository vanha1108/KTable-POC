package org.example.demo.alertingandmonitoring;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;

public class ErrorEvent {

    private String eventId;
    private String errorMessage;
    private Instant timestamp;

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
}
