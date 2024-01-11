package org.example.demo.dataenrichment;

import java.time.Instant;

public class EnrichedUserActivityEvent {

    private String userId;
    private String userName;
    private String activity;
    private String location;
    private Instant timestamp;

    public EnrichedUserActivityEvent(String userId, String userName, String activity, String location, Instant timestamp) {
        this.userId = userId;
        this.userName = userName;
        this.activity = activity;
        this.location = location;
        this.timestamp = timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getActivity() {
        return activity;
    }

    public void setActivity(String activity) {
        this.activity = activity;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
}
