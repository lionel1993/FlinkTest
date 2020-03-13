package uvpv;

public class MyEvent {

    private long logTime;
    private String appId;
    private String userId;
    private String eventId;

    public MyEvent(long logTime, String appId, String userId, String eventId) {
        this.logTime = logTime;
        this.appId = appId;
        this.userId = userId;
        this.eventId = eventId;
    }

    public long getLogTime() {
        return logTime;
    }

    public void setLogTime(long logTime) {
        this.logTime = logTime;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }
}
