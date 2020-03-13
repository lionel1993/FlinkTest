package com.linghit.domain;

public class ProductMap {

    private long time;

    private String app_id;

    private String content;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "ProductMap{" +
                "time=" + time +
                ", app_id='" + app_id + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
