package com.linghit.domain;

public class YiQiWenChannel {
    private int id;
    private String parent_channel;
    private String child_channel;
    private String child_channel_name;
    private String channel_group;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getParent_channel() {
        return parent_channel;
    }

    public void setParent_channel(String parent_channel) {
        this.parent_channel = parent_channel;
    }

    public String getChild_channel() {
        return child_channel;
    }

    public void setChild_channel(String child_channel) {
        this.child_channel = child_channel;
    }

    public String getChild_channel_name() {
        return child_channel_name;
    }

    public void setChild_channel_name(String child_channel_name) {
        this.child_channel_name = child_channel_name;
    }

    public String getChannel_group() {
        return channel_group;
    }

    public void setChannel_group(String channel_group) {
        this.channel_group = channel_group;
    }

}
