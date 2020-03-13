package com.linghit.domain;

/**
 * 灵机融合平台用户标识别
 */
public class LinghitUser {
    /**
     * 大数据生成的唯一ID
     */
    private String linghit_id;
    /**
     * 四者之一的值，cookie、openid、$equipment_id，$phone
     */
    private String user;
    /**
     * userid_type：指定user_id的类型,一般有四种：cookie、openid、$equipment_id，$phone
     */
    private String userid_type;
    /**
     * 在线的cookie
     */
    private String cookie;
    /**
     * 微信体系授权的openid
     */
    private String openid;
    /**
     * app体系的设备号
     */
    private String equipment_id;
    /**
     * 手机号，灵机用户标识
     */
    private String phone;

    public String getLinghit_id() {
        return linghit_id;
    }

    public void setLinghit_id(String linghit_id) {
        this.linghit_id = linghit_id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUserid_type() {
        return userid_type;
    }

    public void setUserid_type(String userid_type) {
        this.userid_type = userid_type;
    }

    public String getCookie() {
        return cookie;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }

    public String getOpenid() {
        return openid;
    }

    public void setOpenid(String openid) {
        this.openid = openid;
    }

    public String getEquipment_id() {
        return equipment_id;
    }

    public void setEquipment_id(String equipment_id) {
        this.equipment_id = equipment_id;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }
}
