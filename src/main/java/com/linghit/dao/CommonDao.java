package com.linghit.dao;

import com.linghit.domain.YiQiWenChannel;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;


public class CommonDao {

    public static List<YiQiWenChannel> findYiQiWenChannel() {
        List<YiQiWenChannel> list = new ArrayList<>();
        String sql = "select parent_channel,child_channel,child_channel_name,channel_group from datacenter_dp_dimension.yiqiwen_channel";
        Vector<String[]> result = DBUtil.query("rds", sql);
        for (String[] ss : result) {

            YiQiWenChannel yiQiWenChannel = new YiQiWenChannel();

            yiQiWenChannel.setParent_channel(ss[0]);
            yiQiWenChannel.setChild_channel(ss[1]);
            yiQiWenChannel.setChild_channel_name(ss[2]);
            yiQiWenChannel.setChannel_group(ss[3]);

            list.add(yiQiWenChannel);
        }
        return list;

    }

}
