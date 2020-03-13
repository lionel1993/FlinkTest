package com.linghit.udf;

import com.linghit.dao.CommonDao;
import com.linghit.domain.YiQiWenChannel;
import com.linghit.util.TextUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChannelUDF {

    private static final Map<String, String> yqwChannelMap = new HashMap<>();

    static {
        // 读取mysql易起问渠道数据
        System.out.println("测试:初始化channel");
        List<YiQiWenChannel> channels = CommonDao.findYiQiWenChannel();
        for (YiQiWenChannel yiQiWenChannel : channels) {
            if (yiQiWenChannel.getChild_channel() == null || "".equals(yiQiWenChannel.getChild_channel())) {
                yqwChannelMap.put(yiQiWenChannel.getParent_channel(), yiQiWenChannel.getChannel_group());
            } else {
                yqwChannelMap.put(yiQiWenChannel.getChild_channel(), yiQiWenChannel.getChannel_group());
            }
        }
    }

    /**
     * 渠道和渠道组的清洗
     *
     * @param returnType 1 ,返回channel的名称
     *                   2 ,返回channel组
     *                   3 ,返回channel组的名称
     * @param app_prex   1、A:在线
     *                   2、F:易起问
     */
    public static String evaluate(String channel, int returnType, String app_prex) {

        if (TextUtils.isEmpty(channel)) return "未知";

        channel = channel.trim();

        if (returnType == 1) {
            return channel;
        } else {
            String code;
            String str_channel = channel.toLowerCase();

            if ("A".equals(app_prex)) { // 针对在线进行处理
                if (str_channel.startsWith("sw")) {
                    code = "商务";
                } else if ("51wnl".equals(str_channel)) {
                    code = "商务";
                } else if ("zhwnl".equals(str_channel)) {
                    code = "商务";
                } else {
                    code = "运营";
                }
            } else if ("F".equals(app_prex)) { // 针对易起问进行处理
                code = yqwChannelMap.get(channel);
                System.out.println("测试:" + channel + ":" + code);
                if (TextUtils.isEmpty(code)) {
                    code = "未知";
                }
            } else { // 剩下的就是app了
                code = channel;
            }
            return code;
        }
    }

}
