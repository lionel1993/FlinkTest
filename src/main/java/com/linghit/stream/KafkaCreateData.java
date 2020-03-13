package com.linghit.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.linghit.domain.SdkLogEntity;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class KafkaCreateData {
    public static final String topic = "test_flink_event";
    public static String brokerList = "172.16.30.96:9092";

    public static void createDate(){

        Properties props = new Properties();
        //声明Kakfa相关信息
        props.put("bootstrap.servers",brokerList);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        props.put("request.required.acks", "1");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

//        SdkLogEntity entity = new SdkLogEntity();
//        //手机信号
//        String phoneArray[] = {"iPhone", "HUAWEI", "xiaomi", "moto", "vivo"};
//        //操作系统
//        String osArray[] = {"Android 7.0", "Mac OS", "Apple Kernel", "Windows","kylin OS","chrome"};
//        //城市
//        String cityArray[] = {"北京","上海","杭州","南京","西藏","西安","合肥","葫芦岛"};
//        //随机产生一个手机型号
//        int k = (int) (Math.random() *5);
//        String phoneName = phoneArray[k];
//        //随机产生一个os
//        int m = (int) (Math.random() *6);
//        String os = osArray[m];
//        //随机产生一个城市地点
//        int n = (int) (Math.random() *8);
//        String city = cityArray[n];
//        //时间戳，存当前时间
//        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String loginTime = sf.format(new Date());
//        //加载数据到实体中
//        entity.setCity(city);
//        entity.setLoginTime(loginTime);
//        entity.setOs(os);
//        entity.setPhoneName(phoneName);

        String json_file = "/Users/xufeng/userEvent.json";
        String s = readJsonFile(json_file);
        JSONObject jsonobj = JSON.parseObject(s);

        //实体类封装成ProducerRecord
//        ProducerRecord record = new ProducerRecord<String, String>(topic, JSON.toJSONString(entity));
        ProducerRecord record = new ProducerRecord<String, String>(topic, JSON.toJSONString(jsonobj));
        producer.send(record);
        System.out.println("发送数据："+ JSON.toJSONString(jsonobj));
    }

    /**
     * 读取json文件，返回json串
     * @param fileName
     * @return
     */
    public static String readJsonFile(String fileName) {
        String jsonStr = "";
        try {
            File jsonFile = new File(fileName);
            FileReader fileReader = new FileReader(jsonFile);

            Reader reader = new InputStreamReader(new FileInputStream(jsonFile),"utf-8");
            int ch = 0;
            StringBuffer sb = new StringBuffer();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws InterruptedException{
        while (true){
            createDate();
            Thread.sleep(10000);
        }
    }
}
