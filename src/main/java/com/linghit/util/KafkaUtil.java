package com.linghit.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Spark的kafka偏移量获取方式:get /kafka-1.1.1/consumers/kafka_sdk_group/offsets/sdklog/47
 * Flink的kafka消费者通过api直接获取保存
 */
public class KafkaUtil {
    private static final String PATH = "/Users/xishuang/Desktop/logs/";
    /**
     * spark的消费者，需要通过zookeeper来获取偏移量
     */
    private static final String SPARK_ORIGIN_GROUP_ID = "kafka_sdk_group";
    private static final String SPARK_NEW_GROUP_ID = "new_kafka_sdk_group";
    /**
     * flink全表的消费者
     */
    private static final String FLINK_ALL_TABLE_ORIGIN_GROUP_ID = "flink_sdk_group";
    private static final String FLINK_ALL_TABLE_NEW_GROUP_ID = "new_flink_sdk_group";
    /**
     * flink融合平台消费者
     */
    private static final String FLINK_LINGHIT_ORIGIN_GROUP_ID = "linghit_sdk_group";
    private static final String FLINK_LINGHIT_NEW_GROUP_ID = "new_linghit_sdk_group";

    public static void main(String[] args) throws Exception {
        ConfigUtil.init();

        String pathName = PATH + FLINK_ALL_TABLE_NEW_GROUP_ID;
        // 记录下原始消费组的偏移量，保存到文件中后续使用
        Map<String, Long> offsets = currentOffset(FLINK_ALL_TABLE_ORIGIN_GROUP_ID);
        writeTxt(pathName, JSONObject.toJSONString(offsets));
//
//        pathName = PATH + FLINK_LINGHIT_NEW_GROUP_ID;
//        Map<String, Long> offsets_linghit = currentOffset(FLINK_LINGHIT_ORIGIN_GROUP_ID);
//        writeTxt(pathName, JSONObject.toJSONString(offsets_linghit));

        // 读取之前记录的偏移量数据，修改新消费者的偏移量
//        pathName = PATH + FLINK_ALL_TABLE_NEW_GROUP_ID;
//        String content = readTxt(pathName);
//        Map<String, Long> result = JSON.parseObject(content, new TypeReference<Map<String, Long>>() {
//        });
//        setConsumeOffset(FLINK_ALL_TABLE_NEW_GROUP_ID, result);
    }

    /**
     * 查询对应消费者组的当前消费偏移量
     *
     * @param groupID 消费组id
     */
    private static Map<String, Long> currentOffset(String groupID) throws TimeoutException {
        Map<String, Long> offsets = new HashMap<>();

        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigUtil.getProperties("kafka_host"));
        try (AdminClient client = AdminClient.create(props)) {
            ListConsumerGroupOffsetsResult result = client.listConsumerGroupOffsets(groupID);
            try {
                Map<TopicPartition, OffsetAndMetadata> consumedOffsets = result.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumedOffsets.keySet());
                    for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
                        System.out.println(entry.getKey().topic() + ":" + entry.getKey().partition() + ":" + consumedOffsets.get(entry.getKey()).offset() + ":" + entry.getValue());
                        offsets.put(entry.getKey().toString(), consumedOffsets.get(entry.getKey()).offset());
                    }
                    return offsets;
//                    return endOffsets.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(),
//                            entry -> entry.getValue() - consumedOffsets.get(entry.getKey()).offset()));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Collections.emptyMap();
            } catch (ExecutionException e) {
                e.printStackTrace();
                return Collections.emptyMap();
            } catch (TimeoutException e) {
                throw new TimeoutException("Timed out when getting lag for consumer group：" + groupID);
            }
        }
    }

    /**
     * 给对应消费组设置新的偏移量
     */
    private static void setConsumeOffset(String groupID, Map<String, Long> offsets) {
        Properties props = new Properties();
        props.put("bootstrap.servers", ConfigUtil.getProperties("kafka_host"));
        props.put("group.id", groupID);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("sdklog"));
        consumer.poll(Duration.ofMillis(10));

        Set<TopicPartition> assignment = new HashSet<>();
//        // 在poll()方法内部执行分区分配逻辑，该循环确保分区已被分配。
//        // 当分区消息为0时进入此循环，如果不为0，则说明已经成功分配到了分区。
        while (assignment.size() == 0) {
            consumer.poll(100);
            // assignment()方法是用来获取消费者所分配到的分区消息的
            // assignment的值为：sdklog-32, sdklog-33...
            assignment = consumer.assignment();
        }
        System.out.println(assignment);

        // 设置新的偏移量
        for (Map.Entry<String, Long> entry : offsets.entrySet()) {
            String[] patition = entry.getKey().split("-");
            TopicPartition tp = new TopicPartition(patition[0], Integer.parseInt(patition[1]));
            System.out.println("分区 " + tp + " 从 " + entry.getValue() + " 开始消费");
            consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(entry.getValue())));
        }
//        for (TopicPartition tp : assignment) {
//            if (offsets.containsKey(tp.toString())) {
//                Long newOffset = offsets.get(tp.toString());
//                System.out.println("分区 " + tp + " 从 " + newOffset + " 开始消费");
//                consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(newOffset)));
//            }
//        }

        // 消费数据
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(1000);
//            // 消费记录
//            for (TopicPartition partition : records.partitions()) {
//                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
//                for (ConsumerRecord<String, String> record : partitionRecords) {
//                    System.out.println(partition + ":" + record.offset() + ": " + record.value().substring(0, 20));
//                    break;
//                }
//                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
//            }
//        }
    }

    /**
     * 写入文件
     */
    private static void writeTxt(String fileName, String content) {
        File file = new File(fileName);
        try {
            FileOutputStream outputStream = new FileOutputStream(file);
            outputStream.write(content.getBytes());
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 文件读取
     */
    private static String readTxt(String fileName) {
        BufferedReader reader = null;
        StringBuilder builder = new StringBuilder();
        try {
            File file = new File(fileName);
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                builder.append(tempStr);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return builder.toString();
    }

}
