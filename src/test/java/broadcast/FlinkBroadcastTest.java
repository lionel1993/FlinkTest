package broadcast;

import com.linghit.util.ConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.connectors.kudu.connector.KuduRow;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;


/*

    元数据广播流

 */
public class FlinkBroadcastTest {

    private static Logger logger = LoggerFactory.getLogger(FlinkBroadcastTest.class);



    public static void main(String[] args)  throws Exception{


        //初始化配置
        ConfigUtil.init();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 设置启动检查点 checkpoint的间隔毫秒数
//        env.enableCheckpointing(10000);

//        env.setParallelism(6); // 设置并行度

        //设置以事件事件为时间基准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
        构建配置类
         */
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", ConfigUtil.getProperties("zookeeper_host"));
        props.setProperty("bootstrap.servers", ConfigUtil.getProperties("kafka_host"));
        props.setProperty("group.id", ConfigUtil.getProperties("linghit_log_group"));
        props.setProperty("enable.auto.commit", "true");
        System.out.println("****配置设置成功****");

        /*
        构建kafka流
         */
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("dev-sdklog",new SimpleStringSchema(),props);
        DataStream<String> stream = env.addSource(consumer);

        //打印日志
//        stream.print().setParallelism(1);


        /*
         构建广播流
         */
        MapStateDescriptor<String,String> metaDataRule = new MapStateDescriptor<String, String>("metaData",
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        MetadataBroadcastSource metadataSource = new MetadataBroadcastSource();

        BroadcastStream broadcastStream = env.addSource(metadataSource).setParallelism(1).broadcast(metaDataRule);


        stream.connect(broadcastStream).process(new BroadcastProcessFunction<String,Map<String,String>, List<KuduRow>>() {

            Map<String,String> meataDataMap;

            @Override
            public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<List<KuduRow>> collector) throws Exception {

                System.out.println("处理广播");

                System.out.println("源数据："+s);

                System.out.println("元数据："+meataDataMap.get("metadata"));





            }

            @Override
            public void processBroadcastElement(Map<String,String> s, Context context, Collector<List<KuduRow>> collector) throws Exception {
                System.out.println("收到广播:"+s);

                meataDataMap = s;
            }



        });

        env.execute();

    }


}
