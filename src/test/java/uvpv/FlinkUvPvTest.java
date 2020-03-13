package uvpv;


import com.alibaba.fastjson.JSONObject;
import com.linghit.util.ConfigUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/*

统计UV/PV


 */
public class FlinkUvPvTest {

    public static void main(String[] args) throws Exception{


        //初始化配置
        ConfigUtil.init();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 设置启动检查点 checkpoint的间隔毫秒数
        env.enableCheckpointing(10000);


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
       stream.print().setParallelism(1);


        stream.map(new MapFunction<String, MyEvent>() {

            @Override
            public MyEvent map(String s) throws Exception {

                JSONObject jsonObj = JSONObject.parseObject(s);


                int log_time = jsonObj.getInteger("log_time");
                String app_id = jsonObj.getString("app_id");
                String user_id = jsonObj.getString("user_id");
                String event_id = jsonObj.getString("event_id");

                MyEvent myEvent = new MyEvent(log_time,app_id,user_id,event_id);

                return myEvent;
            }

        })
        //.returns(TypeInformation.of(MyEvent.class))
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
        .keyBy(1)
        //.timeWindow(Time.hours(1))
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        //.aggregate(new EventAggFun(), new ResultWindowFunction());
        .aggregate(
                new AggregateFunction<MyEvent, EventAcc, EventAcc>() {

                    @Override
                    public EventAcc createAccumulator() {
                        return new EventAcc();
                    }

                    @Override
                    public EventAcc add(MyEvent myEvent, EventAcc eventAcc) {



                        return null;
                    }

                    @Override
                    public EventAcc getResult(EventAcc eventAcc) {
                        return null;
                    }

                    @Override
                    public EventAcc merge(EventAcc eventAcc, EventAcc acc1) {
                        return null;
                    }
                },
                new ProcessWindowFunction<EventAcc, EventWindowResult, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<EventAcc> iterable, Collector<EventWindowResult> collector) throws Exception {

                    }
                }
        );


       env.execute();

    }

}
