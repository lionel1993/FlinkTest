package uvpv;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;

public class ResultWindowFunction implements WindowFunction<EventAcc,EventWindowResult, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<EventAcc> iterable, Collector<EventWindowResult> collector) throws Exception {

    }
}
