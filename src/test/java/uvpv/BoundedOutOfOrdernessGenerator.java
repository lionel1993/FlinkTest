package uvpv;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/*
基于事件时间生成水印
 */
public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<MyEvent> {

    private long currentMaxTime = 0L;
    long maxTimeLag = 5000L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {

        System.out.println("time is "+DateUtils.parseTimestampToTimeStr(System.currentTimeMillis())
                +",new watermark is "+DateUtils.parseTimestampToTimeStr(currentMaxTime-maxTimeLag));

        return new Watermark(currentMaxTime-maxTimeLag);
    }

    @Override
    public long extractTimestamp(MyEvent myEvent, long l) {

        long logTime = myEvent.getLogTime();

        currentMaxTime = Math.max(logTime,currentMaxTime);

        System.out.println(
                "logTime is "+DateUtils.parseTimestampToTimeStr(logTime)
                        + ",currentMaxTime is "+DateUtils.parseTimestampToTimeStr(currentMaxTime)
        );

        return logTime;
    }
}
