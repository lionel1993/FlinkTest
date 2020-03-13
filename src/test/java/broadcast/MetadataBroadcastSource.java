package broadcast;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class MetadataBroadcastSource extends RichSourceFunction<Metadata> {

    //终止标志
    private volatile boolean isRunning=true;

    //间隔
    private Long duration = 1 * 60 * 1000L;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run(SourceContext<Metadata> sourceContext) throws Exception {

        while(isRunning){


            /*
            获取元数据
             */
           String value = new Random().nextInt(100) + "";

            System.out.println("broadcast value:"+value);


            Metadata metadata = new Metadata();


            Map<String,String> map = new HashMap<String,String>();

            map.put("metadata",value);

            sourceContext.collect(metadata);

            Thread.sleep(duration);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
