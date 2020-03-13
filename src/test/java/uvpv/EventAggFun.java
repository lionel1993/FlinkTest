package uvpv;

import org.apache.flink.api.common.functions.AggregateFunction;

public class EventAggFun implements AggregateFunction<MyEvent,EventAcc,EventAcc> {
    @Override
    public EventAcc createAccumulator() {
        return null;
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
}
