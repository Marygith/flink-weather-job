package ru.nms.labs.flinkweatherjob;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import ru.nms.labs.model.SectorWeather;

import java.time.LocalDate;

import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;

public class WeatherBatchTrigger extends Trigger<SectorWeather, GlobalWindow> {

    private transient ValueState<LocalDate> lastSeenDateState;

    @Override
    public TriggerResult onElement(SectorWeather element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
        LocalDate currentDate = LocalDate.parse(element.getObservationDate(), ISO_DATE_TIME);

        if (lastSeenDateState == null) {
            lastSeenDateState = ctx.getPartitionedState(new ValueStateDescriptor<>("lastSeenDate", LocalDate.class));
        }

        LocalDate lastSeenDate = lastSeenDateState.value();

        if (lastSeenDate != null && currentDate.getDayOfYear() > lastSeenDate.getDayOfYear()) {
            System.out.println("New day detected: Fire and purge");
            lastSeenDateState.update(currentDate);
            return TriggerResult.FIRE_AND_PURGE;
        }

        lastSeenDateState.update(currentDate);

        ctx.registerProcessingTimeTimer(ctx.getCurrentProcessingTime() + 10 * 60 * 1000);

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long timestamp, GlobalWindow window, TriggerContext ctx) {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onEventTime(long timestamp, GlobalWindow window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) {
        if (lastSeenDateState != null) {
            lastSeenDateState.clear();
        }
    }
}