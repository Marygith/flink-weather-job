package ru.nms.labs.flinkweatherjob;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class WeatherWindowProcessor extends ProcessWindowFunction<SectorWeather, SectorDispersion, Tuple2<Integer, String>, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

    private static final Set<Tuple2<Integer, String>> PROCESSED_KEYS = new HashSet<>();

    @Override
    public void process(Tuple2<Integer, String> key,
                        Context context,
                        Iterable<SectorWeather> elements,
                        Collector<SectorDispersion> out) throws Exception {
        System.out.println("Processing window for key: " + key);

        if (isDuplicateKey(key)) {
            System.out.println("key was processed already: " + key);

            return;
        }

        List<SectorWeather> batch = new ArrayList<>();
        elements.forEach(batch::add);

        SectorWeather aggregated = WeatherAggregator.aggregateBatch(batch);
        System.out.println("Aggregated " + batch.size() + " elements " + batch + " to this data: " + aggregated);

        SectorWeather forecast = ForecastClient.getForecast(key.f0, key.f1);
        if (forecast == null) return;
        System.out.println("Got forecast: " + forecast);

        double dispersion = WeatherAggregator.calculateDispersion(aggregated, forecast);
        System.out.println("Calculated dispersion: " + dispersion);

        out.collect(new SectorDispersion(key.f0, key.f1, dispersion));
    }

    private static boolean isDuplicateKey(Tuple2<Integer, String> key) {
        synchronized (PROCESSED_KEYS) {
            if (PROCESSED_KEYS.contains(key)) {
                return true;
            }
            PROCESSED_KEYS.add(key);
            return false;
        }
    }
}
