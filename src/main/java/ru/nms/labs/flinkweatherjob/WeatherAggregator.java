package ru.nms.labs.flinkweatherjob;

import java.util.List;

public class WeatherAggregator {

    public static SectorWeather aggregateBatch(List<SectorWeather> batch) {
        double avgTemperature = batch.stream().mapToDouble(SectorWeather::getTemperature).average().orElse(0);
        int avgHumidity = (int) batch.stream().mapToInt(SectorWeather::getHumidity).average().orElse(0);
        double avgWindSpeed = batch.stream().mapToDouble(SectorWeather::getWindSpeed).average().orElse(0);
        double avgPressure = batch.stream().mapToDouble(SectorWeather::getPressure).average().orElse(0);

        SectorWeather first = batch.get(0);
        return new SectorWeather(
                first.getSector(),
                avgTemperature,
                avgHumidity,
                avgWindSpeed,
                avgPressure,
                first.getObservationDate()
        );
    }

    public static double calculateDispersion(SectorWeather actual, SectorWeather forecast) {
        return Math.abs(actual.getTemperature() - forecast.getTemperature()) +
                Math.abs(actual.getHumidity() - forecast.getHumidity()) +
                Math.abs(actual.getWindSpeed() - forecast.getWindSpeed()) +
                Math.abs(actual.getPressure() - forecast.getPressure());
    }
}
