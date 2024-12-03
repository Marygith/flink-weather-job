package ru.nms.labs.flinkweatherjob;

import ru.nms.labs.model.SectorWeather;

import java.util.ArrayList;
import java.util.List;

public class WeatherAccumulator {

    private int sector;
    private String firstObservationTime;
    private final List<SectorWeather> weatherData;

    public WeatherAccumulator() {
        this.weatherData = new ArrayList<>();
    }

    public void add(SectorWeather weather) {
        if (weatherData.isEmpty()) {
            this.sector = weather.getSector();
            this.firstObservationTime = weather.getObservationDate();
        }
        weatherData.add(weather);
    }

    public void merge(WeatherAccumulator other) {
        if (weatherData.isEmpty() && !other.weatherData.isEmpty()) {
            this.sector = other.sector;
            this.firstObservationTime = other.firstObservationTime;
        }
        weatherData.addAll(other.weatherData);
    }

    public SectorWeather calculateAverage() {
        if (weatherData.isEmpty()) {
            throw new IllegalStateException("Cannot calculate average from an empty batch.");
        }

        double totalTemperature = 0.0;
        int totalHumidity = 0;
        double totalWindSpeed = 0.0;
        double totalPressure = 0.0;

        for (SectorWeather weather : weatherData) {
            totalTemperature += weather.getTemperature();
            totalHumidity += weather.getHumidity();
            totalWindSpeed += weather.getWindSpeed();
            totalPressure += weather.getPressure();
        }

        int count = weatherData.size();

        return new SectorWeather(
                sector,
                totalTemperature / count,
                totalHumidity / count,
                totalWindSpeed / count,
                totalPressure / count,
                firstObservationTime
        );
    }

    public double calculateDispersion(SectorWeather forecast) {
        SectorWeather average = calculateAverage();

        double tempDiff = Math.pow(average.getTemperature() - forecast.getTemperature(), 2);
        double humidityDiff = Math.pow(average.getHumidity() - forecast.getHumidity(), 2);
        double windSpeedDiff = Math.pow(average.getWindSpeed() - forecast.getWindSpeed(), 2);
        double pressureDiff = Math.pow(average.getPressure() - forecast.getPressure(), 2);

        return tempDiff + humidityDiff + windSpeedDiff + pressureDiff;
    }

    public int getSector() {
        return sector;
    }

    public String getFirstObservationTime() {
        return firstObservationTime;
    }
}
