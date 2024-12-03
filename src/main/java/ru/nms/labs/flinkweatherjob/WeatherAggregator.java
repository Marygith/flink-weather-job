package ru.nms.labs.flinkweatherjob;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.AggregateFunction;
import ru.nms.labs.model.SectorDispersion;
import ru.nms.labs.model.SectorWeather;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;


public class WeatherAggregator implements AggregateFunction<SectorWeather, WeatherAccumulator, SectorDispersion> {
    private static final String FORECAST_SERVICE_URL = "http://localhost:8082/api/forecast";


    @Override
    public WeatherAccumulator createAccumulator() {
        return new WeatherAccumulator();
    }

    @Override
    public WeatherAccumulator add(SectorWeather weather, WeatherAccumulator accumulator) {
        accumulator.add(weather);
        return accumulator;
    }

    @Override
    public SectorDispersion getResult(WeatherAccumulator accumulator) {
        SectorWeather forecast = null;
        try {
            forecast = getForecast(accumulator.getSector(), accumulator.getFirstObservationTime());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        double dispersion = accumulator.calculateDispersion(forecast);
        return new SectorDispersion(accumulator.getSector(), LocalDate.now(), dispersion);
    }

    @Override
    public WeatherAccumulator merge(WeatherAccumulator a, WeatherAccumulator b) {
        a.merge(b);
        return a;
    }

    public static SectorWeather getForecast(int sector, String observationTime) throws Exception {
        String url = String.format("%s?sector=%d&observationTime=%s", FORECAST_SERVICE_URL, sector, observationTime);

        try{HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return parseSectorWeather(response.body());
            } else {
                throw new Exception("Failed to fetch forecast: " + response.statusCode());
            }
        } catch (UncheckedIOException e) {
            throw new RuntimeException(e);
        }
    }

    private static SectorWeather parseSectorWeather(String json) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(json, SectorWeather.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}