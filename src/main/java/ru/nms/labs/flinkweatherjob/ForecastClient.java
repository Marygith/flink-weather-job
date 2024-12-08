package ru.nms.labs.flinkweatherjob;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ForecastClient {

    private static final String FORECAST_SERVICE_URL = "http://localhost:8082/forecast";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static SectorWeather getForecast(int sector, String observationTime) throws Exception {
        String url = String.format("%s?sector=%d&observationTime=%s", FORECAST_SERVICE_URL, sector, observationTime);

        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return parseSectorWeather(response.body());
            } else if (response.statusCode() == 404) {
                System.out.println("there is no forecast for sector " + sector + " and date " + observationTime);
                return null;
            } else {
                throw new Exception("Failed to fetch forecast: " + response.statusCode());
            }
        } catch (UncheckedIOException e) {
            throw new RuntimeException(e);
        }
    }

    private static SectorWeather parseSectorWeather(String json) {
        try {

            return objectMapper.readValue(json, SectorWeather.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
