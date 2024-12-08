package ru.nms.labs.flinkweatherjob;

import java.io.Serializable;

public class SectorWeather implements Serializable {
    private int sector;
    private double temperature;
    private int humidity;
    private double windSpeed;
    private double pressure;
    private String observationDate;

    public int getSector() {
        return sector;
    }

    public SectorWeather(int sector, double temperature, int humidity, double windSpeed, double pressure, String observationDate) {
        this.sector = sector;
        this.temperature = temperature;
        this.humidity = humidity;
        this.windSpeed = windSpeed;
        this.pressure = pressure;
        this.observationDate = observationDate;
    }

    public void setSector(int sector) {
        this.sector = sector;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public int getHumidity() {
        return humidity;
    }

    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }

    public double getWindSpeed() {
        return windSpeed;
    }

    public void setWindSpeed(double windSpeed) {
        this.windSpeed = windSpeed;
    }

    public double getPressure() {
        return pressure;
    }

    public void setPressure(double pressure) {
        this.pressure = pressure;
    }

    public String getObservationDate() {
        return observationDate;
    }

    public void setObservationDate(String observationDate) {
        this.observationDate = observationDate;
    }

    public SectorWeather() {
    }
}