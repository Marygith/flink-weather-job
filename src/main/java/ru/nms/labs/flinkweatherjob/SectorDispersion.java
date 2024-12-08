package ru.nms.labs.flinkweatherjob;


import java.io.Serializable;

public class SectorDispersion implements Serializable {
    private int sector;
    private String observationDate;
    private double dispersion;

    public SectorDispersion(int sector, String observationDate, double dispersion) {
        this.sector = sector;
        this.observationDate = observationDate;
        this.dispersion = dispersion;
    }

    public SectorDispersion() {
    }

    public int getSector() {
        return sector;
    }

    public void setSector(int sector) {
        this.sector = sector;
    }

    public double getDispersion() {
        return dispersion;
    }

    public String getObservationDate() {
        return observationDate;
    }


    public void setObservationDate(String observationDate) {
        this.observationDate = observationDate;
    }

    public void setDispersion(double dispersion) {
        this.dispersion = dispersion;
    }

}
