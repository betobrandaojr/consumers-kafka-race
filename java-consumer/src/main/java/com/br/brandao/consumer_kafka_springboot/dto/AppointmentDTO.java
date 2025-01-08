package com.br.brandao.consumer_kafka_springboot.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class AppointmentDTO {
    private int id;
    private String eventTime;
    private String insertDate;
    private Device device;
    private Asset asset;
    private Driver driver;
    private Details details;
    private Point point;
    private Telemetry telemetry;
    private String production;
    private String origin;

    @Data
    @NoArgsConstructor
    public static class Device {
        private long imei;
        private String hardware;
    }

    @Data
    @NoArgsConstructor
    public static class Asset {
        private int id;
        private String plate;
        private String prefix;
        private int typeId;
        private Timezone timezone;
        private int customerId;
        private String typeDescription;

        @Data
        @NoArgsConstructor
        public static class Timezone {
            private String zone;
            private int offset;
        }
    }

    @Data
    @NoArgsConstructor
    public static class Driver {
        private int id;
        private String name;
        private boolean status;
        private String language;
        private String badgeCode;
        private int customerId;
        private String insertDate;
        private String updateDate;
        private String matriculation;
    }

    @Data
    @NoArgsConstructor
    public static class Details {
        private String fields;
        private int formId;
        private int formCode;
        private int noteCode;
    }

    @Data
    @NoArgsConstructor
    public static class Point {
        private boolean ignition;
        private double latitude;
        private int driverId;
        private double longitude;
        private String eventTime;
    }

    @Data
    @NoArgsConstructor
    public static class Telemetry {
        private int rpm;
        private int extra;
        private int speed;
        private int odometer;
        private int fuelGauge;
        private double hourmeter;
        private int litermeter;
        private int production;
    }
}