package com.br.brandao.consumer_kafka_springboot.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class CarDTO {

    private String id;
    private String model;
    private String color;

}
