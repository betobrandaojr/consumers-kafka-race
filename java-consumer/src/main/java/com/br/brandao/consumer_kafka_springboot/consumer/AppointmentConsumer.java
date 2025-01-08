package com.br.brandao.consumer_kafka_springboot.consumer;

import com.br.brandao.consumer_kafka_springboot.dto.AppointmentDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AppointmentConsumer {
    private static final Logger log = LoggerFactory.getLogger(AppointmentConsumer.class);

    @KafkaListener(
            topics = "${topic.name.consumer2}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void listenTopicAppointment(ConsumerRecord<String, AppointmentDTO> record) {
        log.info("====> [CarConsumer] Recebeu mensagem na partição: {}", record.partition());
        log.info("====> [CarConsumer] Offset: {}", record.offset());
        log.info("====> [CarConsumer] Chave: {}", record.key());
        log.info("====> [CarConsumer] Objeto AppointmentDTO: {}", record.value());
    }
}


