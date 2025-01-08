package com.br.brandao.consumer_kafka_springboot.consumer;

import com.br.brandao.consumer_kafka_springboot.dto.AppointmentDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaEventListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventListener.class);

    private static final int TOTAL_MESSAGES = 1_000_000;
    private int messageCount = 0;
    private long startTime = 0;

    @KafkaListener(topics = "topic.appointments", containerFactory = "appointmentKafkaListenerContainerFactory")
    public void listenAppointments(AppointmentDTO appointment) {
        if (messageCount == 0) {
            startTime = System.currentTimeMillis();
            log.info("Recebemos a primeira mensagem. Contagem iniciada...");
        }

        messageCount++;

        if (messageCount == TOTAL_MESSAGES) {
            long endTime = System.currentTimeMillis();
            long totalTimeMs = endTime - startTime;
            double totalTimeSec = totalTimeMs / 1000.0;

            log.info("Processamos {} mensagens em {} ms ({} seg).",
                    TOTAL_MESSAGES, totalTimeMs, totalTimeSec);

            messageCount = 0;
            startTime = 0;
        }
    }
}
