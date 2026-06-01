package com.marketpulse.ingestion_service.kafka;

import com.marketpulse.common.event.TickEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class TickPublisher {

    private static final String TOPIC = "market.ticks";
    private final KafkaTemplate<String, TickEvent> kafkaTemplate;

    public void publish(TickEvent tick) {
        kafkaTemplate.send(TOPIC, tick.symbol(), tick)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish tick for {}", tick.symbol(), ex);
                    }
                });
    }
}
