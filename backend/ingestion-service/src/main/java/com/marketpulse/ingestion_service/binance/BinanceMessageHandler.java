package com.marketpulse.ingestion_service.binance;

import tools.jackson.databind.ObjectMapper;
import com.marketpulse.common.event.TickEvent;
import com.marketpulse.ingestion_service.kafka.TickPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;

@Component
@Slf4j
@RequiredArgsConstructor
public class BinanceMessageHandler {

    private final ObjectMapper objectMapper;
    private final TickPublisher tickPublisher;

    public void handle(String rawMessage) {
        try {
            var msg = objectMapper.readValue(rawMessage, BinanceTradeMessage.class);
            var tick = new TickEvent(
                    msg.symbol(),
                    new BigDecimal(msg.price()),
                    new BigDecimal(msg.quantity()),
                    msg.tradeId(),
                    Instant.ofEpochMilli(msg.tradeTime())
            );
            tickPublisher.publish(tick);
        } catch (Exception e) {
            log.error("Failed to parse message: {}", rawMessage, e);
        }
    }
}
