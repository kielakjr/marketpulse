package com.marketpulse.websocket.unit.kafka;

import com.marketpulse.common.event.ProcessedEvent;
import com.marketpulse.websocket.kafka.ProcessedEventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.math.BigDecimal;
import java.time.Instant;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ProcessedEventListenerTest {

    private static final Instant TS = Instant.ofEpochMilli(1780135331773L);

    @Mock
    private SimpMessagingTemplate messagingTemplate;

    private ProcessedEventListener listener;

    @BeforeEach
    void setUp() {
        listener = new ProcessedEventListener(messagingTemplate);
    }

    private ProcessedEvent event(String symbol) {
        return new ProcessedEvent(
                symbol,
                new BigDecimal("73610.36"),
                new BigDecimal("73000"),
                new BigDecimal("72000"),
                55.0,
                0.5,
                TS);
    }

    @Test
    void pushesProcessedEventToSymbolSpecificPriceTopic() {
        var event = event("BTCUSDT");

        listener.onProcessedEvent(event);

        verify(messagingTemplate).convertAndSend("/topic/prices/BTCUSDT", event);
    }

    @Test
    void usesEventSymbolToBuildDestination() {
        var event = event("ETHUSDT");

        listener.onProcessedEvent(event);

        verify(messagingTemplate).convertAndSend("/topic/prices/ETHUSDT", event);
    }
}
