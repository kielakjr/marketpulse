package com.marketpulse.websocket.unit.kafka;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.websocket.kafka.AlertListener;
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
class AlertListenerTest {

    private static final Instant TS = Instant.ofEpochMilli(1780135331773L);

    @Mock
    private SimpMessagingTemplate messagingTemplate;

    private AlertListener listener;

    @BeforeEach
    void setUp() {
        listener = new AlertListener(messagingTemplate);
    }

    private AnomalyAlert alert(String symbol) {
        return new AnomalyAlert(symbol, new BigDecimal("73610.36"), 4.5, AlertSeverity.HIGH, TS);
    }

    @Test
    void pushesAlertToSymbolSpecificAlertsTopic() {
        var alert = alert("BTCUSDT");

        listener.onAlert(alert);

        verify(messagingTemplate).convertAndSend("/topic/alerts/BTCUSDT", alert);
    }

    @Test
    void usesAlertSymbolToBuildDestination() {
        var alert = alert("ETHUSDT");

        listener.onAlert(alert);

        verify(messagingTemplate).convertAndSend("/topic/alerts/ETHUSDT", alert);
    }
}
