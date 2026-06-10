package com.marketpulse.websocket.unit.kafka;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.explanation.AnomalyExplanation;
import com.marketpulse.common.explanation.AnomalySource;
import com.marketpulse.common.explanation.SourcesQuality;
import com.marketpulse.websocket.kafka.ExplanationListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ExplanationListenerTest {

    private static final Instant TS = Instant.ofEpochMilli(1780135331773L);

    @Mock
    private SimpMessagingTemplate messagingTemplate;

    private ExplanationListener listener;

    @BeforeEach
    void setUp() {
        listener = new ExplanationListener(messagingTemplate);
    }

    private AnomalyExplanation explanation(String symbol) {
        return new AnomalyExplanation(
                symbol, new BigDecimal("73610.36"), 6.2, AlertSeverity.CRITICAL,
                "Wyjaśnienie.", List.of(new AnomalySource("t", "https://e.com", "s")),
                7, "wysoka jakość źródeł", SourcesQuality.HIGH, TS);
    }

    @Test
    void pushesExplanationToSymbolSpecificTopic() {
        var explanation = explanation("BTCUSDT");

        listener.onExplanation(explanation);

        verify(messagingTemplate).convertAndSend("/topic/explanations/BTCUSDT", explanation);
    }

    @Test
    void usesExplanationSymbolToBuildDestination() {
        var explanation = explanation("ETHUSDT");

        listener.onExplanation(explanation);

        verify(messagingTemplate).convertAndSend("/topic/explanations/ETHUSDT", explanation);
    }
}
