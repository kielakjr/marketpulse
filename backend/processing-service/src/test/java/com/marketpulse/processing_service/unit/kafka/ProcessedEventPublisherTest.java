package com.marketpulse.processing_service.unit.kafka;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.event.ProcessedEvent;
import com.marketpulse.processing_service.kafka.ProcessedEventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ProcessedEventPublisherTest {

    private static final String PROCESSED_TOPIC = "market.processed";
    private static final String ALERTS_TOPIC = "market.alerts";
    private static final String MAJOR_TOPIC = "market.anomalies.major";
    private static final Instant TS = Instant.ofEpochMilli(1780135331773L);
    private static final BigDecimal CLOSE = new BigDecimal("73610.36");

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private ProcessedEventPublisher publisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        publisher = new ProcessedEventPublisher(kafkaTemplate);
        lenient().when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
    }

    private ProcessedEvent processedEvent() {
        return new ProcessedEvent("BTCUSDT", CLOSE,
                new BigDecimal("73000"), new BigDecimal("72000"), 55.0, 0.5, TS);
    }

    private AnomalyAlert expectedAlert(AlertSeverity severity, double zScore) {
        return new AnomalyAlert("BTCUSDT", CLOSE, zScore, severity, TS);
    }

    @Nested
    class PublishProcessed {

        @Test
        void sendsToProcessedTopicKeyedBySymbol() {
            ProcessedEvent event = processedEvent();
            publisher.publishProcessed(event);
            verify(kafkaTemplate).send(eq(PROCESSED_TOPIC), eq("BTCUSDT"), eq(event));
        }

        @Test
        void neverEmitsAlerts() {
            publisher.publishProcessed(processedEvent());
            verify(kafkaTemplate, never()).send(eq(ALERTS_TOPIC), anyString(), any());
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }
    }

    @Nested
    class EvaluateAnomaly {

        @Test
        void noAlertWhenZScoreIsNull() {
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, null, TS);
            verify(kafkaTemplate, never()).send(eq(ALERTS_TOPIC), anyString(), any());
        }

        @Test
        void noAlertWhenZScoreWithinThreshold() {
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, 2.0, TS);
            verify(kafkaTemplate, never()).send(eq(ALERTS_TOPIC), anyString(), any());
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }

        @Test
        void noAlertExactlyAtThreeBecauseThresholdIsStrict() {
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, 3.0, TS);
            verify(kafkaTemplate, never()).send(eq(ALERTS_TOPIC), anyString(), any());
        }

        @Test
        void mediumAlertWhenZScoreAboveThree() {
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, 3.5, TS);
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.MEDIUM, 3.5)));
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }

        @Test
        void highAlertWhenZScoreAboveFour() {
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, 4.5, TS);
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.HIGH, 4.5)));
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }

        @Test
        void highAlertExactlyAtFiveBecauseCriticalIsStrict() {
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, 5.0, TS);
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.HIGH, 5.0)));
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }

        @Test
        void criticalAlertAndMajorTopicWhenZScoreAboveFive() {
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, 6.0, TS);
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.CRITICAL, 6.0)));
            verify(kafkaTemplate).send(eq(MAJOR_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.CRITICAL, 6.0)));
        }

        @Test
        void usesAbsoluteValueForNegativeZScore() {
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, -6.0, TS);
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.CRITICAL, -6.0)));
            verify(kafkaTemplate).send(eq(MAJOR_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.CRITICAL, -6.0)));
        }
    }
}
