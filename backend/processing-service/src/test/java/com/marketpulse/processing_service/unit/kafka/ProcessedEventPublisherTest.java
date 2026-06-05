package com.marketpulse.processing_service.unit.kafka;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.event.ProcessedEvent;
import com.marketpulse.processing_service.kafka.ProcessedEventPublisher;
import org.jspecify.annotations.NonNull;
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

    private @NonNull ProcessedEvent eventWithZScore(Double zScore) {
        return new ProcessedEvent(
                "BTCUSDT",
                new BigDecimal("73610.36"),
                new BigDecimal("73000"),
                new BigDecimal("72000"),
                55.0,
                zScore,
                TS);
    }

    private AnomalyAlert expectedAlert(AlertSeverity severity, double zScore) {
        return new AnomalyAlert("BTCUSDT", new BigDecimal("73610.36"), zScore, severity, TS);
    }

    @Nested
    class ProcessedTopic {

        @Test
        void alwaysPublishesProcessedEvent() {
            ProcessedEvent event = eventWithZScore(0.5);
            publisher.publish(event);
            verify(kafkaTemplate).send(eq(PROCESSED_TOPIC), eq("BTCUSDT"), eq(event));
        }

        @Test
        void publishesProcessedEventEvenWhenZScoreIsNull() {
            ProcessedEvent event = eventWithZScore(null);
            publisher.publish(event);
            verify(kafkaTemplate).send(eq(PROCESSED_TOPIC), eq("BTCUSDT"), eq(event));
            verify(kafkaTemplate, never()).send(eq(ALERTS_TOPIC), anyString(), any());
        }
    }

    @Nested
    class AlertThreshold {

        @Test
        void noAlertWhenZScoreWithinThreshold() {
            publisher.publish(eventWithZScore(2.0));
            verify(kafkaTemplate, never()).send(eq(ALERTS_TOPIC), anyString(), any());
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }

        @Test
        void noAlertExactlyAtThreeBecauseThresholdIsStrict() {
            publisher.publish(eventWithZScore(3.0));
            verify(kafkaTemplate, never()).send(eq(ALERTS_TOPIC), anyString(), any());
        }

        @Test
        void mediumAlertWhenZScoreAboveThree() {
            publisher.publish(eventWithZScore(3.5));
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.MEDIUM, 3.5)));
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }

        @Test
        void highAlertWhenZScoreAboveFour() {
            publisher.publish(eventWithZScore(4.5));
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.HIGH, 4.5)));
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }

        @Test
        void highAlertExactlyAtFiveBecauseCriticalIsStrict() {
            publisher.publish(eventWithZScore(5.0));
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.HIGH, 5.0)));
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }

        @Test
        void criticalAlertAndMajorTopicWhenZScoreAboveFive() {
            publisher.publish(eventWithZScore(6.0));
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.CRITICAL, 6.0)));
            verify(kafkaTemplate).send(eq(MAJOR_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.CRITICAL, 6.0)));
        }

        @Test
        void usesAbsoluteValueForNegativeZScore() {
            publisher.publish(eventWithZScore(-6.0));
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.CRITICAL, -6.0)));
            verify(kafkaTemplate).send(eq(MAJOR_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.CRITICAL, -6.0)));
        }
    }
}
