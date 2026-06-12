package com.marketpulse.processing_service.unit.kafka;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.event.ProcessedEvent;
import com.marketpulse.processing_service.kafka.AnomalyThresholds;
import com.marketpulse.processing_service.kafka.ProcessedEventPublisher;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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

import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
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
        publisher = new ProcessedEventPublisher(kafkaTemplate, new SimpleMeterRegistry());
        lenient().when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
    }

    private ProcessedEvent processedEvent() {
        return new ProcessedEvent("BTCUSDT", CLOSE,
                new BigDecimal("73000"), new BigDecimal("72000"), 55.0, 0.5, TS);
    }

    private AnomalyAlert expectedAlert(AlertSeverity severity, double zScore) {
        return new AnomalyAlert("BTCUSDT", CLOSE, zScore, severity, null, null, null, TS);
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
        void noAlertWhenZScoreWithinThreshold() {
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, AnomalyThresholds.ALERT - 0.5, null, null, null, TS);
            verify(kafkaTemplate, never()).send(eq(ALERTS_TOPIC), anyString(), any());
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }

        @Test
        void noAlertWhenZScoreIsNull() {
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, null, null, null, null, TS);
            verify(kafkaTemplate, never()).send(eq(ALERTS_TOPIC), anyString(), any());
        }

        @Test
        void noAlertExactlyAtAlertThresholdBecauseThresholdIsStrict() {
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, AnomalyThresholds.ALERT, null, null, null, TS);
            verify(kafkaTemplate, never()).send(eq(ALERTS_TOPIC), anyString(), any());
        }

        @Test
        void mediumAlertWhenZScoreAboveAlertThreshold() {
            double z = AnomalyThresholds.ALERT + 0.5;
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, z, null, null, null, TS);
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.MEDIUM, z)));
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }

        @Test
        void highAlertWhenZScoreAboveHighThreshold() {
            double z = AnomalyThresholds.HIGH + 0.2;
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, z, null, null, null, TS);
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.HIGH, z)));
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }

        @Test
        void highAlertExactlyAtCriticalThresholdBecauseCriticalIsStrict() {
            double z = AnomalyThresholds.CRITICAL;
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, z, null, null, null, TS);
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.HIGH, z)));
            verify(kafkaTemplate, never()).send(eq(MAJOR_TOPIC), anyString(), any());
        }

        @Test
        void criticalAlertAndMajorTopicWhenZScoreAboveCriticalThreshold() {
            double z = AnomalyThresholds.CRITICAL + 2.5;
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, z, null, null, null, TS);
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.CRITICAL, z)));
            verify(kafkaTemplate).send(eq(MAJOR_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.CRITICAL, z)));
        }

        @Test
        void usesAbsoluteValueForNegativeZScore() {
            double z = -(AnomalyThresholds.CRITICAL + 2.5);
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, z, null, null, null, TS);
            verify(kafkaTemplate).send(eq(ALERTS_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.CRITICAL, z)));
            verify(kafkaTemplate).send(eq(MAJOR_TOPIC), eq("BTCUSDT"),
                    eq(expectedAlert(AlertSeverity.CRITICAL, z)));
        }

        @Test
        void criticalAnomalyCarriesIndicatorsToTheMajorTopic() {
            publisher.evaluateAnomaly("BTCUSDT", CLOSE, AnomalyThresholds.CRITICAL + 2.5,
                    new BigDecimal("70000"), new BigDecimal("68000"), 81.3, TS);

            ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
            verify(kafkaTemplate).send(eq(MAJOR_TOPIC), eq("BTCUSDT"), captor.capture());
            AnomalyAlert sent = (AnomalyAlert) captor.getValue();
            assertThat(sent.sma20()).isEqualByComparingTo("70000");
            assertThat(sent.sma50()).isEqualByComparingTo("68000");
            assertThat(sent.rsi()).isEqualTo(81.3);
        }
    }
}
