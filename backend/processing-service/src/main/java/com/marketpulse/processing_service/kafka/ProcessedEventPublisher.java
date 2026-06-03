package com.marketpulse.processing_service.kafka;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.event.ProcessedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class ProcessedEventPublisher {

    private static final String PROCESSED_TOPIC = "market.processed";
    private static final String ALERTS_TOPIC = "market.alerts";
    private static final String MAJOR_ANOMALIES_TOPIC = "market.anomalies.major";

    private static final double ALERT_THRESHOLD = 3.0;
    private static final double HIGH_THRESHOLD = 4.0;
    private static final double CRITICAL_THRESHOLD = 5.0;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publish(ProcessedEvent event) {
        send(PROCESSED_TOPIC, event.symbol(), event);

        Double zScore = event.zScore();
        if (zScore == null) {
            return;
        }
        double absZScore = Math.abs(zScore);
        if (absZScore <= ALERT_THRESHOLD) {
            return;
        }

        var severity = severityFor(absZScore);
        var alert = new AnomalyAlert(
                event.symbol(), event.price(), zScore, severity, event.timestamp());

        log.warn("Anomaly detected for {}: z-score={} severity={}",
                event.symbol(), zScore, severity);
        send(ALERTS_TOPIC, alert.symbol(), alert);

        if (absZScore > CRITICAL_THRESHOLD) {
            send(MAJOR_ANOMALIES_TOPIC, alert.symbol(), alert);
        }
    }

    private static AlertSeverity severityFor(double absZScore) {
        if (absZScore > CRITICAL_THRESHOLD) {
            return AlertSeverity.CRITICAL;
        }
        if (absZScore > HIGH_THRESHOLD) {
            return AlertSeverity.HIGH;
        }
        return AlertSeverity.MEDIUM;
    }

    private void send(String topic, String key, Object value) {
        kafkaTemplate.send(topic, key, value)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish to {} for {}", topic, key, ex);
                    }
                });
    }
}
