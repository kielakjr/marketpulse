package com.marketpulse.explanation.kafka;

import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.explanation.AnomalyExplanation;
import com.marketpulse.explanation.cooldown.CooldownService;
import com.marketpulse.explanation.llm.LlmClient;
import com.marketpulse.explanation.llm.LlmResponse;
import com.marketpulse.explanation.persistence.AnomalyRecord;
import com.marketpulse.explanation.persistence.AnomalyRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Consumes major anomalies, asks the LLM for an explanation (subject to a
 * per-symbol cooldown), persists the result and re-publishes it for clients.
 * The whole flow is guarded so a single failure never stops the consumer.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ExplanationConsumer {

    private static final String EXPLANATIONS_TOPIC = "market.explanations";

    private final CooldownService cooldownService;
    private final LlmClient llmClient;
    private final AnomalyRecordRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "market.anomalies.major", groupId = "explanation-service")
    public void onAnomaly(AnomalyAlert alert) {
        try {
            String symbol = alert.symbol();

            if (cooldownService.isOnCooldown(symbol)) {
                log.info("[{}] On cooldown, skipping", symbol);
                return;
            }

            log.info("[{}] Calling LLM, zScore={}", symbol, alert.zScore());
            LlmResponse response = llmClient.explain(alert);

            var record = AnomalyRecord.builder()
                    .symbol(symbol)
                    .price(alert.price())
                    .zScore(alert.zScore() != null ? alert.zScore() : 0.0)
                    .severity(alert.severity() != null ? alert.severity().name() : null)
                    .explanation(response.explanation())
                    .sources(response.sources())
                    .timestamp(alert.timestamp())
                    .build();
            var saved = repository.save(record);
            log.info("[{}] Saved anomaly record, id={}", symbol, saved.getId());

            var explanation = new AnomalyExplanation(
                    symbol,
                    alert.price(),
                    alert.zScore(),
                    alert.severity(),
                    response.explanation(),
                    response.sources(),
                    alert.timestamp());
            kafkaTemplate.send(EXPLANATIONS_TOPIC, symbol, explanation);

            cooldownService.setCooldown(symbol);
        } catch (Exception e) {
            log.error("Failed to process anomaly for {}",
                    alert != null ? alert.symbol() : "unknown", e);
        }
    }
}
