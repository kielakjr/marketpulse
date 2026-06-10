package com.marketpulse.explanation.kafka;

import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.explanation.AnomalyExplanation;
import com.marketpulse.explanation.cooldown.CooldownService;
import com.marketpulse.explanation.llm.ExplanationMapper;
import com.marketpulse.explanation.llm.LlmClient;
import com.marketpulse.explanation.llm.PromptBuilder;
import com.marketpulse.explanation.llm.StructuredResponse;
import com.marketpulse.explanation.llm.StructuredResponseParser;
import com.marketpulse.explanation.persistence.AnomalyRecord;
import com.marketpulse.explanation.persistence.AnomalyRecordRepository;
import com.marketpulse.explanation.search.SearchClient;
import com.marketpulse.explanation.search.SearchResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Consumes major anomalies, retrieves context (SearxNG), asks the LLM for a
 * structured explanation (subject to a per-symbol cooldown), persists the result
 * and re-publishes it for clients. The whole flow is guarded so a single failure
 * never stops the consumer.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ExplanationConsumer {

    private static final String EXPLANATIONS_TOPIC = "market.explanations";

    private final CooldownService cooldownService;
    private final SearchClient searchClient;
    private final PromptBuilder promptBuilder;
    private final LlmClient llmClient;
    private final StructuredResponseParser parser;
    private final ExplanationMapper mapper;
    private final AnomalyRecordRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "market.anomalies.major", groupId = "explanation-service")
    public void onAnomaly(AnomalyAlert alert) {
        String symbol = null;
        boolean acquired = false;
        try {
            symbol = alert.symbol();

            if (!cooldownService.tryAcquire(symbol)) {
                log.info("[{}] On cooldown, skipping", symbol);
                return;
            }
            acquired = true;

            List<SearchResult> results = searchClient.search(alert);
            String prompt = promptBuilder.build(alert, results);

            log.info("[{}] Calling LLM, zScore={}, sources={}", symbol, alert.zScore(), results.size());
            StructuredResponse response = parser.parse(llmClient.complete(prompt));
            AnomalyExplanation explanation = mapper.toExplanation(alert, response, results);

            var record = AnomalyRecord.builder()
                    .symbol(symbol)
                    .price(alert.price())
                    .zScore(alert.zScore() != null ? alert.zScore() : 0.0)
                    .severity(alert.severity() != null ? alert.severity().name() : null)
                    .explanation(explanation.explanation())
                    .sources(explanation.sources())
                    .confidence(explanation.confidence())
                    .confidenceReason(explanation.confidenceReason())
                    .sourcesQuality(explanation.sourcesQuality())
                    .timestamp(alert.timestamp())
                    .build();
            var saved = repository.save(record);
            log.info("[{}] Saved anomaly record, id={}", symbol, saved.getId());

            kafkaTemplate.send(EXPLANATIONS_TOPIC, symbol, explanation);
        } catch (Exception e) {
            if (acquired) {
                cooldownService.clear(symbol);
            }
            log.error("Failed to process anomaly for {}",
                    symbol != null ? symbol : "unknown", e);
        }
    }
}
