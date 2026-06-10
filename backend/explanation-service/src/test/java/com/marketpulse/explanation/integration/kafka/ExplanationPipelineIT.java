package com.marketpulse.explanation.integration.kafka;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.explanation.SourcesQuality;
import com.marketpulse.explanation.integration.TestcontainersConfiguration;
import com.marketpulse.explanation.llm.LlmClient;
import com.marketpulse.explanation.persistence.AnomalyRecord;
import com.marketpulse.explanation.persistence.AnomalyRecordRepository;
import com.marketpulse.explanation.search.SearchClient;
import com.marketpulse.explanation.search.SearchResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class ExplanationPipelineIT {

    private static final Instant TS = Instant.ofEpochMilli(1780135331773L);
    private static final String EXPLANATION_TEXT = "Nagły spadek spowodowany falą wyprzedaży.";

    @MockitoBean
    LlmClient llmClient;

    @MockitoBean
    SearchClient searchClient;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    KafkaConnectionDetails connectionDetails;

    @Autowired
    AnomalyRecordRepository repository;

    @Autowired
    StringRedisTemplate redisTemplate;

    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        when(searchClient.search(any())).thenReturn(
                List.of(new SearchResult("Artykuł", "https://example.com/a", "fragment")));
        when(llmClient.complete(any())).thenReturn("""
                {"explanation":"Nagły spadek spowodowany falą wyprzedaży.","confidence":8,
                 "confidence_reason":"spójne źródła","used_sources":[1]}
                """);

        repository.deleteAll();
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();

        String bootstrapServers = String.join(",", connectionDetails.getBootstrapServers());
        Map<String, Object> props =
                KafkaTestUtils.consumerProps(bootstrapServers, "explanation-pipeline-test", true);
        consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of("market.explanations"));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void aMajorAnomalyIsExplainedPublishedAndPersisted() {
        var alert = new AnomalyAlert(
                "BTCUSDT", new BigDecimal("73610.36"), 6.2, AlertSeverity.CRITICAL, null, null, null, TS);

        kafkaTemplate.send("market.anomalies.major", alert.symbol(), alert);

        ConsumerRecord<String, String> published =
                KafkaTestUtils.getSingleRecord(consumer, "market.explanations", Duration.ofSeconds(20));
        assertThat(published.key()).isEqualTo("BTCUSDT");
        assertThat(published.value()).contains("BTCUSDT").contains(EXPLANATION_TEXT)
                .contains("MEDIUM");   // sourcesQuality serialized (1 used source -> MEDIUM)

        // The consumer persists before publishing, so the record is already stored.
        List<AnomalyRecord> stored = repository.findTop20BySymbolOrderByTimestampDesc("BTCUSDT");
        assertThat(stored).hasSize(1);
        assertThat(stored.getFirst().getExplanation()).isEqualTo(EXPLANATION_TEXT);
        assertThat(stored.getFirst().getSeverity()).isEqualTo("CRITICAL");
        assertThat(stored.getFirst().getConfidence()).isEqualTo(8);
        assertThat(stored.getFirst().getSourcesQuality()).isEqualTo(SourcesQuality.MEDIUM);
    }
}
