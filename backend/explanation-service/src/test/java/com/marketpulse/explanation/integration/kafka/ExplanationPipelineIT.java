package com.marketpulse.explanation.integration.kafka;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.explanation.AnomalySource;
import com.marketpulse.explanation.integration.TestcontainersConfiguration;
import com.marketpulse.explanation.llm.LlmClient;
import com.marketpulse.explanation.llm.LlmResponse;
import com.marketpulse.explanation.persistence.AnomalyRecord;
import com.marketpulse.explanation.persistence.AnomalyRecordRepository;
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
        when(llmClient.explain(any())).thenReturn(new LlmResponse(
                EXPLANATION_TEXT,
                List.of(new AnomalySource("Artykuł", "https://example.com/a", "fragment"))));

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
                "BTCUSDT", new BigDecimal("73610.36"), 6.2, AlertSeverity.CRITICAL, TS);

        kafkaTemplate.send("market.anomalies.major", alert.symbol(), alert);

        ConsumerRecord<String, String> published =
                KafkaTestUtils.getSingleRecord(consumer, "market.explanations", Duration.ofSeconds(20));
        assertThat(published.key()).isEqualTo("BTCUSDT");
        assertThat(published.value()).contains("BTCUSDT").contains(EXPLANATION_TEXT);

        // The consumer persists before publishing, so the record is already stored.
        List<AnomalyRecord> stored = repository.findTop20BySymbolOrderByTimestampDesc("BTCUSDT");
        assertThat(stored).hasSize(1);
        assertThat(stored.getFirst().getExplanation()).isEqualTo(EXPLANATION_TEXT);
        assertThat(stored.getFirst().getSeverity()).isEqualTo("CRITICAL");
    }
}
