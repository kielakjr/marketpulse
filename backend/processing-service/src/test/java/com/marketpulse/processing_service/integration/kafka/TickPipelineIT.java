package com.marketpulse.processing_service.integration.kafka;

import com.marketpulse.common.event.TickEvent;
import com.marketpulse.processing_service.integration.TestcontainersConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class TickPipelineIT {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    KafkaConnectionDetails connectionDetails;

    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        String bootstrapServers = String.join(",", connectionDetails.getBootstrapServers());
        Map<String, Object> props = KafkaTestUtils.consumerProps(bootstrapServers, "pipeline-test-group", true);
        consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of("market.processed"));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Nested
    class WhenTickIsPublished {

        private static final TickEvent TICK = new TickEvent(
                "BTCUSDT",
                new BigDecimal("73610.36"),
                new BigDecimal("0.00007"),
                6335284352L,
                Instant.ofEpochMilli(1780135331773L)
        );

        private ConsumerRecord<String, String> processedRecord() {
            kafkaTemplate.send("market.ticks", TICK.symbol(), TICK);
            return KafkaTestUtils.getSingleRecord(consumer, "market.processed", Duration.ofSeconds(20));
        }

        @Test
        void aProcessedEventIsEmittedKeyedBySymbol() {
            assertThat(processedRecord().key()).isEqualTo("BTCUSDT");
        }

        @Test
        void theProcessedEventCarriesTheSymbolAndPrice() {
            ConsumerRecord<String, String> record = processedRecord();
            assertThat(record.value()).contains("BTCUSDT").contains("73610.36");
        }
    }
}
