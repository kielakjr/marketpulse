package com.marketpulse.ingestion_service.integration.kafka;

import com.marketpulse.common.event.TickEvent;
import com.marketpulse.ingestion_service.binance.BinanceStreamManager;
import com.marketpulse.ingestion_service.integration.TestcontainersConfiguration;
import com.marketpulse.ingestion_service.kafka.TickPublisher;
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
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class TickPublisherIT {

    @MockitoBean
    BinanceStreamManager binanceStreamManager;

    @Autowired
    TickPublisher tickPublisher;

    @Autowired
    KafkaConnectionDetails connectionDetails;

    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        String bootstrapServers = String.join(",", connectionDetails.getBootstrapServers());
        Map<String, Object> props = KafkaTestUtils.consumerProps(bootstrapServers, "integration-test-group", true);
        consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of("market.ticks"));
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

        private ConsumerRecord<String, String> consumedRecord() {
            tickPublisher.publish(TICK);
            return KafkaTestUtils.getSingleRecord(consumer, "market.ticks", Duration.ofSeconds(10));
        }

        @Test
        void messageArrivesWithSymbolAsKey() {
            assertThat(consumedRecord().key()).isEqualTo("BTCUSDT");
        }

        @Test
        void messagePayloadContainsSymbol() {
            assertThat(consumedRecord().value()).contains("BTCUSDT");
        }

        @Test
        void messagePayloadContainsPrice() {
            assertThat(consumedRecord().value()).contains("73610.36");
        }
    }
}
