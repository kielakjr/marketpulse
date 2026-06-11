package com.marketpulse.processing_service.integration.kafka;

import com.marketpulse.common.event.TickEvent;
import com.marketpulse.processing_service.integration.TestcontainersConfiguration;
import com.marketpulse.processing_service.kafka.AnomalyThresholds;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import java.util.UUID;

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
        String uniqueGroup = "pipeline-test-group-" + UUID.randomUUID();
        Map<String, Object> props = KafkaTestUtils.consumerProps(bootstrapServers, uniqueGroup, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of("market.processed", "market.alerts"));
        // Trigger partition assignment before the test sends messages, so "latest" is
        // resolved against the current end-of-topic rather than an empty assignment.
        consumer.poll(Duration.ofMillis(500));
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

    @Nested
    class WhenAnAnomalousCandleCloses {

        private static final String SYMBOL = "SPIKEUSDT";
        private static final Instant BASE = Instant.parse("2026-06-08T12:00:00Z");

        private TickEvent tick(long price, int minute) {
            return new TickEvent(SYMBOL, BigDecimal.valueOf(price), BigDecimal.ONE,
                    minute, BASE.plusSeconds(minute * 60L + 1));
        }

        @Test
        void aSingleCriticalAlertIsEmittedForTheAnomalousMinute() {
            // Minutes 0..19 hover at 100/101 (small variance); minute 20 spikes to 200.
            // The tick at minute 21 closes the minute-20 candle (close=200); with the
            // window now [~100/101 x20, 200] the z-score is ~4.47, which exceeds AnomalyThresholds.CRITICAL.
            for (int minute = 0; minute <= 21; minute++) {
                long price = (minute == 20) ? 200 : 100 + (minute % 2);
                kafkaTemplate.send("market.ticks", SYMBOL, tick(price, minute));
            }

            ConsumerRecord<String, String> alert =
                    KafkaTestUtils.getSingleRecord(consumer, "market.alerts", Duration.ofSeconds(20));

            assertThat(alert.key()).isEqualTo(SYMBOL);
            assertThat(alert.value()).contains(SYMBOL).contains("CRITICAL");
        }
    }
}
