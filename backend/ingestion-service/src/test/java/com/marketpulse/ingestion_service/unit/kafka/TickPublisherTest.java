package com.marketpulse.ingestion_service.unit.kafka;

import com.marketpulse.common.event.TickEvent;
import com.marketpulse.ingestion_service.kafka.TickPublisher;
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

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TickPublisherTest {

    @Mock
    private KafkaTemplate<String, TickEvent> kafkaTemplate;

    private TickPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new TickPublisher(kafkaTemplate);
    }

    @Nested
    class WhenPublishingATick {

        private static final TickEvent TICK = new TickEvent(
                "BTCUSDT",
                new BigDecimal("73610.36"),
                new BigDecimal("0.00007"),
                6335284352L,
                Instant.ofEpochMilli(1780135331773L)
        );

        @BeforeEach
        @SuppressWarnings("unchecked")
        void stubKafka() {
            when(kafkaTemplate.send(anyString(), anyString(), any()))
                    .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
        }

        @Test
        void sendsToMarketTicksTopic() {
            publisher.publish(TICK);
            verify(kafkaTemplate).send(eq("market.ticks"), anyString(), any());
        }

        @Test
        void usesSymbolAsKey() {
            publisher.publish(TICK);
            verify(kafkaTemplate).send(anyString(), eq("BTCUSDT"), any());
        }

        @Test
        void sendsTheTickAsValue() {
            publisher.publish(TICK);
            verify(kafkaTemplate).send(anyString(), anyString(), eq(TICK));
        }
    }

    @Nested
    class WhenKafkaSendFails {

        @BeforeEach
        void stubFailure() {
            CompletableFuture<SendResult<String, TickEvent>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RuntimeException("Kafka down"));
            when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(failed);
        }

        @Test
        void doesNotThrow() {
            TickEvent tick = new TickEvent("ETHUSDT", BigDecimal.ONE, BigDecimal.ONE, 1L, Instant.now());
            assertThatNoException().isThrownBy(() -> publisher.publish(tick));
        }
    }
}
