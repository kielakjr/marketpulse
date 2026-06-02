package com.marketpulse.ingestion_service.unit.binance;

import com.marketpulse.common.event.TickEvent;
import com.marketpulse.ingestion_service.binance.BinanceMessageHandler;
import com.marketpulse.ingestion_service.kafka.TickPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class BinanceMessageHandlerTest {

    @Mock
    private TickPublisher tickPublisher;

    private BinanceMessageHandler handler;

    @BeforeEach
    void setUp() {
        handler = new BinanceMessageHandler(JsonMapper.builder().build(), tickPublisher);
    }

    @Nested
    class WhenMessageIsValid {

        private static final String TRADE_JSON = """
                {"e":"trade","E":1780135331774,"s":"BTCUSDT","t":6335284352,\
                "p":"73610.36000000","q":"0.00007000","T":1780135331773,"m":true,"M":true}""";

        private TickEvent capturedTick() {
            handler.handle(TRADE_JSON);
            ArgumentCaptor<TickEvent> captor = ArgumentCaptor.forClass(TickEvent.class);
            verify(tickPublisher).publish(captor.capture());
            return captor.getValue();
        }

        @Test
        void parsesSymbol() {
            assertThat(capturedTick().symbol()).isEqualTo("BTCUSDT");
        }

        @Test
        void parsesPrice() {
            assertThat(capturedTick().price()).isEqualByComparingTo(new BigDecimal("73610.36000000"));
        }

        @Test
        void parsesQuantity() {
            assertThat(capturedTick().quantity()).isEqualByComparingTo(new BigDecimal("0.00007000"));
        }

        @Test
        void parsesTradeId() {
            assertThat(capturedTick().tradeId()).isEqualTo(6335284352L);
        }

        @Test
        void parsesTimestamp() {
            assertThat(capturedTick().timestamp()).isEqualTo(Instant.ofEpochMilli(1780135331773L));
        }
    }

    @Nested
    class WhenMessageIsMalformed {

        @Test
        void doesNotPublishOnInvalidJson() {
            handler.handle("not valid json");
            verifyNoInteractions(tickPublisher);
        }

        @Test
        void doesNotPublishOnEmptyInput() {
            handler.handle("");
            verifyNoInteractions(tickPublisher);
        }

        @Test
        void doesNotThrowOnInvalidJson() {
            assertThatNoException().isThrownBy(() -> handler.handle("not valid json"));
        }
    }
}
