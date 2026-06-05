package com.marketpulse.processing_service.unit.kafka;

import com.marketpulse.common.event.ProcessedEvent;
import com.marketpulse.common.event.TickEvent;
import com.marketpulse.processing_service.indicator.PriceWindow;
import com.marketpulse.processing_service.kafka.ProcessedEventPublisher;
import com.marketpulse.processing_service.kafka.TickProcessor;
import com.marketpulse.processing_service.store.PriceWindowStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TickProcessorTest {

    private static final Instant TS = Instant.ofEpochMilli(1780135331773L);

    @Mock
    private ProcessedEventPublisher publisher;

    @Mock
    private PriceWindowStore store;

    @Captor
    private ArgumentCaptor<ProcessedEvent> eventCaptor;

    private TickProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new TickProcessor(publisher, store);
    }

    private TickEvent tick(String symbol, long price) {
        return new TickEvent(symbol, BigDecimal.valueOf(price), BigDecimal.ONE, price, TS);
    }

    @Nested
    class SingleTick {

        @Test
        void publishesProcessedEventCarryingTheTickPriceAndTimestamp() {
            processor.process(tick("BTCUSDT", 100));

            verify(publisher).publish(eventCaptor.capture());
            ProcessedEvent event = eventCaptor.getValue();
            assertThat(event.symbol()).isEqualTo("BTCUSDT");
            assertThat(event.price()).isEqualByComparingTo("100");
            assertThat(event.timestamp()).isEqualTo(TS);
        }

        @Test
        void leavesIndicatorsNullWhenThereIsNotEnoughHistory() {
            processor.process(tick("BTCUSDT", 100));

            verify(publisher).publish(eventCaptor.capture());
            ProcessedEvent event = eventCaptor.getValue();
            assertThat(event.sma20()).isNull();
            assertThat(event.sma50()).isNull();
            assertThat(event.rsi()).isNull();
            assertThat(event.zScore()).isNull();
        }
    }

    @Nested
    class AccumulatingHistory {

        @Test
        void computesSma20OnceTheSymbolHasTwentyTicks() {
            for (int price = 1; price <= 20; price++) {
                processor.process(tick("BTCUSDT", price));
            }

            verify(publisher, org.mockito.Mockito.times(20)).publish(eventCaptor.capture());
            ProcessedEvent last = eventCaptor.getValue();
            assertThat(last.sma20()).isEqualByComparingTo("10.5");
        }

        @Test
        void keepsAnIndependentWindowPerSymbol() {
            for (int price = 1; price <= 20; price++) {
                processor.process(tick("BTCUSDT", price));
            }
            processor.process(tick("ETHUSDT", 5));

            verify(publisher, org.mockito.Mockito.atLeastOnce()).publish(eventCaptor.capture());
            ProcessedEvent ethEvent = eventCaptor.getValue();
            assertThat(ethEvent.symbol()).isEqualTo("ETHUSDT");
            assertThat(ethEvent.sma20()).isNull();
        }
    }

    @Nested
    class Persistence {

        @Test
        void seedsTheWindowFromTheStoreOnFirstSightOfASymbol() {
            PriceWindow stored = new PriceWindow();
            for (int price = 1; price <= 19; price++) {
                stored.add(BigDecimal.valueOf(price));
            }
            when(store.load("BTCUSDT")).thenReturn(Optional.of(stored));

            processor.process(tick("BTCUSDT", 20));

            verify(publisher).publish(eventCaptor.capture());
            assertThat(eventCaptor.getValue().sma20()).isEqualByComparingTo("10.5");
        }

        @Test
        void loadsFromTheStoreOnlyOncePerSymbol() {
            processor.process(tick("BTCUSDT", 100));
            processor.process(tick("BTCUSDT", 101));
            processor.process(tick("BTCUSDT", 102));

            verify(store, times(1)).load("BTCUSDT");
        }

        @Test
        void savesTheWindowAfterEveryTick() {
            processor.process(tick("BTCUSDT", 100));
            processor.process(tick("BTCUSDT", 101));

            verify(store, times(2)).save(eq("BTCUSDT"), any(PriceWindow.class));
        }

        @Test
        void loadsEachSymbolOnceEvenWhenSymbolsInterleave() {
            processor.process(tick("BTCUSDT", 100));
            processor.process(tick("ETHUSDT", 200));
            processor.process(tick("BTCUSDT", 101));

            verify(store, times(1)).load("BTCUSDT");
            verify(store, times(1)).load("ETHUSDT");
        }
    }
}
