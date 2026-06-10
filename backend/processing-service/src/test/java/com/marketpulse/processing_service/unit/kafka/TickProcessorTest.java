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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TickProcessorTest {

    private static final Instant MINUTE0 = Instant.parse("2026-06-08T12:00:00Z");

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

    /** Tick at the given whole-minute offset from MINUTE0 (plus 5s into the minute). */
    private TickEvent tick(String symbol, long price, long minute) {
        return new TickEvent(symbol, BigDecimal.valueOf(price), BigDecimal.ONE, price,
                MINUTE0.plusSeconds(minute * 60L + 5));
    }

    @Nested
    class LiveStream {

        @Test
        void publishesLiveProcessedEventForEveryTickCarryingTickPriceAndTimestamp() {
            TickEvent t = tick("BTCUSDT", 100, 0);
            processor.process(t);

            verify(publisher).publishProcessed(eventCaptor.capture());
            ProcessedEvent event = eventCaptor.getValue();
            assertThat(event.symbol()).isEqualTo("BTCUSDT");
            assertThat(event.price()).isEqualByComparingTo("100");
            assertThat(event.timestamp()).isEqualTo(t.timestamp());
        }

        @Test
        void leavesIndicatorsNullWhileAllTicksFallInTheSameMinute() {
            for (int i = 0; i < 30; i++) {
                processor.process(new TickEvent("BTCUSDT", BigDecimal.valueOf(100 + i),
                        BigDecimal.ONE, i, MINUTE0.plusSeconds(i))); // all within minute 0
            }

            verify(publisher, atLeastOnce()).publishProcessed(eventCaptor.capture());
            ProcessedEvent last = eventCaptor.getValue();
            assertThat(last.sma20()).isNull();
            assertThat(last.zScore()).isNull();
        }
    }

    @Nested
    class IndicatorsFromCandleCloses {

        @Test
        void computesSma20FromCandleClosesOnceTwentyCandlesHaveClosed() {
            // ticks at minutes 0..20; tick m closes candle m-1 with close = price at minute m-1.
            // prices at minutes 0..19 = 1..20 -> 20 closes [1..20], SMA20 = 10.5.
            for (long minute = 0; minute <= 20; minute++) {
                processor.process(tick("BTCUSDT", minute + 1, minute));
            }

            verify(publisher, atLeastOnce()).publishProcessed(eventCaptor.capture());
            assertThat(eventCaptor.getValue().sma20()).isEqualByComparingTo("10.5");
        }

        @Test
        void keepsIndependentStatePerSymbol() {
            for (long minute = 0; minute <= 20; minute++) {
                processor.process(tick("BTCUSDT", minute + 1, minute));
            }
            processor.process(tick("ETHUSDT", 5, 0));

            verify(publisher, atLeastOnce()).publishProcessed(eventCaptor.capture());
            ProcessedEvent eth = eventCaptor.getValue();
            assertThat(eth.symbol()).isEqualTo("ETHUSDT");
            assertThat(eth.sma20()).isNull();
        }
    }

    @Nested
    class AnomalyEvaluation {

        @Test
        void evaluatesAnomalyOnlyWhenACandleCloses() {
            // three ticks in the same minute -> no candle closes
            processor.process(tick("BTCUSDT", 100, 0));
            processor.process(new TickEvent("BTCUSDT", BigDecimal.valueOf(101),
                    BigDecimal.ONE, 1, MINUTE0.plusSeconds(20)));
            processor.process(new TickEvent("BTCUSDT", BigDecimal.valueOf(102),
                    BigDecimal.ONE, 2, MINUTE0.plusSeconds(40)));
            verify(publisher, never()).evaluateAnomaly(any(), any(), any(), any(), any(), any(), any());

            // a tick in the next minute closes one candle
            processor.process(tick("BTCUSDT", 103, 1));
            verify(publisher, times(1)).evaluateAnomaly(eq("BTCUSDT"), any(), any(), any(), any(), any(), any());
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

            // first tick opens a candle; tick in next minute closes it with close=20
            // -> window has [1..19, 20] = 20 closes, SMA20 = 10.5.
            processor.process(tick("BTCUSDT", 20, 0));
            processor.process(tick("BTCUSDT", 999, 1));

            verify(publisher, atLeastOnce()).publishProcessed(eventCaptor.capture());
            assertThat(eventCaptor.getValue().sma20()).isEqualByComparingTo("10.5");
        }

        @Test
        void loadsFromStoreOnlyOncePerSymbol() {
            processor.process(tick("BTCUSDT", 100, 0));
            processor.process(tick("BTCUSDT", 101, 1));
            processor.process(tick("BTCUSDT", 102, 2));

            verify(store, times(1)).load("BTCUSDT");
        }

        @Test
        void savesTheWindowOnlyWhenACandleCloses() {
            processor.process(tick("BTCUSDT", 100, 0));               // opens candle, no close
            verify(store, never()).save(any(), any());

            processor.process(tick("BTCUSDT", 101, 1));               // closes candle m0
            verify(store, times(1)).save(eq("BTCUSDT"), any(PriceWindow.class));
        }
    }
}
