package com.marketpulse.processing_service.unit.indicator;

import com.marketpulse.processing_service.indicator.PriceWindow;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class PriceWindowTest {

    private final PriceWindow window = new PriceWindow();

    private void addRange(int fromInclusive, int toInclusive) {
        int step = fromInclusive <= toInclusive ? 1 : -1;
        for (int i = fromInclusive; i != toInclusive + step; i += step) {
            window.add(BigDecimal.valueOf(i));
        }
    }

    @Nested
    class Sma {

        @Test
        void emptyWhenFewerPricesThanPeriod() {
            addRange(1, 19);
            assertThat(window.calculateSMA(20)).isEmpty();
        }

        @Test
        void averagesTheLastPeriodPrices() {
            addRange(1, 20);
            assertThat(window.calculateSMA(20)).hasValueSatisfying(
                    sma -> assertThat(sma).isEqualByComparingTo("10.5"));
            assertThat(window.calculateSMA(50)).isEmpty();
        }

        @Test
        void usesOnlyTheMostRecentPricesWhenWindowOverflows() {
            addRange(1, 60); // capacity is 50, so 1..10 are evicted; window holds 11..60
            assertThat(window.calculateSMA(50)).hasValueSatisfying(
                    sma -> assertThat(sma).isEqualByComparingTo("35.5"));
        }
    }

    @Nested
    class Readiness {

        @Test
        void notReadyBeforeReachingPeriod() {
            addRange(1, 19);
            assertThat(window.isReady(20)).isFalse();
        }

        @Test
        void readyOncePeriodReached() {
            addRange(1, 20);
            assertThat(window.isReady(20)).isTrue();
        }
    }

    @Nested
    class Rsi {

        @Test
        void emptyWhenFewerThan15Prices() {
            addRange(1, 14);
            assertThat(window.calculateRSI()).isEmpty();
        }

        @Test
        void hundredWhenOnlyGains() {
            addRange(1, 15);
            assertThat(window.calculateRSI()).contains(100.0);
        }

        @Test
        void zeroWhenOnlyLosses() {
            addRange(15, 1);
            assertThat(window.calculateRSI()).contains(0.0);
        }

        @Test
        void fiftyWhenGainsEqualLosses() {
            for (int i = 0; i < 15; i++) {
                window.add(BigDecimal.valueOf(i % 2 == 0 ? 100 : 101));
            }
            assertThat(window.calculateRSI()).contains(50.0);
        }
    }

    @Nested
    class ZScore {

        @Test
        void emptyWhileTheWindowIsStillWarmingUp() {
            addRange(1, 19); // fewer than the 20 samples required for a stable z-score
            assertThat(window.calculateZScore()).isEmpty();
        }

        @Test
        void emptyWhenAllPricesEqual() {
            for (int i = 0; i < 20; i++) {
                window.add(BigDecimal.valueOf(5));
            }
            assertThat(window.calculateZScore()).isEmpty();
        }

        @Test
        void measuresLatestPriceAgainstPopulationDistribution() {
            addRange(1, 20); // mean 10.5, population variance (20^2-1)/12 = 33.25, last price 20
            Optional<Double> z = window.calculateZScore();
            assertThat(z).isPresent();
            assertThat(z.get()).isCloseTo(9.5 / Math.sqrt(33.25), within(1e-9));
        }
    }

    @Nested
    class JsonSerialization {

        @Test
        void survivesRoundTripAndKeepsComputingIndicators() {
            addRange(1, 20);
            JsonMapper mapper = JsonMapper.builder().build();

            String json = mapper.writeValueAsString(window);
            PriceWindow restored = mapper.readValue(json, PriceWindow.class);

            Optional<BigDecimal> originalSma = window.calculateSMA(20);
            Optional<BigDecimal> restoredSma = restored.calculateSMA(20);
            assertThat(restoredSma).hasValueSatisfying(
                    sma -> assertThat(sma).isEqualByComparingTo(originalSma.orElseThrow()));
        }
    }
}
