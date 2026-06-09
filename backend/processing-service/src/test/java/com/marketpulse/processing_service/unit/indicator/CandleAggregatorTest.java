package com.marketpulse.processing_service.unit.indicator;

import com.marketpulse.processing_service.indicator.Candle;
import com.marketpulse.processing_service.indicator.CandleAggregator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class CandleAggregatorTest {

    private final CandleAggregator aggregator = new CandleAggregator();

    private static Instant at(String iso) {
        return Instant.parse(iso);
    }

    @Test
    void firstTickOpensCandleWithoutClosingOne() {
        Optional<Candle> closed = aggregator.add(at("2026-06-08T12:00:05Z"), BigDecimal.valueOf(100));
        assertThat(closed).isEmpty();
    }

    @Test
    void ticksWithinSameMinuteDoNotCloseACandle() {
        aggregator.add(at("2026-06-08T12:00:05Z"), BigDecimal.valueOf(100));
        Optional<Candle> closed = aggregator.add(at("2026-06-08T12:00:45Z"), BigDecimal.valueOf(110));
        assertThat(closed).isEmpty();
    }

    @Test
    void crossingTheMinuteBoundaryClosesTheCandleWithTheLastPriceAsClose() {
        Instant open = at("2026-06-08T12:00:05Z");
        aggregator.add(open, BigDecimal.valueOf(100));
        aggregator.add(at("2026-06-08T12:00:45Z"), BigDecimal.valueOf(110));

        Optional<Candle> closed = aggregator.add(at("2026-06-08T12:01:02Z"), BigDecimal.valueOf(120));

        assertThat(closed).isPresent();
        assertThat(closed.get().openTime()).isEqualTo(open);
        assertThat(closed.get().close()).isEqualByComparingTo("110");
    }

    @Test
    void aMultiMinuteJumpClosesExactlyTheInProgressCandle() {
        Instant open = at("2026-06-08T12:00:05Z");
        aggregator.add(open, BigDecimal.valueOf(100));

        Optional<Candle> closed = aggregator.add(at("2026-06-08T12:05:00Z"), BigDecimal.valueOf(130));

        assertThat(closed).isPresent();
        assertThat(closed.get().openTime()).isEqualTo(open);
        assertThat(closed.get().close()).isEqualByComparingTo("100");
    }

    @Test
    void opensAndClosesSuccessiveCandlesAcrossThreeMinutes() {
        Instant openA = at("2026-06-08T12:00:05Z");
        aggregator.add(openA, BigDecimal.valueOf(100));        // opens candle A
        Instant openB = at("2026-06-08T12:01:05Z");
        Optional<Candle> closedA = aggregator.add(openB, BigDecimal.valueOf(110)); // closes A, opens B
        Optional<Candle> closedB = aggregator.add(at("2026-06-08T12:02:05Z"), BigDecimal.valueOf(120)); // closes B, opens C

        assertThat(closedA).isPresent();
        assertThat(closedA.get().openTime()).isEqualTo(openA);
        assertThat(closedA.get().close()).isEqualByComparingTo("100");

        assertThat(closedB).isPresent();
        assertThat(closedB.get().openTime()).isEqualTo(openB);
        assertThat(closedB.get().close()).isEqualByComparingTo("110");
    }

    @Test
    void staleOutOfOrderTickIsIgnored() {
        aggregator.add(at("2026-06-08T12:01:05Z"), BigDecimal.valueOf(100));
        Optional<Candle> closed = aggregator.add(at("2026-06-08T12:00:05Z"), BigDecimal.valueOf(90));
        assertThat(closed).isEmpty();
    }
}
