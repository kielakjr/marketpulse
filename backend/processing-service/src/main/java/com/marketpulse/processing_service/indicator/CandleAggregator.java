package com.marketpulse.processing_service.indicator;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

/**
 * Aggregates a single symbol's tick stream into 1-minute candles. Tick-driven:
 * a candle closes when a tick arrives in a later minute than the in-progress one,
 * using event time ({@code tick.timestamp}).
 */
public class CandleAggregator {

    private static final long SECONDS_PER_MINUTE = 60;

    private boolean candleOpen;
    private long currentMinute;
    private Instant currentOpenTime;
    private BigDecimal currentClose;

    /**
     * Records a tick. Returns the just-closed candle when this tick crosses into a
     * later minute, otherwise empty.
     */
    public Optional<Candle> add(Instant timestamp, BigDecimal price) {
        long minute = timestamp.getEpochSecond() / SECONDS_PER_MINUTE;
        if (!candleOpen) {
            open(minute, timestamp, price);
            return Optional.empty();
        }
        if (minute == currentMinute) {
            currentClose = price;
            return Optional.empty();
        }
        if (minute < currentMinute) {
            return Optional.empty(); // stale/out-of-order tick, ignore
        }
        var closed = new Candle(currentOpenTime, currentClose);
        open(minute, timestamp, price);
        return Optional.of(closed);
    }

    private void open(long minute, Instant timestamp, BigDecimal price) {
        candleOpen = true;
        currentMinute = minute;
        currentOpenTime = timestamp;
        currentClose = price;
    }
}
