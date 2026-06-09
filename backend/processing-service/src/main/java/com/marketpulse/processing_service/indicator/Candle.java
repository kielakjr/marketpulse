package com.marketpulse.processing_service.indicator;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A closed 1-minute candle. Close-only: indicators need only the close price,
 * {@code openTime} is the timestamp of the first tick that opened the minute.
 */
public record Candle(Instant openTime, BigDecimal close) {}
