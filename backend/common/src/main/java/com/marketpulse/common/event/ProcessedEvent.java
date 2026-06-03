package com.marketpulse.common.event;

import java.math.BigDecimal;
import java.time.Instant;

public record ProcessedEvent(
        String symbol,
        BigDecimal price,
        BigDecimal sma20,
        BigDecimal sma50,
        Double rsi,
        Double zScore,
        Instant timestamp
) {}
