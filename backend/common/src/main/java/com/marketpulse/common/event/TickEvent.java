package com.marketpulse.common.event;

import java.math.BigDecimal;
import java.time.Instant;

public record TickEvent(
        String symbol,
        BigDecimal price,
        BigDecimal quantity,
        long tradeId,
        Instant timestamp
) {}
