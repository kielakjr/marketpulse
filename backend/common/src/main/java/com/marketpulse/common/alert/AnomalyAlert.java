package com.marketpulse.common.alert;

import java.math.BigDecimal;
import java.time.Instant;

public record AnomalyAlert(
        String symbol,
        BigDecimal price,
        Double zScore,
        AlertSeverity severity,
        BigDecimal sma20,
        BigDecimal sma50,
        Double rsi,
        Instant timestamp
) {}
