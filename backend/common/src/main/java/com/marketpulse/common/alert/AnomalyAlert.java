package com.marketpulse.common.alert;

import java.math.BigDecimal;
import java.time.Instant;

public record AnomalyAlert(
        String symbol,
        BigDecimal price,
        Double zScore,
        AlertSeverity severity,
        Instant timestamp
) {}
