package com.marketpulse.common.explanation;

import com.marketpulse.common.alert.AlertSeverity;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record AnomalyExplanation(
        String symbol,
        BigDecimal price,
        Double zScore,
        AlertSeverity severity,
        String explanation,
        List<AnomalySource> sources,
        Instant timestamp
) {}
