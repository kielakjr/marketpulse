package com.marketpulse.explanation.llm;

import com.marketpulse.common.explanation.AnomalySource;

import java.util.List;

public record LlmResponse(
        String explanation,
        List<AnomalySource> sources
) {}
