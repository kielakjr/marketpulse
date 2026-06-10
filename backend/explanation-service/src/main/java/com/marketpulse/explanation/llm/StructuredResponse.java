package com.marketpulse.explanation.llm;

import java.util.List;

public record StructuredResponse(
        String explanation,
        Integer confidence,
        String confidenceReason,
        List<Integer> usedSources
) {}
