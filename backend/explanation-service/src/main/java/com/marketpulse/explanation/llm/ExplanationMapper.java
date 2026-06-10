package com.marketpulse.explanation.llm;

import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.explanation.AnomalyExplanation;
import com.marketpulse.common.explanation.AnomalySource;
import com.marketpulse.common.explanation.SourcesQuality;
import com.marketpulse.explanation.search.SearchResult;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Turns a parsed {@link StructuredResponse} plus the search results into the
 * domain {@link AnomalyExplanation}. Low-confidence results are gated behind a
 * fixed message; sources_quality is derived from the sources actually used.
 */
@Component
public class ExplanationMapper {

    private static final int MIN_CONFIDENCE = 6;
    private static final String FALLBACK = "No clear market cause identified";

    public AnomalyExplanation toExplanation(AnomalyAlert alert,
                                            StructuredResponse response,
                                            List<SearchResult> results) {
        boolean gated = response.confidence() == null || response.confidence() < MIN_CONFIDENCE;
        if (gated) {
            return build(alert, FALLBACK, List.of(),
                    response.confidence(), response.confidenceReason(), SourcesQuality.LOW);
        }
        List<AnomalySource> sources = resolveSources(response.usedSources(), results);
        return build(alert, response.explanation(), sources,
                response.confidence(), response.confidenceReason(), qualityFor(sources.size()));
    }

    private List<AnomalySource> resolveSources(List<Integer> usedSources, List<SearchResult> results) {
        if (results == null || results.isEmpty()) {
            return List.of();
        }
        List<AnomalySource> resolved = new ArrayList<>();
        // usedSources is non-null by the parser's contract; null/out-of-range
        // elements are dropped by the guard below.
        for (Integer index : new LinkedHashSet<>(usedSources)) {
            if (index != null && index >= 1 && index <= results.size()) {
                SearchResult r = results.get(index - 1);
                resolved.add(new AnomalySource(r.title(), r.url(), r.snippet()));
            }
        }
        if (resolved.isEmpty()) {
            // Model returned no usable indices, but we have results — attach them all.
            return results.stream()
                    .map(r -> new AnomalySource(r.title(), r.url(), r.snippet()))
                    .toList();
        }
        return resolved;
    }

    private SourcesQuality qualityFor(int count) {
        if (count >= 3) {
            return SourcesQuality.HIGH;
        }
        if (count >= 1) {
            return SourcesQuality.MEDIUM;
        }
        return SourcesQuality.LOW;
    }

    private AnomalyExplanation build(AnomalyAlert alert, String explanation,
                                     List<AnomalySource> sources, Integer confidence,
                                     String confidenceReason, SourcesQuality quality) {
        return new AnomalyExplanation(
                alert.symbol(), alert.price(), alert.zScore(), alert.severity(),
                explanation, sources, confidence, confidenceReason, quality, alert.timestamp());
    }
}
