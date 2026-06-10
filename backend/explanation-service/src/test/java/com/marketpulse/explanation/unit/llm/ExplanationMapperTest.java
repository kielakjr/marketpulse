package com.marketpulse.explanation.unit.llm;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.explanation.AnomalyExplanation;
import com.marketpulse.common.explanation.SourcesQuality;
import com.marketpulse.explanation.llm.ExplanationMapper;
import com.marketpulse.explanation.llm.StructuredResponse;
import com.marketpulse.explanation.search.SearchResult;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ExplanationMapperTest {

    private static final Instant TS = Instant.ofEpochMilli(1780135331773L);
    private final ExplanationMapper mapper = new ExplanationMapper();

    private AnomalyAlert alert() {
        return new AnomalyAlert("BTCUSDT", new BigDecimal("72000"), 6.0,
                AlertSeverity.CRITICAL, null, null, null, TS);
    }

    private List<SearchResult> results() {
        return List.of(
                new SearchResult("t1", "https://e.com/1", "c1"),
                new SearchResult("t2", "https://e.com/2", "c2"),
                new SearchResult("t3", "https://e.com/3", "c3"));
    }

    @Test
    void belowThresholdYieldsFallbackAndClearsSources() {
        var sr = new StructuredResponse("powód", 4, "słabe źródła", List.of(1));

        AnomalyExplanation exp = mapper.toExplanation(alert(), sr, results());

        assertThat(exp.explanation()).isEqualTo("No clear market cause identified");
        assertThat(exp.sources()).isEmpty();
        assertThat(exp.sourcesQuality()).isEqualTo(SourcesQuality.LOW);
        assertThat(exp.confidence()).isEqualTo(4);
        assertThat(exp.confidenceReason()).isEqualTo("słabe źródła");
    }

    @Test
    void nullConfidenceYieldsFallback() {
        var sr = new StructuredResponse(null, null, null, List.of());
        assertThat(mapper.toExplanation(alert(), sr, results()).explanation())
                .isEqualTo("No clear market cause identified");
    }

    @Test
    void resolvesUsedSourceIndicesAndDropsInvalidOnes() {
        var sr = new StructuredResponse("powód", 8, "ok", List.of(1, 3, 9));

        AnomalyExplanation exp = mapper.toExplanation(alert(), sr, results());

        assertThat(exp.explanation()).isEqualTo("powód");
        assertThat(exp.sources()).extracting("url")
                .containsExactly("https://e.com/1", "https://e.com/3");
        assertThat(exp.sourcesQuality()).isEqualTo(SourcesQuality.MEDIUM);
        // alert fields are forwarded unchanged
        assertThat(exp.symbol()).isEqualTo("BTCUSDT");
        assertThat(exp.price()).isEqualByComparingTo("72000");
        assertThat(exp.severity()).isEqualTo(AlertSeverity.CRITICAL);
        assertThat(exp.timestamp()).isEqualTo(TS);
    }

    @Test
    void fallsBackToAllResultsWhenIndicesAreGarbage() {
        var sr = new StructuredResponse("powód", 8, "ok", List.of(42, 99));

        AnomalyExplanation exp = mapper.toExplanation(alert(), sr, results());

        assertThat(exp.sources()).hasSize(3);
        assertThat(exp.sourcesQuality()).isEqualTo(SourcesQuality.HIGH);
    }

    @Test
    void confidentButNoResultsIsLowQualityWithNoSources() {
        var sr = new StructuredResponse("powód", 8, "ok", List.of());

        AnomalyExplanation exp = mapper.toExplanation(alert(), sr, List.of());

        assertThat(exp.sources()).isEmpty();
        assertThat(exp.sourcesQuality()).isEqualTo(SourcesQuality.LOW);
    }
}
