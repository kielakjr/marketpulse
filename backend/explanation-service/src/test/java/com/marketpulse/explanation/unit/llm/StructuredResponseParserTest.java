package com.marketpulse.explanation.unit.llm;

import com.marketpulse.explanation.llm.StructuredResponse;
import com.marketpulse.explanation.llm.StructuredResponseParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StructuredResponseParserTest {

    private final StructuredResponseParser parser = new StructuredResponseParser();

    @Test
    void parsesCleanJson() {
        StructuredResponse r = parser.parse("""
                {"explanation":"Wzrost po napływie do ETF.","confidence":8,
                 "confidence_reason":"spójne źródła","used_sources":[1,3]}
                """);

        assertThat(r.explanation()).isEqualTo("Wzrost po napływie do ETF.");
        assertThat(r.confidence()).isEqualTo(8);
        assertThat(r.confidenceReason()).isEqualTo("spójne źródła");
        assertThat(r.usedSources()).containsExactly(1, 3);
    }

    @Test
    void parsesJsonWrappedInCodeFenceAndProse() {
        StructuredResponse r = parser.parse("""
                Oto odpowiedź:
                ```json
                {"explanation":"x","confidence":3,"confidence_reason":"brak źródeł","used_sources":[]}
                ```
                """);

        assertThat(r.confidence()).isEqualTo(3);
        assertThat(r.usedSources()).isEmpty();
    }

    @Test
    void returnsNullConfidenceWhenUnparseable() {
        StructuredResponse r = parser.parse("the model rambled with no json");

        assertThat(r.confidence()).isNull();
        assertThat(r.explanation()).isNull();
    }
}
