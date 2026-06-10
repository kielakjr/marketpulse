package com.marketpulse.explanation.unit.llm;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.explanation.llm.PromptBuilder;
import com.marketpulse.explanation.search.SearchResult;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PromptBuilderTest {

    private static final Instant TS = Instant.ofEpochMilli(1780135331773L);
    private final PromptBuilder builder = new PromptBuilder();

    private AnomalyAlert alert(double zScore, BigDecimal sma20, BigDecimal sma50, Double rsi) {
        return new AnomalyAlert("BTCUSDT", new BigDecimal("72000"), zScore,
                AlertSeverity.CRITICAL, sma20, sma50, rsi, TS);
    }

    @Test
    void describesAnUpwardSpikeWithDeviationsAndRsi() {
        String prompt = builder.build(
                alert(6.0, new BigDecimal("69000"), new BigDecimal("66000"), 80.0),
                List.of(new SearchResult("ETF inflows", "https://e.com/1", "big inflows")));

        assertThat(prompt).contains("BTCUSDT");
        assertThat(prompt).contains("WZROST");
        assertThat(prompt).contains("+4.35%");   // (72000-69000)/69000*100
        assertThat(prompt).contains("+9.09%");   // (72000-66000)/66000*100
        assertThat(prompt).contains("80.0");
        assertThat(prompt).contains("[1] ETF inflows");
        assertThat(prompt).contains("used_sources");
    }

    @Test
    void describesADownwardSpike() {
        String prompt = builder.build(
                alert(-6.0, new BigDecimal("69000"), new BigDecimal("66000"), 20.0), List.of());
        assertThat(prompt).contains("SPADEK");
    }

    @Test
    void rendersPlaceholdersWhenIndicatorsMissing() {
        String prompt = builder.build(alert(6.0, null, null, null), List.of());
        assertThat(prompt).contains("niedostępne");
        assertThat(prompt).contains("niedostępny");
        assertThat(prompt).contains("Brak wyników wyszukiwania");
    }

    @Test
    void evaluatesEachSmaDeviationIndependently() {
        String prompt = builder.build(
                alert(6.0, null, new BigDecimal("66000"), 80.0), List.of());

        assertThat(prompt).contains("Odchylenie od SMA20: niedostępne");
        assertThat(prompt).contains("Odchylenie od SMA50: +9.09%");
    }
}
