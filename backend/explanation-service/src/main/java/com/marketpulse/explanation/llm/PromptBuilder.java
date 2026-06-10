package com.marketpulse.explanation.llm;

import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.explanation.search.SearchResult;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

@Component
public class PromptBuilder {

    public String build(AnomalyAlert alert, List<SearchResult> results) {
        String direction = alert.zScore() < 0 ? "SPADEK" : "WZROST";
        return """
                Wykryto anomalię cenową dla %s.

                Dane rynkowe:
                - Cena: %s USD
                - Kierunek: gwałtowny %s
                - Z-score: %s (ekstremalne; normalnie < 3)
                - Odchylenie od SMA20: %s
                - Odchylenie od SMA50: %s
                - RSI: %s

                Wyniki wyszukiwania (mogą być puste lub nietrafione):
                %s

                Na podstawie powyższych danych i WYŁĄCZNIE z wyników wyszukiwania wyjaśnij
                krótko (2-3 zdania, po polsku) prawdopodobną przyczynę ruchu. Jeśli wyniki
                nie dają jasnej przyczyny — powiedz to wprost i ustaw niski confidence.

                Odpowiedz WYŁĄCZNIE poprawnym JSON-em:
                {
                  "explanation": "<2-3 zdania po polsku>",
                  "confidence": <1-10>,
                  "confidence_reason": "<uzasadnienie>",
                  "used_sources": [<indeksy użytych źródeł, np. 1, 3>]
                }
                """
                .formatted(
                        alert.symbol(),
                        alert.price().toPlainString(),
                        direction,
                        alert.zScore(),
                        deviation(alert.price(), alert.sma20()),
                        deviation(alert.price(), alert.sma50()),
                        alert.rsi() != null ? alert.rsi().toString() : "niedostępny",
                        renderResults(results));
    }

    private String deviation(BigDecimal price, BigDecimal sma) {
        if (price == null || sma == null || sma.signum() == 0) {
            return "niedostępne";
        }
        BigDecimal pct = price.subtract(sma)
                .divide(sma, 6, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100))
                .setScale(2, RoundingMode.HALF_UP);
        return (pct.signum() >= 0 ? "+" : "") + pct + "%";
    }

    private String renderResults(List<SearchResult> results) {
        if (results == null || results.isEmpty()) {
            return "Brak wyników wyszukiwania.";
        }
        var sb = new StringBuilder();
        for (int i = 0; i < results.size(); i++) {
            SearchResult r = results.get(i);
            sb.append("[").append(i + 1).append("] ")
                    .append(r.title()).append(" — ").append(r.snippet())
                    .append(" (").append(r.url()).append(")\n");
        }
        return sb.toString().strip();
    }
}
