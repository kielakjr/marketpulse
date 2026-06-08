package com.marketpulse.explanation.llm;

import com.marketpulse.common.alert.AnomalyAlert;
import org.springframework.stereotype.Component;

@Component
public class PromptBuilder {

    public String build(AnomalyAlert alert) {
        return """
                Anomalia cenowa wykryta dla %s.
                Cena: %s USD
                Z-score: %s (normalnie < 3, tu > 5 oznacza ekstremalne odchylenie)
                Czas: %s

                Wyjaśnij w 2-3 zdaniach co mogło spowodować ten gwałtowny ruch cenowy.
                Odpowiedz po polsku.
                """
                .formatted(alert.symbol(), alert.price(), alert.zScore(), alert.timestamp());
    }
}
