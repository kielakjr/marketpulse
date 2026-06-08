package com.marketpulse.explanation.unit.web;

import com.marketpulse.common.explanation.AnomalySource;
import com.marketpulse.explanation.persistence.AnomalyRecord;
import com.marketpulse.explanation.persistence.AnomalyRecordRepository;
import com.marketpulse.explanation.web.AnomalyHistoryController;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@WebMvcTest(AnomalyHistoryController.class)
class AnomalyHistoryControllerTest {

    private static final Instant TS = Instant.parse("2026-06-08T10:00:00Z");

    @Autowired
    private MockMvcTester mvc;

    @MockitoBean
    private AnomalyRecordRepository repository;

    private AnomalyRecord record(String symbol) {
        return AnomalyRecord.builder()
                .id("rec-1")
                .symbol(symbol)
                .price(new BigDecimal("73610.36"))
                .zScore(6.2)
                .severity("CRITICAL")
                .explanation("Nagły spadek.")
                .sources(List.of(new AnomalySource("Artykuł A", "https://example.com/a", "fragment")))
                .timestamp(TS)
                .build();
    }

    @Nested
    class Recent {

        @Test
        void returnsTheRepositoryRecordsAsAJsonArray() {
            when(repository.findTop50ByOrderByTimestampDesc()).thenReturn(List.of(record("BTCUSDT")));

            assertThat(mvc.get().uri("/api/anomalies"))
                    .hasStatusOk()
                    .bodyJson()
                    .extractingPath("$.length()").isEqualTo(1);
        }

        @Test
        void serializesEveryFieldOfARecord() {
            when(repository.findTop50ByOrderByTimestampDesc()).thenReturn(List.of(record("BTCUSDT")));

            var body = assertThat(mvc.get().uri("/api/anomalies")).hasStatusOk().bodyJson();
            body.extractingPath("$[0].id").isEqualTo("rec-1");
            body.extractingPath("$[0].symbol").isEqualTo("BTCUSDT");
            body.extractingPath("$[0].price").asNumber().isEqualTo(73610.36);
            body.extractingPath("$[0].zScore").asNumber().isEqualTo(6.2);
            body.extractingPath("$[0].severity").isEqualTo("CRITICAL");
            body.extractingPath("$[0].explanation").isEqualTo("Nagły spadek.");
            body.extractingPath("$[0].timestamp").isEqualTo("2026-06-08T10:00:00Z");
            body.extractingPath("$[0].sources[0].title").isEqualTo("Artykuł A");
            body.extractingPath("$[0].sources[0].url").isEqualTo("https://example.com/a");
            body.extractingPath("$[0].sources[0].snippet").isEqualTo("fragment");
        }

        @Test
        void returnsAnEmptyArrayWhenThereAreNoRecords() {
            when(repository.findTop50ByOrderByTimestampDesc()).thenReturn(List.of());

            assertThat(mvc.get().uri("/api/anomalies"))
                    .hasStatusOk()
                    .bodyJson()
                    .extractingPath("$.length()").isEqualTo(0);
        }
    }

    @Nested
    class BySymbol {

        @Test
        void passesThePathVariableThroughToTheRepository() {
            when(repository.findTop20BySymbolOrderByTimestampDesc("ETHUSDT")).thenReturn(List.of(record("ETHUSDT")));

            assertThat(mvc.get().uri("/api/anomalies/ETHUSDT"))
                    .hasStatusOk()
                    .bodyJson()
                    .extractingPath("$[0].symbol").isEqualTo("ETHUSDT");

            verify(repository).findTop20BySymbolOrderByTimestampDesc("ETHUSDT");
        }

        @Test
        void returnsAnEmptyArrayForAnUnknownSymbol() {
            when(repository.findTop20BySymbolOrderByTimestampDesc("DOGEUSDT")).thenReturn(List.of());

            assertThat(mvc.get().uri("/api/anomalies/DOGEUSDT"))
                    .hasStatusOk()
                    .bodyJson()
                    .extractingPath("$.length()").isEqualTo(0);
        }
    }
}
