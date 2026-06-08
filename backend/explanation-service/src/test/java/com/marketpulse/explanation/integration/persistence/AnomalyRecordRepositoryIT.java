package com.marketpulse.explanation.integration.persistence;

import com.marketpulse.explanation.integration.TestcontainersConfiguration;
import com.marketpulse.explanation.persistence.AnomalyRecord;
import com.marketpulse.explanation.persistence.AnomalyRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class AnomalyRecordRepositoryIT {

    private static final Instant BASE = Instant.parse("2026-06-08T10:00:00Z");

    @Autowired
    AnomalyRecordRepository repository;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
    }

    private AnomalyRecord record(String symbol, Instant timestamp) {
        return AnomalyRecord.builder()
                .symbol(symbol)
                .price(new BigDecimal("100"))
                .zScore(6.0)
                .severity("CRITICAL")
                .explanation("x")
                .sources(List.of())
                .timestamp(timestamp)
                .build();
    }

    @Nested
    class Recent {

        @Test
        void returnsAllSymbolsNewestFirst() {
            repository.save(record("BTCUSDT", BASE));
            repository.save(record("ETHUSDT", BASE.plus(2, ChronoUnit.MINUTES)));
            repository.save(record("BTCUSDT", BASE.plus(1, ChronoUnit.MINUTES)));

            List<AnomalyRecord> recent = repository.findTop50ByOrderByTimestampDesc();

            assertThat(recent).extracting(AnomalyRecord::getTimestamp)
                    .containsExactly(
                            BASE.plus(2, ChronoUnit.MINUTES),
                            BASE.plus(1, ChronoUnit.MINUTES),
                            BASE);
        }

        @Test
        void capsTheResultAtFifty() {
            IntStream.range(0, 55)
                    .forEach(i -> repository.save(record("BTCUSDT", BASE.plus(i, ChronoUnit.SECONDS))));

            assertThat(repository.findTop50ByOrderByTimestampDesc()).hasSize(50);
        }
    }

    @Nested
    class BySymbol {

        @Test
        void returnsOnlyTheRequestedSymbolNewestFirst() {
            repository.save(record("BTCUSDT", BASE));
            repository.save(record("ETHUSDT", BASE.plus(1, ChronoUnit.MINUTES)));
            repository.save(record("BTCUSDT", BASE.plus(2, ChronoUnit.MINUTES)));

            List<AnomalyRecord> result = repository.findTop20BySymbolOrderByTimestampDesc("BTCUSDT");

            assertThat(result).extracting(AnomalyRecord::getSymbol).containsOnly("BTCUSDT");
            assertThat(result).extracting(AnomalyRecord::getTimestamp)
                    .containsExactly(BASE.plus(2, ChronoUnit.MINUTES), BASE);
        }

        @Test
        void capsTheResultAtTwenty() {
            IntStream.range(0, 25)
                    .forEach(i -> repository.save(record("BTCUSDT", BASE.plus(i, ChronoUnit.SECONDS))));

            assertThat(repository.findTop20BySymbolOrderByTimestampDesc("BTCUSDT")).hasSize(20);
        }
    }
}
