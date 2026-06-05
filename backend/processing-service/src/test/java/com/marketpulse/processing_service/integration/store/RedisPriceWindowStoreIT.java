package com.marketpulse.processing_service.integration.store;

import com.marketpulse.processing_service.indicator.PriceWindow;
import com.marketpulse.processing_service.integration.TestcontainersConfiguration;
import com.marketpulse.processing_service.store.RedisPriceWindowStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class RedisPriceWindowStoreIT {

    @Autowired
    RedisPriceWindowStore store;

    @Autowired
    StringRedisTemplate redisTemplate;

    @BeforeEach
    void setUp() {
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    private static PriceWindow windowOf(long... prices) {
        PriceWindow window = new PriceWindow();
        for (long price : prices) {
            window.add(BigDecimal.valueOf(price));
        }
        return window;
    }

    @Nested
    class RoundTrip {

        @Test
        void aSavedWindowCanBeLoadedBackWithTheSamePrices() {
            store.save("BTCUSDT", windowOf(100, 101, 102));

            Optional<PriceWindow> loaded = store.load("BTCUSDT");

            assertThat(loaded).isPresent();
            assertThat(loaded.get().getPrices())
                    .usingComparatorForType(BigDecimal::compareTo, BigDecimal.class)
                    .containsExactly(BigDecimal.valueOf(100), BigDecimal.valueOf(101), BigDecimal.valueOf(102));
        }

        @Test
        void loadReturnsEmptyForAnUnknownSymbol() {
            assertThat(store.load("UNKNOWN")).isEmpty();
        }
    }

    @Nested
    class Expiry {

        @Test
        void saveSetsATtlOnTheStoredKey() {
            store.save("BTCUSDT", windowOf(100));

            Long ttlSeconds = redisTemplate.getExpire("pricewindow:BTCUSDT");
            assertThat(ttlSeconds).isPositive().isLessThanOrEqualTo(Duration.ofHours(24).toSeconds());
        }
    }
}
