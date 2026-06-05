package com.marketpulse.processing_service.integration.store;

import com.marketpulse.processing_service.indicator.PriceWindow;
import com.marketpulse.processing_service.store.RedisPriceWindowStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class RedisPriceWindowStoreIT {

    private static final Duration TTL = Duration.ofHours(24);

    @Container
    static final GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    private StringRedisTemplate redisTemplate;
    private RedisPriceWindowStore store;

    @BeforeEach
    void setUp() {
        LettuceConnectionFactory connectionFactory =
                new LettuceConnectionFactory(redis.getHost(), redis.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
        store = new RedisPriceWindowStore(redisTemplate, TTL);
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
            assertThat(ttlSeconds).isPositive().isLessThanOrEqualTo(TTL.toSeconds());
        }
    }
}
