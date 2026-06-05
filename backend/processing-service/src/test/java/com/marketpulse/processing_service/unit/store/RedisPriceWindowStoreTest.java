package com.marketpulse.processing_service.unit.store;

import com.marketpulse.processing_service.indicator.PriceWindow;
import com.marketpulse.processing_service.store.RedisPriceWindowStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RedisPriceWindowStoreTest {

    private static final Duration TTL = Duration.ofHours(24);

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOps;

    private RedisPriceWindowStore store;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOps);
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
    class Save {

        @Test
        void writesJsonUnderThePrefixedSymbolKeyWithTtl() {
            store.save("BTCUSDT", windowOf(100, 101, 102));

            verify(valueOps).set(eq("pricewindow:BTCUSDT"), anyString(), eq(TTL));
        }

        @Test
        void writesThePricesAsJson() {
            store.save("BTCUSDT", windowOf(100, 101, 102));

            org.mockito.ArgumentCaptor<String> json = org.mockito.ArgumentCaptor.forClass(String.class);
            verify(valueOps).set(anyString(), json.capture(), any(Duration.class));
            assertThat(json.getValue()).contains("100", "101", "102");
        }

        @Test
        void swallowsExceptionsSoTheTickPipelineKeepsRunning() {
            doThrow(new RuntimeException("redis down"))
                    .when(valueOps).set(anyString(), anyString(), any(Duration.class));

            assertThatCode(() -> store.save("BTCUSDT", windowOf(100)))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    class Load {

        @Test
        void deserializesStoredJsonBackIntoAWindow() {
            when(valueOps.get("pricewindow:BTCUSDT")).thenReturn("{\"prices\":[100,101,102]}");

            Optional<PriceWindow> loaded = store.load("BTCUSDT");

            assertThat(loaded).isPresent();
            assertThat(loaded.get().getPrices())
                    .containsExactly(new BigDecimal("100"), new BigDecimal("101"), new BigDecimal("102"));
        }

        @Test
        void returnsEmptyWhenTheKeyIsMissing() {
            when(valueOps.get("pricewindow:BTCUSDT")).thenReturn(null);

            assertThat(store.load("BTCUSDT")).isEmpty();
        }

        @Test
        void returnsEmptyWhenRedisThrows() {
            when(valueOps.get(anyString())).thenThrow(new RuntimeException("redis down"));

            assertThat(store.load("BTCUSDT")).isEmpty();
        }
    }
}
