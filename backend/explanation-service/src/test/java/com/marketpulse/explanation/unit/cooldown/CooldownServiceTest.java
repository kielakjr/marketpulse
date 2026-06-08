package com.marketpulse.explanation.unit.cooldown;

import com.marketpulse.explanation.cooldown.CooldownService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CooldownServiceTest {

    private static final String KEY = "explanation:cooldown:BTCUSDT";

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private CooldownService service;

    @BeforeEach
    void setUp() {
        service = new CooldownService(redisTemplate);
    }

    @Nested
    class TryAcquire {

        @BeforeEach
        void setUp() {
            when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        }

        @Test
        void trueWhenTheKeyWasClaimedWithATenMinuteTtl() {
            when(valueOperations.setIfAbsent(KEY, "1", Duration.ofMinutes(10))).thenReturn(true);
            assertThat(service.tryAcquire("BTCUSDT")).isTrue();
        }

        @Test
        void falseWhenTheKeyAlreadyExists() {
            when(valueOperations.setIfAbsent(KEY, "1", Duration.ofMinutes(10))).thenReturn(false);
            assertThat(service.tryAcquire("BTCUSDT")).isFalse();
        }

        @Test
        void falseWhenRedisReturnsNull() {
            when(valueOperations.setIfAbsent(any(), any(), any())).thenReturn(null);
            assertThat(service.tryAcquire("BTCUSDT")).isFalse();
        }
    }

    @Nested
    class Clear {

        @Test
        void deletesTheCooldownKey() {
            service.clear("BTCUSDT");

            verify(redisTemplate).delete(KEY);
        }
    }
}
