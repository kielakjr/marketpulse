package com.marketpulse.explanation.integration.cooldown;

import com.marketpulse.explanation.cooldown.CooldownService;
import com.marketpulse.explanation.integration.TestcontainersConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class CooldownServiceIT {

    @Autowired
    CooldownService service;

    @Autowired
    StringRedisTemplate redisTemplate;

    @BeforeEach
    void setUp() {
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @Test
    void theFirstAcquireForASymbolSucceeds() {
        assertThat(service.tryAcquire("BTCUSDT")).isTrue();
    }

    @Test
    void aSecondAcquireWhileOnCooldownFails() {
        service.tryAcquire("BTCUSDT");

        assertThat(service.tryAcquire("BTCUSDT")).isFalse();
    }

    @Test
    void acquireAppliesATtlOfAtMostTenMinutes() {
        service.tryAcquire("BTCUSDT");

        Long ttlSeconds = redisTemplate.getExpire("explanation:cooldown:BTCUSDT");
        assertThat(ttlSeconds).isPositive().isLessThanOrEqualTo(Duration.ofMinutes(10).toSeconds());
    }

    @Test
    void clearReleasesTheCooldownSoTheSymbolCanBeAcquiredAgain() {
        service.tryAcquire("BTCUSDT");

        service.clear("BTCUSDT");

        assertThat(service.tryAcquire("BTCUSDT")).isTrue();
    }

    @Test
    void cooldownIsScopedPerSymbol() {
        service.tryAcquire("BTCUSDT");

        assertThat(service.tryAcquire("BTCUSDT")).isFalse();
        assertThat(service.tryAcquire("ETHUSDT")).isTrue();
    }
}
