package com.marketpulse.explanation.cooldown;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Per-symbol cooldown backed by Redis so that a burst of major anomalies for the
 * same symbol triggers at most one LLM explanation every {@link #TTL}.
 */
@Service
@RequiredArgsConstructor
public class CooldownService {

    private static final String KEY_PREFIX = "explanation:cooldown:";
    private static final Duration TTL = Duration.ofMinutes(10);

    private final StringRedisTemplate redisTemplate;

    public boolean isOnCooldown(String symbol) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(key(symbol)));
    }

    public void setCooldown(String symbol) {
        redisTemplate.opsForValue().set(key(symbol), "1", TTL);
    }

    private static String key(String symbol) {
        return KEY_PREFIX + symbol;
    }
}
