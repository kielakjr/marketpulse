package com.marketpulse.processing_service.store;

import com.marketpulse.processing_service.indicator.PriceWindow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.Optional;

/**
 * Write-through {@link PriceWindowStore} backed by Redis. Each window is stored
 * as JSON under {@code pricewindow:{symbol}} with a TTL that is refreshed on
 * every save, so active symbols never expire while idle ones are reclaimed.
 */
@Component
@Slf4j
public class RedisPriceWindowStore implements PriceWindowStore {

    private static final String KEY_PREFIX = "pricewindow:";

    private final StringRedisTemplate redisTemplate;
    private final Duration ttl;
    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    public RedisPriceWindowStore(StringRedisTemplate redisTemplate,
                                 @Value("${marketpulse.price-window.ttl}") Duration ttl) {
        this.redisTemplate = redisTemplate;
        this.ttl = ttl;
    }

    @Override
    public Optional<PriceWindow> load(String symbol) {
        try {
            String json = redisTemplate.opsForValue().get(key(symbol));
            if (json == null) {
                return Optional.empty();
            }
            return Optional.of(jsonMapper.readValue(json, PriceWindow.class));
        } catch (RuntimeException e) {
            log.warn("Failed to load price window for {} from Redis", symbol, e);
            return Optional.empty();
        }
    }

    @Override
    public void save(String symbol, PriceWindow window) {
        try {
            String json = jsonMapper.writeValueAsString(window);
            redisTemplate.opsForValue().set(key(symbol), json, ttl);
        } catch (RuntimeException e) {
            log.warn("Failed to save price window for {} to Redis", symbol, e);
        }
    }

    private static String key(String symbol) {
        return KEY_PREFIX + symbol;
    }
}
