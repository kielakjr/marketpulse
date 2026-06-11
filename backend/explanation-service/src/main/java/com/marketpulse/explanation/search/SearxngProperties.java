package com.marketpulse.explanation.search;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * SearxNG configuration. {@code symbolNames} maps a trading-pair symbol
 * (e.g. {@code BTCUSDT}) to the human asset name used in the news query
 * (e.g. {@code Bitcoin}).
 */
@ConfigurationProperties(prefix = "searxng")
public record SearxngProperties(String baseUrl, Map<String, String> symbolNames) {

    public SearxngProperties {
        symbolNames = symbolNames == null ? Map.of() : symbolNames;
    }
}
