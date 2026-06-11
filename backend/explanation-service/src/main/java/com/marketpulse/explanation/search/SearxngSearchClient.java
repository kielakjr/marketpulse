package com.marketpulse.explanation.search;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.marketpulse.common.alert.AnomalyAlert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

/**
 * Queries a local SearxNG instance for news that might explain a price move.
 * Provider-agnostic: the same results feed both the dev and prod LLM clients.
 * Any failure degrades to an empty list so the pipeline never aborts on search.
 */
@Component
@Slf4j
public class SearxngSearchClient implements SearchClient {

    private static final int MAX_RESULTS = 5;
    private static final List<String> QUOTE_CURRENCIES = List.of("USDT", "USDC", "BUSD", "USD", "EUR");

    private final RestClient restClient;
    private final Map<String, String> symbolNames;

    public SearxngSearchClient(RestClient.Builder builder, SearxngProperties properties) {
        this.restClient = builder.baseUrl(properties.baseUrl()).build();
        this.symbolNames = properties.symbolNames();
    }

    @Override
    public List<SearchResult> search(AnomalyAlert alert) {
        String direction = alert.zScore() < 0 ? "drop" : "surge";
        String query = "%s price %s".formatted(assetName(alert.symbol()), direction);
        try {
            SearxResponse response = restClient.get()
                    .uri(uri -> uri.path("/search")
                            .queryParam("q", query)
                            .queryParam("time_range", "week")
                            .queryParam("categories", "news")
                            .queryParam("format", "json")
                            .build())
                    .retrieve()
                    .body(SearxResponse.class);

            if (response == null || response.results() == null) {
                return List.of();
            }
            return response.results().stream()
                    .filter(r -> isRecent(r.publishedDate()))
                    .limit(MAX_RESULTS)
                    .map(r -> new SearchResult(r.title(), r.url(), r.content()))
                    .toList();
        } catch (Exception e) {
            log.warn("[{}] SearxNG search failed, continuing without sources: {}",
                    alert.symbol(), e.getMessage());
            return List.of();
        }
    }

    /**
     * Resolves the asset name to search for. Prefers the configured mapping
     * (e.g. {@code BTCUSDT -> Bitcoin}); falls back to stripping the quote
     * currency from the pair (e.g. {@code ADAUSDT -> ADA}).
     */
    private String assetName(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return "";
        }
        String mapped = symbolNames.get(symbol);
        if (mapped == null) {
            mapped = symbolNames.get(symbol.toUpperCase());
        }
        if (mapped != null) {
            return mapped;
        }
        String upper = symbol.toUpperCase();
        for (String quote : QUOTE_CURRENCIES) {
            if (upper.length() > quote.length() && upper.endsWith(quote)) {
                return symbol.substring(0, symbol.length() - quote.length());
            }
        }
        return symbol;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record SearxResponse(List<SearxResult> results) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record SearxResult(String title, String url, String content, String publishedDate) {}

    private boolean isRecent(String publishedDate) {
        if (publishedDate == null || publishedDate.isBlank()) {
            return true;
        }
        try {
            var published = ZonedDateTime.parse(publishedDate);
            return published.isAfter(ZonedDateTime.now().minusHours(24));
        } catch (DateTimeParseException e) {
            log.warn("Could not parse publishedDate: {}", publishedDate);
            return true;
        }
    }
}
