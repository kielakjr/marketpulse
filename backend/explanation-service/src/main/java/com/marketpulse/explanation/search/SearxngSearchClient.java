package com.marketpulse.explanation.search;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.marketpulse.common.alert.AnomalyAlert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * Queries a local SearxNG instance for news that might explain a price move.
 * Provider-agnostic: the same results feed both the dev and prod LLM clients.
 * Any failure degrades to an empty list so the pipeline never aborts on search.
 */
@Component
@Slf4j
public class SearxngSearchClient implements SearchClient {

    private static final int MAX_RESULTS = 5;

    private final RestClient restClient;

    public SearxngSearchClient(RestClient.Builder builder,
                               @Value("${searxng.base-url}") String baseUrl) {
        this.restClient = builder.baseUrl(baseUrl).build();
    }

    @Override
    public List<SearchResult> search(AnomalyAlert alert) {
        String direction = alert.zScore() < 0 ? "drop" : "surge";
        String query = "%s price %s site:coindesk.com OR site:cointelegraph.com %s".formatted(alert.symbol(), direction, LocalDate.now());
        try {
            SearxResponse response = restClient.get()
                    .uri(uri -> uri.path("/search")
                            .queryParam("q", query)
                            .queryParam("time_range", "day")
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
