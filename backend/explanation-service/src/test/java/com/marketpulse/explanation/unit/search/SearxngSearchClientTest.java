package com.marketpulse.explanation.unit.search;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.explanation.search.SearchResult;
import com.marketpulse.explanation.search.SearxngProperties;
import com.marketpulse.explanation.search.SearxngSearchClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.queryParam;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class SearxngSearchClientTest {

    private MockRestServiceServer server;
    private SearxngSearchClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        var properties = new SearxngProperties("http://searxng:8080", Map.of("BTCUSDT", "Bitcoin"));
        client = new SearxngSearchClient(builder, properties);
    }

    private AnomalyAlert alert() {
        return alert("BTCUSDT", 6.2);
    }

    private AnomalyAlert alert(String symbol, double zScore) {
        return new AnomalyAlert(symbol, new BigDecimal("73610.36"), zScore,
                AlertSeverity.CRITICAL, null, null, null, Instant.ofEpochMilli(1780135331773L));
    }

    @Test
    void mapsResultsAndCapsAtFive() {
        server.expect(method(HttpMethod.GET))
                .andExpect(queryParam("format", "json"))
                .andRespond(withSuccess("""
                        {"results":[
                          {"title":"t1","url":"https://e.com/1","content":"c1"},
                          {"title":"t2","url":"https://e.com/2","content":"c2"},
                          {"title":"t3","url":"https://e.com/3","content":"c3"},
                          {"title":"t4","url":"https://e.com/4","content":"c4"},
                          {"title":"t5","url":"https://e.com/5","content":"c5"},
                          {"title":"t6","url":"https://e.com/6","content":"c6"}
                        ]}
                        """, MediaType.APPLICATION_JSON));

        List<SearchResult> results = client.search(alert());

        assertThat(results).hasSize(5);
        assertThat(results.getFirst()).isEqualTo(new SearchResult("t1", "https://e.com/1", "c1"));
        server.verify();
    }

    @Test
    void buildsQueryFromConfiguredAssetNameWithoutSiteFilterOrDate() {
        server.expect(method(HttpMethod.GET))
                .andExpect(request -> {
                    String query = request.getURI().getQuery();
                    assertThat(query).contains("q=Bitcoin price surge");
                    assertThat(query).doesNotContain("BTCUSDT");
                    assertThat(query).doesNotContain("site:");
                    assertThat(query).doesNotContain("2026");
                })
                .andRespond(withSuccess("{\"results\":[]}", MediaType.APPLICATION_JSON));

        client.search(alert());
        server.verify();
    }

    @Test
    void usesDropDirectionForNegativeZScore() {
        server.expect(method(HttpMethod.GET))
                .andExpect(request -> assertThat(request.getURI().getQuery()).contains("q=Bitcoin price drop"))
                .andRespond(withSuccess("{\"results\":[]}", MediaType.APPLICATION_JSON));

        client.search(alert("BTCUSDT", -6.2));
        server.verify();
    }

    @Test
    void fallsBackToStrippedSymbolWhenNotMapped() {
        server.expect(method(HttpMethod.GET))
                .andExpect(request -> {
                    String query = request.getURI().getQuery();
                    assertThat(query).contains("q=ADA price surge");
                    assertThat(query).doesNotContain("ADAUSDT");
                })
                .andRespond(withSuccess("{\"results\":[]}", MediaType.APPLICATION_JSON));

        client.search(alert("ADAUSDT", 6.2));
        server.verify();
    }

    @Test
    void returnsEmptyListOnServerError() {
        server.expect(method(HttpMethod.GET)).andRespond(withServerError());

        assertThat(client.search(alert())).isEmpty();
        server.verify();
    }
}
