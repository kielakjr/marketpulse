package com.marketpulse.explanation.unit.search;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.explanation.search.SearchResult;
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
        client = new SearxngSearchClient(builder, "http://searxng:8080");
    }

    private AnomalyAlert alert() {
        return new AnomalyAlert("BTCUSDT", new BigDecimal("73610.36"), 6.2,
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
    void returnsEmptyListOnServerError() {
        server.expect(method(HttpMethod.GET)).andRespond(withServerError());

        assertThat(client.search(alert())).isEmpty();
        server.verify();
    }
}
