package com.marketpulse.explanation.unit.llm.anthropic;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.explanation.AnomalySource;
import com.marketpulse.explanation.llm.anthropic.AnthropicClient;
import com.marketpulse.explanation.llm.LlmResponse;
import com.marketpulse.explanation.llm.PromptBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class AnthropicClientTest {

    private MockRestServiceServer server;
    private AnthropicClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        client = new AnthropicClient(builder, "test-key", "claude-sonnet-4-20250514", new PromptBuilder());
    }

    private AnomalyAlert alert() {
        return new AnomalyAlert("BTCUSDT", new BigDecimal("73610.36"), 6.2, AlertSeverity.CRITICAL,
                Instant.ofEpochMilli(1780135331773L));
    }

    @Test
    void joinsTextBlocksAndCollectsCitationsAsSources() {
        String body = """
                {"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-20250514",
                 "stop_reason":"end_turn",
                 "content":[
                   {"type":"text","text":"Cena spadła. ","citations":[
                     {"type":"web_search_result_location","url":"https://example.com/a",
                      "title":"Artykuł A","cited_text":"powód A"}]},
                   {"type":"web_search_tool_result","tool_use_id":"srvtoolu_1"},
                   {"type":"text","text":"Powodem był spadek."}
                 ]}
                """;

        server.expect(requestTo("https://api.anthropic.com/v1/messages"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(header("x-api-key", "test-key"))
                .andExpect(header("anthropic-version", "2023-06-01"))
                .andExpect(jsonPath("$.model").value("claude-sonnet-4-20250514"))
                .andExpect(jsonPath("$.max_tokens").value(1024))
                .andExpect(jsonPath("$.tools[0].type").value("web_search_20250305"))
                .andExpect(jsonPath("$.tools[0].name").value("web_search"))
                .andRespond(withSuccess(body, MediaType.APPLICATION_JSON));

        LlmResponse response = client.explain(alert());

        assertThat(response.explanation()).isEqualTo("Cena spadła. Powodem był spadek.");
        assertThat(response.sources()).hasSize(1);
        AnomalySource source = response.sources().getFirst();
        assertThat(source.title()).isEqualTo("Artykuł A");
        assertThat(source.url()).isEqualTo("https://example.com/a");
        assertThat(source.snippet()).isEqualTo("powód A");
        server.verify();
    }

    @Test
    void returnsAnEmptyResponseWhenThereIsNoContent() {
        server.expect(requestTo("https://api.anthropic.com/v1/messages"))
                .andRespond(withSuccess("{\"content\":[]}", MediaType.APPLICATION_JSON));

        LlmResponse response = client.explain(alert());

        assertThat(response.explanation()).isEmpty();
        assertThat(response.sources()).isEmpty();
        server.verify();
    }
}
