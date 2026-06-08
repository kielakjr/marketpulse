package com.marketpulse.explanation.unit.llm.ollama;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.explanation.llm.LlmResponse;
import com.marketpulse.explanation.llm.ollama.OllamaClient;
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
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.jsonPath;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class OllamaClientTest {

    private MockRestServiceServer server;
    private OllamaClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        client = new OllamaClient(builder, "http://localhost:11434", "llama3.2", new PromptBuilder());
    }

    private AnomalyAlert alert() {
        return new AnomalyAlert("BTCUSDT", new BigDecimal("73610.36"), 6.2, AlertSeverity.CRITICAL,
                Instant.ofEpochMilli(1780135331773L));
    }

    @Test
    void parsesTheAssistantMessageContentAndReturnsNoSources() {
        server.expect(requestTo("http://localhost:11434/api/chat"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess("""
                        {"model":"llama3.2",
                         "message":{"role":"assistant","content":"Cena spadła nagle."},
                         "done":true}
                        """, MediaType.APPLICATION_JSON));

        LlmResponse response = client.explain(alert());

        assertThat(response.explanation()).isEqualTo("Cena spadła nagle.");
        assertThat(response.sources()).isEmpty();
        server.verify();
    }

    @Test
    void sendsANonStreamingChatRequestCarryingThePrompt() {
        server.expect(requestTo("http://localhost:11434/api/chat"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(jsonPath("$.model").value("llama3.2"))
                .andExpect(jsonPath("$.stream").value(false))
                .andExpect(jsonPath("$.messages[0].role").value("user"))
                .andExpect(jsonPath("$.messages[0].content").value(containsString("BTCUSDT")))
                .andRespond(withSuccess(
                        "{\"message\":{\"role\":\"assistant\",\"content\":\"ok\"}}",
                        MediaType.APPLICATION_JSON));

        client.explain(alert());

        server.verify();
    }
}
