package com.marketpulse.explanation.unit.llm.ollama;

import com.marketpulse.explanation.llm.ollama.OllamaClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

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
        client = new OllamaClient(builder, "http://localhost:11434", "llama3.2");
    }

    @Test
    void returnsTheAssistantMessageContent() {
        server.expect(requestTo("http://localhost:11434/api/chat"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess(
                        "{\"message\":{\"role\":\"assistant\",\"content\":\"{\\\"confidence\\\":3}\"}}",
                        MediaType.APPLICATION_JSON));

        assertThat(client.complete("prompt")).isEqualTo("{\"confidence\":3}");
        server.verify();
    }

    @Test
    void requestsJsonModeAndCarriesThePrompt() {
        server.expect(requestTo("http://localhost:11434/api/chat"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(jsonPath("$.model").value("llama3.2"))
                .andExpect(jsonPath("$.format").value("json"))
                .andExpect(jsonPath("$.stream").value(false))
                .andExpect(jsonPath("$.messages[0].content").value(containsString("BTCUSDT")))
                .andRespond(withSuccess(
                        "{\"message\":{\"role\":\"assistant\",\"content\":\"{}\"}}",
                        MediaType.APPLICATION_JSON));

        client.complete("explain BTCUSDT");
        server.verify();
    }
}
