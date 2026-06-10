package com.marketpulse.explanation.unit.llm.anthropic;

import com.marketpulse.explanation.llm.anthropic.AnthropicClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import static org.assertj.core.api.Assertions.assertThat;
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
        client = new AnthropicClient(builder, "test-key", "claude-sonnet-4-20250514");
    }

    @Test
    void prefillsOpeningBraceAndReconstructsJson() {
        server.expect(requestTo("https://api.anthropic.com/v1/messages"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(jsonPath("$.messages[1].role").value("assistant"))
                .andExpect(jsonPath("$.messages[1].content").value("{"))
                .andRespond(withSuccess("""
                        {"content":[{"type":"text","text":"\\"confidence\\":8}"}]}
                        """, MediaType.APPLICATION_JSON));

        assertThat(client.complete("prompt")).isEqualTo("{\"confidence\":8}");
        server.verify();
    }

    @Test
    void returnsEmptyStringWhenResponseHasNoContent() {
        server.expect(requestTo("https://api.anthropic.com/v1/messages"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess("{}", MediaType.APPLICATION_JSON));

        assertThat(client.complete("prompt")).isEmpty();
        server.verify();
    }
}
