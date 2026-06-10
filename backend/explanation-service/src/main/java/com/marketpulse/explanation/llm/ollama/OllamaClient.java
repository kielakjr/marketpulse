package com.marketpulse.explanation.llm.ollama;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.marketpulse.explanation.llm.LlmClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;

/**
 * Dev-profile {@link LlmClient} backed by a local Ollama instance with JSON mode
 * enabled so the model returns our structured contract.
 */
@Component
@Profile("dev")
@Slf4j
public class OllamaClient implements LlmClient {

    private final RestClient restClient;
    private final String model;

    public OllamaClient(RestClient.Builder builder,
                        @Value("${ollama.base-url}") String baseUrl,
                        @Value("${ollama.model}") String model) {
        this.restClient = builder.baseUrl(baseUrl).build();
        this.model = model;
    }

    @Override
    public String complete(String prompt) {
        var request = new ChatRequest(model, List.of(new Message("user", prompt)), false, "json");

        var response = restClient.post()
                .uri("/api/chat")
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .body(ChatResponse.class);

        return response != null && response.message() != null
                ? response.message().content().strip()
                : "";
    }

    private record ChatRequest(String model, List<Message> messages, boolean stream, String format) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Message(String role, String content) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ChatResponse(Message message) {}
}
