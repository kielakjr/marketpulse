package com.marketpulse.explanation.llm.ollama;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.explanation.llm.LlmClient;
import com.marketpulse.explanation.llm.LlmResponse;
import com.marketpulse.explanation.llm.PromptBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;

/**
 * Dev-profile {@link LlmClient} backed by a local Ollama instance. Ollama has no
 * web search capability, so the returned sources are always empty.
 */
@Component
@Profile("dev")
@Slf4j
public class OllamaClient implements LlmClient {

    private final RestClient restClient;
    private final String model;
    private final PromptBuilder promptBuilder;

    public OllamaClient(RestClient.Builder builder,
                        @Value("${ollama.base-url}") String baseUrl,
                        @Value("${ollama.model}") String model,
                        PromptBuilder promptBuilder) {
        this.restClient = builder.baseUrl(baseUrl).build();
        this.model = model;
        this.promptBuilder = promptBuilder;
    }

    @Override
    public LlmResponse explain(AnomalyAlert alert) {
        String prompt = promptBuilder.build(alert);
        var request = new ChatRequest(model, List.of(new Message("user", prompt)), false);

        ChatResponse response = restClient.post()
                .uri("/api/chat")
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .body(ChatResponse.class);

        String explanation = response != null && response.message() != null
                ? response.message().content().strip()
                : "";
        return new LlmResponse(explanation, List.of());
    }

    private record ChatRequest(String model, List<Message> messages, boolean stream) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Message(String role, String content) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ChatResponse(Message message) {}
}
