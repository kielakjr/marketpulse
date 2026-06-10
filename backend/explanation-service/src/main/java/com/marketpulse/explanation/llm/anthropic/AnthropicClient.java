package com.marketpulse.explanation.llm.anthropic;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.marketpulse.explanation.llm.LlmClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;

/**
 * Prod-profile {@link LlmClient} backed by the Anthropic Messages API. The
 * assistant turn is prefilled with "{" so the reply is forced to start our JSON
 * object. Web search is no longer used here — sources come from SearxNG.
 */
@Component
@Profile("prod")
@Slf4j
public class AnthropicClient implements LlmClient {

    private static final String MESSAGES_URL = "https://api.anthropic.com/v1/messages";
    private static final String ANTHROPIC_VERSION = "2023-06-01";
    private static final int MAX_TOKENS = 1024;

    private final RestClient restClient;
    private final String model;

    public AnthropicClient(RestClient.Builder builder,
                           @Value("${anthropic.api-key}") String apiKey,
                           @Value("${anthropic.model}") String model) {
        this.restClient = builder
                .baseUrl(MESSAGES_URL)
                .defaultHeader("x-api-key", apiKey)
                .defaultHeader("anthropic-version", ANTHROPIC_VERSION)
                .build();
        this.model = model;
    }

    @Override
    public String complete(String prompt) {
        var request = new MessageRequest(model, MAX_TOKENS, List.of(
                new Message("user", prompt),
                new Message("assistant", "{")));

        MessageResponse response = restClient.post()
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .body(MessageResponse.class);

        if (response == null || response.content() == null) {
            return "";
        }
        // The API continues from the prefilled "{" without echoing it, so we
        // re-seed it here and append the model's text blocks to rebuild the JSON.
        StringBuilder text = new StringBuilder("{");
        for (ContentBlock block : response.content()) {
            if ("text".equals(block.type()) && block.text() != null) {
                text.append(block.text());
            }
        }
        return text.toString();
    }

    private record MessageRequest(
            String model,
            @JsonProperty("max_tokens") int maxTokens,
            List<Message> messages) {}

    private record Message(String role, String content) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record MessageResponse(List<ContentBlock> content) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ContentBlock(String type, String text) {}
}
