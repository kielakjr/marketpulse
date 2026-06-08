package com.marketpulse.explanation.llm.anthropic;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.explanation.AnomalySource;
import com.marketpulse.explanation.llm.LlmClient;
import com.marketpulse.explanation.llm.LlmResponse;
import com.marketpulse.explanation.llm.PromptBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.List;

/**
 * Prod-profile {@link LlmClient} backed by the Anthropic Messages API with the
 * web search tool enabled. Sources are extracted from the citations attached to
 * the model's text blocks.
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
    private final PromptBuilder promptBuilder;

    public AnthropicClient(RestClient.Builder builder,
                           @Value("${anthropic.api-key}") String apiKey,
                           @Value("${anthropic.model}") String model,
                           PromptBuilder promptBuilder) {
        this.restClient = builder
                .baseUrl(MESSAGES_URL)
                .defaultHeader("x-api-key", apiKey)
                .defaultHeader("anthropic-version", ANTHROPIC_VERSION)
                .build();
        this.model = model;
        this.promptBuilder = promptBuilder;
    }

    @Override
    public LlmResponse explain(AnomalyAlert alert) {
        String prompt = promptBuilder.build(alert);
        var request = new MessageRequest(
                model,
                MAX_TOKENS,
                List.of(new Message("user", prompt)),
                List.of(new Tool("web_search_20250305", "web_search")));

        MessageResponse response = restClient.post()
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .body(MessageResponse.class);

        return toLlmResponse(response);
    }

    private LlmResponse toLlmResponse(MessageResponse response) {
        if (response == null || response.content() == null) {
            return new LlmResponse("", List.of());
        }

        StringBuilder explanation = new StringBuilder();
        List<AnomalySource> sources = new ArrayList<>();
        for (ContentBlock block : response.content()) {
            if ("text".equals(block.type()) && block.text() != null) {
                explanation.append(block.text());
            }
            if (block.citations() != null) {
                for (Citation citation : block.citations()) {
                    if (citation.url() != null) {
                        sources.add(new AnomalySource(
                                citation.title(), citation.url(), citation.citedText()));
                    }
                }
            }
        }
        return new LlmResponse(explanation.toString().strip(), sources);
    }

    private record MessageRequest(
            String model,
            @JsonProperty("max_tokens") int maxTokens,
            List<Message> messages,
            List<Tool> tools) {}

    private record Message(String role, String content) {}

    private record Tool(String type, String name) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record MessageResponse(List<ContentBlock> content) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ContentBlock(String type, String text, List<Citation> citations) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Citation(
            String title,
            String url,
            @JsonProperty("cited_text") String citedText) {}
}
