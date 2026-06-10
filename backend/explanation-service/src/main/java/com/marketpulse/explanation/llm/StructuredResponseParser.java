package com.marketpulse.explanation.llm;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Extracts our JSON contract from raw model text. Tolerant of code fences and
 * surrounding prose: it reads from the first '{' to the last '}'. On any failure
 * it returns a response with null confidence, which the mapper treats as "no
 * clear cause".
 */
@Component
@Slf4j
public class StructuredResponseParser {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public StructuredResponse parse(String raw) {
        if (raw == null) {
            return empty();
        }
        int start = raw.indexOf('{');
        int end = raw.lastIndexOf('}');
        if (start < 0 || end <= start) {
            return empty();
        }
        try {
            Dto dto = objectMapper.readValue(raw.substring(start, end + 1), Dto.class);
            return new StructuredResponse(
                    dto.explanation(),
                    dto.confidence(),
                    dto.confidenceReason(),
                    dto.usedSources() != null ? dto.usedSources() : List.of());
        } catch (Exception e) {
            log.warn("Could not parse structured LLM response: {}", e.getMessage());
            return empty();
        }
    }

    private StructuredResponse empty() {
        return new StructuredResponse(null, null, null, List.of());
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Dto(
            String explanation,
            Integer confidence,
            @JsonProperty("confidence_reason") String confidenceReason,
            @JsonProperty("used_sources") List<Integer> usedSources) {}
}
