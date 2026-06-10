package com.marketpulse.explanation.unit.kafka;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.explanation.AnomalyExplanation;
import com.marketpulse.common.explanation.AnomalySource;
import com.marketpulse.common.explanation.SourcesQuality;
import com.marketpulse.explanation.cooldown.CooldownService;
import com.marketpulse.explanation.kafka.ExplanationConsumer;
import com.marketpulse.explanation.llm.ExplanationMapper;
import com.marketpulse.explanation.llm.LlmClient;
import com.marketpulse.explanation.llm.PromptBuilder;
import com.marketpulse.explanation.llm.StructuredResponse;
import com.marketpulse.explanation.llm.StructuredResponseParser;
import com.marketpulse.explanation.persistence.AnomalyRecord;
import com.marketpulse.explanation.persistence.AnomalyRecordRepository;
import com.marketpulse.explanation.search.SearchClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExplanationConsumerTest {

    private static final Instant TS = Instant.ofEpochMilli(1780135331773L);
    private static final String EXPLANATIONS_TOPIC = "market.explanations";

    @Mock
    private CooldownService cooldownService;

    @Mock
    private SearchClient searchClient;

    @Mock
    private PromptBuilder promptBuilder;

    @Mock
    private LlmClient llmClient;

    @Mock
    private StructuredResponseParser parser;

    @Mock
    private ExplanationMapper mapper;

    @Mock
    private AnomalyRecordRepository repository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Captor
    private ArgumentCaptor<AnomalyRecord> recordCaptor;

    @Captor
    private ArgumentCaptor<Object> valueCaptor;

    private ExplanationConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new ExplanationConsumer(
                cooldownService, searchClient, promptBuilder, llmClient,
                parser, mapper, repository, kafkaTemplate);
    }

    private AnomalyAlert alert() {
        return new AnomalyAlert("BTCUSDT", new BigDecimal("73610.36"), 6.2, AlertSeverity.CRITICAL, null, null, null, TS);
    }

    private AnomalyExplanation explanation() {
        return new AnomalyExplanation("BTCUSDT", new BigDecimal("73610.36"), 6.2,
                AlertSeverity.CRITICAL, "Nagły spadek.",
                List.of(new AnomalySource("Artykuł A", "https://example.com/a", "fragment")),
                8, "spójne źródła", SourcesQuality.MEDIUM, TS);
    }

    private void stubHappyPath() {
        when(cooldownService.tryAcquire("BTCUSDT")).thenReturn(true);
        when(searchClient.search(any())).thenReturn(List.of());
        when(promptBuilder.build(any(), any())).thenReturn("prompt");
        when(llmClient.complete("prompt")).thenReturn("{}");
        when(parser.parse("{}")).thenReturn(new StructuredResponse("Nagły spadek.", 8, "spójne źródła", List.of()));
        when(mapper.toExplanation(any(), any(), any())).thenReturn(explanation());
        when(repository.save(any())).thenAnswer(invocation -> {
            AnomalyRecord record = invocation.getArgument(0);
            record.setId("rec-1");
            return record;
        });
    }

    @Nested
    class WhenOnCooldown {

        @Test
        void skipsTheLlmPersistenceAndPublishing() {
            when(cooldownService.tryAcquire("BTCUSDT")).thenReturn(false);

            consumer.onAnomaly(alert());

            verifyNoInteractions(searchClient, llmClient, repository, kafkaTemplate);
            verify(cooldownService, never()).clear(any());
        }
    }

    @Nested
    class WhenNotOnCooldown {

        @BeforeEach
        void setUp() {
            stubHappyPath();
        }

        @Test
        void callsTheLlmWithThePrompt() {
            consumer.onAnomaly(alert());
            verify(llmClient).complete("prompt");
        }

        @Test
        void persistsARecordCarryingTheAlertAndExplanationData() {
            consumer.onAnomaly(alert());

            verify(repository).save(recordCaptor.capture());
            AnomalyRecord record = recordCaptor.getValue();
            assertThat(record.getSymbol()).isEqualTo("BTCUSDT");
            assertThat(record.getPrice()).isEqualByComparingTo("73610.36");
            assertThat(record.getZScore()).isEqualTo(6.2);
            assertThat(record.getSeverity()).isEqualTo("CRITICAL");
            assertThat(record.getExplanation()).isEqualTo("Nagły spadek.");
            assertThat(record.getSources()).hasSize(1);
            assertThat(record.getTimestamp()).isEqualTo(TS);
            assertThat(record.getConfidence()).isEqualTo(8);
            assertThat(record.getSourcesQuality()).isEqualTo(SourcesQuality.MEDIUM);
        }

        @Test
        void publishesAnExplanationKeyedBySymbol() {
            consumer.onAnomaly(alert());

            verify(kafkaTemplate).send(eq(EXPLANATIONS_TOPIC), eq("BTCUSDT"), valueCaptor.capture());
            assertThat(valueCaptor.getValue()).isInstanceOf(AnomalyExplanation.class);
            var explanation = (AnomalyExplanation) valueCaptor.getValue();
            assertThat(explanation.symbol()).isEqualTo("BTCUSDT");
            assertThat(explanation.explanation()).isEqualTo("Nagły spadek.");
            assertThat(explanation.severity()).isEqualTo(AlertSeverity.CRITICAL);
            assertThat(explanation.sources()).hasSize(1);
            assertThat(explanation.timestamp()).isEqualTo(TS);
            assertThat(explanation.confidence()).isEqualTo(8);
            assertThat(explanation.sourcesQuality()).isEqualTo(SourcesQuality.MEDIUM);
        }

        @Test
        void runsTheCooldownSearchAndPromptBeforeCallingTheLlm() {
            consumer.onAnomaly(alert());

            InOrder inOrder = inOrder(cooldownService, searchClient, promptBuilder, llmClient);
            inOrder.verify(cooldownService).tryAcquire("BTCUSDT");
            inOrder.verify(searchClient).search(any());
            inOrder.verify(promptBuilder).build(any(), any());
            inOrder.verify(llmClient).complete(any());
        }

        @Test
        void keepsTheCooldownOnSuccess() {
            consumer.onAnomaly(alert());

            verify(cooldownService, never()).clear(any());
        }
    }

    @Nested
    class WhenTheLlmFails {

        @Test
        void swallowsTheExceptionDoesNotPersistOrPublishAndReleasesTheCooldown() {
            when(cooldownService.tryAcquire("BTCUSDT")).thenReturn(true);
            when(searchClient.search(any())).thenReturn(List.of());
            when(promptBuilder.build(any(), any())).thenReturn("prompt");
            when(llmClient.complete(any())).thenThrow(new RuntimeException("LLM unavailable"));

            assertThatCode(() -> consumer.onAnomaly(alert())).doesNotThrowAnyException();

            verify(repository, never()).save(any());
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
            verify(cooldownService).clear("BTCUSDT");
        }
    }
}
