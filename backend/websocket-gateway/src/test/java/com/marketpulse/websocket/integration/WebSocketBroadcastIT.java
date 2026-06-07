package com.marketpulse.websocket.integration;

import com.marketpulse.common.alert.AlertSeverity;
import com.marketpulse.common.alert.AnomalyAlert;
import com.marketpulse.common.event.ProcessedEvent;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.converter.JacksonJsonMessageConverter;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class WebSocketBroadcastIT {

    private static final Instant TS = Instant.ofEpochMilli(1780135331773L);
    private static final long TIMEOUT_SECONDS = 10;

    @LocalServerPort
    private int port;

    @Autowired
    private KafkaConnectionDetails connectionDetails;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    private KafkaProducer<String, Object> producer;
    private WebSocketStompClient stompClient;
    private StompSession session;

    @BeforeEach
    void setUp() throws Exception {
        String bootstrapServers = String.join(",", connectionDetails.getBootstrapServers());

        createTopics(bootstrapServers, "market.processed", "market.alerts");
        producer = createProducer(bootstrapServers);
        awaitListenersAssigned();
        session = connectStompClient();
    }

    @AfterEach
    void tearDown() {
        if (session != null && session.isConnected()) {
            session.disconnect();
        }
        if (stompClient != null) {
            stompClient.stop();
        }
        if (producer != null) {
            producer.close();
        }
    }

    @Nested
    class WhenProcessedEventIsPublished {

        @Test
        void itIsBroadcastToTheSymbolPriceTopic() throws Exception {
            BlockingQueue<ProcessedEvent> received = subscribe("/topic/prices/BTCUSDT", ProcessedEvent.class);

            producer.send(new ProducerRecord<>("market.processed", "BTCUSDT", event("BTCUSDT")));

            ProcessedEvent broadcast = received.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(broadcast).isNotNull();
            assertThat(broadcast.symbol()).isEqualTo("BTCUSDT");
            assertThat(broadcast.price()).isEqualByComparingTo("73610.36");
        }
    }

    @Nested
    class WhenAlertIsPublished {

        @Test
        void itIsBroadcastToTheSymbolAlertTopic() throws Exception {
            BlockingQueue<AnomalyAlert> received = subscribe("/topic/alerts/BTCUSDT", AnomalyAlert.class);

            producer.send(new ProducerRecord<>("market.alerts", "BTCUSDT", alert("BTCUSDT")));

            AnomalyAlert broadcast = received.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(broadcast).isNotNull();
            assertThat(broadcast.symbol()).isEqualTo("BTCUSDT");
            assertThat(broadcast.severity()).isEqualTo(AlertSeverity.HIGH);
        }
    }

    private <T> BlockingQueue<T> subscribe(String destination, Class<T> payloadType) {
        BlockingQueue<T> received = new LinkedBlockingQueue<>();
        session.subscribe(destination, new StompFrameHandler() {
            @Override
            public @NonNull Type getPayloadType(@NonNull StompHeaders headers) {
                return payloadType;
            }

            @Override
            public void handleFrame(@NonNull StompHeaders headers, Object payload) {
                received.add(payloadType.cast(payload));
            }
        });
        return received;
    }

    private void createTopics(String bootstrapServers, String... topics) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            List<NewTopic> newTopics = Arrays.stream(topics)
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList();
            admin.createTopics(newTopics).all().get();
        } catch (ExecutionException e) {
            // The gateway's consumers auto-create these topics on startup, so a
            // concurrent/earlier creation is expected and harmless.
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
    }

    private KafkaProducer<String, Object> createProducer(String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaProducer<>(props, new StringSerializer(), new JacksonJsonSerializer<>());
    }

    private void awaitListenersAssigned() {
        for (MessageListenerContainer container : listenerRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 1);
        }
    }

    private StompSession connectStompClient() throws Exception {
        stompClient = new WebSocketStompClient(new SockJsClient(
                List.of(new WebSocketTransport(new StandardWebSocketClient()))));
        stompClient.setMessageConverter(new JacksonJsonMessageConverter());
        return stompClient
                .connectAsync("http://localhost:" + port + "/ws", new StompSessionHandlerAdapter() {})
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static ProcessedEvent event(String symbol) {
        return new ProcessedEvent(
                symbol,
                new BigDecimal("73610.36"),
                new BigDecimal("73000"),
                new BigDecimal("72000"),
                55.0,
                0.5,
                TS);
    }

    private static AnomalyAlert alert(String symbol) {
        return new AnomalyAlert(symbol, new BigDecimal("73610.36"), 4.5, AlertSeverity.HIGH, TS);
    }
}
