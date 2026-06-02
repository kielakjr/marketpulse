package com.marketpulse.ingestion_service.unit.binance;

import com.marketpulse.ingestion_service.binance.BinanceMessageHandler;
import com.marketpulse.ingestion_service.binance.BinanceProperties;
import com.marketpulse.ingestion_service.binance.BinanceStreamManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BinanceStreamManagerTest {

    // Immediate, bounded retry so reconnect behaviour is exercised instantly and deterministically.
    private static final Retry IMMEDIATE_RETRY = Retry.max(Long.MAX_VALUE);

    @Mock
    private BinanceMessageHandler messageHandler;

    @Mock
    private WebSocketClient webSocketClient;

    private BinanceStreamManager managerFor(List<String> symbols) {
        return managerFor(symbols, IMMEDIATE_RETRY);
    }

    private BinanceStreamManager managerFor(List<String> symbols, Retry reconnectRetry) {
        return new BinanceStreamManager(new BinanceProperties(symbols), messageHandler, webSocketClient, reconnectRetry);
    }

    @Nested
    class WhenApplicationIsReady {

        @BeforeEach
        void stubClient() {
            when(webSocketClient.execute(any(URI.class), any())).thenReturn(Mono.empty());
        }

        @Test
        void connectsOncePerConfiguredSymbol() {
            managerFor(List.of("btcusdt", "ethusdt")).connectAll();

            verify(webSocketClient, times(2)).execute(any(URI.class), any());
        }

        @Test
        void connectsToTheTradeStreamForEachSymbol() {
            managerFor(List.of("btcusdt", "ethusdt")).connectAll();

            ArgumentCaptor<URI> uriCaptor = ArgumentCaptor.forClass(URI.class);
            verify(webSocketClient, times(2)).execute(uriCaptor.capture(), any());

            assertThat(uriCaptor.getAllValues())
                    .extracting(URI::toString)
                    .containsExactlyInAnyOrder(
                            "wss://stream.binance.com:9443/ws/btcusdt@trade",
                            "wss://stream.binance.com:9443/ws/ethusdt@trade"
                    );
        }

        @Test
        void lowercasesSymbolsBeforeBuildingTheStreamUrl() {
            managerFor(List.of("BTCUSDT")).connectAll();

            ArgumentCaptor<URI> uriCaptor = ArgumentCaptor.forClass(URI.class);
            verify(webSocketClient).execute(uriCaptor.capture(), any());

            assertThat(uriCaptor.getValue().toString())
                    .isEqualTo("wss://stream.binance.com:9443/ws/btcusdt@trade");
        }
    }

    @Nested
    class WhenNoSymbolsAreConfigured {

        @Test
        void doesNotOpenAnyConnection() {
            managerFor(List.of()).connectAll();

            verifyNoInteractions(webSocketClient);
        }
    }

    @Nested
    class WhenAConnectionFails {

        // execute() returns a cold Mono; the retry pipeline reconnects by re-subscribing to it.
        // Mono.defer() re-runs per subscription, so the first {failures} (re)connections error and
        // the next completes. The counter records how many times the stream was (re)connected.
        private AtomicInteger stubFailuresThenSuccess(int failures) {
            AtomicInteger connections = new AtomicInteger();
            when(webSocketClient.execute(any(URI.class), any()))
                    .thenReturn(Mono.defer(() -> connections.getAndIncrement() < failures
                            ? Mono.error(new RuntimeException("stream dropped"))
                            : Mono.empty()));
            return connections;
        }

        @Test
        void reconnectsAfterTheStreamDrops() {
            AtomicInteger connections = stubFailuresThenSuccess(1);

            managerFor(List.of("btcusdt")).connect("btcusdt").block();

            assertThat(connections.get())
                    .as("initial connection plus one reconnect")
                    .isEqualTo(2);
        }

        @Test
        void keepsRetryingThroughRepeatedFailuresUntilItConnects() {
            AtomicInteger connections = stubFailuresThenSuccess(3);

            managerFor(List.of("btcusdt")).connect("btcusdt").block();

            assertThat(connections.get())
                    .as("initial connection plus three reconnects")
                    .isEqualTo(4);
        }

        @Test
        void stopsRetryingOnceTheRetryBudgetIsExhausted() {
            AtomicInteger connections = stubFailuresThenSuccess(Integer.MAX_VALUE);

            Mono<Void> connection = managerFor(List.of("btcusdt"), Retry.max(2)).connect("btcusdt");

            assertThatThrownBy(connection::block).isInstanceOf(Exception.class);
            assertThat(connections.get())
                    .as("initial connection plus two retries before giving up")
                    .isEqualTo(3);
        }
    }
}
