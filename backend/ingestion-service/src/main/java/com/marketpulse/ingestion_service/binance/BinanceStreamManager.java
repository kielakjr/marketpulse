package com.marketpulse.ingestion_service.binance;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.util.retry.Retry;

import java.net.URI;
import java.time.Duration;

@Component
@Slf4j
@RequiredArgsConstructor
public class BinanceStreamManager {

    private static final String WS_URL = "wss://stream.binance.com:9443/ws/%s@trade";

    private final BinanceProperties binanceProperties;
    private final BinanceMessageHandler messageHandler;
    private final WebSocketClient webSocketClient = new ReactorNettyWebSocketClient();

    @EventListener(ApplicationReadyEvent.class)
    public void connectAll() {
        binanceProperties.symbols().forEach(symbol -> connectSymbol(symbol.toLowerCase()));
    }

    private void connectSymbol(String symbol) {
        webSocketClient.execute(
                        URI.create(WS_URL.formatted(symbol)),
                        session -> session.receive()
                                .map(WebSocketMessage::getPayloadAsText)
                                .doOnNext(messageHandler::handle)
                                .doOnError(e -> log.error("[{}] Stream error", symbol, e))
                                .then()
                )
                .doOnError(e -> log.error("[{}] Connection failed", symbol, e))
                .retryWhen(
                        Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                                .maxBackoff(Duration.ofSeconds(30))
                                .doBeforeRetry(signal -> log.info(
                                        "[{}] Reconnecting, attempt {}",
                                        symbol, signal.totalRetries() + 1
                                ))
                )
                .subscribe(
                        null,
                        e -> log.error("[{}] Fatal error, stream terminated", symbol, e)
                );
    }
}