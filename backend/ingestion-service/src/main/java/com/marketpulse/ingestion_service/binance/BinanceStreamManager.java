package com.marketpulse.ingestion_service.binance;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.net.URI;

@Component
@Slf4j
@RequiredArgsConstructor
public class BinanceStreamManager {

    private static final String WS_URL = "wss://stream.binance.com:9443/ws/%s@trade";

    private final BinanceProperties binanceProperties;
    private final BinanceMessageHandler messageHandler;
    private final WebSocketClient webSocketClient;
    private final Retry reconnectRetry;

    @EventListener(ApplicationReadyEvent.class)
    public void connectAll() {
        binanceProperties.symbols().forEach(symbol -> {
            String stream = symbol.toLowerCase();
            connect(stream).subscribe(
                    null,
                    e -> log.error("[{}] Fatal error, stream terminated", stream, e)
            );
        });
    }

    public Mono<Void> connect(String symbol) {
        return webSocketClient.execute(
                        URI.create(WS_URL.formatted(symbol)),
                        session -> session.receive()
                                .map(WebSocketMessage::getPayloadAsText)
                                .doOnNext(messageHandler::handle)
                                .doOnError(e -> log.error("[{}] Stream error", symbol, e))
                                .then()
                )
                .doOnError(e -> log.error("[{}] Connection failed", symbol, e))
                .retryWhen(reconnectRetry);
    }
}