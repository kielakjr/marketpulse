package com.marketpulse.ingestion_service.binance;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.util.retry.Retry;

import java.time.Duration;

@Configuration
@Slf4j
public class BinanceConfig {

    @Bean
    WebSocketClient binanceWebSocketClient() {
        return new ReactorNettyWebSocketClient();
    }

    @Bean
    Retry binanceReconnectRetry() {
        return Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                .maxBackoff(Duration.ofSeconds(30))
                .doBeforeRetry(signal -> log.info(
                        "Reconnecting Binance stream, attempt {}",
                        signal.totalRetries() + 1
                ));
    }
}
