package com.marketpulse.ingestion_service.binance;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "binance")
public record BinanceProperties(List<String> symbols) {
}
