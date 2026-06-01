package com.marketpulse.ingestion_service.binance;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record BinanceTradeMessage(
        @JsonProperty("s") String symbol,
        @JsonProperty("p") String price,
        @JsonProperty("q") String quantity,
        @JsonProperty("t") long tradeId,
        @JsonProperty("T") long tradeTime
) {}
