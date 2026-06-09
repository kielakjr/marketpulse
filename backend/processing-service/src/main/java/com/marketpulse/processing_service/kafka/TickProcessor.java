package com.marketpulse.processing_service.kafka;

import com.marketpulse.common.event.ProcessedEvent;
import com.marketpulse.common.event.TickEvent;
import com.marketpulse.processing_service.indicator.Candle;
import com.marketpulse.processing_service.indicator.CandleAggregator;
import com.marketpulse.processing_service.indicator.PriceWindow;
import com.marketpulse.processing_service.store.PriceWindowStore;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class TickProcessor {

    private static final int SMA_SHORT_PERIOD = 20;
    private static final int SMA_LONG_PERIOD = 50;

    private final ConcurrentHashMap<String, CandleAggregator> aggregators = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PriceWindow> windows = new ConcurrentHashMap<>();
    private final ProcessedEventPublisher publisher;
    private final PriceWindowStore store;

    @KafkaListener(topics = "market.ticks", groupId = "processing-service")
    public void process(TickEvent tick) {
        String symbol = tick.symbol();
        var aggregator = aggregators.computeIfAbsent(symbol, key -> new CandleAggregator());
        var window = windows.computeIfAbsent(
                symbol, key -> store.load(key).orElseGet(PriceWindow::new));

        Optional<Candle> closed = aggregator.add(tick.timestamp(), tick.price());
        closed.ifPresent(candle -> {
            window.add(candle.close());
            store.save(symbol, window);
        });

        BigDecimal sma20 = window.calculateSMA(SMA_SHORT_PERIOD).orElse(null);
        BigDecimal sma50 = window.calculateSMA(SMA_LONG_PERIOD).orElse(null);
        Double rsi = window.calculateRSI().orElse(null);
        Double zScore = window.calculateZScore().orElse(null);

        publisher.publishProcessed(new ProcessedEvent(
                symbol, tick.price(), sma20, sma50, rsi, zScore, tick.timestamp()));

        closed.ifPresent(candle ->
                publisher.evaluateAnomaly(symbol, candle.close(), zScore, candle.openTime()));
    }

}
