package com.marketpulse.processing_service.kafka;

import com.marketpulse.common.event.ProcessedEvent;
import com.marketpulse.common.event.TickEvent;
import com.marketpulse.processing_service.indicator.Candle;
import com.marketpulse.processing_service.indicator.CandleAggregator;
import com.marketpulse.processing_service.indicator.PriceWindow;
import com.marketpulse.processing_service.store.PriceWindowStore;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Component
@RequiredArgsConstructor
public class TickProcessor {

    private static final int SMA_SHORT_PERIOD = 20;
    private static final int SMA_LONG_PERIOD = 50;

    private final ConcurrentHashMap<String, CandleAggregator> aggregators = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PriceWindow> windows = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicReference<Double>> zScoreGauges = new ConcurrentHashMap<>();
    private final ProcessedEventPublisher publisher;
    private final PriceWindowStore store;
    private final MeterRegistry meterRegistry;

    private volatile Timer processingTimer;

    @KafkaListener(topics = "market.ticks", groupId = "processing-service")
    public void process(TickEvent tick) {
        Timer.Sample sample = Timer.start(meterRegistry);
        MDC.put("symbol", tick.symbol());
        MDC.put("tradeId", String.valueOf(tick.tradeId()));
        try {
            doProcess(tick);
        } finally {
            sample.stop(processingTimer());
            MDC.clear();
        }
    }

    private Timer processingTimer() {
        Timer timer = processingTimer;
        if (timer == null) {
            timer = Timer.builder("market.processing.duration")
                    .description("Time to process a single tick")
                    .publishPercentileHistogram()
                    .register(meterRegistry);
            processingTimer = timer;
        }
        return timer;
    }

    private void doProcess(TickEvent tick) {
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

        updateZScoreGauge(symbol, zScore);
        meterRegistry.counter("market.ticks.processed", "symbol", symbol).increment();

        closed.ifPresent(candle ->
                publisher.evaluateAnomaly(symbol, candle.close(), zScore, sma20, sma50, rsi, candle.openTime()));
    }

    private void updateZScoreGauge(String symbol, Double zScore) {
        if (zScore == null) {
            return;
        }
        zScoreGauges.computeIfAbsent(symbol, key -> {
            AtomicReference<Double> ref = new AtomicReference<>(0.0);
            Gauge.builder("market.zscore.current", ref, AtomicReference::get)
                    .tag("symbol", key)
                    .description("Latest z-score per symbol")
                    .register(meterRegistry);
            return ref;
        }).set(zScore);
    }

}
