package com.marketpulse.processing_service.kafka;

import com.marketpulse.common.event.ProcessedEvent;
import com.marketpulse.common.event.TickEvent;
import com.marketpulse.processing_service.indicator.PriceWindow;
import com.marketpulse.processing_service.store.PriceWindowStore;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class TickProcessor {

    private static final int SMA_SHORT_PERIOD = 20;
    private static final int SMA_LONG_PERIOD = 50;

    private final ConcurrentHashMap<String, PriceWindow> windows = new ConcurrentHashMap<>();
    private final ProcessedEventPublisher publisher;
    private final PriceWindowStore store;

    @KafkaListener(topics = "market.ticks", groupId = "processing-service")
    public void process(TickEvent tick) {
        var window = windows.computeIfAbsent(
                tick.symbol(), key -> store.load(key).orElseGet(PriceWindow::new));
        window.add(tick.price());

        BigDecimal sma20 = window.calculateSMA(SMA_SHORT_PERIOD).orElse(null);
        BigDecimal sma50 = window.calculateSMA(SMA_LONG_PERIOD).orElse(null);
        Double rsi = window.calculateRSI().orElse(null);
        Double zScore = toNullable(window.calculateZScore());

        store.save(tick.symbol(), window);

        publisher.publish(new ProcessedEvent(
                tick.symbol(),
                tick.price(),
                sma20,
                sma50,
                rsi,
                zScore,
                tick.timestamp()));
    }

    private static Double toNullable(OptionalDouble value) {
        return value.isPresent() ? value.getAsDouble() : null;
    }
}
