package com.marketpulse.processing_service.indicator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

/**
 * Rolling window of the most recent prices for a single symbol.
 * Holds at most {@value #CAPACITY} prices, evicting the oldest as new ones arrive.
 */
public class PriceWindow {

    private static final int CAPACITY = 50;
    private static final int RSI_PERIOD = 14;
    private static final int ZSCORE_MIN_SAMPLES = 20;

    private final Deque<BigDecimal> prices;

    public PriceWindow() {
        this.prices = new ArrayDeque<>(CAPACITY);
    }

    @JsonCreator
    public PriceWindow(@JsonProperty("prices") Collection<BigDecimal> prices) {
        this();
        if (prices != null) {
            prices.forEach(this::add);
        }
    }

    public void add(BigDecimal price) {
        if (prices.size() == CAPACITY) {
            prices.removeFirst();
        }
        prices.addLast(price);
    }

    @JsonGetter("prices")
    public List<BigDecimal> getPrices() {
        return new ArrayList<>(prices);
    }

    public boolean isReady(int period) {
        return prices.size() >= period;
    }

    public Optional<BigDecimal> calculateSMA(int period) {
        if (!isReady(period)) {
            return Optional.empty();
        }
        List<BigDecimal> recent = lastN(period);
        BigDecimal sum = recent.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
        return Optional.of(sum.divide(BigDecimal.valueOf(period), MathContext.DECIMAL64));
    }

    public Optional<Double> calculateRSI() {
        if (prices.size() < RSI_PERIOD + 1) {
            return Optional.empty();
        }
        List<BigDecimal> recent = lastN(RSI_PERIOD + 1);
        double gains = 0.0;
        double losses = 0.0;
        for (int i = 1; i < recent.size(); i++) {
            double change = recent.get(i).subtract(recent.get(i - 1)).doubleValue();
            if (change > 0) {
                gains += change;
            } else {
                losses += -change;
            }
        }
        double avgGain = gains / RSI_PERIOD;
        double avgLoss = losses / RSI_PERIOD;
        if (avgLoss == 0.0) {
            return Optional.of(avgGain == 0.0 ? 50.0 : 100.0);
        }
        if (avgGain == 0.0) {
            return Optional.of(0.0);
        }
        double rs = avgGain / avgLoss;
        return Optional.of(100.0 - (100.0 / (1.0 + rs)));
    }

    public Optional<Double> calculateZScore() {
        if (prices.size() < ZSCORE_MIN_SAMPLES) {
            return Optional.empty();
        }
        double[] values = prices.stream().mapToDouble(BigDecimal::doubleValue).toArray();
        double mean = 0.0;
        for (double v : values) {
            mean += v;
        }
        mean /= values.length;
        double variance = 0.0;
        for (double v : values) {
            double diff = v - mean;
            variance += diff * diff;
        }
        variance /= values.length; // population variance
        double stdDev = Math.sqrt(variance);
        if (stdDev == 0.0) {
            return Optional.empty();
        }
        double latest = values[values.length - 1];
        return Optional.of((latest - mean) / stdDev);
    }

    private List<BigDecimal> lastN(int n) {
        List<BigDecimal> all = new ArrayList<>(prices);
        return all.subList(all.size() - n, all.size());
    }
}
